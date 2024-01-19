const fs = require('fs');
const moment = require('moment');
const config = require('config');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const dbConfig = require('../helpers/db-config');
const postgres = require('../helpers/postgres');
const salesforce = require('../helpers/salesforce');
const { sleep } = require('../helpers/utils');

const shouldCleanSync = async (objectName, logger) => {
  const lastSystemModeStamp = await dbConfig.get(
    `${objectName}-last-system-mod-stamp`,
    logger
  );
  // If lastSystemModeStamp is null, then return true
  if (!lastSystemModeStamp) {
    return true;
  }

  // If lastSystemModeStamp is not null, then don't process clean sync.
  return false;
};

module.exports = async rawLogger => {
  const logger = rawLogger.child({
    command: 'salesforce-to-postgres-clean-sync'
  });

  logger.info('Start salesforce-to-postgres-clean-sync command');

  const schemaName = config.get('salesforce.postgresSchema');

  const configuredObjects = config.get('salesforce.objects');

  // Get the keys of the salesforceObjects object
  const objectNames = Object.keys(configuredObjects);

  for (const objectName of objectNames) {
    if (!configuredObjects[objectName].fields) {
      logger.info(
        { objectName },
        `No fields configured for object ${objectName}`
      );
      continue;
    }

    if (!(await shouldCleanSync(objectName, logger))) {
      logger.info(
        { objectName },
        `Should not clean sync for object ${objectName} because last system mod stamp is not null.`
      );
      continue;
    }

    logger.info({ objectName }, 'Processing object name');

    const salesforceColumns = await salesforce.getSalesforceColumns(
      objectName,
      logger
    );

    logger.info({ salesforceColumns }, 'Loaded Salesforce columns');

    const lastCleanSyncJob =
      JSON.parse(
        await dbConfig.get(`${objectName}-last-clean-sync-job`, logger)
      ) || null;

    logger.info(
      { lastCleanSyncJob },
      `Loaded last clean sync job for ${objectName}`
    );

    let jobId = '';
    if (!lastCleanSyncJob) {
      const query = `SELECT ${salesforceColumns.join(',')} FROM ${objectName}`;

      logger.info({ query }, 'Salesforce Query, creating query job');

      const queryJobResponse = await salesforce.jobsQueryToCSV(query, logger);

      await dbConfig.set(
        `${objectName}-last-clean-sync-job`,
        JSON.stringify(queryJobResponse.data),
        logger
      );

      jobId = queryJobResponse.data.id;
    } else {
      jobId = lastCleanSyncJob.id;
    }

    let jobState = '';
    const startTime = Date.now();
    const maximumExecutionMins = 90 * 60 * 1000;

    while (jobState !== 'JobComplete') {
      logger.info({ jobState }, 'Waiting for job to complete');

      // Add Try/catch
      // In catch statement, if jobStatusResponse.status === 400, then ignore. Otherwise, throw error
      let jobStatusResponse;
      try {
        jobStatusResponse = await salesforce.jobQueryStatus(jobId, logger);
      } catch (err) {
        if (err.response && err.response.status !== 400) {
          throw err;
        } else {
          logger.info(
            { err },
            'Error getting job status but it is 400, try again.'
          );
        }
      }

      jobState = jobStatusResponse.data.state;
      if (Date.now() - startTime > maximumExecutionMins) {
        logger.info('Job status check exceeded 1 hour 30 minutes. Exiting...');
        break;
      }

      await sleep(2);
    }

    logger.info({ jobState }, 'Job completed');

    // Truncate table
    await postgres.truncate(schemaName, objectName.toLowerCase(), logger);

    // Delete last-system-mod-stamp
    await dbConfig.deleteKey(`${objectName}-last-system-mod-stamp`, logger);

    let fileNumber = 1;
    let totalRecords = 0;

    let lastSystemModStamp = '';
    let sForceLocator;

    while (!sForceLocator || sForceLocator !== 'null') {
      logger.info({ fileNumber, totalRecords }, 'Getting job results');
      const jobResultsResponse = await salesforce.jobQueryResults(
        jobId,
        sForceLocator,
        logger
      );

      const { data, headers } = jobResultsResponse;

      // Extract the Sforce-Locator header from the response
      sForceLocator = headers['sforce-locator'];
      totalRecords += parseInt(headers['sforce-numberofrecords'], 10);

      logger.info(
        { data: { sForceLocator, totalRecords } },
        'Updated total records'
      );

      // Save contents to /tmp/${objectName}-${jobId}-${fileNumber}.csv
      const csvPath = `/tmp/${objectName}-${jobId}-${fileNumber}.csv`;
      const convertedCSVPath = `/tmp/${objectName}-${jobId}-${fileNumber}-converted.csv`;
      logger.info(
        { data: { fileNumber, csvPath } },
        `Saving result for ${objectName} to ${csvPath}`
      );
      fs.writeFileSync(csvPath, data, 'utf8', err => {
        if (err) {
          logger.error({ err }, `Error writing file to ${csvPath}`);
        } else {
          logger.info(`Successfully wrote file to ${csvPath}`);
        }
      });

      // Convert CSV with prepending additional fields
      const csvRows = [];
      // eslint-disable-next-line no-loop-func
      await new Promise((resolve, reject) => {
        fs.createReadStream(csvPath)
          .pipe(csv())
          .on('data', row => {
            // Convert keys to lowercase
            const lowerCaseRow = Object.fromEntries(
              Object.entries(row).map(([key, value]) => [
                key.toLowerCase(),
                value
              ])
            );

            // Prepend the additional columns
            const convertedRow = {
              _sync_update_timestamp: moment.utc().format(),
              _sync_status: 'SYNCED',
              _sync_message: JSON.stringify({ command: 'cleanSync', jobId }),
              ...lowerCaseRow
            };

            // If row.systemmodstamp is greater than lastSystemModStamp, then update lastSystemModStamp
            if (
              !lastSystemModStamp ||
              moment(convertedRow.systemmodstamp).isAfter(lastSystemModStamp)
            ) {
              lastSystemModStamp = convertedRow.systemmodstamp;
            }

            csvRows.push(convertedRow);

            logger.info(
              {
                data: { totalRows: csvRows.length }
              },
              `Processed rows for ${objectName}`
            );
          })
          .on('error', err => reject(err))
          .on('end', () => {
            const csvWriter = createCsvWriter({
              path: convertedCSVPath,
              header: Object.keys(csvRows[0]).map(key => ({
                id: key,
                title: key
              }))
            });

            csvWriter.writeRecords(csvRows).then(() => {
              logger.info(
                `Successfully wrote converted CSV to ${convertedCSVPath}`
              );
              resolve();
            });
          });
      });

      // Load CSV to Postgres
      await postgres.loadCSVToTable(
        schemaName,
        objectName.toLowerCase(),
        convertedCSVPath,
        ',', // delimiter
        logger
      );

      // Delete csvPath and convertedCSVPath
      fs.unlinkSync(csvPath);
      fs.unlinkSync(convertedCSVPath);

      fileNumber += 1;
    }

    // Delete `${objectName}-last-clean-sync-job`
    await dbConfig.deleteKey(`${objectName}-last-clean-sync-job`, logger);

    // Save last-system-mod-stamp
    await dbConfig.set(
      `${objectName}-last-system-mod-stamp`,
      lastSystemModStamp,
      logger
    );
  }

  logger.info('Completed salesforce-to-postgres-clean-sync command');
};
