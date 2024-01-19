/* eslint-disable no-loop-func */
/* eslint-disable no-promise-executor-return */
/* eslint-disable no-continue */
/* eslint-disable max-len */
const fs = require('fs');
const moment = require('moment');
const axios = require('axios');
const config = require('config');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const logger = require('../../helpers/logger');
const dbConfig = require('../../helpers/db-config');
const postgres = require('../../helpers/postgres');
const salesforce = require('../../helpers/salesforce');

const shouldCleanSync = async objectName => {
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

(async () => {
  try {
    // Connect to Postgres
    await postgres.connect(logger);

    const schemaName = config.get('salesforce.postgresSchema');

    const { restUrl, sessionId } = await salesforce.login(logger);

    logger.info({ sessionId, restUrl }, 'Parsed login response');

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
        const query = `SELECT ${salesforceColumns.join(
          ','
        )} FROM ${objectName}`;

        logger.info({ query }, 'Salesforce Query, creating query job');

        const queryJobResponse = await salesforce.jobsQueryToCSV(
          query,
          { restUrl, sessionId },
          logger
        );

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
          jobStatusResponse = await salesforce.jobsQueryToCSV(
            jobId,
            { restUrl, sessionId },
            logger
          );
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
          logger.info(
            'Job status check exceeded 1 hour 30 minutes. Exiting...'
          );
          break;
        }
        await new Promise(resolve => setTimeout(resolve, 2000));
      }

      logger.info({ jobState }, 'Job completed');

      // Get results for the query job - https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/query_get_job_results.htm
      //  Loop until Sforce-Locator is null
      /*
      Sample Request
        GET /jobs/query/${jobId}/results?maxRecords=50000
        Authorization: Bearer ${sessionId}
        Accept: text/csv
      Sample Response
        Sforce-Locator: MTAwMDA
        Sforce-NumberOfRecords: 50000
        ...

        "Id","Name"
        "005R0000000UyrWIAS","Jane Dunn"
        "005R0000000GiwjIAC","George Wright"
        "005R0000000GiwoIAC","Pat Wilson"
      */
      // Save contents to /tmp/${objectName}-${jobId}-1.csv
      // Extract the Sforce-Locator header from the response
      //    Request GET /jobs/query/${jobId}/results?locator=${Sforce-Locator}&maxRecords=50000
      //    Save contents to /tmp/${objectName}-${jobId}-2.csv

      // Truncate table
      await postgres.truncate(schemaName, objectName.toLowerCase(), logger);

      // Delete last-system-mod-stamp
      await dbConfig.deleteKey(`${objectName}-last-system-mod-stamp`, logger);

      let fileNumber = 1;
      let totalRecords = 0;

      let lastSystemModStamp = '';
      let sForceLocator;

      while (!sForceLocator || sForceLocator !== 'null') {
        const jobResultsUrl = `${restUrl}/jobs/query/${jobId}/results?maxRecords=100000${
          sForceLocator && sForceLocator !== 'null'
            ? `&locator=${sForceLocator}`
            : ''
        }`;
        logger.info(
          { fileNumber, totalRecords, jobResultsUrl },
          'Getting job results'
        );
        const jobResultsResponse = await axios.get(
          jobResultsUrl,
          {
            headers: {
              Authorization: `Bearer ${sessionId}`,
              Accept: 'text/csv'
            }
          },
          {
            timeout: 1000 * 60 * 5 // 5 minutes, for some reason, sometimes the request is taking a long time. Then timeout first and then retry.
          }
        );
        logger.info(
          {
            status: jobResultsResponse.status,
            headers: jobResultsResponse.headers
          },
          'Job results response'
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

    logger.info('Done');
    process.exit(0);
  } catch (err) {
    logger.error({ err }, 'Error occurred');
    process.exit(1);
  }
})();
