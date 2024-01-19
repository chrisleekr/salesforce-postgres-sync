const moment = require('moment');
const config = require('config');

const dbConfig = require('../helpers/db-config');
const postgres = require('../helpers/postgres');
const salesforce = require('../helpers/salesforce');

const shouldIncrementalUpdate = async (objectName, logger) => {
  const lastSystemModStamp = await dbConfig.get(
    `${objectName}-last-system-mod-stamp`,
    logger
  );
  // If lastSystemModStamp is not null, then don't process clean sync.
  if (lastSystemModStamp) {
    return true;
  }

  // If lastSystemModStamp is null, then return true
  return !false;
};

module.exports = async rawLogger => {
  const logger = rawLogger.child({
    command: 'salesforce-to-postgres-increment-update'
  });

  logger.info('Start salesforce-to-postgres-increment-update command');

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

    if (!(await shouldIncrementalUpdate(objectName, logger))) {
      logger.info(
        { objectName },
        `Should not incremental update for object ${objectName} because last system mod stamp is null.`
      );
      continue;
    }

    logger.info({ objectName }, 'Processing object name');

    const salesforceColumns = await salesforce.getSalesforceColumns(
      objectName,
      logger
    );

    logger.info({ salesforceColumns }, 'Loaded Salesforce columns');

    const savedLastSystemModStamp = await dbConfig.get(
      `${objectName}-last-system-mod-stamp`,
      logger
    );

    const query = `SELECT ${salesforceColumns.join(
      ', '
    )} FROM ${objectName} WHERE SystemModstamp > ${savedLastSystemModStamp}`;

    logger.info({ query }, 'Querying Salesforce');

    let lastSystemModStamp = '';
    await salesforce.query(
      query,
      async (record, recordLogger) => {
        // Convert all keys in records to lowercase
        const recordLowercase = {};
        Object.keys(record).forEach(k => {
          recordLowercase[k.toLowerCase()] = record[k];
        });

        // Insert/Update record to postgres
        const syncUpdateTimestamp = moment.utc().format();

        // TODO: try/catch error and if error occurred, record it.
        //      It should not break the application.
        await postgres.upsert(
          schemaName,
          objectName.toLowerCase(),
          [
            '_sync_update_timestamp',
            '_sync_status',
            '_sync_message',
            ...salesforceColumns
          ],
          [
            syncUpdateTimestamp,
            'SYNCED',
            JSON.stringify({ command: 'incrementalUpdate' }),
            ...salesforceColumns.reduce((acc, k) => {
              if (recordLowercase[k]) {
                acc.push(recordLowercase[k]);
              } else {
                acc.push(null);
              }
              return acc;
            }, [])
          ],
          'id',
          recordLogger
        );

        // If row.systemmodstamp is greater than lastSystemModStamp, then update lastSystemModStamp
        if (
          !lastSystemModStamp ||
          moment(recordLowercase.systemmodstamp).isAfter(lastSystemModStamp)
        ) {
          lastSystemModStamp = recordLowercase.systemmodstamp;
        }
      },
      logger
    );

    // Save last-system-mod-stamp
    if (lastSystemModStamp) {
      await dbConfig.set(
        `${objectName}-last-system-mod-stamp`,
        lastSystemModStamp,
        logger
      );
    }
  }

  logger.info('Completed salesforce-to-postgres-increment-update command');
};
