/* eslint-disable no-continue */
const moment = require('moment');
const config = require('config');
const logger = require('../../helpers/logger');
const dbConfig = require('../../helpers/db-config');
const postgres = require('../../helpers/postgres');
const salesforce = require('../../helpers/salesforce');

const shouldIncrementalUpdate = async objectName => {
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
        {
          restUrl,
          sessionId
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
    logger.info('Done');
    process.exit(0);
  } catch (err) {
    logger.error({ err }, 'Error occurred');
    process.exit(1);
  }
})();
