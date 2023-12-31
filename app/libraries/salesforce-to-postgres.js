/* eslint-disable no-loop-func */
const config = require('config');
const moment = require('moment');
const salesforce = require('../helpers/salesforce');
const postgres = require('../helpers/postgres');
const dbConfig = require('../helpers/db-config');

module.exports = async rawLogger => {
  const logger = rawLogger.child({ library: 'salesforce-to-postgres' });

  const schemaName = config.get('salesforce.postgresSchema');
  const salesforceObjects = config.get('salesforce.objects');

  // Get the keys of the salesforceObjects object
  const salesforceObjectNames = Object.keys(salesforceObjects);

  // Construct SELECT query for Salesforce based on the fields
  for (const salesforceObjectName of salesforceObjectNames) {
    const tableName = salesforceObjectName.toLowerCase();

    const columns = salesforce.getSalesforceColumns(
      salesforceObjects[salesforceObjectName].fields
    );

    // Construct SELECT query for salesforce with fields
    const lastSyncTimestamp = await dbConfig.get(
      `last-sync-timestamp-${salesforceObjectName}`,
      logger
    );
    const syncUpdateTimestamp = moment.utc().format();

    const selectQuery = `SELECT ${columns.join(
      ','
    )} FROM ${salesforceObjectName}`;

    let whereClause = '';
    let orderByClause = '';

    // If lastSyncTimestamp is null, then do not add LastModifiedDate>${lastSyncTimestamp}
    if (lastSyncTimestamp) {
      whereClause = ` WHERE LastModifiedDate > ${lastSyncTimestamp}`;
      orderByClause = ' ORDER BY LastModifiedDate ASC';
    } else {
      whereClause = '';
      orderByClause = ' ORDER BY CreatedDate ASC';
    }

    await new Promise(resolve => {
      salesforce.query(
        `${selectQuery} ${whereClause} ${orderByClause}`,
        record => {
          logger.debug({ data: { record } }, 'Record');

          postgres.upsert(
            schemaName,
            tableName,
            ['sync_update_timestamp', 'sync_status', ...columns],
            [
              syncUpdateTimestamp,
              'SYNCED',
              ...Object.keys(record).reduce((acc, k) => {
                if (columns.includes(k.toLowerCase())) {
                  acc.push(record[k]);
                }
                return acc;
              }, [])
            ],
            'id',
            logger
          );
        },

        () => {
          logger.info(`Completed query for ${salesforceObjectName}`);
          resolve();
        },
        logger
      );
    }, logger);

    await dbConfig.set(
      `last-sync-timestamp-${salesforceObjectName}`,
      syncUpdateTimestamp,
      logger
    );
  }
};
