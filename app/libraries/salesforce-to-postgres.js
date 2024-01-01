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

    // Set sync datetime
    const syncUpdateTimestamp = moment.utc().format();

    const selectQuery = `SELECT ${columns.join(
      ','
    )} FROM ${salesforceObjectName}`;

    let whereClause = '';
    let orderByClause = '';

    // If lastSyncTimestamp is not null, then full/partial sync has been done.
    // If lastSyncTimestamp is null, then do not add LastModifiedDate>${lastSyncTimestamp}. It's clean copy.
    if (lastSyncTimestamp) {
      whereClause = ` WHERE LastModifiedDate > ${lastSyncTimestamp}`;
      orderByClause = ' ORDER BY LastModifiedDate ASC';
    } else {
      whereClause = '';
      orderByClause = ' ORDER BY LastModifiedDate ASC';
    }

    // This is not working because of it's too large.
    // One way to overcome the issue is querying with start/end date time.
    // But that is still can be an issue if there are large amount of records within the day.
    // await new Promise((resolve, reject) => {
    //   salesforce.bulkQueryV2(
    //     salesforceObjectName,
    //     `${selectQuery} ${whereClause} ${orderByClause}`,
    //     async record => {
    //       logger.debug({ data: { record } }, 'Record');

    //       await postgres.upsert(
    //         schemaName,
    //         tableName,
    //         [
    //           '_sync_update_timestamp',
    //           '_sync_status',
    //           '_sync_message',
    //           ...columns
    //         ],
    //         [
    //           syncUpdateTimestamp,
    //           'SYNCED',
    //           '',
    //           ...Object.keys(record).reduce((acc, k) => {
    //             if (columns.includes(k.toLowerCase())) {
    //               acc.push(record[k]);
    //             }
    //             return acc;
    //           }, [])
    //         ],
    //         'id',
    //         logger
    //       );
    //     },
    //     err => {
    //       logger.error(
    //         { err },
    //         `Error in Salesforce object bulk query for ${salesforceObjectName}`
    //       );

    //       reject();
    //     },
    //     () => {
    //       logger.info(`Completed query for ${salesforceObjectName}`);
    //       dbConfig.set(
    //         `last-sync-timestamp-${salesforceObjectName}`,
    //         syncUpdateTimestamp,
    //         logger
    //       );

    //       resolve();
    //     },
    //     logger
    //   );
    // }, logger);

    // This is working version.
    await salesforce.query(
      `${selectQuery} ${whereClause} ${orderByClause}`,
      async record => {
        logger.debug({ data: { record } }, 'Record');

        await postgres.upsert(
          schemaName,
          tableName,
          [
            '_sync_update_timestamp',
            '_sync_status',
            '_sync_message',
            ...columns
          ],
          [
            syncUpdateTimestamp,
            'SYNCED',
            '',
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
      async record => {
        logger.info(
          { data: { record } },
          `Save for last sync timestamp ${salesforceObjectName}`
        );
        await dbConfig.set(
          `last-sync-timestamp-${salesforceObjectName}`,
          record.LastModifiedDate,
          logger
        );
      },
      async () => {
        logger.info(`Completed query for ${salesforceObjectName}`);
      },
      logger
    );

    // await new Promise((resolve, reject) => {
    //   salesforce.queryV2(
    //     `${selectQuery} ${whereClause} ${orderByClause}`,
    //     async record => {
    //       logger.debug({ data: { record } }, 'Record');

    //       await postgres.upsert(
    //         schemaName,
    //         tableName,
    //         [
    //           '_sync_update_timestamp',
    //           '_sync_status',
    //           '_sync_message',
    //           ...columns
    //         ],
    //         [
    //           syncUpdateTimestamp,
    //           'SYNCED',
    //           '',
    //           ...Object.keys(record).reduce((acc, k) => {
    //             if (columns.includes(k.toLowerCase())) {
    //               acc.push(record[k]);
    //             }
    //             return acc;
    //           }, [])
    //         ],
    //         'id',
    //         logger
    //       );
    //     },
    //     err => {
    //       logger.error(
    //         { err },
    //         `Error in Salesforce object bulk query for ${salesforceObjectName}`
    //       );

    //       reject();
    //     },
    //     () => {
    //       logger.info(`Completed query for ${salesforceObjectName}`);

    //       resolve();
    //     },
    //     logger
    //   );
    // }, logger);
  }
};
