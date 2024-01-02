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
      salesforceObjectName,
      salesforceObjects[salesforceObjectName].fields
    );

    // Construct SELECT query for salesforce with fields
    const lastSyncTimestamp = await dbConfig.get(
      `last-sync-timestamp-${salesforceObjectName}`,
      logger
    );

    // Set sync date/time
    const syncUpdateTimestamp = moment.utc().format();

    if (lastSyncTimestamp) {
      // If lastSyncTimestamp is not null, then full/partial sync has been done.
      // Use query operation.
      await salesforce.query(
        `SELECT ${columns.join(
          ','
        )} FROM ${salesforceObjectName} WHERE LastModifiedDate > ${lastSyncTimestamp} ORDER BY LastModifiedDate ASC`,
        async record => {
          logger.debug({ data: { record } }, 'Record');

          // Convert all keys in records to lowercase
          const recordLowercase = {};
          Object.keys(record).forEach(k => {
            recordLowercase[k.toLowerCase()] = record[k];
          });

          // Insert/Update record to postgres
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
              ...columns.reduce((acc, k) => {
                if (recordLowercase[k]) {
                  acc.push(recordLowercase[k]);
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
        async record => {
          logger.info(
            { data: { record } },
            `Completed query for ${salesforceObjectName}`
          );
          if (record) {
            await dbConfig.set(
              `last-sync-timestamp-${salesforceObjectName}`,
              record.LastModifiedDate,
              logger
            );
          }
        },
        logger
      );
    } else {
      // If lastSyncTimestamp is null, then do not add LastModifiedDate>${lastSyncTimestamp}.
      // It's clean copy.

      const lastBulkJob =
        JSON.parse(
          await dbConfig.get(`last-bulk-job-id-${salesforceObjectName}`, logger)
        ) || {};

      await new Promise((resolve, reject) => {
        salesforce.bulkQueryV2(
          salesforceObjectName,
          `SELECT ${columns.join(
            ','
          )} FROM ${salesforceObjectName} ORDER BY LastModifiedDate ASC`,
          lastBulkJob,
          async batchInfo => {
            await dbConfig.set(
              `last-bulk-job-id-${salesforceObjectName}`,
              JSON.stringify(batchInfo),
              logger
            );
          },
          async records => {
            // Convert all keys in records to lowercase
            // const recordLowercase = {};
            // Object.keys(record).forEach(k => {
            //   recordLowercase[k.toLowerCase()] = record[k];
            // });

            console.log(JSON.stringify(records));
            // Insert/Update record to postgres

            // await postgres.bulkUpsert(
            //   schemaName,
            //   tableName,
            //   [
            //     '_sync_update_timestamp',
            //     '_sync_status',
            //     '_sync_message',
            //     ...columns
            //   ],
            //   [
            //     syncUpdateTimestamp,
            //     'SYNCED',
            //     '',
            //     ...Object.values(record)
            //     // ...columns.reduce((acc, k) => {
            //     //   if (recordLowercase[k]) {
            //     //     acc.push(recordLowercase[k]);
            //     //   }
            //     //   return acc;
            //     // }, [])
            //   ],
            //   'id',
            //   logger
            // );
          },
          async err => {
            logger.error(
              { err },
              `Error in Salesforce object bulk query for ${salesforceObjectName}`
            );

            reject();
          },
          async batchInfo => {
            logger.info(`Completed query for ${salesforceObjectName}`);

            await dbConfig.set(
              `last-sync-timestamp-${salesforceObjectName}`,
              batchInfo.createdDate,
              logger
            );

            await dbConfig.deleteKey(
              `last-bulk-job-id-${salesforceObjectName}`,
              logger
            );

            resolve();
          },
          logger
        );
      }, logger);
    }

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
