const config = require('config');
const moment = require('moment');
const salesforce = require('../helpers/salesforce');
const postgres = require('../helpers/postgres');

module.exports = async rawLogger => {
  const logger = rawLogger.child({ library: 'salesforce-to-postgres' });

  const schemaName = config.get('salesforce.postgresSchema');
  const salesforceObjects = config.get('salesforce.objects');

  // Get the keys of the salesforceObjects object
  const salesforceObjectNames = Object.keys(salesforceObjects);

  // Construct SELECT query for Salesforce based on the fields
  for (const salesforceObjectName of salesforceObjectNames) {
    const columns = salesforce.getSalesforceColumns(
      salesforceObjects[salesforceObjectName].fields
    );

    // Construct SELECT query for salesforce with fields
    const lastModifiedDate = '2023-12-20T00:00:00Z'; // FIX: Get from database
    const syncUpdateTimestamp = moment.utc().format();
    const salesforceQuery = `SELECT ${columns.join(
      ','
    )} FROM ${salesforceObjectName} WHERE LastModifiedDate > ${lastModifiedDate}`;

    // Query Salesforce using Bulk API
    salesforce.bulkQuery(
      salesforceQuery,
      record => {
        const tableName = salesforceObjectName.toLowerCase();

        postgres.upsert(
          schemaName,
          tableName,
          ['sync_update_timestamp', 'sync_status', ...columns],
          [syncUpdateTimestamp, 'SYNCED', ...Object.values(record)],
          'id',
          logger
        );
      },
      error => {
        logger.error(
          { error },
          `Error in bulk query for ${salesforceObjectName}`
        );
      },
      () => {
        logger.info(`Completed bulk query for ${salesforceObjectName}`);
      },
      logger
    );
  }
};
