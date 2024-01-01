const config = require('config');
const moment = require('moment');
const salesforce = require('../helpers/salesforce');
const postgres = require('../helpers/postgres');
const dbConfig = require('../helpers/db-config');

module.exports = async rawLogger => {
  const logger = rawLogger.child({ library: 'postgres-to-salesforce' });

  const schemaName = config.get('salesforce.postgresSchema');
  const salesforceObjects = config.get('salesforce.objects');

  // Get the keys of the salesforceObjects object
  const salesforceObjectNames = Object.keys(salesforceObjects);

  // Set sync date/time
  const syncUpdateTimestamp = moment.utc().format();

  // Loop salesforceObjectNames sequentially
  for (const salesforceObjectName of salesforceObjectNames) {
    const tableName = salesforceObjectName.toLowerCase();

    // Get createable fields from postgres
    const createableFields = JSON.parse(
      await dbConfig.get(`createable-fields-${salesforceObjectName}`, logger)
    ).map(field => field.toLowerCase());

    // Get updateable fields from postgres
    const updateableFields = JSON.parse(
      await dbConfig.get(`updateable-fields-${salesforceObjectName}`, logger)
    ).map(field => field.toLowerCase());

    // Get all _sync_status = 'PENDING' records that need to be synced to Salesforce
    const pendingRecords = await postgres.select(
      schemaName,
      tableName,
      ['*'],
      `_sync_status = 'PENDING'`,
      null,
      null,
      logger
    );

    // Loop pendingRecords sequentially to avoid excessive API call.
    for (const pendingRecord of pendingRecords.rows) {
      // if pendingRecord.id is empty, then it is new record.
      if (!pendingRecord.id) {
        // Construct salesforce record only the updateable fields
        const salesforceRecord = {};
        Object.keys(pendingRecord).forEach(fieldKey => {
          // Create only if the fieldKey is in the createableFields array
          if (createableFields.includes(fieldKey)) {
            salesforceRecord[fieldKey] = pendingRecord[fieldKey];
          }
        });

        await salesforce.create(
          salesforceObjectName,
          salesforceRecord,
          (record, result) => {
            logger.debug(
              { data: { record, result } },
              `Created new record in Salesforce`
            );

            // Update _sync_status to 'SYNCED'
            postgres.update(
              schemaName,
              tableName,
              {
                _sync_update_timestamp: syncUpdateTimestamp,
                _sync_status: 'SYNCED',
                _sync_message: '',
                id: result.id
              },
              pendingRecord._sync_id,
              '_sync_id',
              logger
            );

            logger.info(
              { data: { result } },
              'Completed to create new record in Salesforce'
            );
          },
          (record, err) => {
            logger.error(
              { err, data: { record } },
              'Error in Salesforce object create'
            );

            // Update _sync_status to 'ERROR'
            postgres.update(
              schemaName,
              tableName,
              {
                _sync_update_timestamp: syncUpdateTimestamp,
                _sync_status: 'ERROR',
                _sync_message: err.message
              },
              pendingRecord._sync_id,
              '_sync_id',
              logger
            );
          },
          logger
        );
      } else {
        // Construct salesforce record only the updateable fields
        const salesforceRecord = {};
        Object.keys(pendingRecord).forEach(fieldKey => {
          // Update only if the fieldKey is in the updateableFields array
          if (updateableFields.includes(fieldKey)) {
            salesforceRecord[fieldKey] = pendingRecord[fieldKey];
          }
        });

        await salesforce.update(
          salesforceObjectName,
          salesforceRecord,
          (record, result) => {
            logger.debug(
              { data: { record, result } },
              'Updated record in Salesforce'
            );

            // Update _sync_status to 'SYNCED'
            postgres.update(
              schemaName,
              tableName,
              {
                _sync_update_timestamp: syncUpdateTimestamp,
                _sync_status: 'SYNCED',
                _sync_message: '',
                id: result.id
              },
              pendingRecord._sync_id,
              '_sync_id',
              logger
            );

            logger.info(
              { data: { result } },
              'Completed to update an existing record in Salesforce'
            );
          },
          (record, err) => {
            logger.error(
              { err, data: { record } },
              'Error in Salesforce object create'
            );

            // Update _sync_status to 'ERROR'
            postgres.update(
              schemaName,
              tableName,
              {
                _sync_update_timestamp: syncUpdateTimestamp,
                _sync_status: 'ERROR',
                _sync_message: err.message
              },
              pendingRecord._sync_id,
              '_sync_id',
              logger
            );
          },
          logger
        );
      }
    }
  }
};
