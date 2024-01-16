/* eslint-disable no-continue */
const axios = require('axios');
const moment = require('moment');
const config = require('config');
const logger = require('../../helpers/logger');
const dbConfig = require('../../helpers/db-config');
const postgres = require('../../helpers/postgres');

const { salesforceLogin } = require('./salesforce-login');

const processPendingRecord = (
  objectName,
  salesforceRecord,
  pendingRecord,
  foundField,
  fieldKey
) => {
  const updatedSalesforceRecord = salesforceRecord;

  if (foundField.salesforce.relationshipObjectName === objectName) {
    if (foundField.salesforce.type === 'boolean') {
      updatedSalesforceRecord[foundField.salesforce.name] =
        pendingRecord[fieldKey] === true ? 'true' : 'false';
    } else {
      updatedSalesforceRecord[foundField.salesforce.name] =
        pendingRecord[fieldKey];
    }
  } else {
    if (
      updatedSalesforceRecord[foundField.salesforce.objectName] === undefined
    ) {
      updatedSalesforceRecord[foundField.salesforce.objectName] = {};
    }

    if (foundField.salesforce.type === 'boolean') {
      updatedSalesforceRecord[foundField.salesforce.objectName][
        foundField.salesforce.name
      ] = pendingRecord[fieldKey] === true ? 'true' : 'false';
    } else {
      updatedSalesforceRecord[foundField.salesforce.objectName][
        foundField.salesforce.name
      ] = pendingRecord[fieldKey];
    }
  }

  return updatedSalesforceRecord;
};

(async () => {
  try {
    // Connect to Postgres
    await postgres.connect(logger);

    const schemaName = config.get('salesforce.postgresSchema');

    const { restUrl, sessionId } = await salesforceLogin();

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

      const objectSchema = JSON.parse(
        await dbConfig.get(`${objectName}-schema`, logger)
      );
      // Get name of fields if isSalesforceColumn is true
      const databaseColumns = objectSchema.reduce((acc, field) => {
        acc.push(field.name);
        return acc;
      }, []);

      // Get createable fields from postgres
      const createableFields = objectSchema.reduce((acc, field) => {
        if (field.canCreate) {
          acc.push(field);
        }
        return acc;
      }, []);

      // Get updatable fields from postgres
      const updateableFields = objectSchema.reduce((acc, field) => {
        if (field.canUpdate) {
          acc.push(field);
        }
        return acc;
      }, []);

      // Get all _sync_status = 'PENDING' records that need to be synced to Salesforce
      const pendingRecords = await postgres.select(
        schemaName,
        objectName.toLowerCase(),
        databaseColumns,
        `_sync_status = 'PENDING'`,
        null,
        null,
        logger
      );

      logger.info(
        `Starting to sync postgres ${pendingRecords.rows.length} records to Salesforce`
      );

      // Loop pendingRecords sequentially to avoid excessive API call.
      for (const pendingRecord of pendingRecords.rows) {
        // if pendingRecord.id is empty, then it is new record.
        if (!pendingRecord.id) {
          logger.info(
            {
              data: {
                objectName,
                pendingRecord,
                createableFields
              }
            },
            'Pending record is a new record in Salesforce'
          );

          // Construct salesforce record only the updateable fields
          let salesforceRecord = {};
          Object.keys(pendingRecord).forEach(fieldKey => {
            // Create only if the fieldKey is in the createableFields array
            const foundField = createableFields.find(
              field => field.name === fieldKey
            );

            if (foundField) {
              salesforceRecord = processPendingRecord(
                objectName,
                salesforceRecord,
                pendingRecord,
                foundField,
                fieldKey
              );
            }
          });

          // If salesforceRecord is empty then throw error
          if (Object.keys(salesforceRecord).length === 0) {
            throw new Error('No creatable fields found');
          }

          // Create record in Salesforce
          const createUrl = `${restUrl}/sobjects/${objectName}`;
          logger.info(
            {
              data: {
                createUrl,
                salesforceRecord
              }
            },
            `Creating new record in Salesforce`
          );

          const salesforceResponse = await axios.post(
            createUrl,
            salesforceRecord,
            {
              headers: {
                Authorization: `Bearer ${sessionId}`,
                'Content-Type': 'application/json'
              }
            }
          );

          const result = salesforceResponse.data;
          logger.info({ data: { result } }, `Created new record in Salesforce`);
          const syncUpdateTimestamp = moment.utc().format();

          // Update _sync_status to 'SYNCED'
          await postgres.update(
            schemaName,
            objectName.toLowerCase(),
            {
              _sync_update_timestamp: syncUpdateTimestamp,
              _sync_status: 'SYNCED',
              _sync_message: JSON.stringify({
                command: 'postgresToSalesforce'
              }),
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
        } else {
          logger.info(
            {
              data: {
                objectName,
                pendingRecord,
                updateableFields
              }
            },
            `Pending record is an existing record in Salesforce`
          );

          // Construct salesforce record only the updateable fields
          let salesforceRecord = {};
          Object.keys(pendingRecord).forEach(fieldKey => {
            // Update only if the fieldKey is in the updateableFields array
            const foundField = updateableFields.find(
              field => field.name === fieldKey
            );

            if (foundField) {
              salesforceRecord = processPendingRecord(
                objectName,
                salesforceRecord,
                pendingRecord,
                foundField,
                fieldKey
              );
            }
          });

          if (Object.keys(salesforceRecord).length === 0) {
            throw new Error('No updateable fields found');
          }

          // Update record in Salesforce
          const updateUrl = `${restUrl}/sobjects/${objectName}/${pendingRecord.id}`;
          logger.info(
            {
              data: {
                updateUrl,
                salesforceRecord
              }
            },
            `Updating existing record in Salesforce`
          );

          const salesforceResponse = await axios.patch(
            updateUrl,
            salesforceRecord,
            {
              headers: {
                Authorization: `Bearer ${sessionId}`,
                'Content-Type': 'application/json'
              }
            }
          );

          const result = salesforceResponse.data;
          logger.info(
            { data: { result } },
            `Updated existing record in Salesforce`
          );

          const syncUpdateTimestamp = moment.utc().format();

          // Update _sync_status to 'SYNCED'
          await postgres.update(
            schemaName,
            objectName.toLowerCase(),
            {
              _sync_update_timestamp: syncUpdateTimestamp,
              _sync_status: 'SYNCED',
              _sync_message: JSON.stringify({ command: 'postgresToSalesforce' })
            },
            pendingRecord._sync_id,
            '_sync_id',
            logger
          );

          logger.info(
            { data: { result } },
            'Completed to update an existing record in Salesforce'
          );
        }
      }
    }

    logger.info('Done');
    process.exit(0);
  } catch (err) {
    if (err.response) {
      // The request was made and the server responded with a status code
      // that falls out of the range of 2xx
      const { status, headers, data } = err.response;
      logger.error(
        {
          status,
          headers,
          data
        },
        'Error response from Salesforce'
      );
    } else if (err.request) {
      // The request was made but no response was received
      // `err.request` is an instance of XMLHttpRequest in the browser and an instance of
      // http.ClientRequest in node.js
      const { status, headers, data } = err.response;
      logger.error(
        {
          status,
          headers,
          data
        },
        'Error occurred'
      );
    } else {
      // Something happened in setting up the request that triggered an Error
      logger.error({ err }, 'Error occurred');
    }

    process.exit(1);
  }
})();
