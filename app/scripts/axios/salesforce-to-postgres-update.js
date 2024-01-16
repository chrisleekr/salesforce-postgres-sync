/* eslint-disable no-continue */
const axios = require('axios');
const moment = require('moment');
const config = require('config');
const logger = require('../../helpers/logger');
const dbConfig = require('../../helpers/db-config');
const postgres = require('../../helpers/postgres');

const { salesforceLogin } = require('./salesforce-login');

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

      if (!(await shouldIncrementalUpdate(objectName, logger))) {
        logger.info(
          { objectName },
          `Should not incremental update for object ${objectName} because last system mod stamp is null.`
        );
        continue;
      }

      logger.info({ objectName }, 'Processing object name');

      const objectSchema = JSON.parse(
        await dbConfig.get(`${objectName}-schema`, logger)
      );
      // Get name of fields if isSalesforceColumn is true
      const salesforceColumns = objectSchema.reduce((acc, field) => {
        if (field.isSalesforceColumn) {
          acc.push(field.name);
        }
        return acc;
      }, []);

      logger.info({ salesforceColumns }, 'Loaded Salesforce columns');

      const savedLastSystemModStamp = await dbConfig.get(
        `${objectName}-last-system-mod-stamp`,
        logger
      );

      const query = `SELECT ${salesforceColumns.join(
        ', '
      )} FROM ${objectName} WHERE SystemModstamp > ${savedLastSystemModStamp}`;

      logger.info({ query }, 'Querying Salesforce');

      // https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query.htm?q=query
      // Sample Request
      //  GET /services/data/vXX.X/query?q=query
      //      A SOQL query. To create a valid URI, replace spaces in the query string with a plus sign + or with %20. For example: SELECT+Name+FROM+MyObject. If the SOQL query string is invalid, a MALFORMED_QUERY response is returned.
      // Sample Response
      /*
        {
          "totalSize": 3222,
          "done": false,
          "nextRecordsUrl": "/services/data/v60.0/query/01gRO0000016PIAYA2-500",
          "records": [
            {
              "attributes": {
                "type": "Contact",
                "url": "/services/data/v60.0/sobjects/Contact/003RO0000035WQgYAM"
              },
              "Id": "003RO0000035WQgYAM",
              "Name": "John Smith"
            },
            ...
          ]
        }
      */

      let totalSize;
      let done;
      let nextRecordsUrl;
      let lastSystemModStamp = '';

      while (!done || done === false) {
        // get hostname including https://
        const hostname = restUrl.match(/https:\/\/[^/]+/)[0];

        const queryUrl = nextRecordsUrl
          ? `${hostname}${nextRecordsUrl}`
          : `${restUrl}/query?q=${encodeURIComponent(query)}`;

        logger.info(
          {
            queryUrl,
            totalSize,
            done,
            nextRecordsUrl,
            lastSystemModStamp
          },
          'Querying Salesforce'
        );
        const queryResponse = await axios({
          method: 'get',
          url: queryUrl,
          headers: {
            Authorization: `Bearer ${sessionId}`
          }
        });

        logger.info(
          {
            status: queryResponse.status,
            headers: queryResponse.headers
          },
          'Query results response'
        );

        const { data } = queryResponse;
        totalSize = data.totalSize;
        done = data.done;
        nextRecordsUrl = data.nextRecordsUrl;
        const { records } = data;

        logger.info(
          { totalSize, done, nextRecordsUrl, recordsLength: records.length },
          'Job results response data'
        );

        for (const record of records) {
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
            logger
          );

          // If row.systemmodstamp is greater than lastSystemModStamp, then update lastSystemModStamp
          if (
            !lastSystemModStamp ||
            moment(recordLowercase.systemmodstamp).isAfter(lastSystemModStamp)
          ) {
            lastSystemModStamp = recordLowercase.systemmodstamp;
          }
        }
      }

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
