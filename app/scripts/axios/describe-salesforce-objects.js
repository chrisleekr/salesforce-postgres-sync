// https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_quickstart_intro.htm?q=login
/* eslint-disable max-len */
const axios = require('axios');
const config = require('config');
const logger = require('../../helpers/logger');
const dbConfig = require('../../helpers/db-config');
const postgres = require('../../helpers/postgres');
const initialSetup = require('../../libraries/initial-setup');
const { salesforceLogin } = require('./salesforce-login');

(async () => {
  try {
    // Connect to Postgres
    await postgres.connect(logger);

    const postgresSchema = config.get('salesforce.postgresSchema');

    await postgres.createSchemaIfNotExists(postgresSchema, logger);

    // Setup initial tables
    await initialSetup(logger);

    const { restUrl, sessionId } = await salesforceLogin();

    logger.info(
      {
        sessionId,
        restUrl
      },
      'Parsed login response'
    );

    const configuredObjects = config.get('salesforce.objects');

    // Get the keys of the salesforceObjects object
    const objectNames = Object.keys(configuredObjects);

    for (const objectName of objectNames) {
      // Execute Salesforce REST API with Bearer token with sessionId to execute describe for `Account` object
      const describeUrl = `${restUrl}/sobjects/${objectName}/describe/`;
      const describeResponse = await axios.get(describeUrl, {
        headers: {
          Authorization: `Bearer ${sessionId}`,
          'Content-Type': 'application/json'
        }
      });

      logger.info(
        { status: describeResponse.status, data: describeResponse.data },
        'Describe response'
      );

      await dbConfig.set(
        `${objectName}-describe`,
        JSON.stringify(describeResponse.data),
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
