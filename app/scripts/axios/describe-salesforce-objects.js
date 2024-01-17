// https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_quickstart_intro.htm?q=login
/* eslint-disable max-len */
const config = require('config');
const logger = require('../../helpers/logger');
const dbConfig = require('../../helpers/db-config');
const postgres = require('../../helpers/postgres');
const initialSetup = require('../../libraries/initial-setup');
const salesforce = require('../../helpers/salesforce');

(async () => {
  try {
    // Connect to Postgres
    await postgres.connect(logger);

    // Setup initial tables
    await initialSetup(logger);

    const { restUrl, sessionId } = await salesforce.login(logger);

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
      const describeResponse = await salesforce.describe(
        objectName,
        { restUrl, sessionId },
        logger
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
