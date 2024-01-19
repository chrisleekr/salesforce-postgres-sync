const logger = require('./helpers/logger');
const postgres = require('./helpers/postgres');
const salesforce = require('./helpers/salesforce');
const commands = require('./commands');

(async () => {
  try {
    logger.info('Starting Salesforce Postgres Sync');

    // Connect to Postgres
    await postgres.connect(logger);

    // Login to Salesforce
    await salesforce.login(logger);

    // Create a Postgres schema if not exists and initialise database config table
    await commands.initialSetup(logger);

    // Create a Postgres table from Salesforce objects
    await commands.syncTables(logger);

    // Do clean sync if necessary
    await commands.salesforceToPostgresCleanSync(logger);

    let isSyncRunning = false;

    setInterval(async () => {
      if (!isSyncRunning) {
        isSyncRunning = true;

        logger.info('Starting Salesforce to Postgres - Increment update');
        await commands.salesforceToPostgresIncrementUpdate(logger);
        logger.info('Completed Salesforce to Postgres - Increment update');

        logger.info('Starting Postgres to Salesforce');
        await commands.postgresToSalesforce(logger);
        logger.info('Completed Postgres to Salesforce');

        isSyncRunning = false;
      } else {
        logger.info('Sync is currently running, wait for next tick...');
      }
    }, 60000); // 60000 milliseconds = 1 minute
  } catch (err) {
    logger.error(err);
  }
})();
