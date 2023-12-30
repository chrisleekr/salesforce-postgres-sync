const logger = require('./helpers/logger');
const salesforce = require('./helpers/salesforce');
const createSchema = require('./libraries/create-schema');
const syncTableSchema = require('./libraries/sync-table-schema');
const salesforceToPostgres = require('./libraries/salesforce-to-postgres');

(async () => {
  logger.info('Starting Salesforce Postgres Sync');

  // Login to Salesforce
  await salesforce.login(logger);

  // Create a Postgres schema if not exists
  await createSchema(logger);

  // Create a Postgres table from Salesforce objects
  await syncTableSchema(logger);

  // Sync Salesforce to Postgres
  await salesforceToPostgres(logger);
})();
