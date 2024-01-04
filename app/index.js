const logger = require('./helpers/logger');
const postgres = require('./helpers/postgres');
const salesforce = require('./helpers/salesforce');

const createSchema = require('./libraries/create-schema');
const initialSetup = require('./libraries/initial-setup');
const syncTableSchema = require('./libraries/sync-table-schema');
const salesforceToPostgres = require('./libraries/salesforce-to-postgres');
const postgresToSalesforce = require('./libraries/postgres-to-salesforce');

(async () => {
  logger.info('Starting Salesforce Postgres Sync');

  // Connect to Postgres
  await postgres.connect(logger);

  // Login to Salesforce
  await salesforce.login(logger);

  // Create a Postgres schema if not exists
  await createSchema(logger);

  // Setup initial tables
  await initialSetup(logger);

  // Create a Postgres table from Salesforce objects
  await syncTableSchema(logger);

  // Sync Salesforce to Postgres
  await salesforceToPostgres(logger);

  // Sync Postgres to Salesforce
  await postgresToSalesforce(logger);
})();
