const initialSetup = require('./initial-setup');
const syncTables = require('./sync-tables');
const salesforceToPostgresCleanSync = require('./salesforce-to-postgres-clean-sync');
const salesforceToPostgresIncrementUpdate = require('./salesforce-to-postgres-increment-update');
const postgresToSalesforce = require('./postgres-to-salesforce');

module.exports = {
  initialSetup,
  syncTables,
  salesforceToPostgresCleanSync,
  salesforceToPostgresIncrementUpdate,
  postgresToSalesforce
};
