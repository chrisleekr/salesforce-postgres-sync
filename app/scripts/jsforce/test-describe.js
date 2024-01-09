const logger = require('../../helpers/logger');
const salesforce = require('../../helpers/salesforce');

(async () => {
  // Login to Salesforce
  await salesforce.login(logger);

  const object = await salesforce.describe('Account', logger);
  logger.info({ object }, 'Object description');
})();
