const config = require('config');
const jsforce = require('jsforce');

const salesforceMode = config.get('salesforce.mode');

const salesforceConn = new jsforce.Connection({
  loginUrl:
    salesforceMode === 'production'
      ? 'https://login.salesforce.com'
      : 'https://test.salesforce.com'
});

salesforceConn.bulk.pollTimeout = 25000;

const getSalesforceColumns = salesforceObjectFields => {
  const salesforceCommonFields = config
    .get('salesforce.commonFields')
    .filter(field => field.sfColumn)
    .map(field => field.name);

  return [...salesforceCommonFields, ...salesforceObjectFields].map(field =>
    field.toLowerCase()
  );
};

const login = async rawLogger => {
  const logger = rawLogger.child({ helper: 'salesforce', func: 'login' });

  const username = config.get('salesforce.username');
  const password = config.get('salesforce.password');
  const securityToken = config.get('salesforce.securityToken');

  if (!username || !password || !securityToken) {
    throw new Error('Salesforce credentials not found');
  }

  await salesforceConn.login(username, password + securityToken);
  logger.info('Salesforce login successful');
};

const describe = async (objectName, rawLogger) => {
  const logger = rawLogger.child({
    helper: 'salesforce',
    func: 'describe',
    objectName
  });
  logger.info(`Starting Salesforce object describe for ${objectName}`);

  const describeResult = await salesforceConn.sobject(objectName).describe();
  logger.info(`Completed Salesforce object describe for ${objectName}`);

  return describeResult;
};

const bulkQuery = async (query, onRecord, onError, onEnd, rawLogger) => {
  const logger = rawLogger.child({ helper: 'salesforce' });

  logger.info({ data: { query } }, 'Starting bulk query');

  return salesforceConn.bulk
    .query(query)
    .on('record', onRecord)
    .on('error', onError)
    .on('end', onEnd);
};

module.exports = {
  getSalesforceColumns,
  login,
  describe,
  bulkQuery
};
