const config = require('config');
const jsforce = require('jsforce');

const salesforceMode = config.get('salesforce.mode');

const salesforceConn = new jsforce.Connection({
  loginUrl:
    salesforceMode === 'production'
      ? 'https://login.salesforce.com'
      : 'https://test.salesforce.com'
});

salesforceConn.bulk.pollTimeout = 60000; // 60 seconds

const checkLimit = async rawLogger => {
  const logger = rawLogger.child({ helper: 'salesforce', func: 'checkLimit' });
  logger.info({ apiUsage: salesforceConn.limitInfo.apiUsage }, 'API Usage');
};

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
  await checkLimit(rawLogger);
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
  await checkLimit(rawLogger);
  const logger = rawLogger.child({ helper: 'salesforce' });

  logger.info({ data: { query } }, 'Starting bulk query');

  return salesforceConn.bulk
    .query(query)
    .on('record', onRecord)
    .on('error', onError)
    .on('end', onEnd);
};

const query = async (soqlQuery, onRecord, onEnd, rawLogger) => {
  await checkLimit(rawLogger);
  const logger = rawLogger.child({ helper: 'salesforce' });

  logger.info({ data: { soqlQuery } }, 'Starting query');

  let totalRecords = 0;
  let response = await salesforceConn.query(soqlQuery, {
    autoFetch: true,
    maxFetch: 2000
  });
  let batchCount = 1;

  let { records } = response;
  logger.info(
    {
      data: {
        batchCount,
        recordsLength: records.length,
        nextRecordsUrl: response.nextRecordsUrl
      }
    },
    `Query count: ${batchCount} Records in this batch: ${records.length} nextRecordsUrl: ${response.nextRecordsUrl}`
  );
  batchCount += 1;

  // Loop records and execute onRecord with record
  records.forEach(record => onRecord(record));

  totalRecords += records.length;

  while (!response.done) {
    response = await salesforceConn.queryMore(response.nextRecordsUrl);
    records = response.records;

    logger.info(
      {
        data: {
          batchCount,
          recordsLength: records.length,
          nextRecordsUrl: response.nextRecordsUrl
        }
      },
      `Query count: ${batchCount} Records in this batch: ${
        records.length
      } nextRecordsUrl: ${response.nextRecordsUrl ?? ''}`
    );

    // Loop records and execute onRecord with record
    records.forEach(record => onRecord(record));

    batchCount += 1;
    totalRecords += records.length;
  }
  logger.info(`Total records: ${totalRecords}`);

  onEnd();
};

module.exports = {
  getSalesforceColumns,
  login,
  describe,
  bulkQuery,
  query
};
