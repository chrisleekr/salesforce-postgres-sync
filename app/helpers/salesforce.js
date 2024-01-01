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

  logger.info({ salesforceMode, username }, 'Starting Salesforce login');
  await salesforceConn.login(username, password + securityToken);
  logger.info({ salesforceMode, username }, 'Salesforce login successful');
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

const bulkQueryV2 = async (
  salesforceObjectName,
  query,
  onRecord,
  onError,
  onEnd,
  rawLogger
) => {
  await checkLimit(rawLogger);
  const logger = rawLogger.child({ helper: 'salesforce' });

  logger.info({ data: { query } }, 'Starting bulk query');

  const job = salesforceConn.bulk.createJob(salesforceObjectName, query);
  const batch = job.createBatch();
  batch.execute(query);

  let totalRecords = 0;
  let processedRecords = 0;
  batch.on('queue', batchInfo => {
    logger.info({ data: { batchInfo } }, 'Batch queued');
    totalRecords = batchInfo.totalSize;
    batch.poll(1000, 20000);
  });

  batch.on('response', records => {
    processedRecords += records.length;
    records.map(record => onRecord(record));
    if (processedRecords >= totalRecords) {
      onEnd();
    }
  });

  batch.on('error', err => onError(err));
};

const query = async (soqlQuery, onRecord, onBatch, onEnd, rawLogger) => {
  await checkLimit(rawLogger);
  const logger = rawLogger.child({ helper: 'salesforce' });

  logger.info({ data: { soqlQuery } }, 'Starting query');

  let totalProcessed = 0;
  let response = await salesforceConn.query(soqlQuery, {
    autoFetch: true,
    maxFetch: 10000, // 1000 is max
    scanAll: true
  });
  let batchCount = 1;

  const { totalSize } = response;
  let { records } = response;

  logger.info(
    {
      data: {
        batchCount,
        totalSize,
        recordsLength: records.length,
        nextRecordsUrl: response.nextRecordsUrl
      }
    },
    `Query count: ${batchCount} Records in this batch: ${records.length} nextRecordsUrl: ${response.nextRecordsUrl}`
  );
  batchCount += 1;

  // Loop records and execute onRecord with record

  await Promise.all(records.map(record => onRecord(record)));

  totalProcessed += records.length;

  logger.info(`Total processed records: ${totalProcessed}/${totalSize}`);

  while (!response.done) {
    await checkLimit(rawLogger);

    logger.info(
      { data: { nextRecordsUrl: response.nextRecordsUrl } },
      'Starting query more'
    );
    response = await salesforceConn.queryMore(response.nextRecordsUrl);
    records = response.records;

    logger.info(
      {
        data: {
          totalSize,
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
    await Promise.all(records.map(record => onRecord(record)));

    batchCount += 1;
    totalProcessed += records.length;

    // Pass last records to onBatch
    await onBatch(records[records.length - 1]);

    logger.info(`Total processed records: ${totalProcessed}/${totalSize}`);
  }

  await onEnd();
};

const queryV2 = async (soqlQuery, onRecord, onError, onEnd, rawLogger) => {
  await checkLimit(rawLogger);
  const logger = rawLogger.child({ helper: 'salesforce' });

  logger.info({ data: { soqlQuery } }, 'Starting queryV2');

  salesforceConn
    .query(soqlQuery)
    .on('record', record => {
      onRecord(record);
    })
    .on('end', () => {
      onEnd();
    })
    .on('error', err => {
      onError(err);
    })
    .run({ autoFetch: true, maxFetch: 10000, scanAll: true });
};

const create = async (objectName, record, onError, onEnd, rawLogger) => {
  await checkLimit(rawLogger);
  const logger = rawLogger.child({
    helper: 'salesforce',
    func: 'create',
    objectName
  });
  logger.info(`Starting Salesforce object create for ${objectName}`);

  let result = null;
  try {
    result = await salesforceConn.sobject(objectName).create(record);
  } catch (err) {
    logger.error(
      { err, data: { objectName, record } },
      `Error in Salesforce object create for ${objectName}`
    );
    onError(record, err);
    return;
  }

  logger.info(`Completed Salesforce object create for ${objectName}`);

  onEnd(record, result);
};

const update = async (objectName, record, onError, onEnd, rawLogger) => {
  await checkLimit(rawLogger);
  const logger = rawLogger.child({
    helper: 'salesforce',
    func: 'update',
    objectName
  });
  logger.info(`Starting Salesforce object update for ${objectName}`);

  let result = null;

  try {
    result = await salesforceConn.sobject(objectName).update(record);
  } catch (err) {
    logger.error(
      { err, data: { objectName, record } },
      `Error in Salesforce object update for ${objectName}`
    );
    onError(record, err);
    return;
  }

  logger.info(`Completed Salesforce object update for ${objectName}`);

  onEnd(record, result);
};

module.exports = {
  getSalesforceColumns,
  login,
  describe,
  bulkQuery,
  bulkQueryV2,
  query,
  queryV2,
  create,
  update
};
