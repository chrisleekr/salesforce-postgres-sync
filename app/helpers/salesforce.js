const fs = require('fs');
const csv = require('csv-parser');
const config = require('config');
const jsforce = require('jsforce');

const salesforceMode = config.get('salesforce.mode');

const salesforceConn = new jsforce.Connection({
  loginUrl:
    salesforceMode === 'production'
      ? 'https://login.salesforce.com'
      : 'https://test.salesforce.com'
});

salesforceConn.bulk.pollTimeout = 3600000; // 1 hour

const checkLimit = async rawLogger => {
  const logger = rawLogger.child({ helper: 'salesforce', func: 'checkLimit' });
  logger.info({ apiUsage: salesforceConn.limitInfo.apiUsage }, 'API Usage');
};

const getSalesforceColumns = (salesforceObjectName, salesforceObjectFields) => {
  const salesforceCommonFields = config
    .get('salesforce.commonFields')
    .filter(field => field.sfColumn)
    .map(field => field.name);

  let combinedFields = [...salesforceCommonFields, ...salesforceObjectFields];

  if (salesforceObjectName.toLowerCase() === 'user') {
    const excludeFields = ['IsDeleted'];
    combinedFields = combinedFields.filter(
      field => !excludeFields.includes(field)
    );
  }

  return combinedFields.map(field => field.toLowerCase());
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
  lastBulkJob,
  onQueue,
  onRecord,
  onError,
  onEnd,
  rawLogger
) => {
  await checkLimit(rawLogger);
  const logger = rawLogger.child({
    helper: 'salesforce',
    salesforceObjectName,
    lastBulkJob
  });

  logger.info({ data: { query } }, 'Starting bulk query');

  let job;
  let batch;

  let queuedBatchInfo;

  // If last bulk job id is provided, the retrieve job
  if (lastBulkJob.jobId) {
    job = salesforceConn.bulk.job(lastBulkJob.jobId);
    batch = job.batch(lastBulkJob.id);
    job.operation = 'queryAll'; // Workaround to make sure it's polling for queryAll
    batch.poll(1000, 3600000);
    queuedBatchInfo = lastBulkJob;
  } else {
    job = salesforceConn.bulk.createJob(salesforceObjectName, 'queryAll');
    batch = job.createBatch();
    batch.execute(query);
  }

  let totalRecords = 0;
  let processedRecords = 0;
  batch.on('queue', batchInfo => {
    logger.info({ data: { batchInfo } }, 'Batch queued');
    queuedBatchInfo = batchInfo;
    totalRecords = batchInfo.totalSize;
    batch.poll(1000, 3600000);
    onQueue(batchInfo);
  });

  batch.on('progress', batchInfo => {
    logger.info({ data: { batchInfo } }, 'Batch progress');
  });

  batch.on('error', err => onError(err));

  batch.on('response', results => {
    // Loop results and save to CSV file
    // Wait until all streams are completed

    // const promises = results.map(result => {
    //   const resultId = result.id;
    //   const writeStream = fs.createWriteStream(`/tmp/${result.id}.csv`);
    //   const readStream = batch.result(resultId).stream();

    //   readStream.pipe(writeStream);

    //   return new Promise((resolve, reject) => {
    //     readStream.on('end', err => {
    //       if (err) {
    //         reject(err);
    //       } else {
    //         resolve();
    //       }
    //     });
    //   });
    // });

    // Promise.all(promises)
    //   .then(() => console.log('All streams have ended'))
    //   .catch(err => console.error('An error occurred:', err));

    results.forEach(result => {
      // Save to the CSV file in /tmp folders
      batch
        .result(result.id)
        .stream()
        .pipe(fs.createWriteStream(`/tmp/${result.id}.csv`))
        .on('end', () => {
          logger.info({ data: { result } }, 'End of result stream');
        });
      const records = [];
      batch
        .result(result.id)
        .stream()
        .pipe(csv())
        .on('data', async record => {
          logger.info({ data: { record } }, 'Record');
          records.push(record);
          // await onRecord(record);
          // processedRecords += 1;
        })
        .on('end', async () => {
          await onRecord(records);
          processedRecords += records.length;
          logger.info(
            { data: { result } },
            'End of result stream, checking if all records are processed'
          );
          if (processedRecords >= totalRecords) {
            logger.info(
              { processedRecords, totalRecords, data: { result } },
              'All records are processed, ending bulk query'
            );
            await onEnd(queuedBatchInfo);
          } else {
            logger.info(
              { processedRecords, totalRecords, data: { result } },
              'All records are not processed, waiting for next batch'
            );
          }
        });
    });
  });
};

const query = async (soqlQuery, onRecord, onBatch, onEnd, rawLogger) => {
  await checkLimit(rawLogger);
  const logger = rawLogger.child({ helper: 'salesforce' });

  logger.info({ data: { soqlQuery } }, 'Starting query');

  let totalProcessed = 0;
  let response = await salesforceConn.query(soqlQuery, {
    autoFetch: true,
    maxFetch: 1000 // 1000 is max
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

  await onEnd(records[records.length - 1]);
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
