const axios = require('axios');

/*
  Create bulk query job
  - https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/query_create_job.htm

  Sample Request
    POST restUrl/jobs/query
    Authorization: Bearer ${sessionId}
    Accept: application/json
    Content-Type: application/json
    Sforce-Enable-PKChunking: chunkSize=100000;
    {
      "operation": "query",  // Don't need queryAll because don't need deleted records
      "query": `SELECT Id FROM ${objectName}`,
      "contentType": "CSV"
    }
  Sample Response
    {
      "id" : "750R0000000zlh9IAA",
      "operation" : "query",
      "object" : "Account",
      "createdById" : "005R0000000GiwjIAC",
      "createdDate" : "2018-12-10T17:50:19.000+0000",
      "systemModstamp" : "2018-12-10T17:50:19.000+0000",
      "state" : "UploadComplete",
      "concurrencyMode" : "Parallel",
      "contentType" : "CSV",
      "apiVersion" : 46.0,
      "lineEnding" : "LF",
      "columnDelimiter" : "COMMA"
    }
  */
const jobsQueryToCSV = async (query, { restUrl, sessionId }, logger) => {
  const queryJobResponse = await axios.post(
    `${restUrl}/jobs/query`,
    {
      operation: 'query',
      query,
      contentType: 'CSV'
    },
    {
      headers: {
        Authorization: `Bearer ${sessionId}`,
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'Sforce-Enable-PKChunking': 'chunkSize=100000;'
      }
    }
  );
  logger.info(
    { status: queryJobResponse.status, data: queryJobResponse.data },
    'query job response'
  );
};

/*
Loop until the job state is JobComplete
    Maximum loop is 10 minutes
 Retrieve job status - https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/query_get_one_job.htm
Sample Request
  GET restUrl/jobs/query/${jobId}
  Authorization: Bearer ${sessionId}
Sample Response
  {
    "id" : "750R0000000zlh9IAA",
    "operation" : "query",
    "object" : "Account",
    "createdById" : "005R0000000GiwjIAC",
    "createdDate" : "2018-12-10T17:50:19.000+0000",
    "systemModstamp" : "2018-12-10T17:51:27.000+0000",
    "state" : "JobComplete",
    "concurrencyMode" : "Parallel",
    "contentType" : "CSV",
    "apiVersion" : 46.0,
    "jobType" : "V2Query",
    "lineEnding" : "LF",
    "columnDelimiter" : "COMMA",
    "numberRecordsProcessed" : 500,
    "retries" : 0,
    "totalProcessingTime" : 334
  }
*/
const jobQueryStatus = async (jobId, { restUrl, sessionId }, logger) => {
  const jobStatusResponse = await axios.get(`${restUrl}/jobs/query/${jobId}`, {
    headers: {
      Authorization: `Bearer ${sessionId}`
    }
  });

  logger.info(
    {
      status: jobStatusResponse.status,
      headers: jobStatusResponse.headers,
      data: jobStatusResponse.data
    },
    'job status response'
  );

  return jobStatusResponse;
};

module.exports = { jobsQueryToCSV, jobQueryStatus };
