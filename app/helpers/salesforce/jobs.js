const axios = require('axios');
const { restUrl, sessionId } = require('./login');

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
const jobsQueryToCSV = async (query, logger) => {
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

  return queryJobResponse;
};

/*
Loop until the job state is JobComplete
    Maximum loop is 10 minutes
 Retrieve job status
  - https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/query_get_one_job.htm
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
const jobQueryStatus = async (jobId, logger) => {
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

//
//
/*
  Get results for the query job
- https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/query_get_job_results.htm
  Loop until Sforce-Locator is null
  Sample Request
    GET /jobs/query/${jobId}/results?maxRecords=50000
    Authorization: Bearer ${sessionId}
    Accept: text/csv
  Sample Response
    Sforce-Locator: MTAwMDA
    Sforce-NumberOfRecords: 50000
    ...

    "Id","Name"
    "005R0000000UyrWIAS","Jane Dunn"
    "005R0000000GiwjIAC","George Wright"
    "005R0000000GiwoIAC","Pat Wilson"


  Save contents to /tmp/${objectName}-${jobId}-1.csv
  Extract the Sforce-Locator header from the response
     Request GET /jobs/query/${jobId}/results?locator=${Sforce-Locator}&maxRecords=50000
    Save contents to /tmp/${objectName}-${jobId}-2.csv
  */
const jobQueryResults = async (jobId, sForceLocator, logger) => {
  const jobResultsUrl = `${restUrl}/jobs/query/${jobId}/results?maxRecords=100000${
    sForceLocator && sForceLocator !== 'null' ? `&locator=${sForceLocator}` : ''
  }`;
  logger.info({ jobResultsUrl }, 'Getting job results');
  const jobResultsResponse = await axios.get(
    jobResultsUrl,
    {
      headers: {
        Authorization: `Bearer ${sessionId}`,
        Accept: 'text/csv'
      }
    },
    {
      timeout: 1000 * 60 * 5
      // 5 minutes, for some reason, sometimes the request is taking a long time.
      // Then timeout first and then retry.
    }
  );
  logger.info(
    {
      status: jobResultsResponse.status,
      headers: jobResultsResponse.headers
    },
    'Job results response'
  );

  return jobResultsResponse;
};

module.exports = { jobsQueryToCSV, jobQueryStatus, jobQueryResults };
