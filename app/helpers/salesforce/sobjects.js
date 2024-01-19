const axios = require('axios');
const { getLoginDetails } = require('./login');

const getSobjectDescribe = async (objectName, logger) => {
  const { restUrl, sessionId } = getLoginDetails();
  // Execute Salesforce REST API with Bearer token with sessionId to execute describe for `Account` object
  const describeUrl = `${restUrl}/sobjects/${objectName}/describe/`;
  const describeResponse = await axios.get(describeUrl, {
    headers: {
      Authorization: `Bearer ${sessionId}`,
      'Content-Type': 'application/json'
    }
  });

  logger.info(
    { status: describeResponse.status, data: describeResponse.data },
    'Describe response'
  );

  return describeResponse;
};

const createSobjectRecord = async (objectName, salesforceRecord, logger) => {
  const { restUrl, sessionId } = getLoginDetails();
  const createUrl = `${restUrl}/sobjects/${objectName}`;
  logger.info(
    {
      data: {
        createUrl,
        salesforceRecord
      }
    },
    `Creating new record in Salesforce`
  );

  const salesforceResponse = await axios.post(createUrl, salesforceRecord, {
    headers: {
      Authorization: `Bearer ${sessionId}`,
      'Content-Type': 'application/json'
    }
  });

  return salesforceResponse;
};

const updateSobjectRecord = async (objectName, salesforceRecord, logger) => {
  const { restUrl, sessionId } = getLoginDetails();
  const updateUrl = `${restUrl}/sobjects/${objectName}/${salesforceRecord.id}`;
  logger.info(
    {
      data: {
        updateUrl,
        salesforceRecord
      }
    },
    `Updating existing record in Salesforce`
  );

  const salesforceResponse = await axios.patch(updateUrl, salesforceRecord, {
    headers: {
      Authorization: `Bearer ${sessionId}`,
      'Content-Type': 'application/json'
    }
  });

  return salesforceResponse;
};

module.exports = {
  getSobjectDescribe,
  createSobjectRecord,
  updateSobjectRecord
};
