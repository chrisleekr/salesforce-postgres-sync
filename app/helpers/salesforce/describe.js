const axios = require('axios');

const describe = async (objectName, { restUrl, sessionId }, logger) => {
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

module.exports = { describe };
