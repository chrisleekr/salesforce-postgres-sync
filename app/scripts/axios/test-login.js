// https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_quickstart_intro.htm?q=login
/* eslint-disable max-len */
const axios = require('axios');
const config = require('config');
const logger = require('../../helpers/logger');

const soapUrl = 'https://test.salesforce.com/services/Soap/u/59.0';

const username = config.get('salesforce.username');
const password = config.get('salesforce.password');
const securityToken = config.get('salesforce.securityToken');

const loginEnvelope = `
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Header/>
    <soapenv:Body>
        <login xmlns="urn:partner.soap.sforce.com">
            <username>${username}</username>
            <password>${password}${securityToken}</password>
        </login>
    </soapenv:Body>
</soapenv:Envelope>
`;

(async () => {
  try {
    const response = await axios.post(soapUrl, loginEnvelope, {
      headers: {
        'Content-Type': 'text/xml',
        SOAPAction: '""'
      }
    });
    logger.info(
      { status: response.status, data: response.data },
      'Login successful'
    );

    const { data } = response;

    // Get sessionId, serverUrl, userId, and organizationId from parsedData by string match
    // Note that it's not recommended to parse XML with regex, but this is a simple method without adding another dependency.
    const sessionIdMatch = data.match(/<sessionId>(.*?)<\/sessionId>/);
    const sessionId = sessionIdMatch ? sessionIdMatch[1] : '';

    const serverUrlMatch = data.match(/<serverUrl>(.*?)<\/serverUrl>/);
    const serverUrl = serverUrlMatch ? serverUrlMatch[1] : '';

    const userIdMatch = data.match(/<userId>(.*?)<\/userId>/);
    const userId = userIdMatch ? userIdMatch[1] : '';

    const organizationIdMatch = data.match(
      /<organizationId>(.*?)<\/organizationId>/
    );
    const organizationId = organizationIdMatch ? organizationIdMatch[1] : '';

    logger.info(
      {
        sessionId,
        serverUrl,
        userId,
        organizationId
      },
      'Parsed login response'
    );

    // Create REST URL from serverUrl
    const restUrl = `${new URL(serverUrl).origin}/services/data/v59.0`;

    // Execute Salesforce REST API with Bearer token with sessionId to execute describe for `Account` object
    const describeUrl = `${restUrl}/sobjects/Account/describe/`;
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
  } catch (error) {
    logger.error({ error }, 'Error in login');
  }
})();
