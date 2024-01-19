const axios = require('axios');
const config = require('config');

const salesforceMode = config.get('salesforce.mode');
const soapUrl =
  salesforceMode === 'production'
    ? 'https://login.salesforce.com/services/Soap/u/59.0'
    : 'https://test.salesforce.com/services/Soap/u/59.0';

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

let sessionId = '';
let restUrl = '';

const login = async logger => {
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
  // Note that it's not recommended to parse XML with regex,
  // but this is a simple method without adding another dependency.
  const sessionIdMatch = data.match(/<sessionId>(.*?)<\/sessionId>/);
  sessionId = sessionIdMatch ? sessionIdMatch[1] : '';

  const serverUrlMatch = data.match(/<serverUrl>(.*?)<\/serverUrl>/);
  const serverUrl = serverUrlMatch ? serverUrlMatch[1] : '';

  // Create REST URL from serverUrl
  restUrl = `${new URL(serverUrl).origin}/services/data/v59.0`;

  logger.info(
    {
      sessionId,
      serverUrl,
      restUrl
    },
    'Parsed login response'
  );
};

const getLoginDetails = () => ({
  sessionId,
  restUrl
});

module.exports = {
  sessionId,
  restUrl,
  login,
  getLoginDetails
};
