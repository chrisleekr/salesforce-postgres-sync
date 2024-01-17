const axios = require('axios');
const config = require('config');

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
  const sessionId = sessionIdMatch ? sessionIdMatch[1] : '';

  const serverUrlMatch = data.match(/<serverUrl>(.*?)<\/serverUrl>/);
  const serverUrl = serverUrlMatch ? serverUrlMatch[1] : '';

  const userIdMatch = data.match(/<userId>(.*?)<\/userId>/);
  const userId = userIdMatch ? userIdMatch[1] : '';

  const organizationIdMatch = data.match(
    /<organizationId>(.*?)<\/organizationId>/
  );
  const organizationId = organizationIdMatch ? organizationIdMatch[1] : '';

  return {
    sessionId,
    serverUrl,
    userId,
    organizationId,
    // Create REST URL from serverUrl
    restUrl: `${new URL(serverUrl).origin}/services/data/v59.0`
  };
};

module.exports = {
  login
};
