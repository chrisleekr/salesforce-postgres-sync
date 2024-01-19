const axios = require('axios');
const { getLoginDetails } = require('./login');

// https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_query.htm?q=query
// Sample Request
//  GET /services/data/vXX.X/query?q=query
// A SOQL query. To create a valid URI, replace spaces in the query string with a plus sign + or with %20.
// For example: SELECT+Name+FROM+MyObject.
// If the SOQL query string is invalid, a MALFORMED_QUERY response is returned.
// Sample Response
/*
  {
    "totalSize": 3222,
    "done": false,
    "nextRecordsUrl": "/services/data/v60.0/query/01gRO0000016PIAYA2-500",
    "records": [
      {
        "attributes": {
          "type": "Contact",
          "url": "/services/data/v60.0/sobjects/Contact/003RO0000035WQgYAM"
        },
        "Id": "003RO0000035WQgYAM",
        "Name": "John Smith"
      },
      ...
    ]
  }
*/
const query = async (soqlQuery, onRecord, logger) => {
  const { restUrl, sessionId } = getLoginDetails();

  let totalSize;
  let done;
  let nextRecordsUrl;

  while (!done || done === false) {
    // get hostname including https://
    const hostname = restUrl.match(/https:\/\/[^/]+/)[0];

    const queryUrl = nextRecordsUrl
      ? `${hostname}${nextRecordsUrl}`
      : `${restUrl}/query?q=${encodeURIComponent(soqlQuery)}`;

    logger.info(
      {
        queryUrl,
        totalSize,
        done,
        nextRecordsUrl
      },
      'Querying Salesforce'
    );
    const queryResponse = await axios({
      method: 'get',
      url: queryUrl,
      headers: {
        Authorization: `Bearer ${sessionId}`
      }
    });

    logger.info(
      {
        status: queryResponse.status,
        headers: queryResponse.headers
      },
      'Query results response'
    );

    const { data } = queryResponse;
    totalSize = data.totalSize;
    done = data.done;
    nextRecordsUrl = data.nextRecordsUrl;
    const { records } = data;

    logger.info(
      { totalSize, done, nextRecordsUrl, recordsLength: records.length },
      'Job results response data'
    );

    for (const record of records) {
      await onRecord(record, logger);
    }
  }
};

module.exports = { query };
