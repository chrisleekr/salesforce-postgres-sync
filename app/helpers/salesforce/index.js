const { sessionId, restUrl, login } = require('./login');
const { query } = require('./query');
const {
  getSalesforceColumns,
  getDatabaseColumns,
  getCreateableFields,
  getUpdateableFields
} = require('./schema');
const { jobsQueryToCSV, jobQueryStatus, jobQueryResults } = require('./jobs');
const {
  getSobjectDescribe,
  createSobjectRecord,
  updateSobjectRecord
} = require('./sobjects');

module.exports = {
  sessionId,
  restUrl,
  login,
  query,
  getSalesforceColumns,
  getDatabaseColumns,
  getCreateableFields,
  getUpdateableFields,
  jobsQueryToCSV,
  jobQueryStatus,
  jobQueryResults,
  getSobjectDescribe,
  createSobjectRecord,
  updateSobjectRecord
};
