const { login } = require('./login');
const { describe } = require('./describe');
const { query } = require('./query');
const { getSalesforceColumns } = require('./schema');
const { jobsQueryToCSV } = require('./jobs');

module.exports = {
  login,
  describe,
  query,
  getSalesforceColumns,
  jobsQueryToCSV
};
