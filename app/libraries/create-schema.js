const config = require('config');
const { createSchemaIfNotExists } = require('../helpers/postgres');

module.exports = async rawLogger => {
  const logger = rawLogger.child({ library: 'create-schema' });

  const postgresSchema = config.get('salesforce.postgresSchema');

  return createSchemaIfNotExists(postgresSchema, logger);
};
