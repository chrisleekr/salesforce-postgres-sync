const config = require('config');
const postgres = require('../helpers/postgres');

module.exports = async rawLogger => {
  const logger = rawLogger.child({ library: 'initial-setup' });
  logger.info('Start initial-setup command');

  const postgresSchema = config.get('salesforce.postgresSchema');

  await postgres.createSchemaIfNotExists(postgresSchema, logger);

  await postgres.createOrUpdateTable(
    postgresSchema,
    '_config',
    [
      {
        name: 'id',
        type: 'integer',
        notNull: true,
        primaryKey: true,
        defaultSequence: true
      },
      {
        name: 'key',
        type: 'varchar(255)',
        createUniqueIndex: true
      },
      {
        name: 'value',
        type: 'text'
      },
      {
        name: 'update_timestamp',
        type: 'timestamp',
        defaultNow: true
      }
    ],
    logger
  );

  logger.info('Completed initial-setup command');
};
