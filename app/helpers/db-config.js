const config = require('config');
const postgres = require('./postgres');

const get = async (key, rawLogger) => {
  const logger = rawLogger.child({ helper: 'config', func: 'get' });

  const postgresSchema = config.get('salesforce.postgresSchema');

  const result = await postgres.select(
    postgresSchema,
    '_config',
    ['value'],
    `key = '${key}'`,
    null,
    null,
    logger
  );

  if (result.rows.length > 0) {
    return result.rows[0].value;
  }
  return null;
};

const set = async (key, value, rawLogger) => {
  const logger = rawLogger.child({ helper: 'config', func: 'set' });

  const postgresSchema = config.get('salesforce.postgresSchema');

  return postgres.upsert(
    postgresSchema,
    '_config',
    ['key', 'value'],
    [key, value],
    'key',
    logger
  );
};

module.exports = {
  get,
  set
};
