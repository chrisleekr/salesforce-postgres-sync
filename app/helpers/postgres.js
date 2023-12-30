const config = require('config');
const { Client } = require('pg');

const readwriteConn = new Client({
  host: config.get('postgres.readwrite.host'),
  port: config.get('postgres.readwrite.port'),
  user: config.get('postgres.readwrite.user'),
  password: config.get('postgres.readwrite.password'),
  database: config.get('postgres.readwrite.database')
});

readwriteConn.connect();

const readonlyConn = new Client({
  host: config.get('postgres.readonly.host'),
  port: config.get('postgres.readonly.port'),
  user: config.get('postgres.readonly.user'),
  password: config.get('postgres.readonly.password'),
  database: config.get('postgres.readonly.database')
});

readonlyConn.connect();

const createSchemaIfNotExists = async (schemaName, rawLogger) => {
  const logger = rawLogger.child({
    helper: 'postgres',
    func: 'createSchemaIfNotExists'
  });

  const schemaQuery = `CREATE SCHEMA IF NOT EXISTS ${schemaName}`;
  await readwriteConn.query(schemaQuery);
  logger.info(`Created schema if not exists ${schemaName}`);
};

const createOrUpdateTable = async (
  schemaName,
  tableName,
  tableSchema,
  rawLogger
) => {
  const logger = rawLogger.child({
    helper: 'postgres',
    func: 'createOrUpdateTable',
    schemaName,
    tableName
  });
  logger.info(`Creating or updating table ${schemaName}.${tableName}`);

  // Get fields to create for sequence
  const sequenceToCreate = tableSchema.filter(field => field.defaultSequence);
  // Create sequences
  await Promise.all(
    sequenceToCreate.map(async sequence => {
      const sequenceName = `${tableName}_${sequence.name}_seq`;
      const sequenceQuery = `CREATE SEQUENCE IF NOT EXISTS ${sequenceName}`;
      await readwriteConn.query(sequenceQuery);
      logger.info(
        {
          data: { sequenceQuery }
        },
        `Created sequence if not exists ${sequenceName}`
      );
    })
  );

  // Create table tableName if not exists with the schema tableSchema
  const createTableQuery = `CREATE TABLE IF NOT EXISTS ${schemaName}.${tableName} (${tableSchema
    .map(field => {
      // Set primary if primaryKey is true
      const primary = field.primaryKey ? 'PRIMARY KEY' : '';

      // Set not null if notNull is true
      const notNull = field.notNull ? 'NOT NULL' : '';

      // Set sequenceName for the default value if table schema contains defaultSchema.
      const sequenceName = `${tableName}_${field.name}_seq`;
      const sequence = field.defaultSequence
        ? `DEFAULT nextval('${sequenceName}')`
        : '';

      return `${field.name} ${field.type} ${primary} ${notNull} ${sequence}`;
    })
    .join(', ')})`;

  await readwriteConn.query(createTableQuery);
  logger.info(
    {
      data: { createTableQuery }
    },
    `Created table if not exists ${schemaName}.${tableName}`
  );

  // If the table exists, then compare the columns with the Salesforce "Account" object fields
  // and alter table to add new columns if the columns are not exist
  const describeTableQuery =
    `SELECT column_name FROM information_schema.columns ` +
    `WHERE table_schema = '${schemaName}' AND table_name = '${tableName}'`;
  const describeTableResult = await readonlyConn.query(describeTableQuery);

  const existingColumns = describeTableResult.rows.map(row => row.column_name);

  const newColumns = tableSchema.filter(
    field => !existingColumns.includes(field.name)
  );

  if (newColumns.length > 0) {
    logger.info(
      {
        data: { newColumns }
      },
      `New columns found for ${schemaName}.${tableName}`
    );

    // Alter table to add new columns
    await Promise.all(
      newColumns.map(async column => {
        // Set not null if notNull is true
        const notNull = column.notNull ? 'NOT NULL' : '';

        const alterTableQuery =
          `ALTER TABLE ${schemaName}.${tableName} ` +
          `ADD COLUMN ${column.name} ${column.type} ${notNull}`;

        await readwriteConn.query(alterTableQuery);
        logger.info(
          {
            data: { column }
          },
          `Added column ${column.name} to table ${tableName}`
        );
      })
    );
  } else {
    logger.info(`No new columns found for ${schemaName}.${tableName}`);
  }

  // Get columns to create index
  const indexToCreate = tableSchema.filter(field => field.createIndex);

  // Loop indexToCreate and create index if not exists
  await Promise.all(
    indexToCreate.map(async index => {
      const indexName = `idx_${tableName}_${index.name}_idx`;
      const indexQuery = `CREATE INDEX IF NOT EXISTS ${indexName} ON ${schemaName}.${tableName} (${index.name})`;
      await readwriteConn.query(indexQuery);
      logger.info(`Created index if not exists ${indexName}`);
    })
  );

  // Get columns to create unique index
  const uniqueIndexToCreate = tableSchema.filter(
    field => field.createUniqueIndex
  );

  // Loop uniqueIndexToCreate and create unique index if not exists
  await Promise.all(
    uniqueIndexToCreate.map(async index => {
      const indexName = `idx_${tableName}_${index.name}_idx`;
      const indexQuery = `CREATE UNIQUE INDEX IF NOT EXISTS ${indexName} ON ${schemaName}.${tableName} (${index.name})`;
      await readwriteConn.query(indexQuery);
      logger.info(`Created unique index if not exists ${indexName}`);
    })
  );

  logger.info(`Created or updated table ${schemaName}.${tableName}`);
};

const upsert = async (
  schemaName,
  tableName,
  columns,
  values,
  idColumn,
  rawLogger
) => {
  const logger = rawLogger.child({
    helper: 'postgres',
    func: 'upsert',
    tableName
  });

  const columnsString = columns.join(',');
  const valuesString = values.map(value => `'${value}'`).join(',');

  const upsertQuery = `
    INSERT INTO ${schemaName}.${tableName} (${columnsString})
    VALUES (${valuesString})
    ON CONFLICT (${idColumn})
    DO UPDATE SET
      ${columns
        .map((column, index) => `${column} = '${values[index]}'`)
        .join(',')}
  `;

  logger.debug(
    {
      data: { upsertQuery, schemaName, tableName, columns, values, idColumn }
    },
    `Upserting ${tableName}`
  );

  return readwriteConn.query(upsertQuery);
};

module.exports = {
  createSchemaIfNotExists,
  createOrUpdateTable,
  upsert
};
