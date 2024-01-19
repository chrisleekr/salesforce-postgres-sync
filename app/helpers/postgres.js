const fs = require('fs');
const config = require('config');
const { Pool } = require('pg');
const copyFrom = require('pg-copy-streams').from;

const readwriteConn = new Pool({
  host: config.get('postgres.readwrite.host'),
  port: config.get('postgres.readwrite.port'),
  user: config.get('postgres.readwrite.user'),
  password: config.get('postgres.readwrite.password'),
  database: config.get('postgres.readwrite.database'),
  max: 20
});

const readonlyConn = new Pool({
  host: config.get('postgres.readonly.host'),
  port: config.get('postgres.readonly.port'),
  user: config.get('postgres.readonly.user'),
  password: config.get('postgres.readonly.password'),
  database: config.get('postgres.readonly.database')
});

let readwriteClient;
// eslint-disable-next-line no-unused-vars
let readonlyClient;

const connect = async rawLogger => {
  const logger = rawLogger.child({
    helper: 'postgres',
    func: 'connect'
  });

  readwriteClient = await readwriteConn.connect();
  readonlyClient = await readonlyConn.connect();

  logger.info('Connected to postgres');
};

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
      logger.info({ indexQuery }, `Creating index if not exists ${indexName}`);
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
      logger.info(
        { indexQuery },
        `Creating unique index if not exists ${indexName}`
      );
      await readwriteConn.query(indexQuery);
      logger.info(`Created unique index if not exists ${indexName}`);
    })
  );

  // Get columns to create trigger for default now
  const defaultNowToCreate = tableSchema.filter(field => field.defaultNow);

  // Loop defaultNowToCreate and create trigger if not exists
  await Promise.all(
    defaultNowToCreate.map(async column => {
      const triggerFunctionName = `trg_fn_${tableName}_${column.name}_trg`;

      // Create trigger function if not exists
      const triggerFunctionQuery =
        `CREATE OR REPLACE FUNCTION ${schemaName}.${triggerFunctionName}() ` +
        `RETURNS TRIGGER AS $$ ` +
        `BEGIN ` +
        `NEW.${column.name} = NOW(); ` +
        `RETURN NEW; ` +
        `END; ` +
        `$$ LANGUAGE plpgsql;`;
      await readwriteConn.query(triggerFunctionQuery);

      const triggerName = `trg_${tableName}_${column.name}_trg`;

      // Check trigger is already existing in the table or not
      const selectTriggerQuery =
        `SELECT trigger_name FROM information_schema.triggers ` +
        `WHERE event_object_schema = '${schemaName}' ` +
        `AND event_object_table = '${tableName}' AND trigger_name = '${triggerName}'`;
      const triggerResult = await readwriteConn.query(selectTriggerQuery);

      // Create trigger if not exists
      if (triggerResult.rows.length === 0) {
        const triggerQuery =
          `CREATE TRIGGER ${triggerName} ` +
          `BEFORE INSERT OR UPDATE ON ${schemaName}.${tableName} ` +
          `FOR EACH ROW EXECUTE PROCEDURE ${schemaName}.${triggerFunctionName}()`;
        logger.info(
          { triggerQuery },
          `Creating trigger if not exists ${triggerName}`
        );
        await readwriteConn.query(triggerQuery);
        logger.info(`Created trigger if not exists ${triggerName}`);
      }
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

  let columnIdx = 0;
  const columnValues = [];

  const upsertQuery = `
    INSERT INTO ${schemaName}.${tableName} (${columnsString})
    VALUES (${values
      .map(value => {
        columnValues.push(value);
        columnIdx += 1;
        return `$${columnIdx}`;
      })
      .join(',')})
    ON CONFLICT (${idColumn})
    DO UPDATE SET
      ${columns
        .map((column, index) => {
          columnValues.push(values[index]);
          columnIdx += 1;
          return `${column} = $${columnIdx}`;
        })
        .join(',')}
  `;

  logger.debug(
    {
      data: { upsertQuery, schemaName, tableName, columns, values, idColumn }
    },
    `Upsert ${tableName}`
  );

  return readwriteConn.query(upsertQuery, columnValues).catch(err => {
    logger.error(
      {
        err,
        data: { upsertQuery, schemaName, tableName, columns, values, idColumn }
      },
      `Error in upsert ${tableName}`
    );
    throw err;
  });
};

const select = async (
  schemaName,
  tableName,
  columns,
  where,
  orderBy,
  limit,
  rawLogger
) => {
  const logger = rawLogger.child({
    helper: 'postgres',
    func: 'select',
    tableName
  });

  const columnsString = columns.join(',');
  const whereString = where ? `WHERE ${where}` : '';
  const orderByString = orderBy ? `ORDER BY ${orderBy}` : '';
  const limitString = limit ? `LIMIT ${limit}` : '';

  const selectQuery = `
    SELECT ${columnsString}
    FROM ${schemaName}.${tableName}
    ${whereString}
    ${orderByString}
    ${limitString}
  `;

  logger.debug(
    {
      data: {
        selectQuery,
        schemaName,
        tableName,
        columns,
        where,
        orderBy,
        limit
      }
    },
    `Select ${tableName}`
  );

  return readonlyConn.query(selectQuery);
};

const update = async (
  schemaName,
  tableName,
  values,
  id,
  idColumn,
  rawLogger
) => {
  const logger = rawLogger.child({
    helper: 'postgres',
    func: 'update',
    tableName
  });

  const columnValues = [];

  const updateQuery = `
    UPDATE ${schemaName}.${tableName}
    SET ${Object.keys(values)
      .map((column, index) => {
        columnValues.push(values[column]);
        return `${column} = $${index + 1}`;
      })
      .join(',')}
    WHERE ${idColumn} = ${id}
  `;

  logger.debug(
    {
      data: { updateQuery, schemaName, tableName, values, id, idColumn }
    },
    `Update ${tableName}`
  );

  return readwriteConn.query(updateQuery, columnValues).catch(err => {
    logger.error(
      {
        err,
        data: { updateQuery, schemaName, tableName, values, id, idColumn }
      },
      `Error in update ${tableName}`
    );
    throw err;
  });
};

const deleteRow = async (schemaName, tableName, idColumn, id, rawLogger) => {
  const logger = rawLogger.child({
    helper: 'postgres',
    func: 'deleteRow',
    tableName
  });

  const deleteQuery = `
    DELETE FROM ${schemaName}.${tableName}
    WHERE ${idColumn} = $1
  `;

  logger.debug(
    {
      data: { deleteQuery, schemaName, tableName, idColumn, id }
    },
    `Delete ${tableName}`
  );

  return readwriteConn.query(deleteQuery, [id]).catch(err => {
    logger.error(
      {
        err,
        data: { deleteQuery, schemaName, tableName, idColumn, id }
      },
      `Error in delete ${tableName}`
    );
    throw err;
  });
};

const truncate = async (schemaName, tableName, rawLogger) => {
  const logger = rawLogger.child({
    helper: 'postgres',
    func: 'truncate',
    tableName
  });

  const truncateQuery = `
    TRUNCATE TABLE ${schemaName}.${tableName}
  `;

  logger.debug(
    {
      data: { truncateQuery, schemaName, tableName }
    },
    `Truncate table ${schemaName}.${tableName}`
  );

  return readwriteConn.query(truncateQuery).catch(err => {
    logger.error(
      {
        err,
        data: { truncateQuery, schemaName, tableName }
      },
      `Error in truncate table ${schemaName}.${tableName}`
    );
    throw err;
  });
};

const loadCSVToTable = async (
  schemaName,
  tableName,
  csvPath,
  delimiter,
  rawLogger
) => {
  const logger = rawLogger.child({
    helper: 'postgres',
    func: 'loadCSVToTable',
    csvPath,
    tableName,
    delimiter
  });

  logger.info(`Loading CSV file to ${schemaName}.${tableName}`);

  await new Promise((resolve, reject) => {
    // Get headers from CSV
    const headers = fs
      .readFileSync(csvPath, 'utf8')
      .split('\n')
      .shift()
      .split(delimiter);

    // Begin the COPY operation
    const ingestStream = readwriteClient.query(
      copyFrom(
        `COPY ${schemaName}.${tableName} ( ${headers.join(
          ','
        )}) FROM STDIN DELIMITER '${delimiter}' CSV HEADER`
      )
    );

    const sourceStream = fs.createReadStream(csvPath);

    // Log progress
    let progress = 0;
    sourceStream.on('data', chunk => {
      progress += chunk.length;
      logger.info(
        `Progress: ${Math.round((progress / 1024 / 1024) * 100) / 100}MB`
      );
    });

    sourceStream.on('error', error => {
      logger.error(`Error in READ CSV operation: ${error}`);
      reject(error);
    });

    ingestStream.on('error', error => {
      logger.error(`Error in COPY operation: ${error}`);
      reject(error);
    });

    ingestStream.on('finish', resolve);

    // Pipe the CSV data to the COPY stream
    sourceStream.pipe(ingestStream);
  });

  logger.info(`Completed loading CSV file to ${schemaName}.${tableName}`);
};

module.exports = {
  connect,
  createSchemaIfNotExists,
  createOrUpdateTable,
  upsert,
  select,
  update,
  deleteRow,
  truncate,
  loadCSVToTable
};
