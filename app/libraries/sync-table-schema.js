const config = require('config');
const salesforce = require('../helpers/salesforce');
const postgres = require('../helpers/postgres');
const dbConfig = require('../helpers/db-config');

// Function to convert Salesforce field types to PostgreSQL column types
const convertType = sfType => {
  switch (sfType) {
    case 'id':
    case 'reference':
      return 'varchar(20)';
    case 'string':
    case 'textarea':
    case 'picklist':
    case 'multipicklist':
    case 'combobox':
    case 'phone':
    case 'url':
      return 'text';
    case 'boolean':
      return 'boolean';
    case 'int':
      return 'integer';
    case 'double':
    case 'currency':
    case 'percent':
      return 'double precision';
    case 'date':
      return 'date';
    case 'datetime':
      return 'timestamp';
    case 'email':
      return 'varchar(255)';
    default:
      return sfType;
  }
};

module.exports = async rawLogger => {
  const logger = rawLogger.child({ library: 'sync-table-schema' });

  logger.info('Starting sync table schema');

  const postgresSchema = config.get('salesforce.postgresSchema');

  const salesforceObjects = config.get('salesforce.objects');
  const salesforceCommonFields = config.get('salesforce.commonFields');

  // Get the keys of the salesforceObjects object
  const salesforceObjectNames = Object.keys(salesforceObjects);

  // Loop salesforceObjectNames sequentially to avoid excessive API call.
  for (const salesforceObjectName of salesforceObjectNames) {
    const salesforceObjectFields =
      salesforceObjects[salesforceObjectName].fields;

    // Retrieve the salesforceObject information
    const salesforceObject = await salesforce.describe(
      salesforceObjectName,
      logger
    );

    await dbConfig.set(
      `describe-${salesforceObjectName}`,
      JSON.stringify(salesforceObject),
      logger
    );

    // logger.debug({ data: { salesforceObject } }, 'Salesforce object describe');

    const objectFields = [];
    const createableFields = [];
    const updateableFields = [];

    // Loop salesforceObjectFields and check whether it's in salesforceObject.fields.
    // If not exists, then throw error.

    salesforceObjectFields.forEach(fieldName => {
      // Find field from salesforceObject.field
      const foundField = salesforceObject.fields.find(
        field => field.name.toLowerCase() === fieldName.toLowerCase()
      );

      if (!foundField) {
        logger.error(
          {
            data: { fieldName, salesforceObjectFields }
          },
          `Field ${fieldName} not found in ${salesforceObjectName}`
        );
        throw new Error(
          `Field ${fieldName} not found in ${salesforceObjectName}`
        );
      }

      const objectField = {
        label: foundField.label,
        name: foundField.name,
        type: foundField.type,
        createIndex:
          foundField.unique ||
          foundField.idLookup ||
          foundField.filterable ||
          foundField.sortable
      };

      objectFields.push(objectField);

      // If the field is creatable, then push to createableFields.
      if (foundField.createable) {
        createableFields.push(foundField.name);
      }

      // If the field is updateable, then push to updateableFields.
      if (foundField.updateable) {
        updateableFields.push(foundField.name);
      }
    });

    // Construct the table schema
    let tableSchema = [...salesforceCommonFields, ...objectFields].map(
      schema => ({
        ...schema,
        name: schema.name.toLowerCase(),
        type: convertType(schema.type)
      })
    );

    if (salesforceObjectName.toLowerCase() === 'user') {
      const excludeFields = ['isdeleted'];
      tableSchema = tableSchema.filter(
        field => !excludeFields.includes(field.name)
      );
    }

    // Make tableSchema unique by name of the schema
    const uniqueTableSchema = tableSchema.reduce((acc, current) => {
      const x = acc.find(item => item.name === current.name);
      if (!x) {
        return acc.concat([current]);
      }
      return acc;
    }, []);

    // Create or update the table with the table schema
    await postgres.createOrUpdateTable(
      postgresSchema,
      salesforceObjectName.toLowerCase(),
      uniqueTableSchema,
      logger
    );

    logger.info(
      {
        data: { createableFields }
      },
      `Updating createable fields for ${salesforceObjectName}`
    );
    await dbConfig.set(
      `createable-fields-${salesforceObjectName}`,
      JSON.stringify(createableFields),
      logger
    );

    logger.info(
      { data: { updateableFields } },
      `Updating updatable fields for ${salesforceObjectName}`
    );
    await dbConfig.set(
      `updateable-fields-${salesforceObjectName}`,
      JSON.stringify(updateableFields),
      logger
    );

    logger.info('Completed sync table schema');
  }
};
