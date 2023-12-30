const config = require('config');
const salesforce = require('../helpers/salesforce');
const postgres = require('../helpers/postgres');

// Function to convert Salesforce field types to PostgreSQL column types
const convertType = sfType => {
  switch (sfType) {
    case 'id':
    case 'reference':
      return 'varchar(18)';
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
    default:
      return sfType;
  }
};

module.exports = async rawLogger => {
  const logger = rawLogger.child({ library: 'sync-table-schema' });

  const postgresSchema = config.get('salesforce.postgresSchema');

  const salesforceObjects = config.get('salesforce.objects');
  const salesforceCommonFields = config.get('salesforce.commonFields');

  // Get the keys of the salesforceObjects object
  const salesforceObjectNames = Object.keys(salesforceObjects);

  // Loop salesforceObjectNames sequentially
  for (const salesforceObjectName of salesforceObjectNames) {
    const salesforceObjectFields =
      salesforceObjects[salesforceObjectName].fields;

    const salesforceObject = await salesforce.describe(
      salesforceObjectName,
      logger
    );

    const objectFields = [];
    // Loop all salesforceObject fields to get objectFields
    salesforceObject.fields.forEach(field => {
      const objectField = {
        label: field.label,
        name: field.name,
        type: field.type,
        createIndex:
          field.unique || field.idLookup || field.filterable || field.sortable
      };

      // If the field.name is in the salesforceObjectFields array, then push to objectFields.
      if (
        salesforceObjectFields
          .map(objField => objField.toLowerCase())
          .includes(field.name.toLowerCase())
      ) {
        objectFields.push(objectField);
      }
    });

    // Construct the table schema
    const tableSchema = [...salesforceCommonFields, ...objectFields].map(
      schema => ({
        ...schema,
        name: schema.name.toLowerCase(),
        type: convertType(schema.type)
      })
    );

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
  }
};