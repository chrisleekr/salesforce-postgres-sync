const config = require('config');
const dbConfig = require('../helpers/db-config');
const postgres = require('../helpers/postgres');
const salesforce = require('../helpers/salesforce');

const describeSalesforceObjects = async rawLogger => {
  const logger = rawLogger.child({ function: 'describeSalesforceObjects' });

  const configuredObjects = config.get('salesforce.objects');

  // Get the keys of the salesforceObjects object
  const objectNames = Object.keys(configuredObjects);

  for (const objectName of objectNames) {
    // Execute Salesforce REST API with Bearer token with sessionId to execute describe for `Account` object
    const describeResponse = await salesforce.getSobjectDescribe(
      objectName,
      logger
    );

    await dbConfig.set(
      `${objectName}-describe`,
      JSON.stringify(describeResponse.data),
      logger
    );
  }
};

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

const notApplicableIsDeleted = ['User', 'RecordType', 'UserRole'];
const notApplicableCreatedDate = ['UserRole'];

const upsertTableSchema = async rawLogger => {
  const logger = rawLogger.child({ function: 'upsertTableSchema' });

  const postgresSchema = config.get('salesforce.postgresSchema');

  const configuredCommonFields = config.get('salesforce.commonFields');
  const configuredObjects = config.get('salesforce.objects');

  // Get the keys of the salesforceObjects object
  const objectNames = Object.keys(configuredObjects);

  for (const objectName of objectNames) {
    if (!configuredObjects[objectName].fields) {
      logger.info(
        { objectName },
        `No fields configured for object ${objectName}`
      );
      continue;
    }

    logger.info({ objectName }, 'Processing object name');

    // Remove common fields from salesforce object fields
    const configuredObjectFields = [
      ...configuredCommonFields.reduce((acc, field) => {
        // If it's User, then remove IsDeleted
        if (
          notApplicableIsDeleted.includes(objectName) &&
          field.name === 'IsDeleted'
        ) {
          return acc;
        }

        if (
          notApplicableCreatedDate.includes(objectName) &&
          field.name === 'CreatedDate'
        ) {
          return acc;
        }

        if (field.isSalesforceColumn) {
          acc.push(field.name);
        }

        return acc;
      }, []),
      ...(configuredObjects[objectName].fields.filter(fieldName => {
        const found = configuredCommonFields.find(
          c => c.name.toLowerCase() === fieldName.toLowerCase()
        );

        return !found;
      }) || [])
    ];

    logger.info(
      { configuredObjectFields },
      'Salesforce object fields filtered common fields'
    );

    const objectDescribe = JSON.parse(
      await dbConfig.get(`${objectName}-describe`, logger)
    );

    const tableCommonColumns = configuredCommonFields.reduce((acc, field) => {
      // If isSalesforceColumn is false, then add to tableColumns
      if (!field.isSalesforceColumn) {
        acc.push(field);
      }

      return acc;
    }, []);

    const tableConfiguredColumns = [];
    for (const fieldName of configuredObjectFields) {
      const found = objectDescribe.fields.find(
        field => field.name.toLowerCase() === fieldName.toLowerCase()
      );

      if (found) {
        let fieldObjectName = objectName;

        if (found.calculated) {
          // Field is calculated
          const [fieldObjectNameReferenceTo, fieldObjectRelationshipName] =
            found.calculatedFormula.split('.');
          fieldObjectName = fieldObjectNameReferenceTo.replace('__r', '');

          // Get the object describe
          const fieldObjectDescribe = JSON.parse(
            await dbConfig.get(`${fieldObjectName}-describe`, logger)
          );

          // Find the fieldObjectRelationshipName in the object describe
          const foundReferenceField = fieldObjectDescribe.fields.find(
            field =>
              field.name.toLowerCase() ===
              fieldObjectRelationshipName.toLowerCase()
          );

          if (!foundReferenceField) {
            logger.error(
              {
                data: {
                  fieldObjectName,
                  fieldObjectNameReferenceTo,
                  fieldObjectRelationshipName
                }
              },
              `Field ${fieldObjectRelationshipName} not found in ${fieldObjectName}`
            );
            throw new Error(
              `Field ${fieldObjectRelationshipName} not found in ${fieldObjectName}`
            );
          }

          tableConfiguredColumns.push({
            name: found.name.toLowerCase(),
            type: convertType(foundReferenceField.type),
            createIndex: !found.unique && (found.filterable || found.sortable),
            createUniqueIndex: found.unique,
            isSalesforceColumn: true,
            canCreate: foundReferenceField.createable,
            canUpdate: foundReferenceField.updateable,
            salesforce: {
              objectName: fieldObjectNameReferenceTo,
              relationshipObjectName: fieldObjectNameReferenceTo.replace(
                '__r',
                ''
              ),
              name: found.name,
              label: found.label,
              type: found.type,
              length: found.length,
              custom: found.custom,
              unique: found.unique,
              filterable: found.filterable,
              sortable: found.sortable,
              createable: foundReferenceField.createable,
              updateable: foundReferenceField.updateable,
              calculated: found.calculated,
              calculatedFormula: found.calculatedFormula,
              referenceTo: found.referenceTo,
              relationshipName: found.relationshipName
            }
          });
        } else {
          const isUnique = found.unique || found.name.toLowerCase() === 'id';

          const unprocessableFields = ['Name', 'Account__c'];
          const createable = unprocessableFields.includes(found.name)
            ? false
            : found.createable;
          const updateable = unprocessableFields.includes(found.name)
            ? false
            : found.updateable;

          // Field in the object
          tableConfiguredColumns.push({
            name: found.name.toLowerCase(),
            type: convertType(found.type),
            createIndex: !isUnique && (found.filterable || found.sortable),
            createUniqueIndex: isUnique,
            isSalesforceColumn: true,
            canCreate: createable,
            canUpdate: updateable,
            salesforce: {
              objectName: fieldObjectName,
              relationshipObjectName: fieldObjectName,
              name: found.name,
              label: found.label,
              type: found.type,
              length: found.length,
              custom: found.custom,
              filterable: found.filterable,
              sortable: found.sortable,
              createable: found.createable,
              updateable: found.updateable,
              calculated: found.calculated,
              calculatedFormula: found.calculatedFormula,
              referenceTo: found.referenceTo,
              relationshipName: found.relationshipName
            }
          });
        }
      } else {
        logger.error(
          { data: { fieldName, configuredObjectFields } },
          `Field ${fieldName} not found in ${objectName}`
        );
        throw new Error(`Field ${fieldName} not found in ${objectName}`);
      }
    }

    const tableColumns = [...tableCommonColumns, ...tableConfiguredColumns];

    logger.info({ tableColumns }, `Table columns for ${objectName}`);

    await dbConfig.set(
      `${objectName}-schema`,
      JSON.stringify(tableColumns),
      logger
    );

    await postgres.createOrUpdateTable(
      postgresSchema,
      objectName.toLowerCase(),
      tableColumns,
      logger
    );

    logger.info(
      {
        data: { tableColumns }
      },
      `Updating schema for ${objectName}`
    );
  }
};

module.exports = async rawLogger => {
  const logger = rawLogger.child({ command: 'sync-tables' });
  logger.info('Start sync-tables command');

  await describeSalesforceObjects(logger);

  await upsertTableSchema(logger);

  logger.info('Completed sync-tables command');
};
