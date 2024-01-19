const dbConfig = require('../db-config');

const getSalesforceColumns = async (objectName, logger) => {
  const objectSchema = JSON.parse(
    await dbConfig.get(`${objectName}-schema`, logger)
  );
  // Get name of fields if isSalesforceColumn is true
  return objectSchema.reduce((acc, field) => {
    if (field.isSalesforceColumn) {
      acc.push(field.name);
    }
    return acc;
  }, []);
};

const getDatabaseColumns = async (objectName, logger) => {
  const objectSchema = JSON.parse(
    await dbConfig.get(`${objectName}-schema`, logger)
  );
  // Get name of fields if isSalesforceColumn is true
  return objectSchema.reduce((acc, field) => {
    acc.push(field.name);
    return acc;
  }, []);
};

const getCreateableFields = async (objectName, logger) => {
  const objectSchema = JSON.parse(
    await dbConfig.get(`${objectName}-schema`, logger)
  );
  return objectSchema.reduce((acc, field) => {
    if (field.canCreate) {
      acc.push(field);
    }
    return acc;
  }, []);
};

const getUpdateableFields = async (objectName, logger) => {
  const objectSchema = JSON.parse(
    await dbConfig.get(`${objectName}-schema`, logger)
  );

  return objectSchema.reduce((acc, field) => {
    if (field.canUpdate) {
      acc.push(field);
    }
    return acc;
  }, []);
};

module.exports = {
  getSalesforceColumns,
  getDatabaseColumns,
  getCreateableFields,
  getUpdateableFields
};
