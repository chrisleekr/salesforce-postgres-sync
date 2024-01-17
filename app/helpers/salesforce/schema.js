const dbConfig = require('../db-config');

const getSalesforceColumns = async (objectName, logger) => {
  const objectSchema = JSON.parse(
    await dbConfig.get(`${objectName}-schema`, logger)
  );
  // Get name of fields if isSalesforceColumn is true
  const salesforceColumns = objectSchema.reduce((acc, field) => {
    if (field.isSalesforceColumn) {
      acc.push(field.name);
    }
    return acc;
  }, []);

  return salesforceColumns;
};

module.exports = { getSalesforceColumns };
