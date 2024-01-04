const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

/**
 * Prepend provided columns into CSV file
 *  columns will be prepend on the header.
 *  defaultValues will have matching values of columns. Prepend on each row.
 *
 * @param {*} orgCSVPath
 * @param {*} convertedCSVPath
 * @param {*} columns
 * @param {*} defaultValues
 * @param {*} rawLogger
 */
const prependColumns = (
  orgCSVPath,
  convertedCSVPath,
  newColumns,
  defaultValues,
  rawLogger
) => {
  const logger = rawLogger.child({ helper: 'csv', func: 'prependColumns' });

  logger.info(
    {
      data: { orgCSVPath, convertedCSVPath, newColumns, defaultValues }
    },
    'Start prepending newColumns'
  );
  const data = [];
  let headers = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(orgCSVPath)
      .pipe(csv())
      .on('headers', originalHeaders => {
        // make lowercase
        headers = [...newColumns, ...originalHeaders].map(header =>
          header.toLowerCase()
        );
      })
      .on('data', row => {
        const newRow = {};
        newColumns.forEach((column, index) => {
          newRow[column] = defaultValues[index];
        });
        // Convert keys to lowercase
        const lowerCaseRow = Object.fromEntries(
          Object.entries(row).map(([key, value]) => [key.toLowerCase(), value])
        );

        Object.assign(newRow, lowerCaseRow);
        data.push(newRow);
      })
      .on('end', () => {
        const csvWriter = createCsvWriter({
          path: convertedCSVPath,
          header: headers.map(header => ({ id: header, title: header }))
        });
        csvWriter
          .writeRecords(data)
          .then(() => {
            logger.info('End prepending columns');
            resolve();
          })
          .catch(err => reject(err));
      });
  });
};

module.exports = {
  prependColumns
};
