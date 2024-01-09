const logger = require('../../helpers/logger');
const salesforce = require('../../helpers/salesforce');

(async () => {
  // Login to Salesforce
  await salesforce.login(logger);

  await salesforce.create(
    'Preference__c',
    {
      brandunique__c: 'random-test-1455fe1e-f95d-4a8a-9b07-8dafa8f2f91d',
      brand__c: 'random',
      cadence__c: null,
      city__c: 'melbourne',
      country__c: 'AU',
      subscribed__c: 'true',
      vertical__c: 'test',
      Account__r: {
        accountid__c: '1455fe1e-f95d-4a8a-9b07-8dafa8f2f91d'
      },
      tag__c: null
    },
    (record, err) => {
      logger.error(
        { err, data: { record } },
        'Error in Salesforce object create'
      );
    },
    (record, result) => {
      logger.debug(
        { data: { record, result } },
        `Created record in Salesforce`
      );
    },
    logger
  );

  // await salesforce.update(
  //   'Preference__c',
  //   {
  //     Id: 'a4m1s00000099hlAAA',
  //     brandunique__c: 'random-test-1455fe1e-f95d-4a8a-9b07-8dafa8f2f91d',
  //     brand__c: 'random',
  //     cadence__c: null,
  //     city__c: 'melbourne',
  //     country__c: 'AU',
  //     subscribed__c: 'true',
  //     vertical__c: 'test',
  //     Account__r: {
  //       accountid__c: '1455fe1e-f95d-4a8a-9b07-8dafa8f2f91d'
  //     },
  //     tag__c: null
  //   },
  //   (record, err) => {
  //     logger.error(
  //       { err, data: { record } },
  //       'Error in Salesforce object update'
  //     );
  //   },
  //   (record, result) => {
  //     logger.debug(
  //       { data: { record, result } },
  //       `Updated record in Salesforce`
  //     );
  //   },
  //   logger
  // );
})();
