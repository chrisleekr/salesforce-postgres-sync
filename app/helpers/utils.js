const sleep = async seconds =>
  // eslint-disable-next-line no-promise-executor-return
  new Promise(resolve => setTimeout(resolve, seconds * 1000));

module.exports = {
  sleep
};
