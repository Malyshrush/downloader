const { buildEventRuntimeConfig } = require('./event-runtime-config');
const {
  META_RULE_ID,
  buildStructuredRuleScope,
  createStructuredRuleSheetStore,
  isStructuredRuleSheetStoreEnabled
} = require('./structured-rule-sheet-store');

function isMessageRuleStoreEnabled(config = buildEventRuntimeConfig(process.env)) {
  return isStructuredRuleSheetStoreEnabled(config, config && config.ydbMessageRulesTable);
}

function createMessageRuleStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  return createStructuredRuleSheetStore(
    config,
    {
      tableName: config.ydbMessageRulesTable,
      backendName: 'ydb-message-rules'
    },
    overrides
  );
}

module.exports = {
  META_RULE_ID,
  buildMessageRuleScope: buildStructuredRuleScope,
  createMessageRuleStore,
  isMessageRuleStoreEnabled
};
