const { buildEventRuntimeConfig } = require('./event-runtime-config');
const {
  META_RULE_ID,
  buildStructuredRuleScope,
  createStructuredRuleSheetStore,
  isStructuredRuleSheetStoreEnabled
} = require('./structured-rule-sheet-store');

function isCommentRuleStoreEnabled(config = buildEventRuntimeConfig(process.env)) {
  return isStructuredRuleSheetStoreEnabled(config, config && config.ydbCommentRulesTable);
}

function createCommentRuleStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  return createStructuredRuleSheetStore(
    config,
    {
      tableName: config.ydbCommentRulesTable,
      backendName: 'ydb-comment-rules'
    },
    overrides
  );
}

module.exports = {
  META_RULE_ID,
  buildCommentRuleScope: buildStructuredRuleScope,
  createCommentRuleStore,
  isCommentRuleStoreEnabled
};
