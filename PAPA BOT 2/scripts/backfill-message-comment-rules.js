const path = require('path');
const fs = require('fs');
const Module = require('module');

const functionNodeModules = path.join(__dirname, '..', 'yandex-function', 'node_modules');
if (fs.existsSync(functionNodeModules)) {
  process.env.NODE_PATH = process.env.NODE_PATH
    ? `${functionNodeModules}${path.delimiter}${process.env.NODE_PATH}`
    : functionNodeModules;
  Module._initPaths();
}

try {
  require('dotenv').config({ path: path.join(__dirname, '..', 'yandex-function', '.env') });
  require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
} catch (error) {}

const MESSAGE_SHEET = 'СООБЩЕНИЯ';
const COMMENT_SHEET = 'КОММЕНТАРИИ В ПОСТАХ';

async function resolveProfileIds(overrides) {
  if (Array.isArray(overrides.profileIds) && overrides.profileIds.length) {
    return overrides.profileIds.map(id => String(id || '').trim()).filter(Boolean);
  }
  const { getAllProfileIds } = require('../src/modules/admin-profiles');
  return getAllProfileIds();
}

async function resolveCommunityIds(profileId, overrides) {
  const loadBotConfig = overrides.loadBotConfig || require('../src/modules/config').loadBotConfig;
  const fullConfig = await loadBotConfig(profileId);
  return Object.keys(fullConfig && fullConfig.communities || {});
}

async function backfillMessageCommentRules(overrides = {}) {
  const { getSheetData } = require('../src/modules/storage');
  const { createMessageRuleStore } = require('../src/modules/message-rule-store');
  const { createCommentRuleStore } = require('../src/modules/comment-rule-store');
  const profileIds = await resolveProfileIds(overrides);
  const sheetGetter = overrides.getSheetData || getSheetData;
  const messageStore = overrides.messageStore || createMessageRuleStore();
  const commentStore = overrides.commentStore || createCommentRuleStore();
  const log = overrides.log || (message => process.stdout.write(String(message) + '\n'));

  if (!messageStore || typeof messageStore.isEnabled !== 'function' || !messageStore.isEnabled()) {
    throw new Error('message rule store is disabled');
  }
  if (!commentStore || typeof commentStore.isEnabled !== 'function' || !commentStore.isEnabled()) {
    throw new Error('comment rule store is disabled');
  }

  const summary = {
    profiles: profileIds.length,
    communities: 0,
    messageRows: 0,
    commentRows: 0
  };

  for (const profileId of profileIds) {
    const communityIds = await resolveCommunityIds(profileId, overrides);
    for (const communityId of communityIds) {
      const messageRows = await sheetGetter(MESSAGE_SHEET, communityId, profileId);
      const commentRows = await sheetGetter(COMMENT_SHEET, communityId, profileId);
      await messageStore.replaceRuleRows(communityId, messageRows, profileId);
      await commentStore.replaceRuleRows(communityId, commentRows, profileId);
      summary.communities += 1;
      summary.messageRows += Array.isArray(messageRows) ? messageRows.length : 0;
      summary.commentRows += Array.isArray(commentRows) ? commentRows.length : 0;
      log(
        `Backfilled rules profile=${profileId} community=${communityId} messages=${Array.isArray(messageRows) ? messageRows.length : 0} comments=${Array.isArray(commentRows) ? commentRows.length : 0}`
      );
    }
  }

  return summary;
}

if (require.main === module) {
  backfillMessageCommentRules()
    .then(summary => {
      process.stdout.write(JSON.stringify(summary, null, 2) + '\n');
    })
    .catch(error => {
      process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
      process.exit(1);
    });
}

module.exports = {
  backfillMessageCommentRules
};
