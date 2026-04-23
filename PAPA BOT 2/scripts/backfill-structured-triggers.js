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

const TRIGGERS_SHEET = 'ТРИГГЕРЫ';

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

async function backfillStructuredTriggers(overrides = {}) {
  const { getSheetData } = require('../src/modules/storage');
  const { createStructuredTriggerStore } = require('../src/modules/structured-trigger-store');
  const profileIds = await resolveProfileIds(overrides);
  const sheetGetter = overrides.getSheetData || getSheetData;
  const store = overrides.store || createStructuredTriggerStore();
  const log = overrides.log || (message => process.stdout.write(String(message) + '\n'));

  if (!store || typeof store.isEnabled !== 'function' || !store.isEnabled()) {
    throw new Error('structured trigger store is disabled');
  }

  const summary = {
    profiles: profileIds.length,
    communities: 0,
    rows: 0
  };

  for (const profileId of profileIds) {
    const communityIds = await resolveCommunityIds(profileId, overrides);
    for (const communityId of communityIds) {
      const rows = await sheetGetter(TRIGGERS_SHEET, communityId, profileId);
      await store.replaceTriggerRows(communityId, rows, profileId);
      summary.communities += 1;
      summary.rows += Array.isArray(rows) ? rows.length : 0;
      log(`Backfilled triggers profile=${profileId} community=${communityId} rows=${Array.isArray(rows) ? rows.length : 0}`);
    }
  }

  return summary;
}

if (require.main === module) {
  backfillStructuredTriggers()
    .then(summary => {
      process.stdout.write(JSON.stringify(summary, null, 2) + '\n');
    })
    .catch(error => {
      process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
      process.exit(1);
    });
}

module.exports = {
  backfillStructuredTriggers
};
