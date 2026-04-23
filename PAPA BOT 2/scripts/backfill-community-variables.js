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

const VARIABLES_SHEET = 'ПЕРЕМЕННЫЕ';
const COLUMN_USER_VARIABLE = 'Пользовательская';
const COLUMN_GLOBAL_VARIABLE = 'Глобальная';
const COLUMN_GLOBAL_VALUE = 'Значение ГП';
const COLUMN_VK_VARIABLE = 'ПЕРЕМЕННЫЕ ВК';
const COLUMN_VK_VARIABLE_LEGACY = 'Переменные ВК';
const COLUMN_VK_VALUE = 'Значение/Описание ПВК';

function normalizeName(value) {
  return String(value || '').trim().toLowerCase();
}

function buildCommunityVariableStateFromRows(rows) {
  const globalVars = {};
  const vkVars = {};
  const userVariableNames = [];
  const seenUserVariableNames = new Set();

  for (const row of Array.isArray(rows) ? rows : []) {
    const globalName = normalizeName(row && row[COLUMN_GLOBAL_VARIABLE]);
    if (globalName) {
      globalVars[globalName] = String(row && row[COLUMN_GLOBAL_VALUE] || '').trim();
    }

    const vkName = normalizeName(row && (row[COLUMN_VK_VARIABLE] || row[COLUMN_VK_VARIABLE_LEGACY]));
    if (vkName) {
      vkVars[vkName] = String(row && row[COLUMN_VK_VALUE] || '').trim();
    }

    const userName = normalizeName(row && row[COLUMN_USER_VARIABLE]);
    if (userName && !seenUserVariableNames.has(userName)) {
      seenUserVariableNames.add(userName);
      userVariableNames.push(userName);
    }
  }

  return {
    globalVars,
    vkVars,
    userVariableNames
  };
}

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

async function backfillCommunityVariables(overrides = {}) {
  const { getSheetData } = require('../src/modules/storage');
  const { createCommunityVariablesStore } = require('../src/modules/community-variables-store');
  const profileIds = await resolveProfileIds(overrides);
  const sheetGetter = overrides.getSheetData || getSheetData;
  const store = overrides.store || createCommunityVariablesStore();
  const log = overrides.log || (message => process.stdout.write(String(message) + '\n'));
  const includeGlobalScope = overrides.includeGlobalScope !== false;

  if (!store || typeof store.isEnabled !== 'function' || !store.isEnabled()) {
    throw new Error('community variables store is disabled');
  }

  const summary = {
    profiles: profileIds.length,
    scopes: 0,
    globalVariables: 0,
    vkVariables: 0,
    userVariableNames: 0
  };

  for (const profileId of profileIds) {
    const configuredCommunityIds = await resolveCommunityIds(profileId, overrides);
    const communityIds = includeGlobalScope
      ? [null].concat(configuredCommunityIds)
      : configuredCommunityIds;

    for (const communityId of communityIds) {
      const rows = await sheetGetter(VARIABLES_SHEET, communityId, profileId);
      const state = buildCommunityVariableStateFromRows(rows);

      await store.replaceGlobalVariables(communityId, state.globalVars, profileId);
      await store.replaceVkVariables(communityId, state.vkVars, profileId);
      await store.ensureUserVariableCatalog(communityId, state.userVariableNames, profileId);

      summary.scopes += 1;
      summary.globalVariables += Object.keys(state.globalVars).length;
      summary.vkVariables += Object.keys(state.vkVars).length;
      summary.userVariableNames += state.userVariableNames.length;
      log(`Backfilled variables profile=${profileId} community=${communityId || 'global'} gp=${Object.keys(state.globalVars).length} vk=${Object.keys(state.vkVars).length} pp=${state.userVariableNames.length}`);
    }
  }

  return summary;
}

if (require.main === module) {
  backfillCommunityVariables()
    .then(summary => {
      process.stdout.write(JSON.stringify(summary, null, 2) + '\n');
    })
    .catch(error => {
      process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
      process.exit(1);
    });
}

module.exports = {
  buildCommunityVariableStateFromRows,
  backfillCommunityVariables
};
