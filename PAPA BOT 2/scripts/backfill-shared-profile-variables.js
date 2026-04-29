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

const PROFILE_USER_SHARED_SHEET = 'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ';
const SHARED_VARIABLES_SHEET = 'ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ';

function normalizeVariableName(value) {
  return String(value || '').trim().toLowerCase();
}

function buildProfileUserSharedEntriesFromRows(rows) {
  const byUserId = new Map();

  for (const row of Array.isArray(rows) ? rows : []) {
    const userId = String(row && row.ID || '').trim();
    const variableName = normalizeVariableName(row && row['Переменная ПВС']);
    if (!userId || !variableName) continue;
    if (!byUserId.has(userId)) {
      byUserId.set(userId, {});
    }
    byUserId.get(userId)[variableName] = String(row && row['Значение ПВС'] || '').trim();
  }

  return Array.from(byUserId.entries()).map(([userId, variables]) => ({
    userId,
    variables
  }));
}

function buildSharedVariableCatalogMap(rows) {
  const byName = new Map();

  for (const row of Array.isArray(rows) ? rows : []) {
    const variableName = String(row && row['Переменная ПВС'] || '').trim();
    const value = String(row && row['Значение ПВС'] || '').trim();
    if (!variableName) continue;
    const key = variableName.toLowerCase();
    if (!byName.has(key)) {
      byName.set(key, { name: key, values: new Set() });
    }
    if (value) {
      byName.get(key).values.add(value);
    }
  }

  const variables = {};
  for (const item of byName.values()) {
    variables[item.name] = Array.from(item.values).join('\n');
  }
  return variables;
}

async function resolveProfileIds(overrides) {
  if (Array.isArray(overrides.profileIds) && overrides.profileIds.length) {
    return overrides.profileIds.map(id => String(id || '').trim()).filter(Boolean);
  }
  const { getAllProfileIds } = require('../src/modules/admin-profiles');
  return getAllProfileIds();
}

async function backfillSharedProfileVariables(overrides = {}) {
  const { getSheetData } = require('../src/modules/storage');
  const { createProfileUserSharedStore } = require('../src/modules/profile-user-shared-store');
  const { createSharedVariablesStore } = require('../src/modules/shared-variables-store');
  const profileIds = await resolveProfileIds(overrides);
  const sheetGetter = overrides.getSheetData || getSheetData;
  const profileStore = overrides.profileUserSharedStore || createProfileUserSharedStore();
  const sharedStore = overrides.sharedVariablesStore || createSharedVariablesStore();
  const log = overrides.log || (message => process.stdout.write(String(message) + '\n'));

  if (!profileStore || typeof profileStore.isEnabled !== 'function' || !profileStore.isEnabled()) {
    throw new Error('profile user shared store is disabled');
  }
  if (!sharedStore || typeof sharedStore.isEnabled !== 'function' || !sharedStore.isEnabled()) {
    throw new Error('shared variables store is disabled');
  }

  const summary = {
    profiles: profileIds.length,
    profileUserEntries: 0,
    profileUserVariables: 0,
    sharedCatalogVariables: 0,
    fallbackSharedCatalogVariables: 0
  };

  for (const profileId of profileIds) {
    const profileRows = await sheetGetter(PROFILE_USER_SHARED_SHEET, null, profileId);
    const sharedRows = await sheetGetter(SHARED_VARIABLES_SHEET, null, profileId);
    const entries = buildProfileUserSharedEntriesFromRows(profileRows);
    const catalogFromProfileRows = buildSharedVariableCatalogMap(profileRows);
    const fallbackCatalog = buildSharedVariableCatalogMap(sharedRows);
    const mergedCatalog = Object.keys(catalogFromProfileRows).length
      ? { ...fallbackCatalog, ...catalogFromProfileRows }
      : fallbackCatalog;

    await profileStore.replaceUserEntries(profileId, entries);
    await sharedStore.replaceVariables(profileId, mergedCatalog);

    const variableCount = entries.reduce((sum, entry) => sum + Object.keys(entry.variables || {}).length, 0);
    const sharedCount = Object.keys(mergedCatalog).length;
    const fallbackOnlyCount = Object.keys(fallbackCatalog).filter(name => !Object.prototype.hasOwnProperty.call(catalogFromProfileRows, name)).length;

    summary.profileUserEntries += entries.length;
    summary.profileUserVariables += variableCount;
    summary.sharedCatalogVariables += sharedCount;
    summary.fallbackSharedCatalogVariables += fallbackOnlyCount;

    log(
      `Backfilled shared/profile variables profile=${profileId} users=${entries.length} userVars=${variableCount} shared=${sharedCount} fallbackShared=${fallbackOnlyCount}`
    );
  }

  return summary;
}

if (require.main === module) {
  backfillSharedProfileVariables()
    .then(summary => {
      process.stdout.write(JSON.stringify(summary, null, 2) + '\n');
    })
    .catch(error => {
      process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
      process.exit(1);
    });
}

module.exports = {
  buildProfileUserSharedEntriesFromRows,
  buildSharedVariableCatalogMap,
  backfillSharedProfileVariables
};
