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

const MAILING_SHEET = 'РАССЫЛКА';
const DELAYED_SHEET = 'ОТЛОЖЕННЫЕ';

async function resolveProfileIds(overrides) {
  if (Array.isArray(overrides.profileIds) && overrides.profileIds.length) {
    return overrides.profileIds.map(id => String(id || '').trim()).filter(Boolean);
  }
  const { getAllProfileIds } = require('../src/modules/admin-profiles');
  return getAllProfileIds();
}

async function resolveCommunityIds(profileId, overrides) {
  if (overrides.communityIdsByProfile && Array.isArray(overrides.communityIdsByProfile[profileId])) {
    return overrides.communityIdsByProfile[profileId].map(id => String(id || '').trim()).filter(Boolean);
  }
  const { loadBotConfig, getAllCommunityIds } = require('../src/modules/config');
  await loadBotConfig(profileId);
  return getAllCommunityIds(profileId);
}

async function backfillMailingDelayedRuntime(overrides = {}) {
  const { getSheetData } = require('../src/modules/storage');
  const { createMailingDeliveryStore } = require('../src/modules/mailing-delivery-store');
  const { createDelayedDeliveryStore } = require('../src/modules/delayed-delivery-store');

  const profileIds = await resolveProfileIds(overrides);
  const sheetGetter = overrides.getSheetData || getSheetData;
  const mailingStore = overrides.mailingDeliveryStore || createMailingDeliveryStore();
  const delayedStore = overrides.delayedDeliveryStore || createDelayedDeliveryStore();
  const log = overrides.log || (message => process.stdout.write(String(message) + '\n'));

  if (!mailingStore || typeof mailingStore.isEnabled !== 'function' || !mailingStore.isEnabled()) {
    throw new Error('mailing delivery store is disabled');
  }
  if (!delayedStore || typeof delayedStore.isEnabled !== 'function' || !delayedStore.isEnabled()) {
    throw new Error('delayed delivery store is disabled');
  }

  const summary = {
    profiles: profileIds.length,
    scopes: 0,
    mailingRows: 0,
    delayedRows: 0
  };

  for (const profileId of profileIds) {
    const communityIds = await resolveCommunityIds(profileId, overrides);
    for (const communityId of communityIds) {
      const mailingRows = await sheetGetter(MAILING_SHEET, communityId, profileId);
      const delayedRows = await sheetGetter(DELAYED_SHEET, communityId, profileId);

      await mailingStore.replaceMailingRows(communityId, mailingRows, profileId);
      await delayedStore.replaceDelayedRows(communityId, delayedRows, profileId);

      summary.scopes += 1;
      summary.mailingRows += Array.isArray(mailingRows) ? mailingRows.length : 0;
      summary.delayedRows += Array.isArray(delayedRows) ? delayedRows.length : 0;

      log(
        `Backfilled runtime rows profile=${profileId} community=${communityId} mailing=${Array.isArray(mailingRows) ? mailingRows.length : 0} delayed=${Array.isArray(delayedRows) ? delayedRows.length : 0}`
      );
    }
  }

  return summary;
}

if (require.main === module) {
  backfillMailingDelayedRuntime()
    .then(summary => {
      process.stdout.write(JSON.stringify(summary, null, 2) + '\n');
    })
    .catch(error => {
      process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
      process.exit(1);
    });
}

module.exports = {
  backfillMailingDelayedRuntime
};
