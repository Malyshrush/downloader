const assert = require('node:assert/strict');

const {
  buildProfileUserSharedEntriesFromRows,
  buildSharedVariableCatalogMap,
  backfillSharedProfileVariables
} = require('../scripts/backfill-shared-profile-variables');

async function run(name, fn) {
  try {
    await fn();
    process.stdout.write('PASS ' + name + '\n');
  } catch (error) {
    process.stderr.write('FAIL ' + name + '\n');
    throw error;
  }
}

(async function main() {
  await run('buildProfileUserSharedEntriesFromRows groups rows by user', async () => {
    const entries = buildProfileUserSharedEntriesFromRows([
      { ID: '42', 'Переменная ПВС': 'pvs_score', 'Значение ПВС': '100' },
      { ID: '42', 'Переменная ПВС': 'pvs_level', 'Значение ПВС': '7' },
      { ID: '77', 'Переменная ПВС': 'pvs_score', 'Значение ПВС': '200' }
    ]);

    assert.deepEqual(entries, [
      { userId: '42', variables: { pvs_score: '100', pvs_level: '7' } },
      { userId: '77', variables: { pvs_score: '200' } }
    ]);
  });

  await run('buildSharedVariableCatalogMap aggregates unique values per variable', async () => {
    const catalog = buildSharedVariableCatalogMap([
      { 'Переменная ПВС': 'pvs_score', 'Значение ПВС': '100' },
      { 'Переменная ПВС': 'pvs_score', 'Значение ПВС': '200' },
      { 'Переменная ПВС': 'pvs_level', 'Значение ПВС': '7' }
    ]);

    assert.deepEqual(catalog, {
      pvs_score: '100\n200',
      pvs_level: '7'
    });
  });

  await run('backfillSharedProfileVariables fills both structured stores', async () => {
    const profileCalls = [];
    const sharedCalls = [];

    const summary = await backfillSharedProfileVariables({
      profileIds: ['8'],
      getSheetData: async (sheetName, communityId, profileId) => {
        assert.equal(communityId, null);
        assert.equal(profileId, '8');
        if (sheetName === 'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ') {
          return [
            { ID: '42', 'Переменная ПВС': 'pvs_score', 'Значение ПВС': '100' },
            { ID: '42', 'Переменная ПВС': 'pvs_level', 'Значение ПВС': '7' },
            { ID: '77', 'Переменная ПВС': 'pvs_score', 'Значение ПВС': '200' }
          ];
        }
        if (sheetName === 'ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ') {
          return [
            { 'Переменная ПВС': 'legacy_only', 'Значение ПВС': '1' }
          ];
        }
        throw new Error('unexpected sheet ' + sheetName);
      },
      profileUserSharedStore: {
        isEnabled: () => true,
        replaceUserEntries: async (profileScope, entries) => {
          profileCalls.push({ profileScope, entries });
        }
      },
      sharedVariablesStore: {
        isEnabled: () => true,
        replaceVariables: async (profileScope, variables) => {
          sharedCalls.push({ profileScope, variables });
        }
      },
      log: () => {}
    });

    assert.deepEqual(summary, {
      profiles: 1,
      profileUserEntries: 2,
      profileUserVariables: 3,
      sharedCatalogVariables: 3,
      fallbackSharedCatalogVariables: 1
    });
    assert.deepEqual(profileCalls, [
      {
        profileScope: '8',
        entries: [
          { userId: '42', variables: { pvs_score: '100', pvs_level: '7' } },
          { userId: '77', variables: { pvs_score: '200' } }
        ]
      }
    ]);
    assert.deepEqual(sharedCalls, [
      {
        profileScope: '8',
        variables: {
          legacy_only: '1',
          pvs_score: '100\n200',
          pvs_level: '7'
        }
      }
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
