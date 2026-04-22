const assert = require('node:assert/strict');

const variables = require('../src/modules/variables');

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
  await run('updateGlobalVariablesWithDependencies rewrites only global rows through updateSheetData', async () => {
    const updates = [];

    await variables.__testOnly.updateGlobalVariablesWithDependencies(
      { gp_limit: '500', gp_mode: 'auto' },
      'community-1',
      '8',
      {
        updateSheetData: async (sheetName, communityId, profileId, updater) => {
          const nextRows = await updater([
            {
              'Пользовательская': 'pp_score',
              'Глобальная': '',
              'Значение ГП': ''
            },
            {
              'Пользовательская': '',
              'Глобальная': 'gp_old',
              'Значение ГП': '123'
            }
          ]);
          updates.push({ sheetName, communityId, profileId, nextRows });
          return { changed: true, value: nextRows };
        }
      }
    );

    assert.equal(updates.length, 1);
    assert.equal(updates[0].sheetName, 'ПЕРЕМЕННЫЕ');
    assert.equal(updates[0].nextRows.length, 3);
    assert.equal(updates[0].nextRows[0]['Пользовательская'], 'pp_score');
    assert.equal(updates[0].nextRows[1]['Глобальная'], 'gp_limit');
    assert.equal(updates[0].nextRows[1]['Значение ГП'], '500');
    assert.equal(updates[0].nextRows[2]['Глобальная'], 'gp_mode');
  });

  await run('syncUserVariableCatalogWithDependencies appends only missing catalog rows through updateSheetData', async () => {
    const updates = [];

    await variables.__testOnly.syncUserVariableCatalogWithDependencies(
      ['pp_score', 'pp_level'],
      'community-1',
      '8',
      {
        updateSheetData: async (sheetName, communityId, profileId, updater) => {
          const nextRows = await updater([
            {
              'Пользовательская': 'pp_score',
              'Глобальная': '',
              'Значение ГП': '',
              'ПЕРЕМЕННЫЕ ВК': '',
              'Значение/Описание ПВК': ''
            }
          ]);
          updates.push({ sheetName, communityId, profileId, nextRows });
          return { changed: true, value: nextRows };
        }
      }
    );

    assert.equal(updates.length, 1);
    assert.equal(updates[0].nextRows.length, 2);
    assert.equal(updates[0].nextRows[1]['Пользовательская'], 'pp_level');
  });
  await run('syncProfileUserSharedVariablesToUsersWithDependencies updates structured user rows per community when enabled', async () => {
    const updates = [];

    await variables.__testOnly.syncProfileUserSharedVariablesToUsersWithDependencies(
      '42',
      { pvs_score: '100', pvs_level: '7' },
      '8',
      {
        updateSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        loadBotConfig: async profileId => {
          assert.equal(profileId, '8');
        },
        getAllCommunityIds: () => ['community-1', 'community-2'],
        userStateStore: {
          isEnabled: () => true,
          updateUserRow: async (userScope, userId, mutator) => {
            const row = {
              ID: userId,
              'Переменная ПВС': '',
              'Значение ПВС': ''
            };
            await mutator(row);
            updates.push({ userScope, userId, row });
            return { found: true, changed: true, value: row };
          }
        }
      }
    );

    assert.deepEqual(updates, [
      {
        userScope: '8:community-1',
        userId: '42',
        row: {
          ID: '42',
          'Переменная ПВС': 'pvs_score\npvs_level',
          'Значение ПВС': '100\n7'
        }
      },
      {
        userScope: '8:community-2',
        userId: '42',
        row: {
          ID: '42',
          'Переменная ПВС': 'pvs_score\npvs_level',
          'Значение ПВС': '100\n7'
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
