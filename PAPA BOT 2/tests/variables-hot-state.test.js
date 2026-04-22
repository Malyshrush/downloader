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
  await run('getGlobalVariablesWithDependencies reads structured community variables when enabled', async () => {
    const result = await variables.__testOnly.getGlobalVariablesWithDependencies(
      'community-1',
      '8',
      {
        communityVariablesStore: {
          isEnabled: () => true,
          listVariableState: async (communityId, profileId) => {
            assert.equal(communityId, 'community-1');
            assert.equal(profileId, '8');
            return {
              globalVars: {
                gp_limit: '500'
              },
              vkVars: {
                '%vk_user%': 'Имя пользователя'
              },
              userVariableNames: ['pp_score']
            };
          }
        }
      }
    );

    assert.deepEqual(result, {
      globalVars: {
        gp_limit: '500'
      },
      vkVars: {
        '%vk_user%': 'Имя пользователя'
      }
    });
  });

  await run('updateGlobalVariablesWithDependencies writes structured global entries when enabled', async () => {
    const calls = [];

    await variables.__testOnly.updateGlobalVariablesWithDependencies(
      { gp_limit: '500', gp_mode: 'auto' },
      'community-1',
      '8',
      {
        updateSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        communityVariablesStore: {
          isEnabled: () => true,
          replaceGlobalVariables: async (communityId, vars, profileId) => {
            calls.push({ communityId, vars, profileId });
            return { stored: 2, deleted: 1, backend: 'ydb-community-variables' };
          }
        }
      }
    );

    assert.deepEqual(calls, [
      {
        communityId: 'community-1',
        vars: {
          gp_limit: '500',
          gp_mode: 'auto'
        },
        profileId: '8'
      }
    ]);
  });

  await run('syncUserVariableCatalogWithDependencies stores missing structured user catalog entries when enabled', async () => {
    const calls = [];

    await variables.__testOnly.syncUserVariableCatalogWithDependencies(
      ['pp_score', 'pp_level'],
      'community-1',
      '8',
      {
        updateSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        communityVariablesStore: {
          isEnabled: () => true,
          ensureUserVariableCatalog: async (communityId, variableNames, profileId) => {
            calls.push({ communityId, variableNames, profileId });
            return { stored: 1, backend: 'ydb-community-variables' };
          }
        }
      }
    );

    assert.deepEqual(calls, [
      {
        communityId: 'community-1',
        variableNames: ['pp_score', 'pp_level'],
        profileId: '8'
      }
    ]);
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

  await run('getProfileUserSharedVariablesWithDependencies reads structured profile user variables when enabled', async () => {
    const sharedVariables = await variables.__testOnly.getProfileUserSharedVariablesWithDependencies(
      '42',
      '8',
      {
        profileUserSharedStore: {
          isEnabled: () => true,
          getUserVariables: async (profileScope, userId) => {
            assert.equal(profileScope, '8');
            assert.equal(userId, '42');
            return {
              pvs_score: '100',
              pvs_level: '7'
            };
          }
        }
      }
    );

    assert.deepEqual(sharedVariables, {
      pvs_score: '100',
      pvs_level: '7'
    });
  });

  await run('syncProfileSharedVariableCatalogWithDependencies rebuilds structured catalog from structured user entries', async () => {
    const calls = [];

    await variables.__testOnly.syncProfileSharedVariableCatalogWithDependencies(
      '8',
      null,
      {
        updateSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        profileUserSharedStore: {
          isEnabled: () => true,
          listUserEntries: async profileScope => {
            assert.equal(profileScope, '8');
            return [
              {
                userId: '42',
                variables: {
                  pvs_score: '100',
                  pvs_level: '7'
                }
              },
              {
                userId: '77',
                variables: {
                  pvs_score: '200'
                }
              }
            ];
          }
        },
        sharedVariablesStore: {
          isEnabled: () => true,
          replaceVariables: async (profileScope, variables) => {
            calls.push({ profileScope, variables });
            return { stored: 2, deleted: 0, backend: 'ydb-shared-variables' };
          }
        }
      }
    );

    assert.deepEqual(calls, [
      {
        profileScope: '8',
        variables: {
          pvs_score: '100\n200',
          pvs_level: '7'
        }
      }
    ]);
  });

  await run('updateProfileUserSharedVariablesWithDependencies writes structured user vars and syncs dependents when enabled', async () => {
    const storeCalls = [];
    const syncCatalogCalls = [];
    const syncUsersCalls = [];

    await variables.__testOnly.updateProfileUserSharedVariablesWithDependencies(
      '42',
      {
        pvs_score: '100',
        pvs_level: '7'
      },
      '8',
      {
        updateSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        profileUserSharedStore: {
          isEnabled: () => true,
          putUserVariables: async (profileScope, userId, vars) => {
            storeCalls.push({ profileScope, userId, vars });
            return { stored: true, backend: 'ydb-profile-user-shared' };
          }
        },
        syncProfileSharedVariableCatalog: async (profileId, rows, overrides) => {
          syncCatalogCalls.push({ profileId, rows, hasOverrides: Boolean(overrides) });
        },
        syncProfileUserSharedVariablesToUsers: async (userId, vars, profileId, overrides) => {
          syncUsersCalls.push({ userId, vars, profileId, hasOverrides: Boolean(overrides) });
        }
      }
    );

    assert.deepEqual(storeCalls, [
      {
        profileScope: '8',
        userId: '42',
        vars: {
          pvs_score: '100',
          pvs_level: '7'
        }
      }
    ]);
    assert.deepEqual(syncCatalogCalls, [
      {
        profileId: '8',
        rows: null,
        hasOverrides: true
      }
    ]);
    assert.deepEqual(syncUsersCalls, [
      {
        userId: '42',
        vars: {
          pvs_score: '100',
          pvs_level: '7'
        },
        profileId: '8',
        hasOverrides: true
      }
    ]);
  });

  await run('getSharedVariablesWithDependencies reads structured shared catalog when enabled', async () => {
    const sharedVariables = await variables.__testOnly.getSharedVariablesWithDependencies(
      '8',
      {
        sharedVariablesStore: {
          isEnabled: () => true,
          listVariables: async profileScope => {
            assert.equal(profileScope, '8');
            return {
              pvs_score: '100\n200',
              pvs_level: '7'
            };
          }
        }
      }
    );

    assert.deepEqual(sharedVariables, {
      pvs_score: '100\n200',
      pvs_level: '7'
    });
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
