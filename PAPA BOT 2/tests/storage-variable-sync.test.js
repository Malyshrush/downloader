const assert = require('node:assert/strict');

const storage = require('../src/modules/storage');

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
  await run('syncCommunityVariablesSheet writes structured community variable state from variables sheet', async () => {
    const calls = [];

    const result = await storage.__testOnly.syncCommunityVariablesSheet(
      'ПЕРЕМЕННЫЕ',
      [
        { 'Глобальная': 'gp_limit', 'Значение ГП': '500' },
        { 'ПЕРЕМЕННЫЕ ВК': 'vk_user', 'Значение/Описание ПВК': 'Имя' },
        { 'Пользовательская': 'pp_score' }
      ],
      'community-1',
      '8',
      {
        communityVariablesStore: {
          isEnabled: () => true,
          replaceGlobalVariables: async (communityId, vars, profileId) => calls.push({ method: 'gp', communityId, vars, profileId }),
          replaceVkVariables: async (communityId, vars, profileId) => calls.push({ method: 'vk', communityId, vars, profileId }),
          ensureUserVariableCatalog: async (communityId, names, profileId) => calls.push({ method: 'pp', communityId, names, profileId })
        }
      }
    );

    assert.equal(result.synced, true);
    assert.deepEqual(calls, [
      { method: 'gp', communityId: 'community-1', vars: { gp_limit: '500' }, profileId: '8' },
      { method: 'vk', communityId: 'community-1', vars: { vk_user: 'Имя' }, profileId: '8' },
      { method: 'pp', communityId: 'community-1', names: ['pp_score'], profileId: '8' }
    ]);
  });

  await run('syncProfileUserSharedSheet rewrites profile and shared stores from profile sheet', async () => {
    const profileCalls = [];
    const sharedCalls = [];

    const result = await storage.__testOnly.syncProfileUserSharedSheet(
      'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ',
      [
        { ID: '42', 'Переменная ПВС': 'pvs_score', 'Значение ПВС': '100' },
        { ID: '77', 'Переменная ПВС': 'pvs_score', 'Значение ПВС': '200' }
      ],
      null,
      '8',
      {
        profileUserSharedStore: {
          isEnabled: () => true,
          replaceUserEntries: async (profileScope, entries) => profileCalls.push({ profileScope, entries })
        },
        sharedVariablesStore: {
          isEnabled: () => true,
          replaceVariables: async (profileScope, vars) => sharedCalls.push({ profileScope, vars })
        }
      }
    );

    assert.equal(result.synced, true);
    assert.deepEqual(profileCalls, [
      {
        profileScope: '8',
        entries: [
          { userId: '42', variables: { pvs_score: '100' } },
          { userId: '77', variables: { pvs_score: '200' } }
        ]
      }
    ]);
    assert.deepEqual(sharedCalls, [
      {
        profileScope: '8',
        vars: {
          pvs_score: '100\n200'
        }
      }
    ]);
  });

  await run('syncSharedVariablesSheet rewrites shared catalog from shared sheet', async () => {
    const calls = [];

    const result = await storage.__testOnly.syncSharedVariablesSheet(
      'ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ',
      [
        { 'Переменная ПВС': 'pvs_score', 'Значение ПВС': '100\n200' }
      ],
      null,
      '8',
      {
        sharedVariablesStore: {
          isEnabled: () => true,
          replaceVariables: async (profileScope, vars) => {
            calls.push({ profileScope, vars });
            return { stored: 1, backend: 'ydb-shared-variables' };
          }
        }
      }
    );

    assert.equal(result.synced, true);
    assert.deepEqual(calls, [
      {
        profileScope: '8',
        vars: {
          pvs_score: '100\n200'
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
