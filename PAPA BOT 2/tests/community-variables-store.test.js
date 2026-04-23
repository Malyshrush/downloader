const assert = require('node:assert/strict');

const {
  createCommunityVariablesStore,
  buildCommunityVariablesScope
} = require('../src/modules/community-variables-store');

async function run(name, fn) {
  try {
    await fn();
    process.stdout.write('PASS ' + name + '\n');
  } catch (error) {
    process.stderr.write('FAIL ' + name + '\n');
    throw error;
  }
}

function createConfig(overrides = {}) {
  return {
    mode: 'cloud',
    ymqRegion: 'ru-central1',
    ydbDocApiEndpoint: 'https://docapi.example.test',
    ydbCommunityVariablesTable: 'community_variable_entries',
    awsAccessKeyId: 'key',
    awsSecretAccessKey: 'secret',
    ...overrides
  };
}

(async function main() {
  await run('buildCommunityVariablesScope normalizes profile and community identifiers', async () => {
    assert.equal(buildCommunityVariablesScope('community-7', '8'), '8:community-7');
    assert.equal(buildCommunityVariablesScope('', ''), '1:global');
  });

  await run('community variables store lists global, vk and user catalog entries', async () => {
    const store = createCommunityVariablesStore(
      createConfig(),
      {
        queryItems: async request => {
          assert.equal(request.communityScope, '8:community-7');
          return {
            Items: [
              {
                communityScope: '8:community-7',
                variableKey: 'global:gp_limit',
                entryType: 'global',
                variableName: 'gp_limit',
                value: '500'
              },
              {
                communityScope: '8:community-7',
                variableKey: 'vk:%vk_user%',
                entryType: 'vk',
                variableName: '%vk_user%',
                value: 'Имя пользователя'
              },
              {
                communityScope: '8:community-7',
                variableKey: 'user:pp_score',
                entryType: 'user',
                variableName: 'pp_score',
                value: ''
              }
            ]
          };
        }
      }
    );

    const snapshot = await store.listVariableState('community-7', '8');

    assert.deepEqual(snapshot, {
      globalVars: {
        gp_limit: '500'
      },
      vkVars: {
        '%vk_user%': 'Имя пользователя'
      },
      userVariableNames: ['pp_score']
    });
  });

  await run('community variables store replaces only global entries', async () => {
    const batchCalls = [];
    const store = createCommunityVariablesStore(
      createConfig(),
      {
        queryItems: async request => {
          assert.equal(request.communityScope, '8:community-7');
          return {
            Items: [
              {
                communityScope: '8:community-7',
                variableKey: 'global:gp_old',
                entryType: 'global',
                variableName: 'gp_old',
                value: 'legacy'
              },
              {
                communityScope: '8:community-7',
                variableKey: 'user:pp_score',
                entryType: 'user',
                variableName: 'pp_score',
                value: ''
              },
              {
                communityScope: '8:community-7',
                variableKey: 'vk:%vk_user%',
                entryType: 'vk',
                variableName: '%vk_user%',
                value: 'Имя пользователя'
              }
            ]
          };
        },
        batchWriteItems: async operations => {
          batchCalls.push(operations);
          return { ok: true };
        }
      }
    );

    const result = await store.replaceGlobalVariables('community-7', {
      gp_limit: '500',
      gp_mode: 'auto'
    }, '8');

    assert.deepEqual(result, {
      stored: 2,
      deleted: 1,
      backend: 'ydb-community-variables'
    });
    assert.equal(batchCalls.length, 1);
    assert.deepEqual(batchCalls[0], {
      deleteKeys: [
        {
          communityScope: '8:community-7',
          variableKey: 'global:gp_old'
        }
      ],
      putItems: [
        {
          communityScope: '8:community-7',
          variableKey: 'global:gp_limit',
          entryType: 'global',
          variableName: 'gp_limit',
          value: '500'
        },
        {
          communityScope: '8:community-7',
          variableKey: 'global:gp_mode',
          entryType: 'global',
          variableName: 'gp_mode',
          value: 'auto'
        }
      ]
    });
  });

  await run('community variables store replaces only vk entries', async () => {
    const batchCalls = [];
    const store = createCommunityVariablesStore(
      createConfig(),
      {
        queryItems: async request => {
          assert.equal(request.communityScope, '8:community-7');
          return {
            Items: [
              {
                communityScope: '8:community-7',
                variableKey: 'vk:%vk_old%',
                entryType: 'vk',
                variableName: '%vk_old%',
                value: 'legacy'
              },
              {
                communityScope: '8:community-7',
                variableKey: 'global:gp_limit',
                entryType: 'global',
                variableName: 'gp_limit',
                value: '500'
              },
              {
                communityScope: '8:community-7',
                variableKey: 'user:pp_score',
                entryType: 'user',
                variableName: 'pp_score',
                value: ''
              }
            ]
          };
        },
        batchWriteItems: async operations => {
          batchCalls.push(operations);
          return { ok: true };
        }
      }
    );

    const result = await store.replaceVkVariables('community-7', {
      '%vk_user%': 'Имя пользователя',
      '%vk_group%': 'Название сообщества'
    }, '8');

    assert.deepEqual(result, {
      stored: 2,
      deleted: 1,
      backend: 'ydb-community-variables'
    });
    assert.deepEqual(batchCalls[0], {
      deleteKeys: [
        {
          communityScope: '8:community-7',
          variableKey: 'vk:%vk_old%'
        }
      ],
      putItems: [
        {
          communityScope: '8:community-7',
          variableKey: 'vk:%vk_user%',
          entryType: 'vk',
          variableName: '%vk_user%',
          value: 'Имя пользователя'
        },
        {
          communityScope: '8:community-7',
          variableKey: 'vk:%vk_group%',
          entryType: 'vk',
          variableName: '%vk_group%',
          value: 'Название сообщества'
        }
      ]
    });
  });

  await run('community variables store appends only missing user catalog entries', async () => {
    const putCalls = [];
    const store = createCommunityVariablesStore(
      createConfig(),
      {
        queryItems: async request => {
          assert.equal(request.communityScope, '8:community-7');
          return {
            Items: [
              {
                communityScope: '8:community-7',
                variableKey: 'user:pp_score',
                entryType: 'user',
                variableName: 'pp_score',
                value: ''
              },
              {
                communityScope: '8:community-7',
                variableKey: 'global:pp_level',
                entryType: 'global',
                variableName: 'pp_level',
                value: '100'
              }
            ]
          };
        },
        putItems: async items => {
          putCalls.push(items);
          return { ok: true };
        }
      }
    );

    const result = await store.ensureUserVariableCatalog('community-7', ['pp_score', 'pp_level'], '8');

    assert.deepEqual(result, {
      stored: 1,
      backend: 'ydb-community-variables'
    });
    assert.deepEqual(putCalls, [
      [
        {
          communityScope: '8:community-7',
          variableKey: 'user:pp_level',
          entryType: 'user',
          variableName: 'pp_level',
          value: ''
        }
      ]
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
