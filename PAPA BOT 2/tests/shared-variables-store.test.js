const assert = require('node:assert/strict');

const {
  createSharedVariablesStore,
  buildSharedVariablesScope
} = require('../src/modules/shared-variables-store');

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
    ydbSharedVariablesTable: 'shared_variables_catalog',
    awsAccessKeyId: 'key',
    awsSecretAccessKey: 'secret',
    ...overrides
  };
}

(async function main() {
  await run('buildSharedVariablesScope normalizes profile identifier', async () => {
    assert.equal(buildSharedVariablesScope('7'), '7');
    assert.equal(buildSharedVariablesScope(''), '1');
  });

  await run('shared variables store lists variables for one profile', async () => {
    const store = createSharedVariablesStore(
      createConfig(),
      {
        queryItems: async request => {
          assert.equal(request.profileScope, '7');
          return {
            Items: [
              {
                profileScope: '7',
                variableName: 'pvs_score',
                value: '100'
              },
              {
                profileScope: '7',
                variableName: 'pvs_level',
                value: '7'
              }
            ]
          };
        }
      }
    );

    const variables = await store.listVariables('7');

    assert.deepEqual(variables, {
      pvs_score: '100',
      pvs_level: '7'
    });
  });

  await run('shared variables store replaces the profile catalog', async () => {
    const queryCalls = [];
    const batchCalls = [];
    const store = createSharedVariablesStore(
      createConfig(),
      {
        queryItems: async request => {
          queryCalls.push(request);
          return {
            Items: [
              {
                profileScope: '7',
                variableName: 'old_var',
                value: 'legacy'
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

    const result = await store.replaceVariables('7', {
      pvs_score: '100',
      pvs_level: '7'
    });

    assert.deepEqual(result, {
      stored: 2,
      deleted: 1,
      backend: 'ydb-shared-variables'
    });
    assert.equal(queryCalls.length, 1);
    assert.equal(batchCalls.length, 1);
    assert.deepEqual(batchCalls[0], {
      deleteKeys: [
        {
          profileScope: '7',
          variableName: 'old_var'
        }
      ],
      putItems: [
        {
          profileScope: '7',
          variableName: 'pvs_score',
          value: '100'
        },
        {
          profileScope: '7',
          variableName: 'pvs_level',
          value: '7'
        }
      ]
    });
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
