const assert = require('node:assert/strict');

const { createUserStateStore, buildUserScope } = require('../src/modules/user-state-store');

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
    ydbUserStateTable: 'user_state_rows',
    awsAccessKeyId: 'key',
    awsSecretAccessKey: 'secret',
    ...overrides
  };
}

(async function main() {
  await run('buildUserScope normalizes profile and community identifiers', async () => {
    assert.equal(buildUserScope('community-a', '7'), '7:community-a');
    assert.equal(buildUserScope('', '7'), '7:global');
  });

  await run('user state store updates one user row inside one scope', async () => {
    const getCalls = [];
    const putCalls = [];
    const store = createUserStateStore(
      createConfig(),
      {
        getItem: async key => {
          getCalls.push(key);
          return {
            userScope: '7:community-a',
            userId: '42',
            row: {
              ID: '42',
              'РџРѕР»СЊР·РѕРІР°С‚РµР»СЊСЃРєР°СЏ': 'score',
              'Р—РЅР°С‡РµРЅРёСЏ РџРџ': '100'
            }
          };
        },
        putItem: async item => {
          putCalls.push(item);
          return { ok: true };
        }
      }
    );

    const result = await store.updateUserRow('7:community-a', '42', row => {
      row['Р—РЅР°С‡РµРЅРёСЏ РџРџ'] = '200';
      return { value: row };
    });

    assert.equal(result.found, true);
    assert.equal(result.changed, true);
    assert.equal(getCalls.length, 1);
    assert.deepEqual(getCalls[0], {
      userScope: '7:community-a',
      userId: '42'
    });
    assert.equal(putCalls.length, 1);
    assert.equal(putCalls[0].userScope, '7:community-a');
    assert.equal(putCalls[0].userId, '42');
    assert.equal(putCalls[0].row['Р—РЅР°С‡РµРЅРёСЏ РџРџ'], '200');
  });

  await run('user state store lists rows from one scope only', async () => {
    const queryCalls = [];
    const store = createUserStateStore(
      createConfig(),
      {
        queryItems: async request => {
          queryCalls.push(request);
          return {
            Items: [
              {
                userScope: '7:community-a',
                userId: '42',
                row: { ID: '42', 'РРњРЇ': 'Alice' }
              },
              {
                userScope: '7:community-a',
                userId: '77',
                row: { ID: '77', 'РРњРЇ': 'Bob' }
              }
            ]
          };
        }
      }
    );

    const rows = await store.listUserRows('7:community-a');

    assert.equal(queryCalls.length, 1);
    assert.equal(queryCalls[0].userScope, '7:community-a');
    assert.deepEqual(rows, [
      { ID: '42', 'РРњРЇ': 'Alice' },
      { ID: '77', 'РРњРЇ': 'Bob' }
    ]);
  });

  await run('user state store deletes one user row by scope and id', async () => {
    const deleteCalls = [];
    const store = createUserStateStore(
      createConfig(),
      {
        deleteItem: async key => {
          deleteCalls.push(key);
          return { ok: true };
        }
      }
    );

    const result = await store.deleteUserRow('7:community-a', '42');

    assert.deepEqual(result, {
      deleted: true,
      backend: 'ydb-user-state'
    });
    assert.deepEqual(deleteCalls, [
      {
        userScope: '7:community-a',
        userId: '42'
      }
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
