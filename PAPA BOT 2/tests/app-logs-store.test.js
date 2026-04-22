const assert = require('node:assert/strict');

const { createAppLogsStore, buildAppLogsScope } = require('../src/modules/app-logs-store');

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
    ydbAppLogsTable: 'app_logs_entries',
    awsAccessKeyId: 'key',
    awsSecretAccessKey: 'secret',
    ...overrides
  };
}

(async function main() {
  await run('buildAppLogsScope normalizes profile and community identifiers', async () => {
    assert.equal(buildAppLogsScope('community-a', '7'), '7:community-a');
    assert.equal(buildAppLogsScope('', '7'), '7:global');
  });

  await run('app log store writes one item per log entry', async () => {
    const calls = [];
    const store = createAppLogsStore(
      createConfig(),
      {
        putItem: async item => {
          calls.push(item);
          return { ok: true };
        }
      }
    );

    await store.addLog('7:community-a', {
      id: 'log_1',
      createdAt: '2026-04-22T15:00:00.000Z',
      tab: 'USERS'
    });

    assert.equal(calls.length, 1);
    assert.equal(calls[0].logScope, '7:community-a');
    assert.equal(calls[0].row.id, 'log_1');
    assert.equal(calls[0].row.tab, 'USERS');
    assert.match(calls[0].logId, /^2026-04-22T15:00:00\.000Z#/);
  });

  await run('app log store lists rows from newest to oldest with requested limit', async () => {
    const calls = [];
    const store = createAppLogsStore(
      createConfig(),
      {
        queryItems: async request => {
          calls.push(request);
          return {
            Items: [
              {
                logScope: '7:community-a',
                logId: '2026-04-22T16:00:00.000Z#b',
                row: { id: 'log_2', title: 'Second' }
              },
              {
                logScope: '7:community-a',
                logId: '2026-04-22T15:00:00.000Z#a',
                row: { id: 'log_1', title: 'First' }
              }
            ]
          };
        }
      }
    );

    const rows = await store.listLogs('7:community-a', 2);

    assert.deepEqual(rows, [
      { id: 'log_2', title: 'Second' },
      { id: 'log_1', title: 'First' }
    ]);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].limit, 2);
    assert.equal(calls[0].logScope, '7:community-a');
  });

  await run('app log store clears all rows for one scope', async () => {
    const queryCalls = [];
    const deleteCalls = [];
    const store = createAppLogsStore(
      createConfig(),
      {
        queryItems: async request => {
          queryCalls.push(request);
          if (queryCalls.length === 1) {
            return {
              Items: [
                { logScope: '7:community-a', logId: '2026-04-22T16:00:00.000Z#b' },
                { logScope: '7:community-a', logId: '2026-04-22T15:00:00.000Z#a' }
              ]
            };
          }
          return { Items: [] };
        },
        batchDeleteItems: async items => {
          deleteCalls.push(items);
          return { ok: true };
        }
      }
    );

    const result = await store.clearLogs('7:community-a');

    assert.deepEqual(result, { deletedCount: 2, backend: 'ydb-app-logs' });
    assert.equal(queryCalls.length, 1);
    assert.deepEqual(deleteCalls, [
      [
        { logScope: '7:community-a', logId: '2026-04-22T16:00:00.000Z#b' },
        { logScope: '7:community-a', logId: '2026-04-22T15:00:00.000Z#a' }
      ]
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
