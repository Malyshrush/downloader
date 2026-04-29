const assert = require('node:assert/strict');

const {
  createDelayedDeliveryStore,
  buildDelayedDeliveryScope
} = require('../src/modules/delayed-delivery-store');

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
    ydbDelayedDeliveriesTable: 'delayed_delivery_entries',
    awsAccessKeyId: 'key',
    awsSecretAccessKey: 'secret',
    ...overrides
  };
}

(async function main() {
  await run('buildDelayedDeliveryScope normalizes profile and community identifiers', async () => {
    assert.equal(buildDelayedDeliveryScope('community-7', '8'), '8:community-7');
    assert.equal(buildDelayedDeliveryScope('', ''), '1:global');
  });

  await run('delayed delivery store appends a pending row with generated id', async () => {
    const putCalls = [];
    const store = createDelayedDeliveryStore(
      createConfig(),
      {
        now: () => new Date('2026-04-23T10:00:00.000Z'),
        createId: () => 'delayed-1',
        putItem: async item => {
          putCalls.push(item);
          return { ok: true };
        }
      }
    );

    const result = await store.appendDelayedRow('community-7', {
      'Шаг': 'welcome',
      'ID Пользователя': '42',
      'Дата и время отправки': '2026-04-23 13:10:00',
      'Статус': 'Ожидает'
    }, '8');

    assert.equal(result.row['№'], 'delayed-1');
    assert.equal(result.row._delayedId, 'delayed-1');
    assert.equal(putCalls.length, 2);
    assert.equal(putCalls[0].delayedScope, '8:community-7');
    assert.equal(putCalls[0].delayedId, 'delayed-1');
    assert.equal(putCalls[0].status, 'Ожидает');
    assert.equal(putCalls[0].scheduledAtMs, new Date('2026-04-23T13:10:00+03:00').getTime());
  });

  await run('delayed delivery store lists due pending rows only', async () => {
    const store = createDelayedDeliveryStore(
      createConfig(),
      {
        queryItems: async request => {
          assert.equal(request.delayedScope, '8:community-7');
          return {
            Items: [
              {
                delayedScope: '8:community-7',
                delayedId: 'due-1',
                status: 'Ожидает',
                scheduledAtMs: new Date('2026-04-23T10:00:00.000Z').getTime(),
                row: { '№': 'due-1', 'Статус': 'Ожидает' }
              },
              {
                delayedScope: '8:community-7',
                delayedId: 'future-1',
                status: 'Ожидает',
                scheduledAtMs: new Date('2026-04-23T10:10:00.000Z').getTime(),
                row: { '№': 'future-1', 'Статус': 'Ожидает' }
              },
              {
                delayedScope: '8:community-7',
                delayedId: 'sent-1',
                status: 'Отправлено',
                scheduledAtMs: new Date('2026-04-23T09:00:00.000Z').getTime(),
                row: { '№': 'sent-1', 'Статус': 'Отправлено' }
              }
            ]
          };
        }
      }
    );

    const rows = await store.listDueRows('community-7', new Date('2026-04-23T10:05:00.000Z'), '8');

    assert.equal(rows.length, 1);
    assert.equal(rows[0]._delayedId, 'due-1');
  });

  await run('delayed delivery store updates a row status with mutator', async () => {
    const putCalls = [];
    const store = createDelayedDeliveryStore(
      createConfig(),
      {
        getItem: async key => ({
          delayedScope: key.delayedScope,
          delayedId: key.delayedId,
          status: 'Ожидает',
          scheduledAtMs: 1000,
          row: {
            '№': key.delayedId,
            'Статус': 'Ожидает'
          }
        }),
        putItem: async item => {
          putCalls.push(item);
          return { ok: true };
        }
      }
    );

    const result = await store.updateDelayedRow('community-7', 'delayed-1', row => {
      row['Статус'] = 'В обработке';
      return { value: row };
    }, '8');

    assert.equal(result.found, true);
    assert.equal(result.value['Статус'], 'В обработке');
    assert.equal(putCalls[0].status, 'В обработке');
  });
  await run('delayed delivery store replaces and lists rows with initialized marker', async () => {
    const items = new Map();
    const store = createDelayedDeliveryStore(
      createConfig(),
      {
        putItem: async item => {
          items.set(item.delayedScope + '|' + item.delayedId, JSON.parse(JSON.stringify(item)));
        },
        getItem: async key => {
          const item = items.get(key.delayedScope + '|' + key.delayedId);
          return item ? JSON.parse(JSON.stringify(item)) : null;
        },
        deleteItem: async key => {
          items.delete(key.delayedScope + '|' + key.delayedId);
        },
        queryItems: async request => ({
          Items: Array.from(items.values())
            .filter(item => item.delayedScope === request.delayedScope)
            .map(item => JSON.parse(JSON.stringify(item)))
        })
      }
    );

    await store.replaceDelayedRows('community-7', [
      {
        '№': '10',
        'Шаг': 'welcome',
        'Статус': 'Ожидает',
        'Дата и время отправки': '2026-04-23 13:00:00'
      }
    ], '8');

    const listed = await store.listRows('community-7', '8');
    assert.equal(listed.initialized, true);
    assert.equal(listed.rows.length, 1);
    assert.equal(listed.rows[0]['Шаг'], 'welcome');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
