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
  await run('syncStructuredReadModelSheet syncs mailing rows into structured mailing store', async () => {
    const calls = [];
    const rows = [
      { '№': '5', 'Сообщение Рассылки': 'Новость дня', 'Статус': 'Ожидает' }
    ];

    const result = await storage.__testOnly.syncStructuredReadModelSheet(
      'РАССЫЛКА',
      rows,
      'community-1',
      '7',
      {
        mailingDeliveryStore: {
          isEnabled: () => true,
          replaceMailingRows: async (communityId, nextRows, profileId) => {
            calls.push({ communityId, nextRows, profileId });
            return { backend: 'ydb-mailing-delivery', rows: nextRows.length };
          }
        }
      }
    );

    assert.equal(result.synced, true);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].communityId, 'community-1');
    assert.equal(calls[0].profileId, '7');
    assert.equal(calls[0].nextRows[0]['Сообщение Рассылки'], 'Новость дня');
  });

  await run('syncStructuredReadModelSheet syncs delayed rows into structured delayed store', async () => {
    const calls = [];
    const rows = [
      { '№': '9', 'Шаг': 'welcome', 'Статус': 'Ожидает', 'Дата и время отправки': '2026-04-22 12:00:00' }
    ];

    const result = await storage.__testOnly.syncStructuredReadModelSheet(
      'ОТЛОЖЕННЫЕ',
      rows,
      'community-1',
      '7',
      {
        delayedDeliveryStore: {
          isEnabled: () => true,
          replaceDelayedRows: async (communityId, nextRows, profileId) => {
            calls.push({ communityId, nextRows, profileId });
            return { backend: 'ydb-delayed-delivery', rows: nextRows.length };
          }
        }
      }
    );

    assert.equal(result.synced, true);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].communityId, 'community-1');
    assert.equal(calls[0].profileId, '7');
    assert.equal(calls[0].nextRows[0]['Шаг'], 'welcome');
  });
  await run('syncStructuredReadModelSheet syncs users rows into structured user state', async () => {
    const calls = [];
    const rows = [
      { ID: '42', 'Текущий Бот': 'Bot A', 'Текущий Шаг': 'Step 1' },
      { ID: '', 'Текущий Бот': 'Ignored' }
    ];

    const result = await storage.__testOnly.syncStructuredReadModelSheet(
      'ПОЛЬЗОВАТЕЛИ',
      rows,
      'community-1',
      '7',
      {
        userStateStore: {
          isEnabled: () => true,
          replaceUserRows: async (userScope, nextRows) => {
            calls.push({ userScope, nextRows });
            return { backend: 'ydb-user-state', stored: nextRows.length, deleted: 3 };
          }
        }
      }
    );

    assert.equal(result.synced, true);
    assert.equal(result.backend, 'ydb-user-state');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].userScope, '7:community-1');
    assert.deepEqual(calls[0].nextRows, [
      { ID: '42', 'Текущий Бот': 'Bot A', 'Текущий Шаг': 'Step 1' }
    ]);
  });

  await run('syncStructuredReadModelSheet syncs users PVS columns back into structured shared variables', async () => {
    const userRows = [];
    const sharedWrites = [];
    const catalogWrites = [];
    const sharedMemory = new Map([
      ['42', { old: 'stale' }],
      ['77', { keep: 'yes' }]
    ]);
    const rows = [
      { ID: '42', 'Переменная ПВС': '', 'Значение ПВС': '' },
      { ID: '77', 'Переменная ПВС': '777', 'Значение ПВС': '444' }
    ];

    const result = await storage.__testOnly.syncStructuredReadModelSheet(
      'ПОЛЬЗОВАТЕЛИ',
      rows,
      'community-1',
      '7',
      {
        userStateStore: {
          isEnabled: () => true,
          replaceUserRows: async (userScope, nextRows) => {
            userRows.push({ userScope, nextRows });
            return { backend: 'ydb-user-state', stored: nextRows.length, deleted: 0 };
          }
        },
        profileUserSharedStore: {
          isEnabled: () => true,
          putUserVariables: async (profileScope, userId, variables) => {
            sharedWrites.push({ profileScope, userId, variables });
            sharedMemory.set(String(userId), Object.assign({}, variables));
          },
          listUserEntries: async profileScope => {
            assert.equal(profileScope, '7');
            return Array.from(sharedMemory.entries()).map(([userId, variables]) => ({ userId, variables }));
          }
        },
        sharedVariablesStore: {
          isEnabled: () => true,
          replaceVariables: async (profileScope, variables) => {
            catalogWrites.push({ profileScope, variables });
            return { stored: true };
          }
        }
      }
    );

    assert.equal(result.synced, true);
    assert.equal(userRows.length, 1);
    assert.deepEqual(sharedWrites, [
      { profileScope: '7', userId: '42', variables: {} },
      { profileScope: '7', userId: '77', variables: { '777': '444' } }
    ]);
    assert.deepEqual(catalogWrites, [
      { profileScope: '7', variables: { '777': '444' } }
    ]);
  });

  await run('applySheetRuntimeOverlay reads users from structured user state for admin table', async () => {
    const rows = await storage.__testOnly.applySheetRuntimeOverlay(
      'ПОЛЬЗОВАТЕЛИ',
      [{ ID: '42', 'Текущий Бот': 'Legacy Bot' }],
      'community-1',
      '7',
      {
        userStateStore: {
          isEnabled: () => true,
          listUserRows: async userScope => {
            assert.equal(userScope, '7:community-1');
            return [{ ID: '42', 'Текущий Бот': 'Runtime Bot', 'Текущий Шаг': 'Runtime Step' }];
          }
        }
      }
    );

    assert.deepEqual(rows, [
      { ID: '42', 'Текущий Бот': 'Runtime Bot', 'Текущий Шаг': 'Runtime Step' }
    ]);
  });

  await run('applySheetRuntimeOverlay reads profile shared variables from structured store', async () => {
    const rows = await storage.__testOnly.applySheetRuntimeOverlay(
      'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ',
      [
        { ID: '42', 'Переменная ПВС': 'legacy', 'Значение ПВС': 'stale' }
      ],
      null,
      '7',
      {
        profileUserSharedStore: {
          isEnabled: () => true,
          listUserEntries: async profileScope => {
            assert.equal(profileScope, '7');
            return [
              { userId: '77', variables: { '777': '444' } },
              { userId: '42', variables: {} }
            ];
          }
        }
      }
    );

    assert.deepEqual(rows, [
      { ID: '77', 'Переменная ПВС': '777', 'Значение ПВС': '444' }
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
