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
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
