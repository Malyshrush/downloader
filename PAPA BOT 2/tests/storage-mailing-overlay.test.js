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
  await run('applySheetRuntimeOverlay merges mailing runtime state from YDB store', async () => {
    const sourceRows = [
      {
        '№': '5',
        'Статус': 'Ожидает',
        'Дата и время отправки (по мск.)': '2026-04-22 12:00:00',
        'Сообщение Рассылки': 'Новость дня',
        'Ошибка': ''
      }
    ];

    const result = await storage.__testOnly.applySheetRuntimeOverlay(
      'РАССЫЛКА',
      sourceRows,
      'file-community-1',
      '7',
      {
        mailingDeliveryStore: {
          isEnabled: () => true,
          getMailingState: async (communityId, mailingId, profileId) => {
            assert.equal(communityId, 'file-community-1');
            assert.equal(mailingId, '5');
            assert.equal(profileId, '7');
            return {
              '№': '5',
              'Статус': 'Отправлено (с ошибками)',
              'Ошибка': 'Отправлено: 1, ошибок: 1',
              'Факт. время отправки (по мск.)': '2026-04-22 12:05:00'
            };
          }
        }
      }
    );

    assert.equal(result.length, 1);
    assert.equal(result[0]['Сообщение Рассылки'], 'Новость дня');
    assert.equal(result[0]['Статус'], 'Отправлено (с ошибками)');
    assert.equal(result[0]['Ошибка'], 'Отправлено: 1, ошибок: 1');
    assert.equal(result[0]['Факт. время отправки (по мск.)'], '2026-04-22 12:05:00');
    assert.equal(sourceRows[0]['Статус'], 'Ожидает');
  });

  await run('applySheetRuntimeOverlay leaves non-mailing sheets unchanged', async () => {
    const sourceRows = [{ '№': '1', 'Шаг': 'welcome' }];
    const result = await storage.__testOnly.applySheetRuntimeOverlay(
      'СООБЩЕНИЯ',
      sourceRows,
      'file-community-1',
      '7',
      {
        mailingDeliveryStore: {
          isEnabled: () => true,
          getMailingState: async () => {
            throw new Error('mailing store should not be used');
          }
        }
      }
    );

    assert.deepEqual(result, sourceRows);
  });

  await run('applySheetRuntimeOverlay merges delayed runtime state from YDB store', async () => {
    const sourceRows = [
      {
        '№': '9',
        'Шаг': 'welcome',
        'Статус': 'Ожидает',
        'Дата и время отправки': '2026-04-22 12:00:00',
        'Ошибка': ''
      }
    ];

    const result = await storage.__testOnly.applySheetRuntimeOverlay(
      'ОТЛОЖЕННЫЕ',
      sourceRows,
      'file-community-1',
      '7',
      {
        delayedDeliveryStore: {
          isEnabled: () => true,
          getDelayedRow: async (communityId, delayedId, profileId) => {
            assert.equal(communityId, 'file-community-1');
            assert.equal(delayedId, '9');
            assert.equal(profileId, '7');
            return {
              '№': '9',
              'Статус': 'Отправлено',
              'Ошибка': '',
              'Факт. время отправки (по мск.)': '2026-04-22 12:03:00'
            };
          }
        }
      }
    );

    assert.equal(result.length, 1);
    assert.equal(result[0]['Шаг'], 'welcome');
    assert.equal(result[0]['Статус'], 'Отправлено');
    assert.equal(result[0]['Факт. время отправки (по мск.)'], '2026-04-22 12:03:00');
    assert.equal(sourceRows[0]['Статус'], 'Ожидает');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
