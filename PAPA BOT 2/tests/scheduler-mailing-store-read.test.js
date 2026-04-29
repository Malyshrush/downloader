const assert = require('node:assert/strict');

const scheduler = require('../src/modules/scheduler');

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
  await run('processMailing reads initialized mailing rows from YDB store without sheet fallback', async () => {
    const actions = [];
    const updates = [];
    const mailingRows = [
      {
        '№': '6',
        'Статус': 'Ожидает',
        'Дата и время отправки (по мск.)': '2026-04-22 12:05:00',
        'Сообщение Рассылки': 'Новость дня',
        'ID Получателей': '1001,1002',
        'Ошибка': ''
      }
    ];

    const response = await scheduler.__testOnly.processMailingWithDependencies('community-mailing-store-read', '7', {
      now: new Date('2026-04-22T10:00:00.000Z'),
      getCommunityFileContext: async () => ({
        fileCommunityId: 'file-community-1',
        actualGroupId: '123456'
      }),
      getSheetData: async () => {
        throw new Error('mailing sheet fallback should not be used');
      },
      saveSheetData: async () => {
        throw new Error('mailing sheet save should not be used');
      },
      legacySchedulerAdapter: {
        listMailingRows: async () => {
          throw new Error('legacy mailing adapter should not be used');
        }
      },
      mailingDeliveryStore: {
        isEnabled: () => true,
        listRows: async (communityId, profileId) => {
          assert.equal(communityId, 'file-community-1');
          assert.equal(profileId, '7');
          return { initialized: true, rows: mailingRows };
        },
        getMailingState: async () => null,
        updateMailingState: async (communityId, mailingId, mutator, profileId) => {
          const row = { '№': mailingId, 'Статус': 'Ожидает', 'Ошибка': '' };
          await mutator(row);
          updates.push({ communityId, mailingId, profileId, row });
          return { found: true, changed: true, value: row };
        }
      },
      publishOutboundAction: async action => {
        actions.push(action);
        return { accepted: true, actionId: action.actionId };
      }
    });

    assert.equal(response.queuedCount, 1);
    assert.equal(actions.length, 1);
    assert.equal(actions[0].payload.mailingRowNumber, '6');
    assert.equal(updates.length, 1);
    assert.equal(updates[0].row['Статус'], 'В обработке');
  });

  await run('processMailingDeliveryAction reads initialized mailing row from YDB store without sheet fallback', async () => {
    const updates = [];
    const sendCalls = [];
    const mailingRows = [
      {
        '№': '5',
        'Статус': 'Ожидает',
        'Дата и время отправки (по мск.)': '2026-04-22 12:00:00',
        'Сообщение Рассылки': 'Новость дня',
        'ID Получателей': '1001,1002',
        'Ошибка': ''
      }
    ];

    const result = await scheduler.__testOnly.processMailingDeliveryActionWithDependencies(
      {
        payload: {
          mailingRowNumber: '5',
          fileCommunityId: 'file-community-1',
          actualGroupId: '123456',
          communityId: 'community-1',
          profileId: '7'
        }
      },
      {
        now: new Date('2026-04-22T10:00:00.000Z'),
        getSheetData: async () => {
          throw new Error('mailing sheet fallback should not be used');
        },
        saveSheetData: async () => {
          throw new Error('mailing sheet save should not be used');
        },
        legacySchedulerAdapter: {
          listMailingRows: async () => {
            throw new Error('legacy mailing adapter should not be used');
          }
        },
        collectMailingRecipients: async () => ['1001', '1002'],
        createMailingKeyboard: () => ({ buttons: [] }),
        getAttachmentsFromRow: () => ['photo1_1'],
        sendMessageWithTokenRetry: async userId => {
          sendCalls.push(userId);
          return userId === '1001';
        },
        addAppLog: async () => {},
        mailingDeliveryStore: {
          isEnabled: () => true,
          listRows: async (communityId, profileId) => {
            assert.equal(communityId, 'file-community-1');
            assert.equal(profileId, '7');
            return { initialized: true, rows: mailingRows };
          },
          getMailingState: async (communityId, mailingId, profileId) => {
            assert.equal(communityId, 'file-community-1');
            assert.equal(mailingId, '5');
            assert.equal(profileId, '7');
            return { '№': '5', 'Статус': 'В обработке', 'Ошибка': '' };
          },
          updateMailingState: async (communityId, mailingId, mutator, profileId) => {
            const row = { '№': mailingId, 'Статус': 'В обработке', 'Ошибка': '' };
            await mutator(row);
            updates.push({ communityId, mailingId, profileId, row });
            return { found: true, changed: true, value: row };
          }
        }
      }
    );

    assert.equal(result.ok, true);
    assert.deepEqual(sendCalls, ['1001', '1002']);
    assert.equal(updates.length, 1);
    assert.equal(updates[0].row['Статус'], 'Отправлено (с ошибками)');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
