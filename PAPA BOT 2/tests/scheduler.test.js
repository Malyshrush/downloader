const assert = require('node:assert/strict');

const scheduler = require('../src/modules/scheduler');
const { processOutboundAction } = require('../src/modules/outbound-actions');

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
  await run('processDelayed queues due delayed steps instead of sending inline', async () => {
    const delayedRows = [
      {
        '№': '1',
        'Шаг': 'welcome',
        'ID Пользователя': '777',
        'Тип': 'message',
        'Дата и время отправки': '2026-04-22 12:00:00',
        'Статус': 'Ожидает',
        'Ошибка': ''
      }
    ];
    const messageRows = [
      {
        'Шаг': 'welcome',
        'Ответ': 'Привет',
        'Кнопка Ответа': '',
        'Цвет/Ссылка Ответа': ''
      }
    ];
    const saves = [];
    const actions = [];

    const response = await scheduler.__testOnly.processDelayedWithDependencies('community-1', '7', {
      now: new Date('2026-04-22T10:00:00.000Z'),
      getCommunityFileContext: async () => ({
        fileCommunityId: 'file-community-1',
        actualGroupId: '123456'
      }),
      getSheetData: async sheetName => {
        if (sheetName === 'ОТЛОЖЕННЫЕ') return delayedRows;
        if (sheetName === 'СООБЩЕНИЯ') return messageRows;
        if (sheetName === 'КОММЕНТАРИИ В ПОСТАХ') return [];
        throw new Error('unexpected sheet ' + sheetName);
      },
      saveSheetData: async (sheetName, rows) => {
        saves.push({ sheetName, rows: JSON.parse(JSON.stringify(rows)) });
      },
      invalidateCache: () => {},
      publishOutboundAction: async action => {
        actions.push(action);
        return { accepted: true, actionId: action.actionId };
      }
    });

    assert.equal(response.queuedCount, 1);
    assert.equal(actions.length, 1);
    assert.equal(actions[0].actionType, 'send_delayed_delivery');
    assert.equal(actions[0].payload.delayedRowNumber, '1');
    assert.equal(actions[0].payload.userId, '777');
    assert.equal(delayedRows[0]['Статус'], 'В обработке');
    assert.equal(saves.length, 1);
    assert.equal(saves[0].sheetName, 'ОТЛОЖЕННЫЕ');
  });

  await run('processDelayed uses structured delayed store when enabled', async () => {
    const actions = [];
    const updates = [];

    const response = await scheduler.__testOnly.processDelayedWithDependencies('community-structured', '7', {
      now: new Date('2026-04-22T10:00:00.000Z'),
      getCommunityFileContext: async () => ({
        fileCommunityId: 'file-community-1',
        actualGroupId: '123456'
      }),
      getSheetData: async sheetName => {
        if (sheetName === 'РЎРћРћР‘Р©Р•РќРРЇ') {
          return [{ 'Шаг': 'welcome', 'Ответ': 'Привет' }];
        }
        if (sheetName === 'РљРћРњРњР•РќРўРђР РР Р’ РџРћРЎРўРђРҐ') return [];
        throw new Error('delayed sheet fallback should not be used');
      },
      saveSheetData: async () => {
        throw new Error('delayed sheet save should not be used');
      },
      invalidateCache: () => {},
      delayedDeliveryStore: {
        isEnabled: () => true,
        listDueRows: async (communityId, now, profileId) => {
          assert.equal(communityId, 'file-community-1');
          assert.equal(profileId, '7');
          return [
            {
              _delayedId: 'delayed-1',
              '№': 'delayed-1',
              'Шаг': 'welcome',
              'ID Пользователя': '777',
              'Тип': 'message',
              'Дата и время отправки': '2026-04-22 12:00:00',
              'Статус': 'Ожидает',
              'Ошибка': ''
            }
          ];
        },
        updateDelayedRow: async (communityId, delayedId, mutator, profileId) => {
          const row = {
            _delayedId: delayedId,
            'в„–': delayedId,
            'РЎС‚Р°С‚СѓСЃ': 'РћР¶РёРґР°РµС‚',
            'РћС€РёР±РєР°': ''
          };
          await mutator(row);
          updates.push({ communityId, delayedId, profileId, row });
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
    assert.equal(actions[0].payload.delayedRowNumber, 'delayed-1');
    assert.equal(updates.length, 1);
    assert.equal(updates[0].row['Статус'], 'В обработке');
  });

  await run('processMailing queues one outbound action per due mailing row', async () => {
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
    const saves = [];
    const actions = [];

    const response = await scheduler.__testOnly.processMailingWithDependencies('community-1', '7', {
      now: new Date('2026-04-22T10:00:00.000Z'),
      getCommunityFileContext: async () => ({
        fileCommunityId: 'file-community-1',
        actualGroupId: '123456'
      }),
      getSheetData: async sheetName => {
        if (/АССЫЛ|РђРЎРЎР«Р›/.test(sheetName)) return mailingRows;
        throw new Error('unexpected sheet ' + sheetName);
      },
      saveSheetData: async (sheetName, rows) => {
        saves.push({ sheetName, rows: JSON.parse(JSON.stringify(rows)) });
      },
      invalidateCache: () => {},
      publishOutboundAction: async action => {
        actions.push(action);
        return { accepted: true, actionId: action.actionId };
      }
    });

    assert.equal(response.queuedCount, 1);
    assert.equal(actions.length, 1);
    assert.equal(actions[0].actionType, 'send_mailing_delivery');
    assert.equal(actions[0].payload.mailingRowNumber, '5');
    assert.equal(mailingRows[0]['Статус'], 'В обработке');
    assert.equal(saves.length, 1);
    assert.equal(saves[0].sheetName, 'РАССЫЛКА');
  });

  await run('processMailing uses structured mailing state store when enabled', async () => {
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

    const response = await scheduler.__testOnly.processMailingWithDependencies('community-mailing-structured', '7', {
      now: new Date('2026-04-22T10:00:00.000Z'),
      getCommunityFileContext: async () => ({
        fileCommunityId: 'file-community-1',
        actualGroupId: '123456'
      }),
      getSheetData: async sheetName => {
        if (sheetName === 'РАССЫЛКА') return mailingRows;
        throw new Error('unexpected sheet ' + sheetName);
      },
      saveSheetData: async () => {
        throw new Error('mailing sheet save should not be used');
      },
      invalidateCache: () => {},
      mailingDeliveryStore: {
        isEnabled: () => true,
        getMailingState: async () => null,
        updateMailingState: async (communityId, mailingId, mutator, profileId) => {
          const row = { '№': mailingId, 'Статус': 'Ожидает', 'Ошибка': '' };
          await mutator(row);
          updates.push({ communityId, mailingId, profileId, row });
          return { found: false, changed: true, value: row };
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

  await run('processOutboundAction routes scheduler delivery actions to scheduler senders', async () => {
    const calls = [];

    await processOutboundAction(
      {
        actionId: 'act_sched_1',
        actionType: 'send_delayed_delivery',
        payload: {
          delayedRowNumber: '1'
        }
      },
      {
        claimIncomingEvent: async () => ({ acquired: true }),
        markProcessedEvent: async () => {},
        releaseIncomingEventClaim: async () => {},
        processDelayedDeliveryAction: async action => calls.push('delayed:' + action.payload.delayedRowNumber),
        processMailingDeliveryAction: async action => calls.push('mailing:' + action.payload.mailingRowNumber)
      }
    );

    await processOutboundAction(
      {
        actionId: 'act_sched_2',
        actionType: 'send_mailing_delivery',
        payload: {
          mailingRowNumber: '5'
        }
      },
      {
        claimIncomingEvent: async () => ({ acquired: true }),
        markProcessedEvent: async () => {},
        releaseIncomingEventClaim: async () => {},
        processDelayedDeliveryAction: async action => calls.push('delayed:' + action.payload.delayedRowNumber),
        processMailingDeliveryAction: async action => calls.push('mailing:' + action.payload.mailingRowNumber)
      }
    );

    assert.deepEqual(calls, ['delayed:1', 'mailing:5']);
  });

  await run('processDelayedDeliveryAction sends queued delayed step and marks the source row sent', async () => {
    const delayedRows = [
      {
        '№': '1',
        'Шаг': 'welcome',
        'ID Пользователя': '777',
        'Тип': 'message',
        'Дата и время отправки': '2026-04-22 12:00:00',
        'Статус': 'В обработке',
        'Ошибка': ''
      }
    ];
    const messageRows = [
      {
        'Шаг': 'welcome',
        'Ответ': 'Привет',
        'Кнопка Ответа': '',
        'Цвет/Ссылка Ответа': ''
      }
    ];
    const saves = [];
    const rowActionCalls = [];

    const result = await scheduler.__testOnly.processDelayedDeliveryActionWithDependencies(
      {
        payload: {
          delayedRowNumber: '1',
          fileCommunityId: 'file-community-1',
          actualGroupId: '123456',
          communityId: 'community-1',
          profileId: '7'
        }
      },
      {
        now: new Date('2026-04-22T10:00:00.000Z'),
        getSheetData: async sheetName => {
          if (sheetName === 'ОТЛОЖЕННЫЕ') return delayedRows;
          if (sheetName === 'СООБЩЕНИЯ') return messageRows;
          if (sheetName === 'КОММЕНТАРИИ В ПОСТАХ') return [];
          throw new Error('unexpected sheet ' + sheetName);
        },
        saveSheetData: async (sheetName, rows) => {
          saves.push({ sheetName, rows: JSON.parse(JSON.stringify(rows)) });
        },
        invalidateCache: () => {},
        replaceVariables: async text => text,
        getAttachmentsFromRow: () => [],
        createKeyboard: () => ({ buttons: [] }),
        sendMessageWithTokenRetry: async () => true,
        performRowActions: async (...args) => rowActionCalls.push(args),
        addAppLog: async () => {}
      }
    );

    assert.equal(result.ok, true);
    assert.equal(delayedRows[0]['Статус'], 'Отправлено');
    assert.equal(rowActionCalls.length, 1);
    assert.equal(saves.length, 1);
    assert.equal(saves[0].sheetName, 'ОТЛОЖЕННЫЕ');
  });

  await run('processDelayedDeliveryAction uses structured delayed store when enabled', async () => {
    const updates = [];
    const rowActionCalls = [];
    const sendCalls = [];

    const storeRow = {
      _delayedId: 'delayed-1',
      '№': 'delayed-1',
      'Шаг': 'welcome',
      'ID Пользователя': '777',
      'Тип': 'message',
      'Дата и время отправки': '2026-04-22 12:00:00',
      'Статус': 'В обработке',
      'Ошибка': ''
    };

    const result = await scheduler.__testOnly.processDelayedDeliveryActionWithDependencies(
      {
        payload: {
          delayedRowNumber: 'delayed-1',
          fileCommunityId: 'file-community-1',
          actualGroupId: '123456',
          communityId: 'community-1',
          profileId: '7'
        }
      },
      {
        now: new Date('2026-04-22T10:00:00.000Z'),
        getSheetData: async sheetName => {
          if (sheetName === 'СООБЩЕНИЯ') {
            return [
              {
                'Шаг': 'welcome',
                'Ответ': 'Привет',
                'Кнопка Ответа': '',
                'Цвет/Ссылка Ответа': ''
              }
            ];
          }
          if (sheetName === 'КОММЕНТАРИИ В ПОСТАХ') return [];
          throw new Error('delayed sheet fallback should not be used');
        },
        saveSheetData: async () => {
          throw new Error('delayed sheet save should not be used');
        },
        invalidateCache: () => {},
        replaceVariables: async text => text,
        getAttachmentsFromRow: () => [],
        createKeyboard: () => ({ buttons: [] }),
        sendMessageWithTokenRetry: async (...args) => {
          sendCalls.push(args);
          return true;
        },
        performRowActions: async (...args) => rowActionCalls.push(args),
        addAppLog: async () => {},
        delayedDeliveryStore: {
          isEnabled: () => true,
          getDelayedRow: async (communityId, delayedId, profileId) => {
            assert.equal(communityId, 'file-community-1');
            assert.equal(delayedId, 'delayed-1');
            assert.equal(profileId, '7');
            return JSON.parse(JSON.stringify(storeRow));
          },
          updateDelayedRow: async (communityId, delayedId, mutator, profileId) => {
            const row = JSON.parse(JSON.stringify(storeRow));
            await mutator(row);
            updates.push({ communityId, delayedId, profileId, row });
            return { found: true, changed: true, value: row };
          }
        }
      }
    );

    assert.equal(result.ok, true);
    assert.equal(sendCalls.length, 1);
    assert.equal(rowActionCalls.length, 1);
    assert.equal(updates.length, 1);
    assert.equal(updates[0].row['Статус'], 'Отправлено');
  });

  await run('processMailingDeliveryAction sends queued mailing and stores aggregated result', async () => {
    const mailingRows = [
      {
        '№': '5',
        'Статус': 'В обработке',
        'Дата и время отправки (по мск.)': '2026-04-22 12:00:00',
        'Сообщение Рассылки': 'Новость дня',
        'ID Получателей': '1001,1002',
        'Ошибка': ''
      }
    ];
    const saves = [];
    const sendCalls = [];

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
        getSheetData: async sheetName => {
          if (/АССЫЛ|РђРЎРЎР«Р›/.test(sheetName)) return mailingRows;
          throw new Error('unexpected sheet ' + sheetName);
        },
        saveSheetData: async (sheetName, rows) => {
          saves.push({ sheetName, rows: JSON.parse(JSON.stringify(rows)) });
        },
        invalidateCache: () => {},
        collectMailingRecipients: async () => ['1001', '1002'],
        createMailingKeyboard: () => ({ buttons: [] }),
        getAttachmentsFromRow: () => ['photo1_1'],
        sendMessageWithTokenRetry: async userId => {
          sendCalls.push(userId);
          return userId === '1001';
        },
        addAppLog: async () => {}
      }
    );

    assert.equal(result.ok, true);
    assert.deepEqual(sendCalls, ['1001', '1002']);
    assert.equal(mailingRows[0]['Статус'], 'Отправлено (с ошибками)');
    assert.match(mailingRows[0]['Ошибка'], /Отправлено: 1, ошибок: 1/);
    assert.equal(saves.length, 1);
    assert.equal(saves[0].sheetName, 'РАССЫЛКА');
  });

  await run('processMailingDeliveryAction uses structured mailing state store when enabled', async () => {
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
        getSheetData: async sheetName => {
          if (sheetName === 'РАССЫЛКА') return mailingRows;
          throw new Error('unexpected sheet ' + sheetName);
        },
        saveSheetData: async () => {
          throw new Error('mailing sheet save should not be used');
        },
        invalidateCache: () => {},
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
    assert.match(updates[0].row['Ошибка'], /Отправлено: 1, ошибок: 1/);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
