const assert = require('node:assert/strict');

const structuredTriggers = require('../src/modules/structured-triggers');

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
  await run('loadStructuredTriggerRows uses structured store when initialized', async () => {
    const rows = await structuredTriggers.__testOnly.loadStructuredTriggerRows(
      'community-1',
      '7',
      {
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        structuredTriggerStore: {
          isEnabled: () => true,
          listTriggerRows: async (communityId, profileId) => {
            assert.equal(communityId, 'community-1');
            assert.equal(profileId, '7');
            return {
              initialized: true,
              rows: [{ 'Название': 'Stored trigger' }]
            };
          }
        }
      }
    );

    assert.deepEqual(rows, [{ 'Название': 'Stored trigger' }]);
  });

  await run('loadStructuredTriggerRows falls back to sheet when store is uninitialized', async () => {
    const rows = await structuredTriggers.__testOnly.loadStructuredTriggerRows(
      'community-1',
      '7',
      {
        getSheetData: async (sheetName, communityId, profileId) => {
          assert.equal(sheetName, 'ТРИГГЕРЫ');
          assert.equal(communityId, 'community-1');
          assert.equal(profileId, '7');
          return [{ 'Название': 'Sheet trigger' }];
        },
        structuredTriggerStore: {
          isEnabled: () => true,
          listTriggerRows: async () => ({
            initialized: false,
            rows: []
          })
        }
      }
    );

    assert.deepEqual(rows, [{ 'Название': 'Sheet trigger' }]);
  });

  await run('processStructuredTriggers uses structured trigger rows in runtime path', async () => {
    const calls = [];
    const result = await structuredTriggers.__testOnly.processStructuredTriggersWithDependencies(
      {
        type: 'message_new',
        group_id: 229445618,
        object: {
          message: {
            from_id: 42,
            text: 'hello'
          }
        }
      },
      '7',
      {
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        structuredTriggerStore: {
          isEnabled: () => true,
          listTriggerRows: async () => ({
            initialized: true,
            rows: [
              {
                'Название': 'Stored trigger',
                'Код события': 'incoming_message'
              }
            ]
          })
        },
        addAppLog: async payload => {
          calls.push(['log', payload.summary]);
        },
        recordStructuredTriggerExecution: async (profileId, communityId) => {
          calls.push(['record', profileId, communityId]);
        }
      }
    );

    assert.deepEqual(result, { matched: true, handled: true });
    assert.deepEqual(calls, [
      ['log', 'Stored trigger'],
      ['record', '7', '229445618']
    ]);
  });

  await run('processStructuredTriggers infers add_group action from legacy group column', async () => {
    const calls = [];
    const result = await structuredTriggers.__testOnly.processStructuredTriggersWithDependencies(
      {
        type: 'message_new',
        group_id: 229445618,
        object: {
          message: {
            from_id: 42,
            text: '1212'
          }
        }
      },
      '7',
      {
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        structuredTriggerStore: {
          isEnabled: () => true,
          listTriggerRows: async () => ({
            initialized: true,
            rows: [
              {
                'Название': 'Legacy add group trigger',
                'Код события': 'incoming_message',
                'Код условия': 'text_equals',
                'Значение': '1212',
                'ДОБАВИТЬ ГРУППУ': '121212'
              }
            ]
          })
        },
        updateUserGroups: async (userId, addGroupsStr, removeGroupsStr, communityId, profileId) => {
          calls.push(['group', userId, addGroupsStr, removeGroupsStr, communityId, profileId]);
        },
        addAppLog: async payload => {
          calls.push(['log', payload.summary]);
        },
        recordStructuredTriggerExecution: async (profileId, communityId) => {
          calls.push(['record', profileId, communityId]);
        }
      }
    );

    assert.deepEqual(result, { matched: true, handled: true });
    assert.deepEqual(calls, [
      ['log', 'Legacy add group trigger'],
      ['group', 42, '121212', '', '229445618', '7'],
      ['record', '7', '229445618']
    ]);
  });

  await run('processStructuredTriggers executes full step flow for add_to_bot using bot plus step lookup', async () => {
    const calls = [];
    const result = await structuredTriggers.__testOnly.processStructuredTriggersWithDependencies(
      {
        type: 'message_new',
        group_id: 229445618,
        object: {
          message: {
            from_id: 42,
            text: 'go'
          }
        }
      },
      '7',
      {
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        structuredTriggerStore: {
          isEnabled: () => true,
          listTriggerRows: async () => ({
            initialized: true,
            rows: [
              {
                'Название': 'Trigger bot handoff',
                'Код события': 'incoming_message',
                'Код условия': 'text_equals',
                'Значение': 'go',
                'Код действия': 'add_to_bot',
                'Бот': 'bot-target',
                'Шаг': 'step-target'
              }
            ]
          })
        },
        loadMessageRows: async () => ([
          {
            'Бот': 'bot-target',
            'Шаг': 'step-target',
            'Ответ': 'Step answer'
          }
        ]),
        loadCommentRows: async () => ([]),
        sendMessageAndPerformActions: async (userId, row, originalMessage, isComment, communityId, profileId) => {
          calls.push([
            'send-message-step',
            userId,
            row['Бот'],
            row['Шаг'],
            originalMessage.group_id,
            isComment,
            communityId,
            profileId
          ]);
        },
        updateUserBotAndStep: async () => {
          calls.push(['unexpected-updateUserBotAndStep']);
        },
        addAppLog: async payload => {
          calls.push(['log', payload.summary]);
        },
        recordStructuredTriggerExecution: async (profileId, communityId) => {
          calls.push(['record', profileId, communityId]);
        }
      }
    );

    assert.deepEqual(result, { matched: true, handled: true });
    assert.deepEqual(calls, [
      ['log', 'Trigger bot handoff'],
      ['send-message-step', 42, 'bot-target', 'step-target', 229445618, false, '229445618', '7'],
      ['record', '7', '229445618']
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
