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

  await run('processStructuredTriggers sends only selected bot step answer without step actions', async () => {
    const calls = [];
    const result = await structuredTriggers.__testOnly.processStructuredTriggersWithDependencies(
      {
        type: 'message_new',
        group_id: 229445618,
        object: {
          message: {
            from_id: 42,
            text: 'answer-only'
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
                ['\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435']: 'Trigger answer only',
                ['\u041a\u043e\u0434 \u0441\u043e\u0431\u044b\u0442\u0438\u044f']: 'incoming_message',
                ['\u041a\u043e\u0434 \u0443\u0441\u043b\u043e\u0432\u0438\u044f']: 'text_equals',
                ['\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435']: 'answer-only',
                ['\u041a\u043e\u0434 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044f']: 'send_bot_answer',
                ['\u0411\u043e\u0442']: 'bot-target',
                ['\u0428\u0430\u0433']: 'step-target'
              }
            ]
          })
        },
        loadMessageRows: async () => ([
          {
            ['\u0411\u043e\u0442']: 'bot-target',
            ['\u0428\u0430\u0433']: 'step-target',
            ['\u041e\u0442\u0432\u0435\u0442']: 'Only answer',
            ['\u0414\u041e\u0411\u0410\u0412\u0418\u0422\u042c \u0413\u0420\u0423\u041f\u041f\u0423']: 'must-not-add',
            ['\u041e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u043d\u0430 \u0428\u0430\u0433']: 'must-not-schedule',
            ['\u0414\u0435\u0439\u0441\u0442\u0432\u0438\u044f \u0441 \u041f\u041f']: 'score=1'
          }
        ]),
        loadCommentRows: async () => ([]),
        sendMessageAndPerformActions: async (userId, row, originalMessage, isComment, communityId, profileId) => {
          calls.push([
            'send-message-answer-only',
            userId,
            row['\u041e\u0442\u0432\u0435\u0442'],
            row['\u0411\u043e\u0442'],
            row['\u0428\u0430\u0433'],
            row['\u0414\u041e\u0411\u0410\u0412\u0418\u0422\u042c \u0413\u0420\u0423\u041f\u041f\u0423'],
            row['\u041e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u043d\u0430 \u0428\u0430\u0433'],
            row['\u0414\u0435\u0439\u0441\u0442\u0432\u0438\u044f \u0441 \u041f\u041f'],
            originalMessage.group_id,
            isComment,
            communityId,
            profileId
          ]);
        },
        updateUserGroups: async () => {
          calls.push(['unexpected-updateUserGroups']);
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
      ['log', 'Trigger answer only'],
      ['send-message-answer-only', 42, 'Only answer', '', '', '', '', '', 229445618, false, '229445618', '7'],
      ['record', '7', '229445618']
    ]);
  });

  await run('processStructuredTriggers sends only selected comment bot step answer without step actions', async () => {
    const calls = [];
    const result = await structuredTriggers.__testOnly.processStructuredTriggersWithDependencies(
      {
        type: 'wall_reply_new',
        group_id: 229445618,
        object: {
          id: 55,
          post_id: 77,
          from_id: 42,
          text: 'comment-answer-only'
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
                ['\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435']: 'Comment answer only trigger',
                ['\u041a\u043e\u0434 \u0441\u043e\u0431\u044b\u0442\u0438\u044f']: 'wall_comment_add',
                ['\u041a\u043e\u0434 \u0443\u0441\u043b\u043e\u0432\u0438\u044f']: 'any_post',
                ['\u041a\u043e\u0434 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044f']: 'send_bot_answer',
                ['\u0411\u043e\u0442']: 'comment-bot',
                ['\u0428\u0430\u0433']: 'comment-step'
              }
            ]
          })
        },
        loadMessageRows: async () => ([]),
        loadCommentRows: async () => ([
          {
            ['\u0411\u043e\u0442']: 'comment-bot',
            ['\u0428\u0430\u0433']: 'comment-step',
            ['\u041e\u0442\u0432\u0435\u0442']: 'Only comment answer',
            ['\u0423\u0414\u0410\u041b\u0418\u0422\u042c \u0413\u0420\u0423\u041f\u041f\u0423']: 'must-not-remove',
            ['\u041e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u043d\u0430 \u0428\u0430\u0433']: 'must-not-schedule'
          }
        ]),
        sendCommentAndPerformActions: async (comment, groupId, row, communityId, profileId) => {
          calls.push([
            'send-comment-answer-only',
            comment.id,
            groupId,
            row['\u041e\u0442\u0432\u0435\u0442'],
            row['\u0411\u043e\u0442'],
            row['\u0428\u0430\u0433'],
            row['\u0423\u0414\u0410\u041b\u0418\u0422\u042c \u0413\u0420\u0423\u041f\u041f\u0423'],
            row['\u041e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u043d\u0430 \u0428\u0430\u0433'],
            communityId,
            profileId
          ]);
        },
        updateUserGroups: async () => {
          calls.push(['unexpected-updateUserGroups']);
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
      ['log', 'Comment answer only trigger'],
      ['send-comment-answer-only', 55, 229445618, 'Only comment answer', '', '', '', '', '229445618', '7'],
      ['record', '7', '229445618']
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
