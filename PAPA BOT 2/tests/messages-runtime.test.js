const assert = require('node:assert/strict');

const messages = require('../src/modules/messages');

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
  await run('loadMessageRows uses structured store when initialized', async () => {
    const rows = await messages.__testOnly.loadMessageRows(
      'community-1',
      '7',
      {
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        messageRuleStore: {
          isEnabled: () => true,
          listRuleRows: async (communityId, profileId) => {
            assert.equal(communityId, 'community-1');
            assert.equal(profileId, '7');
            return {
              initialized: true,
              rows: [{ 'Ответ': 'Stored message rule' }]
            };
          }
        }
      }
    );

    assert.deepEqual(rows, [{ 'Ответ': 'Stored message rule' }]);
  });

  await run('loadMessageRows falls back to sheet when store is uninitialized', async () => {
    const rows = await messages.__testOnly.loadMessageRows(
      'community-1',
      '7',
      {
        getSheetData: async (sheetName, communityId, profileId) => {
          assert.equal(sheetName, 'СООБЩЕНИЯ');
          assert.equal(communityId, 'community-1');
          assert.equal(profileId, '7');
          return [{ 'Ответ': 'Sheet message rule' }];
        },
        messageRuleStore: {
          isEnabled: () => true,
          listRuleRows: async () => ({
            initialized: false,
            rows: []
          })
        }
      }
    );

    assert.deepEqual(rows, [{ 'Ответ': 'Sheet message rule' }]);
  });

  await run('handleMessage uses structured message rows in runtime path', async () => {
    const outbound = [];

    await messages.__testOnly.handleMessageWithDependencies(
      {
        type: 'message_new',
        group_id: 229445618,
        object: {
          message: {
            id: 41,
            conversation_message_id: 41,
            from_id: 42,
            peer_id: 42,
            text: 'hello'
          }
        }
      },
      '7',
      {
        addAppLog: async () => {},
        getVkToken: async () => 'token',
        updateUserData: async () => true,
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        messageRuleStore: {
          isEnabled: () => true,
          listRuleRows: async () => ({
            initialized: true,
            rows: [
              {
                'Триггер': 'hello',
                'Точно/Не точно': 'ТОЧНО'
              }
            ]
          })
        },
        checkTriggerExists: async () => true,
        checkAllConditions: async () => true,
        checkTriggerMatch: async () => true,
        publishOutboundAction: async action => {
          outbound.push(action.actionType);
        }
      }
    );

    assert.deepEqual(outbound, ['send_message_response']);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
