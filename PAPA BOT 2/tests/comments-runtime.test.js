const assert = require('node:assert/strict');

const comments = require('../src/modules/comments');

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
  await run('loadCommentRows uses structured store when initialized', async () => {
    const rows = await comments.__testOnly.loadCommentRows(
      'community-1',
      '7',
      {
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        commentRuleStore: {
          isEnabled: () => true,
          listRuleRows: async (communityId, profileId) => {
            assert.equal(communityId, 'community-1');
            assert.equal(profileId, '7');
            return {
              initialized: true,
              rows: [{ 'Ответ': 'Stored comment rule' }]
            };
          }
        }
      }
    );

    assert.deepEqual(rows, [{ 'Ответ': 'Stored comment rule' }]);
  });

  await run('loadCommentRows falls back to sheet when store is uninitialized', async () => {
    const rows = await comments.__testOnly.loadCommentRows(
      'community-1',
      '7',
      {
        getSheetData: async (sheetName, communityId, profileId) => {
          assert.equal(sheetName, 'КОММЕНТАРИИ В ПОСТАХ');
          assert.equal(communityId, 'community-1');
          assert.equal(profileId, '7');
          return [{ 'Ответ': 'Sheet comment rule' }];
        },
        commentRuleStore: {
          isEnabled: () => true,
          listRuleRows: async () => ({
            initialized: false,
            rows: []
          })
        }
      }
    );

    assert.deepEqual(rows, [{ 'Ответ': 'Sheet comment rule' }]);
  });

  await run('handleComment uses structured comment rows in runtime path', async () => {
    const outbound = [];

    await comments.__testOnly.handleCommentWithDependencies(
      {
        group_id: 229445618,
        object: {
          id: 51,
          from_id: 42,
          post_id: 33,
          text: 'hello'
        }
      },
      '7',
      {
        addAppLog: async () => {},
        updateUserData: async () => true,
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        commentRuleStore: {
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

    assert.deepEqual(outbound, ['send_comment_response']);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
