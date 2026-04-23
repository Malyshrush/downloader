const assert = require('node:assert/strict');

const { backfillMessageCommentRules } = require('../scripts/backfill-message-comment-rules');

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
  await run('backfillMessageCommentRules copies both sheets into stores', async () => {
    const calls = [];

    const summary = await backfillMessageCommentRules({
      profileIds: ['7'],
      loadBotConfig: async () => ({
        communities: {
          'community-1': {},
          'community-2': {}
        }
      }),
      getSheetData: async (sheetName, communityId) => {
        if (sheetName === 'СООБЩЕНИЯ') {
          return [{ id: `${communityId}-message` }];
        }
        return [{ id: `${communityId}-comment` }];
      },
      messageStore: {
        isEnabled: () => true,
        replaceRuleRows: async (communityId, rows, profileId) => {
          calls.push(['message', communityId, rows, profileId]);
        }
      },
      commentStore: {
        isEnabled: () => true,
        replaceRuleRows: async (communityId, rows, profileId) => {
          calls.push(['comment', communityId, rows, profileId]);
        }
      },
      log: () => {}
    });

    assert.deepEqual(calls, [
      ['message', 'community-1', [{ id: 'community-1-message' }], '7'],
      ['comment', 'community-1', [{ id: 'community-1-comment' }], '7'],
      ['message', 'community-2', [{ id: 'community-2-message' }], '7'],
      ['comment', 'community-2', [{ id: 'community-2-comment' }], '7']
    ]);
    assert.deepEqual(summary, {
      profiles: 1,
      communities: 2,
      messageRows: 2,
      commentRows: 2
    });
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
