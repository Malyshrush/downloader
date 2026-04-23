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
  await run('syncMessageRuleSheet writes message rows into structured store', async () => {
    const calls = [];
    const rows = [{ 'Триггер': 'one' }, { 'Триггер': 'two' }];
    const result = await storage.__testOnly.syncMessageRuleSheet(
      'СООБЩЕНИЯ',
      rows,
      'community-1',
      '7',
      {
        messageRuleStore: {
          isEnabled: () => true,
          replaceRuleRows: async (communityId, nextRows, profileId) => {
            calls.push({ communityId, nextRows, profileId });
            return { stored: nextRows.length, backend: 'ydb-message-rules' };
          }
        }
      }
    );

    assert.deepEqual(calls, [
      {
        communityId: 'community-1',
        nextRows: rows,
        profileId: '7'
      }
    ]);
    assert.deepEqual(result, {
      synced: true,
      backend: 'ydb-message-rules',
      stored: 2
    });
  });

  await run('syncCommentRuleSheet writes comment rows into structured store', async () => {
    const calls = [];
    const rows = [{ 'Триггер': 'one' }];
    const result = await storage.__testOnly.syncCommentRuleSheet(
      'КОММЕНТАРИИ В ПОСТАХ',
      rows,
      'community-1',
      '7',
      {
        commentRuleStore: {
          isEnabled: () => true,
          replaceRuleRows: async (communityId, nextRows, profileId) => {
            calls.push({ communityId, nextRows, profileId });
            return { stored: nextRows.length, backend: 'ydb-comment-rules' };
          }
        }
      }
    );

    assert.deepEqual(calls, [
      {
        communityId: 'community-1',
        nextRows: rows,
        profileId: '7'
      }
    ]);
    assert.deepEqual(result, {
      synced: true,
      backend: 'ydb-comment-rules',
      stored: 1
    });
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
