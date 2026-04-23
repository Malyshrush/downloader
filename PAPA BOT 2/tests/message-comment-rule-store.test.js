const assert = require('node:assert/strict');

const { createMessageRuleStore } = require('../src/modules/message-rule-store');
const { createCommentRuleStore } = require('../src/modules/comment-rule-store');

function createCloudConfig(overrides = {}) {
  return {
    mode: 'cloud',
    ymqRegion: 'ru-central1',
    ydbDocApiEndpoint: 'https://docapi.example.test',
    awsAccessKeyId: 'test-key',
    awsSecretAccessKey: 'test-secret',
    ydbMessageRulesTable: 'message_rule_entries',
    ydbCommentRulesTable: 'comment_rule_entries',
    ...overrides
  };
}

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
  await run('message rule store preserves row order', async () => {
    const calls = [];
    const store = createMessageRuleStore(
      createCloudConfig(),
      {
        queryItems: async () => ({
          Items: []
        }),
        batchWriteItems: async operations => {
          calls.push(operations);
          return { ok: true };
        }
      }
    );

    const result = await store.replaceRuleRows('community-1', [{ id: 2 }, { id: 1 }], '7');

    assert.equal(result.backend, 'ydb-message-rules');
    assert.equal(calls.length, 1);
    assert.deepEqual(
      calls[0].putItems.map(item => item.row),
      [
        undefined,
        { id: 2 },
        { id: 1 }
      ]
    );
  });

  await run('comment rule store distinguishes initialized empty scope', async () => {
    const store = createCommentRuleStore(
      createCloudConfig(),
      {
        queryItems: async () => ({
          Items: [
            {
              ruleScope: '7:community-1',
              ruleId: '__meta__',
              rowIndex: -1,
              meta: { initialized: true, rowCount: 0 }
            }
          ]
        })
      }
    );

    const result = await store.listRuleRows('community-1', '7');

    assert.deepEqual(result, {
      initialized: true,
      rows: []
    });
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
