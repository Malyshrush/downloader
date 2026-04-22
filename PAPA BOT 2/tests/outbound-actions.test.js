const assert = require('node:assert/strict');

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
  await run('processOutboundAction dispatches message response actions to the message sender', async () => {
    const calls = [];

    await processOutboundAction(
      {
        actionId: 'act_msg_1',
        actionType: 'send_message_response',
        payload: {
          userId: 777,
          row: { 'Ответ': 'hello' },
          originalMessage: { group_id: 123456 },
          communityId: '123456',
          profileId: '7'
        }
      },
      {
        sendMessageAndPerformActions: async (...args) => calls.push(args),
        sendFallbackResponseFromRow: async () => {
          throw new Error('unexpected fallback');
        },
        sendCommentAndPerformActions: async () => {
          throw new Error('unexpected comment send');
        },
        sendFallbackCommentFromRow: async () => {
          throw new Error('unexpected comment fallback');
        }
      }
    );

    assert.equal(calls.length, 1);
    assert.equal(calls[0][0], 777);
    assert.equal(calls[0][1]['Ответ'], 'hello');
    assert.equal(calls[0][3], false);
    assert.equal(calls[0][4], '123456');
    assert.equal(calls[0][5], '7');
  });

  await run('processOutboundAction dispatches comment fallback actions to the comment sender', async () => {
    const calls = [];

    await processOutboundAction(
      {
        actionId: 'act_comment_1',
        actionType: 'send_comment_fallback',
        payload: {
          comment: { id: 42, from_id: 777 },
          groupId: 123456,
          row: { 'Заготовленный ответ': 'fallback' },
          communityId: '123456',
          profileId: '7'
        }
      },
      {
        sendMessageAndPerformActions: async () => {
          throw new Error('unexpected message send');
        },
        sendFallbackResponseFromRow: async () => {
          throw new Error('unexpected message fallback');
        },
        sendCommentAndPerformActions: async () => {
          throw new Error('unexpected comment send');
        },
        sendFallbackCommentFromRow: async (...args) => calls.push(args)
      }
    );

    assert.equal(calls.length, 1);
    assert.equal(calls[0][0].id, 42);
    assert.equal(calls[0][1], 123456);
    assert.equal(calls[0][2]['Заготовленный ответ'], 'fallback');
  });

  await run('processOutboundAction rejects unknown action types', async () => {
    await assert.rejects(
      processOutboundAction(
        {
          actionId: 'act_unknown_1',
          actionType: 'unknown_action',
          payload: {}
        },
        {}
      ),
      /Unsupported outbound action type/
    );
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
