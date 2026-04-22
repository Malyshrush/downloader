const assert = require('node:assert/strict');

const { __testOnly } = require('../src/handler');

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
  await run('senderHandler processes YMQ trigger action batches', async () => {
    const processed = [];
    const response = await __testOnly.senderHandlerWithDependencies(
      {
        messages: [
          {
            details: {
              message: {
                body: JSON.stringify({
                  actionId: 'act_batch_1',
                  actionType: 'send_message_response',
                  payload: { userId: 1 }
                })
              }
            }
          },
          {
            details: {
              message: {
                body: JSON.stringify({
                  actionId: 'act_batch_2',
                  actionType: 'send_comment_response',
                  payload: { comment: { id: 2 } }
                })
              }
            }
          }
        ]
      },
      {
        processOutboundAction: async action => {
          processed.push(action.actionId);
        },
        consumeOutboundAction: async () => {
          throw new Error('unexpected outbound queue consume');
        }
      }
    );

    assert.equal(response.statusCode, 200);
    assert.equal(response.body, 'sender-ok:2');
    assert.deepEqual(processed, ['act_batch_1', 'act_batch_2']);
  });

  await run('senderHandler falls back to outbound queue consume when trigger payload is empty', async () => {
    let invokedWithHandler = false;
    const response = await __testOnly.senderHandlerWithDependencies(
      {},
      {
        processOutboundAction: async () => {},
        consumeOutboundAction: async handler => {
          invokedWithHandler = typeof handler === 'function';
          return 4;
        }
      }
    );

    assert.equal(invokedWithHandler, true);
    assert.equal(response.statusCode, 200);
    assert.equal(response.body, 'sender-ok:4');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
