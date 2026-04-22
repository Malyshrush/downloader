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
  await run('workerHandler processes YMQ trigger message batches', async () => {
    const processed = [];
    const response = await __testOnly.workerHandlerWithDependencies(
      {
        messages: [
          {
            details: {
              message: {
                body: JSON.stringify({
                  eventId: 'evt_batch_1',
                  eventType: 'message_new',
                  profileId: '7',
                  communityId: '123456',
                  payload: { type: 'message_new', object: { message: { id: 1, from_id: 777 } } }
                })
              }
            }
          },
          {
            details: {
              message: {
                body: JSON.stringify({
                  eventId: 'evt_batch_2',
                  eventType: 'wall_reply_new',
                  profileId: '7',
                  communityId: '123456',
                  payload: { type: 'wall_reply_new', object: { id: 2, from_id: 777 } }
                })
              }
            }
          }
        ]
      },
      {
        processIncomingEvent: async envelope => {
          processed.push(envelope.eventId);
        },
        consumeIncomingEvent: async () => {
          throw new Error('unexpected stub consume');
        }
      }
    );

    assert.equal(response.statusCode, 200);
    assert.equal(response.body, 'worker-ok:2');
    assert.deepEqual(processed, ['evt_batch_1', 'evt_batch_2']);
  });

  await run('workerHandler falls back to queue consume when trigger payload is empty', async () => {
    let invokedWithHandler = false;
    const response = await __testOnly.workerHandlerWithDependencies(
      {},
      {
        processIncomingEvent: async () => {},
        consumeIncomingEvent: async handler => {
          invokedWithHandler = typeof handler === 'function';
          return 3;
        }
      }
    );

    assert.equal(invokedWithHandler, true);
    assert.equal(response.statusCode, 200);
    assert.equal(response.body, 'worker-ok:3');
  });

  await run('workerHandler routes outbound action batches to the sender path', async () => {
    const processed = [];
    const response = await __testOnly.workerHandlerWithDependencies(
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
          }
        ]
      },
      {
        processIncomingEvent: async () => {
          throw new Error('unexpected incoming event handling');
        },
        processOutboundAction: async action => {
          processed.push(action.actionId);
        },
        consumeIncomingEvent: async () => {
          throw new Error('unexpected stub consume');
        }
      }
    );

    assert.equal(response.statusCode, 200);
    assert.equal(response.body, 'worker-ok:1');
    assert.deepEqual(processed, ['act_batch_1']);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
