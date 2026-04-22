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
  await run('handleVkWebhook publishes supported message event and returns ok', async () => {
    const calls = [];

    const response = await __testOnly.handleVkWebhookWithDependencies(
      {
        body: JSON.stringify({
          type: 'message_new',
          group_id: 123456,
          object: {
            message: { id: 42, conversation_message_id: 7, from_id: 777, peer_id: 777, text: 'hello' }
          }
        })
      },
      {
        resolveCommunityContext: async () => ({ communityId: '123456', profileId: '7' }),
        setActiveCommunity: (communityId, profileId) => calls.push(`active:${communityId}:${profileId}`),
        recordProfileEventUsage: async () => ({ allowed: true }),
        buildEventEnvelope: (data, context) => ({
          eventId: 'evt_msg_1',
          eventType: data.type,
          profileId: context.profileId,
          communityId: context.communityId,
          payload: data
        }),
        publishIncomingEvent: async envelope => calls.push('publish:' + envelope.eventId),
        processStructuredTriggers: async () => calls.push('structured-inline'),
        handleMessage: async () => calls.push('message-inline'),
        processDelayed: async () => calls.push('delayed-inline'),
        processMailing: async () => calls.push('mailing-inline'),
        log: () => {}
      }
    );

    assert.equal(response.statusCode, 200);
    assert.equal(response.body, 'ok');
    assert.deepEqual(calls, ['active:123456:7', 'publish:evt_msg_1']);
  });

  await run('handleVkWebhook returns 500 when publishIncomingEvent throws', async () => {
    const response = await __testOnly.handleVkWebhookWithDependencies(
      {
        body: JSON.stringify({
          type: 'message_new',
          group_id: 123456,
          object: { message: { id: 42, from_id: 777 } }
        })
      },
      {
        resolveCommunityContext: async () => ({ communityId: '123456', profileId: '7' }),
        setActiveCommunity: () => {},
        recordProfileEventUsage: async () => ({ allowed: true }),
        buildEventEnvelope: () => ({
          eventId: 'evt_msg_1',
          eventType: 'message_new',
          profileId: '7',
          communityId: '123456',
          payload: {}
        }),
        publishIncomingEvent: async () => {
          throw new Error('queue unavailable');
        },
        log: () => {}
      }
    );

    assert.equal(response.statusCode, 500);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
