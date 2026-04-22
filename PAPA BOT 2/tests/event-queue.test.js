const assert = require('node:assert/strict');

const {
  publishIncomingEvent,
  drainIncomingEvents,
  claimIncomingEvent,
  hasProcessedEvent,
  markProcessedEvent,
  releaseIncomingEventClaim,
  setIncomingEventConsumer,
  resetEventQueueForTests
} = require('../src/modules/event-queue');

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
  await run('publishIncomingEvent queues envelopes in FIFO order', async () => {
    resetEventQueueForTests();

    await publishIncomingEvent({ eventId: 'evt_1', eventType: 'message_new' });
    await publishIncomingEvent({ eventId: 'evt_2', eventType: 'wall_reply_new' });

    const drained = await drainIncomingEvents();
    assert.deepEqual(drained.map(item => item.eventId), ['evt_1', 'evt_2']);
  });

  await run('markProcessedEvent and hasProcessedEvent track idempotency', async () => {
    resetEventQueueForTests();

    assert.equal(await hasProcessedEvent('evt_1'), false);
    await markProcessedEvent('evt_1');
    assert.equal(await hasProcessedEvent('evt_1'), true);
  });

  await run('claimIncomingEvent blocks duplicates until the claim is released', async () => {
    resetEventQueueForTests();

    const first = await claimIncomingEvent('evt_claim_1');
    const second = await claimIncomingEvent('evt_claim_1');

    assert.equal(first.acquired, true);
    assert.equal(second.acquired, false);
    assert.equal(await hasProcessedEvent('evt_claim_1'), false);

    await releaseIncomingEventClaim('evt_claim_1', { error: 'retry me' });
    const third = await claimIncomingEvent('evt_claim_1');
    assert.equal(third.acquired, true);
  });

  await run('publishIncomingEvent auto-dispatches to registered consumer', async () => {
    const consumed = [];
    resetEventQueueForTests();
    setIncomingEventConsumer(async envelope => {
      consumed.push(envelope.eventId);
    });

    await publishIncomingEvent({ eventId: 'evt_async_1', eventType: 'message_new' });
    await publishIncomingEvent({ eventId: 'evt_async_2', eventType: 'wall_reply_new' });
    await new Promise(resolve => setTimeout(resolve, 25));

    assert.deepEqual(consumed, ['evt_async_1', 'evt_async_2']);
    assert.deepEqual(await drainIncomingEvents(), []);
  });
})();
