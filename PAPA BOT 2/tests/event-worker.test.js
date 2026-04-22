const assert = require('node:assert/strict');

const { processIncomingEvent } = require('../src/modules/event-worker');

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
  await run('processIncomingEvent claims event then runs message flow in legacy order', async () => {
    const calls = [];

    await processIncomingEvent(
      {
        eventId: 'evt_msg_1',
        eventType: 'message_new',
        profileId: '7',
        communityId: '123456',
        payload: {
          type: 'message_new',
          group_id: 123456,
          object: { message: { id: 42, from_id: 777 } }
        }
      },
      {
        claimIncomingEvent: async id => {
          calls.push('claim:' + id);
          return { acquired: true, eventId: id };
        },
        markProcessedEvent: async id => calls.push('mark:' + id),
        releaseIncomingEventClaim: async id => calls.push('release:' + id),
        processStructuredTriggers: async () => calls.push('structured'),
        handleMessage: async () => calls.push('message'),
        handleComment: async () => calls.push('comment')
      }
    );

    assert.deepEqual(calls, [
      'claim:evt_msg_1',
      'structured',
      'message',
      'mark:evt_msg_1'
    ]);
  });

  await run('processIncomingEvent skips duplicate eventIds when the claim is denied', async () => {
    const calls = [];

    const result = await processIncomingEvent(
      {
        eventId: 'evt_dup_1',
        eventType: 'message_new',
        profileId: '7',
        communityId: '123456',
        payload: { type: 'message_new', group_id: 123456, object: { message: { id: 42, from_id: 777 } } }
      },
      {
        claimIncomingEvent: async () => ({ acquired: false, reason: 'duplicate' }),
        markProcessedEvent: async () => calls.push('mark'),
        processStructuredTriggers: async () => calls.push('structured')
      }
    );

    assert.equal(result.skipped, true);
    assert.equal(result.reason, 'duplicate');
    assert.deepEqual(calls, []);
  });

  await run('processIncomingEvent releases the claim when downstream handling fails', async () => {
    const calls = [];
    await assert.rejects(
      processIncomingEvent(
        {
          eventId: 'evt_fail_1',
          eventType: 'message_new',
          profileId: '7',
          communityId: '123456',
          payload: { type: 'message_new', group_id: 123456, object: { message: { id: 42, from_id: 777 } } }
        },
        {
          claimIncomingEvent: async id => {
            calls.push('claim:' + id);
            return { acquired: true, eventId: id };
          },
          markProcessedEvent: async id => calls.push('mark:' + id),
          releaseIncomingEventClaim: async id => calls.push('release:' + id),
          processStructuredTriggers: async () => calls.push('structured'),
          handleMessage: async () => {
            calls.push('message');
            throw new Error('message failed');
          }
        }
      ),
      /message failed/
    );

    assert.deepEqual(calls, [
      'claim:evt_fail_1',
      'structured',
      'message',
      'release:evt_fail_1'
    ]);
  });

  await run('processIncomingEvent skips classic message/comment handlers for event types that do not use them', async () => {
    const calls = [];

    await processIncomingEvent(
      {
        eventId: 'evt_sys_1',
        eventType: 'message_event',
        profileId: '7',
        communityId: '123456',
        payload: {
          type: 'message_event',
          group_id: 123456,
          object: { event_id: 'abc', user_id: 777 }
        }
      },
      {
        claimIncomingEvent: async id => {
          calls.push('claim:' + id);
          return { acquired: true, eventId: id };
        },
        markProcessedEvent: async id => calls.push('mark:' + id),
        releaseIncomingEventClaim: async id => calls.push('release:' + id),
        processStructuredTriggers: async () => calls.push('structured'),
        handleMessage: async () => calls.push('message'),
        handleComment: async () => calls.push('comment')
      }
    );

    assert.deepEqual(calls, [
      'claim:evt_sys_1',
      'structured',
      'mark:evt_sys_1'
    ]);
  });

  await run('processIncomingEvent does not trigger scheduler scans from worker path', async () => {
    await processIncomingEvent(
      {
        eventId: 'evt_no_sched_1',
        eventType: 'message_new',
        profileId: '7',
        communityId: '123456',
        payload: { type: 'message_new', group_id: 123456, object: { message: { id: 42, from_id: 777 } } }
      },
      {
        claimIncomingEvent: async id => ({ acquired: true, eventId: id }),
        markProcessedEvent: async () => {},
        releaseIncomingEventClaim: async () => {},
        processStructuredTriggers: async () => {},
        handleMessage: async () => {},
        processDelayed: async () => {
          throw new Error('processDelayed must not be called from event-worker');
        },
        processMailing: async () => {
          throw new Error('processMailing must not be called from event-worker');
        }
      }
    );
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
