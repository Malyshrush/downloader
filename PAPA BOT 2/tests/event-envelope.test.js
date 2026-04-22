const assert = require('node:assert/strict');

const {
  SUPPORTED_EVENT_TYPES,
  buildEventId,
  buildEventEnvelope
} = require('../src/modules/event-envelope');

function run(name, fn) {
  try {
    fn();
    process.stdout.write('PASS ' + name + '\n');
  } catch (error) {
    process.stderr.write('FAIL ' + name + '\n');
    throw error;
  }
}

run('SUPPORTED_EVENT_TYPES contains message and comment ingress events', () => {
  assert.equal(SUPPORTED_EVENT_TYPES.includes('message_new'), true);
  assert.equal(SUPPORTED_EVENT_TYPES.includes('wall_reply_new'), true);
});

run('buildEventEnvelope normalizes message_new callback', () => {
  const envelope = buildEventEnvelope(
    {
      type: 'message_new',
      group_id: 123456,
      object: {
        message: {
          id: 42,
          conversation_message_id: 7,
          from_id: 777,
          peer_id: 777,
          text: 'hello'
        }
      }
    },
    {
      profileId: '9',
      communityId: '123456',
      receivedAt: '2026-04-22T10:00:00.000Z'
    }
  );

  assert.equal(envelope.eventType, 'message_new');
  assert.equal(envelope.profileId, '9');
  assert.equal(envelope.communityId, '123456');
  assert.equal(envelope.userId, '777');
  assert.equal(envelope.source, 'vk-callback');
  assert.equal(envelope.payload.type, 'message_new');
  assert.match(envelope.eventId, /^vk:message_new:123456:/);
});

run('buildEventEnvelope normalizes wall_reply_new callback', () => {
  const envelope = buildEventEnvelope(
    {
      type: 'wall_reply_new',
      group_id: 654321,
      object: {
        id: 88,
        from_id: 333,
        post_id: 999,
        text: 'comment'
      }
    },
    {
      profileId: '4',
      communityId: '654321',
      receivedAt: '2026-04-22T10:00:00.000Z'
    }
  );

  assert.equal(envelope.eventType, 'wall_reply_new');
  assert.equal(envelope.userId, '333');
  assert.equal(envelope.communityId, '654321');
  assert.equal(envelope.payload.object.id, 88);
});

run('buildEventEnvelope returns null for unsupported events', () => {
  const envelope = buildEventEnvelope(
    { type: 'confirmation', group_id: 123456, object: {} },
    { profileId: '1', communityId: '123456', receivedAt: '2026-04-22T10:00:00.000Z' }
  );

  assert.equal(envelope, null);
});

run('buildEventId is deterministic for the same VK payload', () => {
  const left = buildEventId({
    type: 'message_new',
    communityId: '123456',
    object: { message: { id: 42, conversation_message_id: 7, from_id: 777 } }
  });
  const right = buildEventId({
    type: 'message_new',
    communityId: '123456',
    object: { message: { id: 42, conversation_message_id: 7, from_id: 777 } }
  });

  assert.equal(left, right);
});
