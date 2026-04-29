# Ingress Worker Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move VK webhook processing out of `src/handler.js` ingress and behind a normalized event envelope, stub queue, and worker path while keeping current behavior intact.

**Architecture:** Stage 1 keeps the existing message, comment, trigger, and scheduler engines, but inserts a new boundary in front of them. `src/handler.js` becomes a thin ingress publisher for supported VK events, `src/modules/event-envelope.js` defines the normalized transport contract, `src/modules/event-queue.js` provides a stub queue plus minimal idempotency state, and `src/modules/event-worker.js` runs the legacy heavy processing path outside ingress. `yandex-function/index.js` re-exports the new worker handler so the separation is explicit even before a real YMQ transport exists.

**Tech Stack:** Node.js 18, CommonJS modules, Yandex Cloud Functions, existing S3/Object Storage-backed modules, plain `node` test files in `tests/`, no mocking framework.

---

### Task 1: Add The Normalized Event Envelope Contract

**Files:**
- Create: `C:\PROJECT\GPT\PAPA BOT 2\src\modules\event-envelope.js`
- Test: `C:\PROJECT\GPT\PAPA BOT 2\tests\event-envelope.test.js`

- [ ] **Step 1: Write the failing envelope tests**

```js
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node tests\event-envelope.test.js`  
Expected: FAIL with `Cannot find module '../src/modules/event-envelope'`.

- [ ] **Step 3: Write the minimal envelope module**

```js
const SUPPORTED_EVENT_TYPES = [
  'message_new',
  'message_reply',
  'message_event',
  'wall_reply_new',
  'wall_reply_edit',
  'wall_reply_delete',
  'photo_new',
  'video_new',
  'group_join',
  'group_leave',
  'wall_repost',
  'like_add'
];

function isSupportedEventType(type) {
  return SUPPORTED_EVENT_TYPES.includes(String(type || '').trim());
}

function extractUserId(type, object = {}) {
  if (type === 'message_new' || type === 'message_reply') return object.message?.from_id || null;
  if (type === 'message_event') return object.user_id || null;
  if (type === 'wall_reply_new' || type === 'wall_reply_edit' || type === 'wall_reply_delete') return object.from_id || object.deleter_id || null;
  if (type === 'group_join' || type === 'group_leave') return object.user_id || object.joined?.user_id || null;
  if (type === 'wall_repost') return object.from_id || object.owner_id || null;
  if (type === 'like_add') return object.liker_id || object.user_id || object.from_id || null;
  if (type === 'photo_new' || type === 'video_new') return object.user_id || object.owner_id || object.from_id || null;
  return null;
}

function buildEventId({ type, communityId, object = {} }) {
  const eventType = String(type || '').trim();
  const normalizedCommunityId = String(communityId || 'default').trim() || 'default';
  const objectId =
    object.message?.id ||
    object.message?.conversation_message_id ||
    object.id ||
    object.event_id ||
    object.post_id ||
    object.object_id ||
    'no_object_id';
  const userId = extractUserId(eventType, object) || 'no_user_id';
  return `vk:${eventType}:${normalizedCommunityId}:${objectId}:${userId}`;
}

function buildEventEnvelope(data, context = {}) {
  const type = String(data?.type || '').trim();
  if (!isSupportedEventType(type)) return null;

  const communityId = String(context.communityId || data?.group_id || 'default').trim() || 'default';
  const receivedAt = context.receivedAt || new Date().toISOString();
  const object = data?.object || {};

  return {
    eventId: buildEventId({ type, communityId, object }),
    eventType: type,
    profileId: String(context.profileId || '1'),
    communityId,
    userId: String(extractUserId(type, object) || ''),
    payload: data,
    createdAt: receivedAt,
    receivedAt,
    source: 'vk-callback',
    idempotencyKey: buildEventId({ type, communityId, object }),
    traceId: `evt_${Date.now().toString(36)}`,
    rawMeta: {
      hasSecret: Boolean(data?.secret),
      objectKeys: Object.keys(object)
    }
  };
}

module.exports = {
  SUPPORTED_EVENT_TYPES,
  buildEventId,
  buildEventEnvelope,
  extractUserId,
  isSupportedEventType
};
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `node tests\event-envelope.test.js`  
Expected: all cases print `PASS ...`.

- [ ] **Step 5: Commit**

```bash
git add src/modules/event-envelope.js tests/event-envelope.test.js
git commit -m "feat: add normalized event envelope contract"
```

### Task 2: Add The Stub Event Queue And Processed Event Registry

**Files:**
- Create: `C:\PROJECT\GPT\PAPA BOT 2\src\modules\event-queue.js`
- Test: `C:\PROJECT\GPT\PAPA BOT 2\tests\event-queue.test.js`

- [ ] **Step 1: Write the failing queue tests**

```js
const assert = require('node:assert/strict');

const {
  publishIncomingEvent,
  drainIncomingEvents,
  hasProcessedEvent,
  markProcessedEvent,
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
})();
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node tests\event-queue.test.js`  
Expected: FAIL with `Cannot find module '../src/modules/event-queue'`.

- [ ] **Step 3: Write the minimal stub queue**

```js
const incomingEvents = [];
const processedEventIds = new Map();

async function publishIncomingEvent(eventEnvelope) {
  if (!eventEnvelope || !eventEnvelope.eventId) {
    throw new Error('eventEnvelope.eventId is required');
  }

  incomingEvents.push(JSON.parse(JSON.stringify(eventEnvelope)));

  return {
    accepted: true,
    queue: 'stub-in-memory',
    eventId: eventEnvelope.eventId
  };
}

async function publishOutboundAction(actionEnvelope) {
  return {
    accepted: true,
    queue: 'stub-outbound',
    eventId: actionEnvelope?.eventId || ''
  };
}

async function drainIncomingEvents() {
  return incomingEvents.splice(0, incomingEvents.length);
}

async function consumeIncomingEvent(handler) {
  const batch = await drainIncomingEvents();
  for (const envelope of batch) {
    await handler(envelope);
  }
  return batch.length;
}

async function hasProcessedEvent(eventId) {
  return processedEventIds.has(String(eventId || ''));
}

async function markProcessedEvent(eventId, meta = {}) {
  processedEventIds.set(String(eventId || ''), {
    processedAt: new Date().toISOString(),
    ...meta
  });
}

function resetEventQueueForTests() {
  incomingEvents.length = 0;
  processedEventIds.clear();
}

module.exports = {
  publishIncomingEvent,
  publishOutboundAction,
  drainIncomingEvents,
  consumeIncomingEvent,
  hasProcessedEvent,
  markProcessedEvent,
  resetEventQueueForTests
};
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `node tests\event-queue.test.js`  
Expected: all cases print `PASS ...`.

- [ ] **Step 5: Commit**

```bash
git add src/modules/event-queue.js tests/event-queue.test.js
git commit -m "feat: add stub event queue and processed registry"
```

### Task 3: Add The Worker Orchestration Layer

**Files:**
- Create: `C:\PROJECT\GPT\PAPA BOT 2\src\modules\event-worker.js`
- Test: `C:\PROJECT\GPT\PAPA BOT 2\tests\event-worker.test.js`

- [ ] **Step 1: Write the failing worker tests**

```js
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
  await run('processIncomingEvent runs message flow in legacy order', async () => {
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
        hasProcessedEvent: async () => false,
        markProcessedEvent: async id => calls.push('mark:' + id),
        processStructuredTriggers: async () => calls.push('structured'),
        handleMessage: async () => calls.push('message'),
        handleComment: async () => calls.push('comment'),
        processDelayed: async () => calls.push('delayed'),
        processMailing: async () => calls.push('mailing')
      }
    );

    assert.deepEqual(calls, [
      'structured',
      'message',
      'delayed',
      'mailing',
      'mark:evt_msg_1'
    ]);
  });

  await run('processIncomingEvent skips duplicate eventIds', async () => {
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
        hasProcessedEvent: async () => true,
        markProcessedEvent: async () => calls.push('mark'),
        processStructuredTriggers: async () => calls.push('structured')
      }
    );

    assert.equal(result.skipped, true);
    assert.equal(result.reason, 'duplicate');
    assert.deepEqual(calls, []);
  });
})();
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node tests\event-worker.test.js`  
Expected: FAIL with `Cannot find module '../src/modules/event-worker'`.

- [ ] **Step 3: Write the worker orchestration module**

```js
const { processStructuredTriggers } = require('./structured-triggers');
const { handleMessage } = require('./messages');
const { handleComment } = require('./comments');
const { processDelayed, processMailing } = require('./scheduler');
const { hasProcessedEvent, markProcessedEvent } = require('./event-queue');

async function processIncomingEvent(envelope, overrides = {}) {
  if (!envelope || !envelope.eventId || !envelope.eventType || !envelope.payload) {
    throw new Error('Invalid event envelope');
  }

  const hasProcessedEventImpl = overrides.hasProcessedEvent || hasProcessedEvent;
  const markProcessedEventImpl = overrides.markProcessedEvent || markProcessedEvent;
  const processStructuredTriggersImpl = overrides.processStructuredTriggers || processStructuredTriggers;
  const handleMessageImpl = overrides.handleMessage || handleMessage;
  const handleCommentImpl = overrides.handleComment || handleComment;
  const processDelayedImpl = overrides.processDelayed || processDelayed;
  const processMailingImpl = overrides.processMailing || processMailing;

  if (await hasProcessedEventImpl(envelope.eventId)) {
    return { skipped: true, reason: 'duplicate', eventId: envelope.eventId };
  }

  const profileId = String(envelope.profileId || '1');
  const communityId = String(envelope.communityId || envelope.payload?.group_id || 'default');
  const data = envelope.payload;

  await processStructuredTriggersImpl(data, profileId);

  if (envelope.eventType === 'message_new' || envelope.eventType === 'message_reply') {
    await handleMessageImpl(data, profileId);
  }

  if (envelope.eventType === 'wall_reply_new' || envelope.eventType === 'wall_reply_edit') {
    await handleCommentImpl(data, profileId);
  }

  await processDelayedImpl(communityId, profileId);
  await processMailingImpl(communityId, profileId);

  await markProcessedEventImpl(envelope.eventId, {
    eventType: envelope.eventType,
    profileId,
    communityId
  });

  return { ok: true, eventId: envelope.eventId };
}

module.exports = {
  processIncomingEvent
};
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `node tests\event-worker.test.js`  
Expected: all cases print `PASS ...`.

- [ ] **Step 5: Commit**

```bash
git add src/modules/event-worker.js tests/event-worker.test.js
git commit -m "feat: add event worker orchestration"
```

### Task 4: Rewire `src/handler.js` To Publish Instead Of Processing Inline

**Files:**
- Modify: `C:\PROJECT\GPT\PAPA BOT 2\src\handler.js`
- Modify: `C:\PROJECT\GPT\PAPA BOT 2\yandex-function\index.js`
- Test: `C:\PROJECT\GPT\PAPA BOT 2\tests\handler-ingress.test.js`

- [ ] **Step 1: Write the failing ingress extraction tests**

```js
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
})();
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `node tests\handler-ingress.test.js`  
Expected: FAIL because `__testOnly.handleVkWebhookWithDependencies` does not exist yet.

- [ ] **Step 3: Refactor the webhook branch inside `src/handler.js`**

Add the new imports near the existing module imports:

```js
const { buildEventEnvelope, isSupportedEventType } = require('./modules/event-envelope');
const { publishIncomingEvent } = require('./modules/event-queue');
const { processIncomingEvent } = require('./modules/event-worker');
```

Extract the VK webhook body processing into an injectable helper:

```js
async function handleVkWebhookWithDependencies(event, overrides = {}) {
  const logImpl = overrides.log || log;
  const resolveCommunityContextImpl = overrides.resolveCommunityContext || resolveCommunityContext;
  const setActiveCommunityImpl = overrides.setActiveCommunity || setActiveCommunity;
  const recordProfileEventUsageImpl = overrides.recordProfileEventUsage || recordProfileEventUsage;
  const buildEventEnvelopeImpl = overrides.buildEventEnvelope || buildEventEnvelope;
  const publishIncomingEventImpl = overrides.publishIncomingEvent || publishIncomingEvent;

  const data = JSON.parse(event.body || '{}');

  if (data.type === 'confirmation') {
    const groupId = data.group_id?.toString() || null;
    const resolved = await resolveCommunityContextImpl(groupId);
    const confirmationCode = resolved?.config?.confirmation_token || process.env.CONFIRMATION_TOKEN || '';
    return { statusCode: 200, body: confirmationCode || 'error_no_token' };
  }

  if (!isSupportedEventType(data.type)) {
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
      body: 'ok'
    };
  }

  const groupId = data.group_id?.toString() || 'default';
  const resolved = await resolveCommunityContextImpl(groupId);

  if (resolved?.communityId) {
    setActiveCommunityImpl(resolved.communityId, resolved.profileId);
  }

  const profileId = resolved?.profileId || '1';
  const usage = await recordProfileEventUsageImpl(profileId, groupId, data.type);
  if (!usage.allowed) {
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
      body: 'ok'
    };
  }

  const envelope = buildEventEnvelopeImpl(data, {
    profileId,
    communityId: groupId,
    receivedAt: new Date().toISOString()
  });

  if (!envelope) {
    logImpl('warn', 'VK event skipped: envelope builder returned null');
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
      body: 'ok'
    };
  }

  await publishIncomingEventImpl(envelope);

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
    body: 'ok'
  };
}
```

Change `handleVkWebhook(event)` to call that helper and keep the old confirmation branch behavior. Remove direct calls to:

- `processStructuredTriggers`
- `handleMessage`
- `handleComment`
- `processDelayed`
- `processMailing`

- [ ] **Step 4: Add the explicit worker entrypoint**

In `src/handler.js`, add:

```js
async function handler(event) {
  log('info', '🔔 RAW REQUEST:', {
    method: event.httpMethod,
    path: event.path,
    query: event.queryStringParameters,
    bodyPreview: event.body?.substring(0, 200)
  });

  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
      },
      body: ''
    };
  }

  const q = event.queryStringParameters || event.query || event.params || {};
  if (q.source === 'timer' || (event.event_metadata && event.event_metadata.event_type === 'yandex.cloud.events.serverless.triggers.TimerMessage')) {
    return handleTimerTrigger(event);
  }
  if (event.httpMethod === 'GET') return handleGetRequest(event);
  if (event.httpMethod === 'POST') return handlePostRequest(event);
  return { statusCode: 404, body: 'Not Found' };
}

async function workerHandler(event) {
  const rawBody = typeof event?.body === 'string' ? JSON.parse(event.body || '{}') : (event?.body || event || {});
  const envelopes = Array.isArray(rawBody?.events)
    ? rawBody.events
    : [rawBody?.envelope || rawBody];

  for (const envelope of envelopes) {
    await processIncomingEvent(envelope);
  }

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
    body: 'worker-ok'
  };
}

module.exports = {
  handler,
  workerHandler,
  __testOnly: {
  handleVkWebhookWithDependencies
  }
};
```

In `yandex-function/index.js`, re-export the worker handler:

```js
const { handler, workerHandler } = require('./src/handler');

module.exports.handler = handler;
module.exports.workerHandler = workerHandler;
```

- [ ] **Step 5: Run the new ingress test**

Run: `node tests\handler-ingress.test.js`  
Expected: all cases print `PASS ...`.

- [ ] **Step 6: Commit**

```bash
git add src/handler.js yandex-function/index.js tests/handler-ingress.test.js
git commit -m "refactor: publish vk ingress events to worker queue"
```

### Task 5: Verify The Worker Path End-To-End And Keep Existing Tests Green

**Files:**
- Modify: `C:\PROJECT\GPT\PAPA BOT 2\src\modules\event-worker.js`
- Modify: `C:\PROJECT\GPT\PAPA BOT 2\src\handler.js`
- Modify if needed: `C:\PROJECT\GPT\PAPA BOT 2\tests\event-worker.test.js`

- [ ] **Step 1: Add the worker behavior edge-case test**

Append this case to `tests/event-worker.test.js`:

```js
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
      hasProcessedEvent: async () => false,
      markProcessedEvent: async id => calls.push('mark:' + id),
      processStructuredTriggers: async () => calls.push('structured'),
      handleMessage: async () => calls.push('message'),
      handleComment: async () => calls.push('comment'),
      processDelayed: async () => calls.push('delayed'),
      processMailing: async () => calls.push('mailing')
    }
  );

  assert.deepEqual(calls, [
    'structured',
    'delayed',
    'mailing',
    'mark:evt_sys_1'
  ]);
});
```

- [ ] **Step 2: Run the worker test first to verify the edge case**

Run: `node tests\event-worker.test.js`  
Expected: PASS with the new `message_event` case.

- [ ] **Step 3: Run the full verification set**

Run:

```bash
node tests\event-envelope.test.js
node tests\event-queue.test.js
node tests\event-worker.test.js
node tests\handler-ingress.test.js
node tests\admin-auth-flow.test.js
node tests\admin-panel-auth-contract.test.js
node tests\admin-security-captcha.test.js
node tests\admin-sessions.test.js
node tests\profile-promo.test.js
node --check src\handler.js
node --check src\modules\event-envelope.js
node --check src\modules\event-queue.js
node --check src\modules\event-worker.js
node --check yandex-function\index.js
```

Expected:

- all test files print `PASS ...`
- `node --check` reports no syntax errors

- [ ] **Step 4: Manual local smoke check**

Run:

```bash
node src\local-server.js
```

Then send a webhook sample:

```bash
curl.exe -X POST "http://localhost:3000/" ^
  -H "Content-Type: application/json" ^
  -d "{\"type\":\"message_new\",\"group_id\":123456,\"object\":{\"message\":{\"id\":42,\"conversation_message_id\":7,\"from_id\":777,\"peer_id\":777,\"text\":\"hello\"}}}"
```

Expected:

- HTTP response body is `ok`
- no inline VK business processing is triggered from ingress
- the published envelope can be consumed by calling `workerHandler` directly in a follow-up local script or test

- [ ] **Step 5: Commit**

```bash
git add src/handler.js src/modules/event-worker.js tests/event-worker.test.js
git commit -m "test: verify ingress worker extraction flow"
```

## Self-Review

### Spec coverage

Covered:

- normalized event contract: Task 1
- stub queue abstraction: Task 2
- separate worker processing path: Task 3 and Task 4
- ingress publishing instead of inline processing: Task 4
- minimal idempotency: Task 2 and Task 3
- separate worker export: Task 4
- tests for envelope and ingress publishing: Task 1 and Task 4
- keeping current admin/auth tests green: Task 5

Intentional non-goals preserved:

- no YMQ integration in this plan
- no YDB/Redis migration in this plan
- no deep scheduler rewrite in this plan
- no deep trigger/message/comment rewrite in this plan

### Placeholder scan

Checked for placeholder markers and vague instructions. The final plan uses concrete file paths, commands, test code, and implementation snippets only.

### Type consistency

Consistent names used throughout:

- `buildEventEnvelope`
- `buildEventId`
- `publishIncomingEvent`
- `processIncomingEvent`
- `workerHandler`
- `handleVkWebhookWithDependencies`
- `markProcessedEvent`
- `hasProcessedEvent`

No later task introduces a conflicting function or property name.
