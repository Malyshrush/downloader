const incomingEvents = [];
const outboundActions = [];
const processedEventIds = new Map();
const activeClaims = new Map();
let incomingEventConsumer = null;
let flushScheduled = false;

function setIncomingEventConsumer(handler) {
  incomingEventConsumer = typeof handler === 'function' ? handler : null;
}

async function flushIncomingEvents() {
  if (!incomingEventConsumer) return 0;
  const batch = await drainIncomingEvents();
  for (const envelope of batch) {
    await incomingEventConsumer(envelope);
  }
  return batch.length;
}

function scheduleFlush() {
  if (!incomingEventConsumer || flushScheduled) return;
  flushScheduled = true;

  setImmediate(async () => {
    try {
      await flushIncomingEvents();
    } finally {
      flushScheduled = false;
      if (incomingEventConsumer && incomingEvents.length > 0) {
        scheduleFlush();
      }
    }
  });
}

async function publishIncomingEvent(eventEnvelope) {
  if (!eventEnvelope || !eventEnvelope.eventId) {
    throw new Error('eventEnvelope.eventId is required');
  }

  incomingEvents.push(JSON.parse(JSON.stringify(eventEnvelope)));
  scheduleFlush();

  return {
    accepted: true,
    queue: 'stub-in-memory',
    eventId: eventEnvelope.eventId
  };
}

async function publishOutboundAction(actionEnvelope) {
  if (!actionEnvelope || !actionEnvelope.actionId) {
    throw new Error('actionEnvelope.actionId is required');
  }

  outboundActions.push(JSON.parse(JSON.stringify(actionEnvelope)));
  return {
    accepted: true,
    queue: 'stub-outbound',
    actionId: actionEnvelope.actionId
  };
}

async function drainIncomingEvents() {
  return incomingEvents.splice(0, incomingEvents.length);
}

async function drainOutboundActions() {
  return outboundActions.splice(0, outboundActions.length);
}

async function consumeIncomingEvent(handler) {
  const batch = await drainIncomingEvents();
  for (const envelope of batch) {
    await handler(envelope);
  }
  return batch.length;
}

async function consumeOutboundAction(handler) {
  const batch = await drainOutboundActions();
  for (const action of batch) {
    await handler(action);
  }
  return batch.length;
}

async function claimIncomingEvent(eventId, meta = {}) {
  const normalizedEventId = String(eventId || '').trim();
  if (!normalizedEventId) {
    throw new Error('eventId is required');
  }

  if (processedEventIds.has(normalizedEventId)) {
    return {
      acquired: false,
      reason: 'duplicate',
      eventId: normalizedEventId
    };
  }

  if (activeClaims.has(normalizedEventId)) {
    return {
      acquired: false,
      reason: 'inflight',
      eventId: normalizedEventId
    };
  }

  activeClaims.set(normalizedEventId, {
    claimedAt: new Date().toISOString(),
    ...meta
  });

  return {
    acquired: true,
    eventId: normalizedEventId,
    backend: 'stub-in-memory'
  };
}

async function hasProcessedEvent(eventId) {
  return processedEventIds.has(String(eventId || ''));
}

async function markProcessedEvent(eventId, meta = {}) {
  const normalizedEventId = String(eventId || '');
  processedEventIds.set(normalizedEventId, {
    processedAt: new Date().toISOString(),
    ...meta
  });
  activeClaims.delete(normalizedEventId);
}

async function releaseIncomingEventClaim(eventId, meta = {}) {
  const normalizedEventId = String(eventId || '');
  const current = activeClaims.get(normalizedEventId);
  if (!current) return;
  activeClaims.delete(normalizedEventId);
  if (meta && Object.keys(meta).length > 0) {
    processedEventIds.delete(normalizedEventId);
  }
}

function resetEventQueueForTests() {
  incomingEvents.length = 0;
  outboundActions.length = 0;
  processedEventIds.clear();
  activeClaims.clear();
  incomingEventConsumer = null;
  flushScheduled = false;
}

module.exports = {
  publishIncomingEvent,
  publishOutboundAction,
  drainIncomingEvents,
  drainOutboundActions,
  consumeIncomingEvent,
  consumeOutboundAction,
  flushIncomingEvents,
  claimIncomingEvent,
  hasProcessedEvent,
  markProcessedEvent,
  releaseIncomingEventClaim,
  setIncomingEventConsumer,
  resetEventQueueForTests
};
