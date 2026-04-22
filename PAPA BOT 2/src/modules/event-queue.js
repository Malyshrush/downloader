const { buildEventRuntimeConfig, isCloudEventRuntimeEnabled } = require('./event-runtime-config');
const stubBackend = require('./event-queue-stub');

let activeBackend = null;

function createDefaultBackend() {
  const config = buildEventRuntimeConfig(process.env);
  if (!isCloudEventRuntimeEnabled(config)) {
    return stubBackend;
  }

  const { createYmqEventQueueBackend } = require('./event-queue-ymq');
  return createYmqEventQueueBackend(config);
}

function getEventQueueBackend() {
  if (!activeBackend) {
    activeBackend = createDefaultBackend();
  }
  return activeBackend;
}

function setEventQueueBackendForTests(backend) {
  activeBackend = backend || null;
}

async function publishIncomingEvent(eventEnvelope) {
  return getEventQueueBackend().publishIncomingEvent(eventEnvelope);
}

async function publishOutboundAction(actionEnvelope) {
  return getEventQueueBackend().publishOutboundAction(actionEnvelope);
}

async function drainIncomingEvents() {
  return getEventQueueBackend().drainIncomingEvents();
}

async function drainOutboundActions() {
  return getEventQueueBackend().drainOutboundActions();
}

async function consumeIncomingEvent(handler) {
  return getEventQueueBackend().consumeIncomingEvent(handler);
}

async function consumeOutboundAction(handler) {
  return getEventQueueBackend().consumeOutboundAction(handler);
}

async function flushIncomingEvents() {
  return getEventQueueBackend().flushIncomingEvents();
}

async function claimIncomingEvent(eventId, meta = {}) {
  return getEventQueueBackend().claimIncomingEvent(eventId, meta);
}

async function hasProcessedEvent(eventId) {
  return getEventQueueBackend().hasProcessedEvent(eventId);
}

async function markProcessedEvent(eventId, meta = {}) {
  return getEventQueueBackend().markProcessedEvent(eventId, meta);
}

async function releaseIncomingEventClaim(eventId, meta = {}) {
  return getEventQueueBackend().releaseIncomingEventClaim(eventId, meta);
}

function setIncomingEventConsumer(handler) {
  return getEventQueueBackend().setIncomingEventConsumer(handler);
}

function resetEventQueueForTests() {
  if (activeBackend && typeof activeBackend.resetEventQueueForTests === 'function') {
    activeBackend.resetEventQueueForTests();
  }
  activeBackend = null;
  stubBackend.resetEventQueueForTests();
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
  resetEventQueueForTests,
  __testOnly: {
    getEventQueueBackend,
    setEventQueueBackendForTests
  }
};
