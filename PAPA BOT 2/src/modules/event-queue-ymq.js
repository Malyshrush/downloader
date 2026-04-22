const {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient
} = require('@aws-sdk/client-sqs');
const { createYdbStateStore } = require('./event-state-ydb');

function createQueueClient(config) {
  return new SQSClient({
    region: config.ymqRegion,
    endpoint: config.ymqEndpoint,
    credentials: {
      accessKeyId: config.awsAccessKeyId,
      secretAccessKey: config.awsSecretAccessKey
    }
  });
}

function buildMessageAttributes(envelope = {}) {
  const attributes = {};

  for (const [key, value] of Object.entries({
    eventId: envelope.eventId,
    eventType: envelope.eventType,
    profileId: envelope.profileId,
    communityId: envelope.communityId,
    traceId: envelope.traceId
  })) {
    if (!value) continue;
    attributes[key] = {
      DataType: 'String',
      StringValue: String(value)
    };
  }

  return attributes;
}

function parseQueueMessageBody(message) {
  if (!message || typeof message.Body !== 'string' || !message.Body.trim()) {
    throw new Error('Queue message body is empty');
  }
  return JSON.parse(message.Body);
}

function createYmqEventQueueBackend(config) {
  const queueClient = createQueueClient(config);
  const stateStore = createYdbStateStore(config);
  let incomingEventConsumer = null;

  async function publishIncomingEvent(eventEnvelope) {
    if (!eventEnvelope || !eventEnvelope.eventId) {
      throw new Error('eventEnvelope.eventId is required');
    }

    const response = await queueClient.send(new SendMessageCommand({
      QueueUrl: config.incomingQueueUrl,
      MessageBody: JSON.stringify(eventEnvelope),
      MessageAttributes: buildMessageAttributes(eventEnvelope)
    }));

    return {
      accepted: true,
      queue: config.incomingQueueUrl,
      eventId: eventEnvelope.eventId,
      messageId: response.MessageId || ''
    };
  }

  async function publishOutboundAction(actionEnvelope) {
    const response = await queueClient.send(new SendMessageCommand({
      QueueUrl: config.outboundQueueUrl,
      MessageBody: JSON.stringify(actionEnvelope || {}),
      MessageAttributes: buildMessageAttributes(actionEnvelope || {})
    }));

    return {
      accepted: true,
      queue: config.outboundQueueUrl,
      eventId: actionEnvelope?.eventId || '',
      messageId: response.MessageId || ''
    };
  }

  async function drainIncomingEvents() {
    return [];
  }

  async function consumeIncomingEvent(handler) {
    const response = await queueClient.send(new ReceiveMessageCommand({
      QueueUrl: config.incomingQueueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 0,
      VisibilityTimeout: config.idempotencyLeaseSeconds,
      AttributeNames: ['All'],
      MessageAttributeNames: ['All']
    }));

    const messages = Array.isArray(response.Messages) ? response.Messages : [];
    let processedCount = 0;

    for (const message of messages) {
      const envelope = parseQueueMessageBody(message);
      await handler(envelope);
      if (message.ReceiptHandle) {
        await queueClient.send(new DeleteMessageCommand({
          QueueUrl: config.incomingQueueUrl,
          ReceiptHandle: message.ReceiptHandle
        }));
      }
      processedCount += 1;
    }

    return processedCount;
  }

  function setIncomingEventConsumer(handler) {
    incomingEventConsumer = typeof handler === 'function' ? handler : null;
  }

  async function flushIncomingEvents() {
    if (!incomingEventConsumer) return 0;
    return consumeIncomingEvent(incomingEventConsumer);
  }

  function resetEventQueueForTests() {
    incomingEventConsumer = null;
  }

  return {
    publishIncomingEvent,
    publishOutboundAction,
    drainIncomingEvents,
    consumeIncomingEvent,
    flushIncomingEvents,
    claimIncomingEvent: stateStore.claimIncomingEvent,
    hasProcessedEvent: stateStore.hasProcessedEvent,
    markProcessedEvent: stateStore.markProcessedEvent,
    releaseIncomingEventClaim: stateStore.releaseIncomingEventClaim,
    setIncomingEventConsumer,
    resetEventQueueForTests
  };
}

module.exports = {
  buildMessageAttributes,
  createYmqEventQueueBackend,
  parseQueueMessageBody
};
