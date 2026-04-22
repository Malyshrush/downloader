const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, UpdateCommand } = require('@aws-sdk/lib-dynamodb');

function createDocumentClient(config) {
  const client = new DynamoDBClient({
    region: config.ymqRegion,
    endpoint: config.ydbDocApiEndpoint,
    credentials: {
      accessKeyId: config.awsAccessKeyId,
      secretAccessKey: config.awsSecretAccessKey
    }
  });

  return DynamoDBDocumentClient.from(client, {
    marshallOptions: {
      removeUndefinedValues: true
    }
  });
}

function buildRetentionExpiryEpoch(retentionDays) {
  return Math.floor((Date.now() + retentionDays * 24 * 60 * 60 * 1000) / 1000);
}

function isConditionalFailure(error) {
  return error && error.name === 'ConditionalCheckFailedException';
}

function createYdbStateStore(config) {
  const documentClient = createDocumentClient(config);
  const tableName = config.ydbIdempotencyTable;

  async function getEventRecord(eventId) {
    const response = await documentClient.send(new GetCommand({
      TableName: tableName,
      Key: { eventId }
    }));
    return response.Item || null;
  }

  async function claimIncomingEvent(eventId, meta = {}) {
    const normalizedEventId = String(eventId || '').trim();
    if (!normalizedEventId) {
      throw new Error('eventId is required');
    }

    const nowEpoch = Date.now();
    const nowIso = new Date(nowEpoch).toISOString();
    const leaseUntilEpoch = nowEpoch + (config.idempotencyLeaseSeconds * 1000);

    try {
      await documentClient.send(new UpdateCommand({
        TableName: tableName,
        Key: { eventId: normalizedEventId },
        UpdateExpression: [
          'SET #status = :processing',
          'leaseUntilEpoch = :leaseUntilEpoch',
          'claimedAt = :claimedAt',
          'updatedAt = :updatedAt',
          'expiresAtEpoch = :expiresAtEpoch',
          'eventType = :eventType',
          'profileId = :profileId',
          'communityId = :communityId',
          'traceId = :traceId'
        ].join(', '),
        ConditionExpression: 'attribute_not_exists(eventId) OR (#status <> :processed AND leaseUntilEpoch < :nowEpoch)',
        ExpressionAttributeNames: {
          '#status': 'status'
        },
        ExpressionAttributeValues: {
          ':processing': 'processing',
          ':processed': 'processed',
          ':eventType': String(meta.eventType || ''),
          ':profileId': String(meta.profileId || ''),
          ':communityId': String(meta.communityId || ''),
          ':traceId': String(meta.traceId || ''),
          ':claimedAt': nowIso,
          ':updatedAt': nowIso,
          ':leaseUntilEpoch': leaseUntilEpoch,
          ':expiresAtEpoch': buildRetentionExpiryEpoch(config.idempotencyRetentionDays),
          ':nowEpoch': nowEpoch
        }
      }));

      return {
        acquired: true,
        eventId: normalizedEventId,
        backend: 'ydb-document-api'
      };
    } catch (error) {
      if (!isConditionalFailure(error)) {
        throw error;
      }

      const existing = await getEventRecord(normalizedEventId);
      return {
        acquired: false,
        reason: existing?.status === 'processed' ? 'duplicate' : 'inflight',
        eventId: normalizedEventId
      };
    }
  }

  async function hasProcessedEvent(eventId) {
    const item = await getEventRecord(String(eventId || '').trim());
    return item?.status === 'processed';
  }

  async function markProcessedEvent(eventId, meta = {}) {
    const normalizedEventId = String(eventId || '').trim();
    const nowIso = new Date().toISOString();
    await documentClient.send(new UpdateCommand({
      TableName: tableName,
      Key: { eventId: normalizedEventId },
      UpdateExpression: [
        'SET #status = :processed',
        'processedAt = :processedAt',
        'updatedAt = :updatedAt',
        'leaseUntilEpoch = :leaseUntilEpoch',
        'eventType = :eventType',
        'profileId = :profileId',
        'communityId = :communityId'
      ].join(', '),
      ExpressionAttributeNames: {
        '#status': 'status'
      },
      ExpressionAttributeValues: {
        ':processed': 'processed',
        ':processedAt': nowIso,
        ':updatedAt': nowIso,
        ':leaseUntilEpoch': 0,
        ':eventType': String(meta.eventType || ''),
        ':profileId': String(meta.profileId || ''),
        ':communityId': String(meta.communityId || '')
      }
    }));
  }

  async function releaseIncomingEventClaim(eventId, meta = {}) {
    const normalizedEventId = String(eventId || '').trim();
    const nowIso = new Date().toISOString();
    await documentClient.send(new UpdateCommand({
      TableName: tableName,
      Key: { eventId: normalizedEventId },
      UpdateExpression: [
        'SET #status = :failed',
        'updatedAt = :updatedAt',
        'leaseUntilEpoch = :leaseUntilEpoch',
        'lastError = :lastError'
      ].join(', '),
      ExpressionAttributeNames: {
        '#status': 'status'
      },
      ExpressionAttributeValues: {
        ':failed': 'failed',
        ':updatedAt': nowIso,
        ':leaseUntilEpoch': 0,
        ':lastError': String(meta.errorMessage || '')
      }
    }));
  }

  function resetEventQueueForTests() {
    return undefined;
  }

  return {
    claimIncomingEvent,
    hasProcessedEvent,
    markProcessedEvent,
    releaseIncomingEventClaim,
    resetEventQueueForTests
  };
}

module.exports = {
  createYdbStateStore
};
