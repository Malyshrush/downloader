const { buildEventRuntimeConfig } = require('./event-runtime-config');

const META_TRIGGER_ID = '__meta__';

function cloneValue(value) {
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function normalizeProfileId(profileId) {
  const normalized = String(profileId || '1').trim();
  return normalized || '1';
}

function normalizeCommunityId(communityId) {
  const normalized = String(communityId || '').trim();
  return normalized || 'global';
}

function buildStructuredTriggerScope(communityId = null, profileId = '1') {
  return `${normalizeProfileId(profileId)}:${normalizeCommunityId(communityId)}`;
}

function buildTriggerId(index) {
  return String(index + 1).padStart(6, '0');
}

function createDocumentClient(config) {
  const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
  const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
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

function isStructuredTriggerStoreEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    config.ydbStructuredTriggersTable &&
    config.awsAccessKeyId &&
    config.awsSecretAccessKey
  );
}

function chunkItems(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function createStructuredTriggerStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  const enabled = isStructuredTriggerStoreEnabled(config);
  const tableName = String(config.ydbStructuredTriggersTable || '').trim();
  let documentClient = overrides.documentClient || null;

  function getDocumentClient() {
    if (!documentClient) {
      documentClient = createDocumentClient(config);
    }
    return documentClient;
  }

  const queryItems = overrides.queryItems || (async ({ triggerScope, startKey }) => {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    return getDocumentClient().send(new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'triggerScope = :triggerScope',
      ExpressionAttributeValues: {
        ':triggerScope': triggerScope
      },
      ExclusiveStartKey: startKey
    }));
  });

  const batchWriteItems = overrides.batchWriteItems || (async operations => {
    const { BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');
    const deleteRequests = (Array.isArray(operations && operations.deleteKeys) ? operations.deleteKeys : [])
      .map(key => ({
        DeleteRequest: {
          Key: {
            triggerScope: key.triggerScope,
            triggerId: key.triggerId
          }
        }
      }));
    const putRequests = (Array.isArray(operations && operations.putItems) ? operations.putItems : [])
      .map(item => ({
        PutRequest: {
          Item: item
        }
      }));
    const requests = deleteRequests.concat(putRequests);

    for (const chunk of chunkItems(requests, 25)) {
      await getDocumentClient().send(new BatchWriteCommand({
        RequestItems: {
          [tableName]: chunk
        }
      }));
    }

    return { ok: true };
  });

  async function listAllItems(communityId = null, profileId = '1') {
    if (!enabled) return [];

    const triggerScope = buildStructuredTriggerScope(communityId, profileId);
    const items = [];
    let startKey;

    do {
      const response = await queryItems({
        triggerScope,
        startKey
      });
      items.push(...(Array.isArray(response && response.Items) ? response.Items : []));
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    return cloneValue(items);
  }

  async function listTriggerRows(communityId = null, profileId = '1') {
    if (!enabled) {
      return {
        initialized: false,
        rows: []
      };
    }

    const items = await listAllItems(communityId, profileId);
    const initialized = items.some(item => String(item && item.triggerId || '').trim() === META_TRIGGER_ID);
    const rows = items
      .filter(item => String(item && item.triggerId || '').trim() !== META_TRIGGER_ID)
      .sort((left, right) => Number(left && left.rowIndex || 0) - Number(right && right.rowIndex || 0))
      .map(item => cloneValue(item && item.row ? item.row : {}));

    return {
      initialized,
      rows
    };
  }

  async function replaceTriggerRows(communityId = null, rows = [], profileId = '1') {
    if (!enabled) {
      return {
        stored: 0,
        deleted: 0,
        backend: 'disabled'
      };
    }

    const triggerScope = buildStructuredTriggerScope(communityId, profileId);
    const existing = await listAllItems(communityId, profileId);
    const deleteKeys = existing
      .map(item => ({
        triggerScope,
        triggerId: String(item && item.triggerId || '').trim()
      }))
      .filter(item => item.triggerId);

    const normalizedRows = Array.isArray(rows) ? rows : [];
    const putItems = normalizedRows.map((row, index) => ({
      triggerScope,
      triggerId: buildTriggerId(index),
      rowIndex: index,
      updatedAt: new Date().toISOString(),
      row: cloneValue(row)
    }));
    putItems.unshift({
      triggerScope,
      triggerId: META_TRIGGER_ID,
      rowIndex: -1,
      updatedAt: new Date().toISOString(),
      meta: {
        initialized: true,
        rowCount: normalizedRows.length
      }
    });

    if (deleteKeys.length) {
      await batchWriteItems({
        deleteKeys
      });
    }
    if (putItems.length) {
      await batchWriteItems({
        putItems
      });
    }

    return {
      stored: normalizedRows.length,
      deleted: deleteKeys.length,
      backend: 'ydb-structured-triggers'
    };
  }

  return {
    isEnabled: () => enabled,
    listTriggerRows,
    replaceTriggerRows
  };
}

module.exports = {
  META_TRIGGER_ID,
  buildStructuredTriggerScope,
  createStructuredTriggerStore,
  isStructuredTriggerStoreEnabled
};
