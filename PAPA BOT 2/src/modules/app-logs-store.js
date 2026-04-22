const { buildEventRuntimeConfig } = require('./event-runtime-config');

function normalizeProfileId(profileId) {
  const normalized = String(profileId || '1').trim();
  return normalized || '1';
}

function normalizeCommunityId(communityId) {
  const normalized = String(communityId || '').trim();
  return normalized || 'global';
}

function buildAppLogsScope(communityId, profileId = '1') {
  return `${normalizeProfileId(profileId)}:${normalizeCommunityId(communityId)}`;
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

function isAppLogsStoreEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    config.ydbAppLogsTable &&
    config.awsAccessKeyId &&
    config.awsSecretAccessKey
  );
}

function buildLogSortKey(row) {
  const createdAt = String(row && row.createdAt || new Date().toISOString()).trim() || new Date().toISOString();
  const rowId = String(row && row.id || '').trim() || Math.random().toString(36).slice(2, 10);
  return `${createdAt}#${rowId}`;
}

function chunkItems(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function createAppLogsStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  const enabled = isAppLogsStoreEnabled(config);
  const tableName = String(config.ydbAppLogsTable || '').trim();
  let documentClient = overrides.documentClient || null;

  function getDocumentClient() {
    if (!documentClient) {
      documentClient = createDocumentClient(config);
    }
    return documentClient;
  }

  const putItem = overrides.putItem || (async item => {
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    await getDocumentClient().send(new PutCommand({
      TableName: tableName,
      Item: item
    }));
    return { ok: true };
  });

  const queryItems = overrides.queryItems || (async ({ logScope, limit, startKey }) => {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    return getDocumentClient().send(new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'logScope = :logScope',
      ExpressionAttributeValues: {
        ':logScope': logScope
      },
      ScanIndexForward: false,
      Limit: limit,
      ExclusiveStartKey: startKey
    }));
  });

  const batchDeleteItems = overrides.batchDeleteItems || (async items => {
    const { BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');
    if (!items.length) return { ok: true };

    for (const chunk of chunkItems(items, 25)) {
      await getDocumentClient().send(new BatchWriteCommand({
        RequestItems: {
          [tableName]: chunk.map(item => ({
            DeleteRequest: {
              Key: {
                logScope: item.logScope,
                logId: item.logId
              }
            }
          }))
        }
      }));
    }

    return { ok: true };
  });

  async function addLog(logScope, row) {
    if (!enabled) {
      return { stored: false, backend: 'disabled' };
    }

    const item = {
      logScope: String(logScope || '').trim(),
      logId: buildLogSortKey(row),
      row: row && typeof row === 'object' ? row : {}
    };
    await putItem(item);
    return { stored: true, backend: 'ydb-app-logs' };
  }

  async function listLogs(logScope, limit = 150) {
    if (!enabled) {
      return [];
    }

    const response = await queryItems({
      logScope: String(logScope || '').trim(),
      limit: Math.max(1, Number(limit) || 150)
    });

    return (Array.isArray(response && response.Items) ? response.Items : [])
      .map(item => item && item.row)
      .filter(Boolean);
  }

  async function clearLogs(logScope) {
    if (!enabled) {
      return { deletedCount: 0, backend: 'disabled' };
    }

    const normalizedScope = String(logScope || '').trim();
    let deletedCount = 0;
    let startKey;

    do {
      const response = await queryItems({
        logScope: normalizedScope,
        limit: 100,
        startKey
      });
      const items = Array.isArray(response && response.Items) ? response.Items : [];
      if (!items.length) {
        break;
      }

      const deleteKeys = items.map(item => ({
        logScope: item.logScope,
        logId: item.logId
      }));
      await batchDeleteItems(deleteKeys);
      deletedCount += deleteKeys.length;
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    return { deletedCount, backend: 'ydb-app-logs' };
  }

  return {
    isEnabled: () => enabled,
    addLog,
    listLogs,
    clearLogs
  };
}

module.exports = {
  buildAppLogsScope,
  createAppLogsStore,
  isAppLogsStoreEnabled
};
