const { buildEventRuntimeConfig } = require('./event-runtime-config');

function normalizeProfileScope(profileId) {
  const normalized = String(profileId || '1').trim();
  return normalized || '1';
}

function cloneValue(value) {
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function buildSharedVariablesScope(profileId = '1') {
  return normalizeProfileScope(profileId);
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

function isSharedVariablesStoreEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    config.ydbSharedVariablesTable &&
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

function createSharedVariablesStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  const enabled = isSharedVariablesStoreEnabled(config);
  const tableName = String(config.ydbSharedVariablesTable || '').trim();
  let documentClient = overrides.documentClient || null;

  function getDocumentClient() {
    if (!documentClient) {
      documentClient = createDocumentClient(config);
    }
    return documentClient;
  }

  const queryItems = overrides.queryItems || (async ({ profileScope, startKey }) => {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    return getDocumentClient().send(new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'profileScope = :profileScope',
      ExpressionAttributeValues: {
        ':profileScope': profileScope
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
            profileScope: key.profileScope,
            variableName: key.variableName
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

  async function listVariables(profileScope) {
    if (!enabled) {
      return {};
    }

    const normalizedScope = normalizeProfileScope(profileScope);
    const variables = {};
    let startKey;

    do {
      const response = await queryItems({
        profileScope: normalizedScope,
        startKey
      });
      const items = Array.isArray(response && response.Items) ? response.Items : [];
      for (const item of items) {
        const variableName = String(item && item.variableName || '').trim();
        if (!variableName) continue;
        variables[variableName.toLowerCase()] = String(item && item.value || '').trim();
      }
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    return variables;
  }

  async function replaceVariables(profileScope, variables) {
    if (!enabled) {
      return { stored: 0, deleted: 0, backend: 'disabled' };
    }

    const normalizedScope = normalizeProfileScope(profileScope);
    const existing = [];
    let startKey;

    do {
      const response = await queryItems({
        profileScope: normalizedScope,
        startKey
      });
      const items = Array.isArray(response && response.Items) ? response.Items : [];
      existing.push(...items);
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    const deleteKeys = existing.map(item => ({
      profileScope: normalizedScope,
      variableName: String(item && item.variableName || '').trim()
    })).filter(item => item.variableName);

    const putItems = Object.entries(variables || {}).map(([name, value]) => ({
      profileScope: normalizedScope,
      variableName: String(name || '').trim(),
      value: String(value || '').trim()
    })).filter(item => item.variableName);

    await batchWriteItems({
      deleteKeys,
      putItems
    });

    return {
      stored: putItems.length,
      deleted: deleteKeys.length,
      backend: 'ydb-shared-variables'
    };
  }

  return {
    isEnabled: () => enabled,
    listVariables,
    replaceVariables
  };
}

module.exports = {
  buildSharedVariablesScope,
  createSharedVariablesStore,
  isSharedVariablesStoreEnabled
};
