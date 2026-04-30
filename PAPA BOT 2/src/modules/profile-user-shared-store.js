const { buildEventRuntimeConfig } = require('./event-runtime-config');

function cloneValue(value) {
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function chunkItems(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function normalizeProfileScope(profileId) {
  const normalized = String(profileId || '1').trim();
  return normalized || '1';
}

function normalizeUserId(userId) {
  return String(userId || '').trim();
}

function buildProfileUserSharedScope(profileId = '1') {
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

function isProfileUserSharedStoreEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    config.ydbProfileUserSharedTable &&
    config.awsAccessKeyId &&
    config.awsSecretAccessKey
  );
}

function createProfileUserSharedStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  const enabled = isProfileUserSharedStoreEnabled(config);
  const tableName = String(config.ydbProfileUserSharedTable || '').trim();
  let documentClient = overrides.documentClient || null;

  function getDocumentClient() {
    if (!documentClient) {
      documentClient = createDocumentClient(config);
    }
    return documentClient;
  }

  const getItem = overrides.getItem || (async key => {
    const { GetCommand } = require('@aws-sdk/lib-dynamodb');
    const response = await getDocumentClient().send(new GetCommand({
      TableName: tableName,
      Key: key
    }));
    return response.Item || null;
  });

  const putItem = overrides.putItem || (async item => {
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    await getDocumentClient().send(new PutCommand({
      TableName: tableName,
      Item: item
    }));
    return { ok: true };
  });

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
            userId: key.userId
          }
        }
      }));
    const putRequests = (Array.isArray(operations && operations.putItems) ? operations.putItems : [])
      .map(item => ({
        PutRequest: {
          Item: item
        }
      }));
    for (const requests of [deleteRequests, putRequests]) {
      for (const chunk of chunkItems(requests, 25)) {
        await getDocumentClient().send(new BatchWriteCommand({
          RequestItems: {
            [tableName]: chunk
          }
        }));
      }
    }

    return { ok: true };
  });

  async function getUserVariables(profileScope, userId) {
    if (!enabled) {
      return null;
    }

    const normalizedScope = normalizeProfileScope(profileScope);
    const normalizedUserId = normalizeUserId(userId);
    if (!normalizedUserId) {
      return null;
    }

    const item = await getItem({
      profileScope: normalizedScope,
      userId: normalizedUserId
    });

    return cloneValue(item && item.variables ? item.variables : null);
  }

  async function putUserVariables(profileScope, userId, variables) {
    if (!enabled) {
      return { stored: false, backend: 'disabled' };
    }

    const normalizedScope = normalizeProfileScope(profileScope);
    const normalizedUserId = normalizeUserId(userId);
    if (!normalizedUserId) {
      throw new Error('userId is required');
    }

    const nextVariables = cloneValue(variables && typeof variables === 'object' ? variables : {});
    await putItem({
      profileScope: normalizedScope,
      userId: normalizedUserId,
      updatedAt: new Date().toISOString(),
      variables: nextVariables
    });

    return { stored: true, backend: 'ydb-profile-user-shared' };
  }

  async function listUserEntries(profileScope) {
    if (!enabled) {
      return [];
    }

    const normalizedScope = normalizeProfileScope(profileScope);
    const entries = [];
    let startKey;

    do {
      const response = await queryItems({
        profileScope: normalizedScope,
        startKey
      });
      const items = Array.isArray(response && response.Items) ? response.Items : [];
      for (const item of items) {
        entries.push({
          userId: normalizeUserId(item && item.userId),
          variables: cloneValue(item && item.variables ? item.variables : {})
        });
      }
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    return entries;
  }

  async function replaceUserEntries(profileScope, entries) {
    if (!enabled) {
      return { stored: 0, deleted: 0, backend: 'disabled' };
    }

    const normalizedScope = normalizeProfileScope(profileScope);
    const existing = await listUserEntries(normalizedScope);
    const deleteKeys = existing
      .map(entry => ({
        profileScope: normalizedScope,
        userId: normalizeUserId(entry && entry.userId)
      }))
      .filter(item => item.userId);

    const putItems = [];
    for (const entry of Array.isArray(entries) ? entries : []) {
      const normalizedUserId = normalizeUserId(entry && entry.userId);
      if (!normalizedUserId) continue;
      putItems.push({
        profileScope: normalizedScope,
        userId: normalizedUserId,
        updatedAt: new Date().toISOString(),
        variables: cloneValue(entry && entry.variables && typeof entry.variables === 'object' ? entry.variables : {})
      });
    }

    await batchWriteItems({
      deleteKeys,
      putItems
    });

    return {
      stored: putItems.length,
      deleted: deleteKeys.length,
      backend: 'ydb-profile-user-shared'
    };
  }

  return {
    isEnabled: () => enabled,
    getUserVariables,
    putUserVariables,
    listUserEntries,
    replaceUserEntries
  };
}

module.exports = {
  buildProfileUserSharedScope,
  createProfileUserSharedStore,
  isProfileUserSharedStoreEnabled
};
