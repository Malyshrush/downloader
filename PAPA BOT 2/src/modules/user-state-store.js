const { buildEventRuntimeConfig } = require('./event-runtime-config');

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

function normalizeUserId(userId) {
  return String(userId || '').trim();
}

function buildUserScope(communityId, profileId = '1') {
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

function isUserStateStoreEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    config.ydbUserStateTable &&
    config.awsAccessKeyId &&
    config.awsSecretAccessKey
  );
}

function createUserStateStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  const enabled = isUserStateStoreEnabled(config);
  const tableName = String(config.ydbUserStateTable || '').trim();
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

  const deleteItem = overrides.deleteItem || (async key => {
    const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');
    await getDocumentClient().send(new DeleteCommand({
      TableName: tableName,
      Key: key
    }));
    return { ok: true };
  });

  const batchWriteItems = overrides.batchWriteItems || (async operations => {
    const { BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');
    const deleteRequests = (Array.isArray(operations && operations.deleteKeys) ? operations.deleteKeys : [])
      .map(key => ({
        DeleteRequest: {
          Key: {
            userScope: key.userScope,
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
      for (let index = 0; index < requests.length; index += 25) {
        const chunk = requests.slice(index, index + 25);
        if (!chunk.length) continue;
        await getDocumentClient().send(new BatchWriteCommand({
          RequestItems: {
            [tableName]: chunk
          }
        }));
      }
    }

    return { ok: true };
  });

  const queryItems = overrides.queryItems || (async ({ userScope, startKey }) => {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    return getDocumentClient().send(new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'userScope = :userScope',
      ExpressionAttributeValues: {
        ':userScope': userScope
      },
      ExclusiveStartKey: startKey
    }));
  });

  async function getUserRow(userScope, userId) {
    if (!enabled) {
      return null;
    }

    const normalizedScope = String(userScope || '').trim();
    const normalizedUserId = normalizeUserId(userId);
    if (!normalizedScope || !normalizedUserId) {
      return null;
    }

    const item = await getItem({
      userScope: normalizedScope,
      userId: normalizedUserId
    });
    return cloneValue(item && item.row ? item.row : null);
  }

  async function putUserRow(userScope, row) {
    if (!enabled) {
      return { stored: false, backend: 'disabled' };
    }

    const normalizedScope = String(userScope || '').trim();
    const normalizedRow = cloneValue(row && typeof row === 'object' ? row : {});
    const normalizedUserId = normalizeUserId(normalizedRow && normalizedRow.ID);
    if (!normalizedScope || !normalizedUserId) {
      throw new Error('userScope and row.ID are required');
    }

    normalizedRow.ID = normalizedUserId;
    await putItem({
      userScope: normalizedScope,
      userId: normalizedUserId,
      updatedAt: new Date().toISOString(),
      row: normalizedRow
    });
    return { stored: true, backend: 'ydb-user-state' };
  }

  async function updateUserRow(userScope, userId, mutator) {
    if (typeof mutator !== 'function') {
      throw new Error('mutator must be a function');
    }
    if (!enabled) {
      return { found: false, changed: false, value: undefined, backend: 'disabled' };
    }

    const normalizedScope = String(userScope || '').trim();
    const normalizedUserId = normalizeUserId(userId);
    const currentRow = await getUserRow(normalizedScope, normalizedUserId);
    if (!currentRow) {
      return { found: false, changed: false, value: undefined, backend: 'ydb-user-state' };
    }

    const draftRow = cloneValue(currentRow);
    const mutationResult = await mutator(draftRow);
    const nextRow = mutationResult && typeof mutationResult === 'object' && Object.prototype.hasOwnProperty.call(mutationResult, 'value')
      ? cloneValue(mutationResult.value)
      : draftRow;

    if (!nextRow || typeof nextRow !== 'object') {
      throw new Error('mutator must return an object row');
    }

    nextRow.ID = normalizedUserId;
    const changed = JSON.stringify(nextRow) !== JSON.stringify(currentRow);
    if (!changed) {
      return {
        found: true,
        changed: false,
        value: cloneValue(nextRow),
        backend: 'ydb-user-state'
      };
    }

    await putUserRow(normalizedScope, nextRow);
    return {
      found: true,
      changed: true,
      value: cloneValue(nextRow),
      backend: 'ydb-user-state'
    };
  }

  async function deleteUserRow(userScope, userId) {
    if (!enabled) {
      return { deleted: false, backend: 'disabled' };
    }

    const normalizedScope = String(userScope || '').trim();
    const normalizedUserId = normalizeUserId(userId);
    if (!normalizedScope || !normalizedUserId) {
      return { deleted: false, backend: 'ydb-user-state' };
    }

    await deleteItem({
      userScope: normalizedScope,
      userId: normalizedUserId
    });
    return { deleted: true, backend: 'ydb-user-state' };
  }

  async function listUserRows(userScope) {
    if (!enabled) {
      return [];
    }

    const normalizedScope = String(userScope || '').trim();
    if (!normalizedScope) {
      return [];
    }

    const rows = [];
    let startKey;

    do {
      const response = await queryItems({
        userScope: normalizedScope,
        startKey
      });
      const items = Array.isArray(response && response.Items) ? response.Items : [];
      for (const item of items) {
        if (item && item.row) {
          rows.push(cloneValue(item.row));
        }
      }
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    return rows;
  }

  async function listUserItems(userScope) {
    if (!enabled) {
      return [];
    }

    const normalizedScope = String(userScope || '').trim();
    if (!normalizedScope) {
      return [];
    }

    const items = [];
    let startKey;

    do {
      const response = await queryItems({
        userScope: normalizedScope,
        startKey
      });
      items.push(...(Array.isArray(response && response.Items) ? response.Items : []));
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    return cloneValue(items);
  }

  async function replaceUserRows(userScope, rows = []) {
    if (!enabled) {
      return { stored: 0, deleted: 0, backend: 'disabled' };
    }

    const normalizedScope = String(userScope || '').trim();
    if (!normalizedScope) {
      throw new Error('userScope is required');
    }

    const existing = await listUserItems(normalizedScope);
    const deleteKeys = existing
      .map(item => ({
        userScope: normalizedScope,
        userId: normalizeUserId(item && item.userId)
      }))
      .filter(item => item.userId);

    const seenIds = new Set();
    const putItems = [];
    for (const row of Array.isArray(rows) ? rows : []) {
      const nextRow = cloneValue(row && typeof row === 'object' ? row : {});
      const userId = normalizeUserId(nextRow && nextRow.ID);
      if (!userId || seenIds.has(userId)) continue;
      seenIds.add(userId);
      nextRow.ID = userId;
      putItems.push({
        userScope: normalizedScope,
        userId,
        updatedAt: new Date().toISOString(),
        row: nextRow
      });
    }

    await batchWriteItems({
      deleteKeys,
      putItems
    });

    return {
      stored: putItems.length,
      deleted: deleteKeys.length,
      backend: 'ydb-user-state'
    };
  }

  return {
    isEnabled: () => enabled,
    getUserRow,
    putUserRow,
    updateUserRow,
    deleteUserRow,
    listUserRows,
    replaceUserRows
  };
}

module.exports = {
  buildUserScope,
  createUserStateStore,
  isUserStateStoreEnabled
};
