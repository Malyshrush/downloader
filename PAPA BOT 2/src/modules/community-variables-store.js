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

function normalizeVariableName(variableName) {
  return String(variableName || '').trim().toLowerCase();
}

function buildCommunityVariablesScope(communityId = null, profileId = '1') {
  return `${normalizeProfileId(profileId)}:${normalizeCommunityId(communityId)}`;
}

function buildVariableKey(entryType, variableName) {
  return `${String(entryType || '').trim()}:${normalizeVariableName(variableName)}`;
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

function isCommunityVariablesStoreEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    config.ydbCommunityVariablesTable &&
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

function createCommunityVariablesStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  const enabled = isCommunityVariablesStoreEnabled(config);
  const tableName = String(config.ydbCommunityVariablesTable || '').trim();
  let documentClient = overrides.documentClient || null;

  function getDocumentClient() {
    if (!documentClient) {
      documentClient = createDocumentClient(config);
    }
    return documentClient;
  }

  const queryItems = overrides.queryItems || (async ({ communityScope, startKey }) => {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    return getDocumentClient().send(new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'communityScope = :communityScope',
      ExpressionAttributeValues: {
        ':communityScope': communityScope
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
            communityScope: key.communityScope,
            variableKey: key.variableKey
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

  const putItems = overrides.putItems || (async items => {
    await batchWriteItems({
      putItems: Array.isArray(items) ? items : []
    });
    return { ok: true };
  });

  async function listAllEntries(communityId = null, profileId = '1') {
    if (!enabled) {
      return [];
    }

    const communityScope = buildCommunityVariablesScope(communityId, profileId);
    const items = [];
    let startKey;

    do {
      const response = await queryItems({
        communityScope,
        startKey
      });
      items.push(...(Array.isArray(response && response.Items) ? response.Items : []));
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    return cloneValue(items);
  }

  async function listVariableState(communityId = null, profileId = '1') {
    if (!enabled) {
      return {
        globalVars: {},
        vkVars: {},
        userVariableNames: []
      };
    }

    const entries = await listAllEntries(communityId, profileId);
    const globalVars = {};
    const vkVars = {};
    const userVariableNames = [];
    const seenUserVariableNames = new Set();

    for (const item of entries) {
      const entryType = String(item && item.entryType || '').trim().toLowerCase();
      const variableName = normalizeVariableName(item && item.variableName);
      if (!variableName) continue;

      if (entryType === 'global') {
        globalVars[variableName] = String(item && item.value || '').trim();
        continue;
      }
      if (entryType === 'vk') {
        vkVars[variableName] = String(item && item.value || '').trim();
        continue;
      }
      if (entryType === 'user' && !seenUserVariableNames.has(variableName)) {
        seenUserVariableNames.add(variableName);
        userVariableNames.push(variableName);
      }
    }

    return {
      globalVars,
      vkVars,
      userVariableNames
    };
  }

  async function replaceGlobalVariables(communityId = null, variables = {}, profileId = '1') {
    if (!enabled) {
      return { stored: 0, deleted: 0, backend: 'disabled' };
    }

    const communityScope = buildCommunityVariablesScope(communityId, profileId);
    const existing = await listAllEntries(communityId, profileId);
    const deleteKeys = existing
      .filter(item => String(item && item.entryType || '').trim().toLowerCase() === 'global')
      .map(item => ({
        communityScope,
        variableKey: String(item && item.variableKey || '').trim()
      }))
      .filter(item => item.variableKey);

    const putEntries = Object.entries(variables || {}).map(([name, value]) => {
      const variableName = normalizeVariableName(name);
      if (!variableName) return null;
      return {
        communityScope,
        variableKey: buildVariableKey('global', variableName),
        entryType: 'global',
        variableName,
        value: String(value || '').trim()
      };
    }).filter(Boolean);

    await batchWriteItems({
      deleteKeys,
      putItems: putEntries
    });

    return {
      stored: putEntries.length,
      deleted: deleteKeys.length,
      backend: 'ydb-community-variables'
    };
  }

  async function ensureUserVariableCatalog(communityId = null, variableNames = [], profileId = '1') {
    if (!enabled) {
      return { stored: 0, backend: 'disabled' };
    }

    const communityScope = buildCommunityVariablesScope(communityId, profileId);
    const existing = await listAllEntries(communityId, profileId);
    const existingNames = new Set(
      existing
        .filter(item => String(item && item.entryType || '').trim().toLowerCase() === 'user')
        .map(item => normalizeVariableName(item && item.variableName))
        .filter(Boolean)
    );

    const missingEntries = [];
    const seenNames = new Set();
    for (const name of Array.isArray(variableNames) ? variableNames : []) {
      const variableName = normalizeVariableName(name);
      if (!variableName || existingNames.has(variableName) || seenNames.has(variableName)) {
        continue;
      }
      seenNames.add(variableName);
      missingEntries.push({
        communityScope,
        variableKey: buildVariableKey('user', variableName),
        entryType: 'user',
        variableName,
        value: ''
      });
    }

    if (missingEntries.length) {
      await putItems(missingEntries);
    }

    return {
      stored: missingEntries.length,
      backend: 'ydb-community-variables'
    };
  }

  return {
    isEnabled: () => enabled,
    listVariableState,
    replaceGlobalVariables,
    ensureUserVariableCatalog
  };
}

module.exports = {
  buildCommunityVariablesScope,
  createCommunityVariablesStore,
  isCommunityVariablesStoreEnabled
};
