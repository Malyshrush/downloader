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

function buildMetaVariableKey(entryType) {
  return `__meta__:${String(entryType || '').trim().toLowerCase()}`;
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
        globalInitialized: false,
        globalVars: {},
        vkInitialized: false,
        vkVars: {},
        userCatalogInitialized: false,
        userVariableNames: []
      };
    }

    const entries = await listAllEntries(communityId, profileId);
    const globalVars = {};
    const vkVars = {};
    const userVariableNames = [];
    const seenUserVariableNames = new Set();
    let globalInitialized = false;
    let vkInitialized = false;
    let userCatalogInitialized = false;

    for (const item of entries) {
      const entryType = String(item && item.entryType || '').trim().toLowerCase();
      const variableName = normalizeVariableName(item && item.variableName);
      if (entryType === 'meta') {
        if (variableName === 'global') globalInitialized = true;
        if (variableName === 'vk') vkInitialized = true;
        if (variableName === 'user') userCatalogInitialized = true;
        continue;
      }
      if (!variableName) continue;

      if (entryType === 'global') {
        globalInitialized = true;
        globalVars[variableName] = String(item && item.value || '').trim();
        continue;
      }
      if (entryType === 'vk') {
        vkInitialized = true;
        vkVars[variableName] = String(item && item.value || '').trim();
        continue;
      }
      if (entryType === 'user' && !seenUserVariableNames.has(variableName)) {
        userCatalogInitialized = true;
        seenUserVariableNames.add(variableName);
        userVariableNames.push(variableName);
      }
    }

    return {
      globalInitialized,
      globalVars,
      vkInitialized,
      vkVars,
      userCatalogInitialized,
      userVariableNames
    };
  }

  async function replaceGlobalVariables(communityId = null, variables = {}, profileId = '1') {
    if (!enabled) {
      return { stored: 0, deleted: 0, backend: 'disabled' };
    }

    return replaceTypedVariables(communityId, 'global', variables, profileId);
  }

  async function replaceVkVariables(communityId = null, variables = {}, profileId = '1') {
    if (!enabled) {
      return { stored: 0, deleted: 0, backend: 'disabled' };
    }

    return replaceTypedVariables(communityId, 'vk', variables, profileId);
  }

  async function replaceTypedVariables(communityId = null, entryType = '', variables = {}, profileId = '1') {
    const communityScope = buildCommunityVariablesScope(communityId, profileId);
    const normalizedEntryType = String(entryType || '').trim().toLowerCase();
    const existing = await listAllEntries(communityId, profileId);
    const deleteKeys = existing
      .filter(item => {
        const currentEntryType = String(item && item.entryType || '').trim().toLowerCase();
        const currentVariableName = normalizeVariableName(item && item.variableName);
        return currentEntryType === normalizedEntryType
          || (currentEntryType === 'meta' && currentVariableName === normalizedEntryType);
      })
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
        variableKey: buildVariableKey(normalizedEntryType, variableName),
        entryType: normalizedEntryType,
        variableName,
        value: String(value || '').trim()
      };
    }).filter(Boolean);
    putEntries.push({
      communityScope,
      variableKey: buildMetaVariableKey(normalizedEntryType),
      entryType: 'meta',
      variableName: normalizedEntryType,
      value: 'initialized'
    });

    await batchWriteItems({
      deleteKeys,
      putItems: putEntries
    });

    return {
      stored: Object.keys(variables || {}).filter(name => normalizeVariableName(name)).length,
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
    const hasUserMeta = existing.some(item => {
      const currentEntryType = String(item && item.entryType || '').trim().toLowerCase();
      const currentVariableName = normalizeVariableName(item && item.variableName);
      return currentEntryType === 'meta' && currentVariableName === 'user';
    });
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

    if (!hasUserMeta) {
      missingEntries.push({
        communityScope,
        variableKey: buildMetaVariableKey('user'),
        entryType: 'meta',
        variableName: 'user',
        value: 'initialized'
      });
    }

    if (missingEntries.length) {
      await putItems(missingEntries);
    }

    return {
      stored: seenNames.size,
      backend: 'ydb-community-variables'
    };
  }

  return {
    isEnabled: () => enabled,
    listVariableState,
    replaceGlobalVariables,
    replaceVkVariables,
    ensureUserVariableCatalog
  };
}

module.exports = {
  buildCommunityVariablesScope,
  createCommunityVariablesStore,
  isCommunityVariablesStoreEnabled
};
