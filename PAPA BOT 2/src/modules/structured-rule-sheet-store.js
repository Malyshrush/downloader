const { buildEventRuntimeConfig } = require('./event-runtime-config');

const META_RULE_ID = '__meta__';

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

function buildStructuredRuleScope(communityId = null, profileId = '1') {
  return `${normalizeProfileId(profileId)}:${normalizeCommunityId(communityId)}`;
}

function buildRuleId(index) {
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

function isStructuredRuleSheetStoreEnabled(config, tableName) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    String(tableName || '').trim() &&
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

function createStructuredRuleSheetStore(
  config = buildEventRuntimeConfig(process.env),
  options = {},
  overrides = {}
) {
  const tableName = String(options.tableName || '').trim();
  const backendName = String(options.backendName || 'ydb-structured-rules').trim() || 'ydb-structured-rules';
  const enabled = isStructuredRuleSheetStoreEnabled(config, tableName);
  let documentClient = overrides.documentClient || null;

  function getDocumentClient() {
    if (!documentClient) {
      documentClient = createDocumentClient(config);
    }
    return documentClient;
  }

  const queryItems = overrides.queryItems || (async ({ ruleScope, startKey }) => {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    return getDocumentClient().send(new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'ruleScope = :ruleScope',
      ExpressionAttributeValues: {
        ':ruleScope': ruleScope
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
            ruleScope: key.ruleScope,
            ruleId: key.ruleId
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

    const ruleScope = buildStructuredRuleScope(communityId, profileId);
    const items = [];
    let startKey;

    do {
      const response = await queryItems({
        ruleScope,
        startKey
      });
      items.push(...(Array.isArray(response && response.Items) ? response.Items : []));
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    return cloneValue(items);
  }

  async function listRuleRows(communityId = null, profileId = '1') {
    if (!enabled) {
      return {
        initialized: false,
        rows: []
      };
    }

    const items = await listAllItems(communityId, profileId);
    const initialized = items.some(item => String(item && item.ruleId || '').trim() === META_RULE_ID);
    const rows = items
      .filter(item => String(item && item.ruleId || '').trim() !== META_RULE_ID)
      .sort((left, right) => Number(left && left.rowIndex || 0) - Number(right && right.rowIndex || 0))
      .map(item => cloneValue(item && item.row ? item.row : {}));

    return {
      initialized,
      rows
    };
  }

  async function replaceRuleRows(communityId = null, rows = [], profileId = '1') {
    if (!enabled) {
      return {
        stored: 0,
        deleted: 0,
        backend: 'disabled'
      };
    }

    const ruleScope = buildStructuredRuleScope(communityId, profileId);
    const existing = await listAllItems(communityId, profileId);
    const deleteKeys = existing
      .map(item => ({
        ruleScope,
        ruleId: String(item && item.ruleId || '').trim()
      }))
      .filter(item => item.ruleId);

    const normalizedRows = Array.isArray(rows) ? rows : [];
    const timestamp = new Date().toISOString();
    const putItems = normalizedRows.map((row, index) => ({
      ruleScope,
      ruleId: buildRuleId(index),
      rowIndex: index,
      updatedAt: timestamp,
      row: cloneValue(row)
    }));
    putItems.unshift({
      ruleScope,
      ruleId: META_RULE_ID,
      rowIndex: -1,
      updatedAt: timestamp,
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
      backend: backendName
    };
  }

  return {
    isEnabled: () => enabled,
    listRuleRows,
    replaceRuleRows
  };
}

module.exports = {
  META_RULE_ID,
  buildStructuredRuleScope,
  createStructuredRuleSheetStore,
  isStructuredRuleSheetStoreEnabled
};
