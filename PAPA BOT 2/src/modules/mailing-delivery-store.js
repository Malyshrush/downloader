const { buildEventRuntimeConfig } = require('./event-runtime-config');

const COLUMN_NUMBER = '№';
const COLUMN_NUMBER_LEGACY = 'в„–';
const COLUMN_STATUS = 'Статус';
const COLUMN_STATUS_LEGACY = 'РЎС‚Р°С‚СѓСЃ';
const META_MAILING_ID = '__meta__';

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

function buildMailingDeliveryScope(communityId = null, profileId = '1') {
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

function isMailingDeliveryStoreEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    config.ydbMailingDeliveriesTable &&
    config.awsAccessKeyId &&
    config.awsSecretAccessKey
  );
}

function createMailingDeliveryStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  const enabled = isMailingDeliveryStoreEnabled(config);
  const tableName = String(config.ydbMailingDeliveriesTable || '').trim();
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

  const getItem = overrides.getItem || (async key => {
    const { GetCommand } = require('@aws-sdk/lib-dynamodb');
    const response = await getDocumentClient().send(new GetCommand({
      TableName: tableName,
      Key: key
    }));
    return response.Item || null;
  });

  const deleteItem = overrides.deleteItem || (async key => {
    const { DeleteCommand } = require('@aws-sdk/lib-dynamodb');
    await getDocumentClient().send(new DeleteCommand({
      TableName: tableName,
      Key: key
    }));
    return { ok: true };
  });

  const queryItems = overrides.queryItems || (async ({ mailingScope, startKey }) => {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    return getDocumentClient().send(new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'mailingScope = :mailingScope',
      ExpressionAttributeValues: {
        ':mailingScope': mailingScope
      },
      ExclusiveStartKey: startKey
    }));
  });

  function buildItem(mailingScope, mailingId, state, sheetIndex = null) {
    const normalizedState = cloneValue(state && typeof state === 'object' ? state : {});
    normalizedState[COLUMN_NUMBER] = String(normalizedState[COLUMN_NUMBER] || normalizedState[COLUMN_NUMBER_LEGACY] || mailingId);
    normalizedState[COLUMN_NUMBER_LEGACY] = String(normalizedState[COLUMN_NUMBER_LEGACY] || normalizedState[COLUMN_NUMBER] || mailingId);
    normalizedState._mailingId = String(mailingId || '').trim();
    if (sheetIndex !== null && sheetIndex !== undefined) {
      normalizedState._sheetIndex = Number(sheetIndex);
    }
    const status = String(normalizedState[COLUMN_STATUS] || normalizedState[COLUMN_STATUS_LEGACY] || '').trim();

    return {
      mailingScope,
      mailingId,
      status,
      sheetIndex: normalizedState._sheetIndex,
      updatedAt: new Date().toISOString(),
      state: normalizedState
    };
  }

  function buildMetaItem(mailingScope) {
    return {
      mailingScope,
      mailingId: META_MAILING_ID,
      status: 'meta',
      updatedAt: new Date().toISOString(),
      state: {
        _meta: true,
        initialized: true
      }
    };
  }

  function isMetaItem(item) {
    return Boolean(item && item.mailingId === META_MAILING_ID);
  }

  async function listRawItems(communityId = null, profileId = '1') {
    if (!enabled) {
      return { initialized: false, items: [] };
    }

    const mailingScope = buildMailingDeliveryScope(communityId, profileId);
    const items = [];
    let startKey;

    do {
      const response = await queryItems({
        mailingScope,
        startKey
      });
      const chunk = Array.isArray(response && response.Items) ? response.Items : [];
      items.push(...chunk);
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    const initialized = items.some(isMetaItem);
    return { initialized, items };
  }

  async function listRows(communityId = null, profileId = '1') {
    const { initialized, items } = await listRawItems(communityId, profileId);
    const rows = items
      .filter(item => item && item.state && !isMetaItem(item))
      .map(item => {
        const row = cloneValue(item.state);
        row._mailingId = String(item.mailingId || row._mailingId || row[COLUMN_NUMBER] || '').trim();
        if (item.sheetIndex !== undefined && item.sheetIndex !== null) {
          row._sheetIndex = Number(item.sheetIndex);
        }
        return row;
      })
      .sort((left, right) => {
        const leftIndex = Number.isFinite(left._sheetIndex) ? left._sheetIndex : Number.MAX_SAFE_INTEGER;
        const rightIndex = Number.isFinite(right._sheetIndex) ? right._sheetIndex : Number.MAX_SAFE_INTEGER;
        if (leftIndex !== rightIndex) {
          return leftIndex - rightIndex;
        }
        return String(left._mailingId || '').localeCompare(String(right._mailingId || ''));
      });

    return { initialized, rows };
  }

  async function replaceMailingRows(communityId = null, rows = [], profileId = '1') {
    if (!enabled) {
      return { stored: false, backend: 'disabled', initialized: false };
    }

    const mailingScope = buildMailingDeliveryScope(communityId, profileId);
    const normalizedRows = Array.isArray(rows) ? cloneValue(rows) : [];
    const nextIds = new Set();

    for (let index = 0; index < normalizedRows.length; index += 1) {
      const row = normalizedRows[index] || {};
      const mailingId = String(row._mailingId || row[COLUMN_NUMBER] || row[COLUMN_NUMBER_LEGACY] || '').trim();
      if (!mailingId) continue;
      nextIds.add(mailingId);
      await putItem(buildItem(mailingScope, mailingId, row, index));
    }

    const current = await listRawItems(communityId, profileId);
    for (const item of current.items) {
      if (!item || isMetaItem(item)) continue;
      const mailingId = String(item.mailingId || '').trim();
      if (!mailingId || nextIds.has(mailingId)) continue;
      await deleteItem({
        mailingScope,
        mailingId
      });
    }

    await putItem(buildMetaItem(mailingScope));
    return {
      stored: true,
      backend: 'ydb-mailing-delivery',
      initialized: true,
      rows: normalizedRows.length
    };
  }

  async function getMailingRow(communityId = null, mailingId = '', profileId = '1') {
    if (!enabled) return null;

    const item = await getItem({
      mailingScope: buildMailingDeliveryScope(communityId, profileId),
      mailingId: String(mailingId || '').trim()
    });
    if (!item || !item.state || isMetaItem(item)) return null;
    const state = cloneValue(item.state);
    state._mailingId = String(item.mailingId || mailingId || '').trim();
    if (item.sheetIndex !== undefined && item.sheetIndex !== null) {
      state._sheetIndex = Number(item.sheetIndex);
    }
    return state;
  }

  async function getMailingState(communityId = null, mailingId = '', profileId = '1') {
    return getMailingRow(communityId, mailingId, profileId);
  }

  async function updateMailingState(communityId = null, mailingId = '', mutator, profileId = '1') {
    if (typeof mutator !== 'function') {
      throw new Error('mutator must be a function');
    }
    if (!enabled) {
      return { found: false, changed: false, backend: 'disabled' };
    }

    const normalizedId = String(mailingId || '').trim();
    const mailingScope = buildMailingDeliveryScope(communityId, profileId);
    const current = await getItem({
      mailingScope,
      mailingId: normalizedId
    });
    const draft = cloneValue(current && current.state ? current.state : {
      [COLUMN_NUMBER]: normalizedId,
      [COLUMN_NUMBER_LEGACY]: normalizedId
    });
    draft._mailingId = normalizedId;
    if (current && current.sheetIndex !== undefined && current.sheetIndex !== null) {
      draft._sheetIndex = Number(current.sheetIndex);
    }

    const mutationResult = await mutator(draft);
    const nextState = mutationResult && typeof mutationResult === 'object' && Object.prototype.hasOwnProperty.call(mutationResult, 'value')
      ? mutationResult.value
      : draft;
    const item = buildItem(mailingScope, normalizedId, nextState, nextState && nextState._sheetIndex);
    await putItem(item);
    return {
      found: Boolean(current && current.state),
      changed: true,
      backend: 'ydb-mailing-delivery',
      value: cloneValue(item.state)
    };
  }

  return {
    isEnabled: () => enabled,
    listRows,
    replaceMailingRows,
    getMailingRow,
    getMailingState,
    updateMailingState
  };
}

module.exports = {
  buildMailingDeliveryScope,
  createMailingDeliveryStore,
  isMailingDeliveryStoreEnabled
};
