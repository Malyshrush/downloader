const crypto = require('crypto');
const { buildEventRuntimeConfig } = require('./event-runtime-config');

const COLUMN_NUMBER = '№';
const COLUMN_STATUS = 'Статус';
const COLUMN_STATUS_LEGACY = 'РЎС‚Р°С‚СѓСЃ';
const COLUMN_SCHEDULED_AT = 'Дата и время отправки';
const COLUMN_SCHEDULED_AT_LEGACY = 'Р”Р°С‚Р° Рё РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё';

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

function buildDelayedDeliveryScope(communityId = null, profileId = '1') {
  return `${normalizeProfileId(profileId)}:${normalizeCommunityId(communityId)}`;
}

function parseScheduledAtMs(row) {
  const rawValue = String(row && (row[COLUMN_SCHEDULED_AT] || row[COLUMN_SCHEDULED_AT_LEGACY]) || '').trim();
  if (!rawValue) return 0;
  const parsed = new Date(rawValue.replace(' ', 'T') + '+03:00');
  const time = parsed.getTime();
  return Number.isFinite(time) ? time : 0;
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

function isDelayedDeliveryStoreEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    config.ydbDelayedDeliveriesTable &&
    config.awsAccessKeyId &&
    config.awsSecretAccessKey
  );
}

function createDelayedDeliveryStore(config = buildEventRuntimeConfig(process.env), overrides = {}) {
  const enabled = isDelayedDeliveryStoreEnabled(config);
  const tableName = String(config.ydbDelayedDeliveriesTable || '').trim();
  let documentClient = overrides.documentClient || null;

  function getDocumentClient() {
    if (!documentClient) {
      documentClient = createDocumentClient(config);
    }
    return documentClient;
  }

  const now = overrides.now || (() => new Date());
  const createId = overrides.createId || (() => `${Date.now().toString(36)}-${crypto.randomUUID()}`);

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

  const queryItems = overrides.queryItems || (async ({ delayedScope, startKey }) => {
    const { QueryCommand } = require('@aws-sdk/lib-dynamodb');
    return getDocumentClient().send(new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: 'delayedScope = :delayedScope',
      ExpressionAttributeValues: {
        ':delayedScope': delayedScope
      },
      ExclusiveStartKey: startKey
    }));
  });

  function buildItem(delayedScope, delayedId, row) {
    const normalizedRow = cloneValue(row && typeof row === 'object' ? row : {});
    normalizedRow[COLUMN_NUMBER] = String(normalizedRow[COLUMN_NUMBER] || delayedId);
    normalizedRow['в„–'] = String(normalizedRow['в„–'] || normalizedRow[COLUMN_NUMBER] || delayedId);
    normalizedRow._delayedId = delayedId;
    const status = String(normalizedRow[COLUMN_STATUS] || normalizedRow[COLUMN_STATUS_LEGACY] || '').trim();

    return {
      delayedScope,
      delayedId,
      status,
      scheduledAtMs: parseScheduledAtMs(normalizedRow),
      updatedAt: now().toISOString(),
      row: normalizedRow
    };
  }

  async function appendDelayedRow(communityId = null, row = {}, profileId = '1') {
    if (!enabled) {
      return { stored: false, backend: 'disabled', row: cloneValue(row) };
    }

    const delayedScope = buildDelayedDeliveryScope(communityId, profileId);
    const delayedId = String(row && (row._delayedId || row[COLUMN_NUMBER]) || createId()).trim();
    const item = buildItem(delayedScope, delayedId, row);
    await putItem(item);
    return {
      stored: true,
      backend: 'ydb-delayed-delivery',
      row: cloneValue(item.row)
    };
  }

  async function listRows(communityId = null, profileId = '1') {
    if (!enabled) return [];

    const delayedScope = buildDelayedDeliveryScope(communityId, profileId);
    const rows = [];
    let startKey;

    do {
      const response = await queryItems({
        delayedScope,
        startKey
      });
      const items = Array.isArray(response && response.Items) ? response.Items : [];
      for (const item of items) {
        if (!item || !item.row) continue;
        const row = cloneValue(item.row);
        row._delayedId = String(item.delayedId || row._delayedId || row[COLUMN_NUMBER] || '').trim();
        row._scheduledAtMs = Number(item.scheduledAtMs || parseScheduledAtMs(row) || 0);
        rows.push(row);
      }
      startKey = response && response.LastEvaluatedKey;
    } while (startKey);

    return rows;
  }

  async function listDueRows(communityId = null, inputNow = new Date(), profileId = '1') {
    const currentMs = inputNow instanceof Date ? inputNow.getTime() : new Date(inputNow).getTime();
    return (await listRows(communityId, profileId)).filter(row => {
      const status = String(row[COLUMN_STATUS] || row[COLUMN_STATUS_LEGACY] || '').trim();
      const scheduledAtMs = Number(row._scheduledAtMs || parseScheduledAtMs(row) || 0);
      return (status === 'Ожидает' || status === 'РћР¶РёРґР°РµС‚') && scheduledAtMs <= currentMs;
    });
  }

  async function getDelayedRow(communityId = null, delayedId = '', profileId = '1') {
    if (!enabled) return null;

    const item = await getItem({
      delayedScope: buildDelayedDeliveryScope(communityId, profileId),
      delayedId: String(delayedId || '').trim()
    });
    if (!item || !item.row) return null;
    const row = cloneValue(item.row);
    row._delayedId = String(item.delayedId || delayedId || '').trim();
    return row;
  }

  async function updateDelayedRow(communityId = null, delayedId = '', mutator, profileId = '1') {
    if (typeof mutator !== 'function') {
      throw new Error('mutator must be a function');
    }
    if (!enabled) {
      return { found: false, changed: false, backend: 'disabled' };
    }

    const normalizedId = String(delayedId || '').trim();
    const delayedScope = buildDelayedDeliveryScope(communityId, profileId);
    const current = await getItem({
      delayedScope,
      delayedId: normalizedId
    });
    if (!current || !current.row) {
      return { found: false, changed: false, backend: 'ydb-delayed-delivery' };
    }

    const draft = cloneValue(current.row);
    draft._delayedId = normalizedId;
    const mutationResult = await mutator(draft);
    const nextRow = mutationResult && typeof mutationResult === 'object' && Object.prototype.hasOwnProperty.call(mutationResult, 'value')
      ? mutationResult.value
      : draft;
    const item = buildItem(delayedScope, normalizedId, nextRow);
    await putItem(item);
    return {
      found: true,
      changed: true,
      backend: 'ydb-delayed-delivery',
      value: cloneValue(item.row)
    };
  }

  return {
    isEnabled: () => enabled,
    appendDelayedRow,
    listDueRows,
    getDelayedRow,
    updateDelayedRow
  };
}

module.exports = {
  buildDelayedDeliveryScope,
  createDelayedDeliveryStore,
  isDelayedDeliveryStoreEnabled
};
