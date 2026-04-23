const { buildEventRuntimeConfig } = require('./event-runtime-config');

const COLUMN_NUMBER = '№';
const COLUMN_NUMBER_LEGACY = 'в„–';
const COLUMN_STATUS = 'Статус';
const COLUMN_STATUS_LEGACY = 'РЎС‚Р°С‚СѓСЃ';

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

  function buildItem(mailingScope, mailingId, state) {
    const normalizedState = cloneValue(state && typeof state === 'object' ? state : {});
    normalizedState[COLUMN_NUMBER] = String(normalizedState[COLUMN_NUMBER] || normalizedState[COLUMN_NUMBER_LEGACY] || mailingId);
    normalizedState[COLUMN_NUMBER_LEGACY] = String(normalizedState[COLUMN_NUMBER_LEGACY] || normalizedState[COLUMN_NUMBER] || mailingId);
    normalizedState._mailingId = String(mailingId || '').trim();
    const status = String(normalizedState[COLUMN_STATUS] || normalizedState[COLUMN_STATUS_LEGACY] || '').trim();

    return {
      mailingScope,
      mailingId,
      status,
      updatedAt: new Date().toISOString(),
      state: normalizedState
    };
  }

  async function getMailingState(communityId = null, mailingId = '', profileId = '1') {
    if (!enabled) return null;

    const item = await getItem({
      mailingScope: buildMailingDeliveryScope(communityId, profileId),
      mailingId: String(mailingId || '').trim()
    });
    if (!item || !item.state) return null;
    const state = cloneValue(item.state);
    state._mailingId = String(item.mailingId || mailingId || '').trim();
    return state;
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

    const mutationResult = await mutator(draft);
    const nextState = mutationResult && typeof mutationResult === 'object' && Object.prototype.hasOwnProperty.call(mutationResult, 'value')
      ? mutationResult.value
      : draft;
    const item = buildItem(mailingScope, normalizedId, nextState);
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
    getMailingState,
    updateMailingState
  };
}

module.exports = {
  buildMailingDeliveryScope,
  createMailingDeliveryStore,
  isMailingDeliveryStoreEnabled
};
