const DEFAULT_YMQ_ENDPOINT = 'https://message-queue.api.cloud.yandex.net';
const DEFAULT_REGION = 'ru-central1';

function normalizeMode(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized === 'cloud' ? 'cloud' : 'stub';
}

function normalizePositiveInteger(value, fallback) {
  const numeric = Number.parseInt(String(value || '').trim(), 10);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : fallback;
}

function buildEventRuntimeConfig(env = process.env) {
  const incomingQueueUrl = String(env.YMQ_INCOMING_QUEUE_URL || '').trim();
  const outboundQueueUrl = String(env.YMQ_OUTBOUND_QUEUE_URL || '').trim();
  const ydbDocApiEndpoint = String(env.YDB_DOCAPI_ENDPOINT || '').trim();
  const hasCloudPrerequisites = Boolean(
    incomingQueueUrl &&
    outboundQueueUrl &&
    ydbDocApiEndpoint &&
    env.AWS_ACCESS_KEY_ID &&
    env.AWS_SECRET_ACCESS_KEY
  );

  const requestedMode = normalizeMode(env.EVENT_QUEUE_MODE || env.EVENT_RUNTIME_MODE);
  const mode = requestedMode === 'cloud' || hasCloudPrerequisites
    ? (hasCloudPrerequisites ? 'cloud' : 'stub')
    : 'stub';

  return {
    mode,
    ymqEndpoint: String(env.YMQ_ENDPOINT || DEFAULT_YMQ_ENDPOINT).trim() || DEFAULT_YMQ_ENDPOINT,
    ymqRegion: String(env.YMQ_REGION || DEFAULT_REGION).trim() || DEFAULT_REGION,
    incomingQueueUrl,
    outboundQueueUrl,
    ydbDocApiEndpoint,
    ydbIdempotencyTable: String(env.YDB_IDEMPOTENCY_TABLE || 'event_idempotency').trim() || 'event_idempotency',
    ydbHotStateTable: String(env.YDB_HOT_STATE_TABLE || 'hot_state_objects').trim() || 'hot_state_objects',
    ydbAppLogsTable: String(env.YDB_APP_LOGS_TABLE || 'app_logs_entries').trim() || 'app_logs_entries',
    ydbUserStateTable: String(env.YDB_USER_STATE_TABLE || 'user_state_rows').trim() || 'user_state_rows',
    ydbCommunityVariablesTable: String(env.YDB_COMMUNITY_VARIABLES_TABLE || 'community_variable_entries').trim() || 'community_variable_entries',
    ydbDelayedDeliveriesTable: String(env.YDB_DELAYED_DELIVERIES_TABLE || 'delayed_delivery_entries').trim() || 'delayed_delivery_entries',
    ydbMailingDeliveriesTable: String(env.YDB_MAILING_DELIVERIES_TABLE || 'mailing_delivery_entries').trim() || 'mailing_delivery_entries',
    ydbStructuredTriggersTable: String(env.YDB_STRUCTURED_TRIGGERS_TABLE || 'structured_trigger_entries').trim() || 'structured_trigger_entries',
    ydbProfileUserSharedTable: String(env.YDB_PROFILE_USER_SHARED_TABLE || 'profile_user_shared_state').trim() || 'profile_user_shared_state',
    ydbSharedVariablesTable: String(env.YDB_SHARED_VARIABLES_TABLE || 'shared_variables_catalog').trim() || 'shared_variables_catalog',
    idempotencyLeaseSeconds: normalizePositiveInteger(env.EVENT_IDEMPOTENCY_LEASE_SECONDS, 300),
    idempotencyRetentionDays: normalizePositiveInteger(env.EVENT_IDEMPOTENCY_RETENTION_DAYS, 30),
    awsAccessKeyId: String(env.AWS_ACCESS_KEY_ID || '').trim(),
    awsSecretAccessKey: String(env.AWS_SECRET_ACCESS_KEY || '').trim()
  };
}

function isCloudEventRuntimeEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.incomingQueueUrl &&
    config.outboundQueueUrl &&
    config.ydbDocApiEndpoint &&
    config.awsAccessKeyId &&
    config.awsSecretAccessKey
  );
}

module.exports = {
  DEFAULT_REGION,
  DEFAULT_YMQ_ENDPOINT,
  buildEventRuntimeConfig,
  isCloudEventRuntimeEnabled
};
