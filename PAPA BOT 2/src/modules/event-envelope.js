const SUPPORTED_EVENT_TYPES = [
  'message_new',
  'message_reply',
  'message_event',
  'wall_reply_new',
  'wall_reply_edit',
  'wall_reply_delete',
  'photo_new',
  'video_new',
  'group_join',
  'group_leave',
  'wall_repost',
  'like_add'
];

function isSupportedEventType(type) {
  return SUPPORTED_EVENT_TYPES.includes(String(type || '').trim());
}

function extractUserId(type, object = {}) {
  if (type === 'message_new' || type === 'message_reply') return object.message?.from_id || null;
  if (type === 'message_event') return object.user_id || null;
  if (type === 'wall_reply_new' || type === 'wall_reply_edit' || type === 'wall_reply_delete') return object.from_id || object.deleter_id || null;
  if (type === 'group_join' || type === 'group_leave') return object.user_id || object.joined?.user_id || null;
  if (type === 'wall_repost') return object.from_id || object.owner_id || null;
  if (type === 'like_add') return object.liker_id || object.user_id || object.from_id || null;
  if (type === 'photo_new' || type === 'video_new') return object.user_id || object.owner_id || object.from_id || null;
  return null;
}

function buildEventId({ type, communityId, object = {} }) {
  const eventType = String(type || '').trim();
  const normalizedCommunityId = String(communityId || 'default').trim() || 'default';
  const objectId =
    object.message?.id ||
    object.message?.conversation_message_id ||
    object.id ||
    object.event_id ||
    object.post_id ||
    object.object_id ||
    'no_object_id';
  const userId = extractUserId(eventType, object) || 'no_user_id';
  return `vk:${eventType}:${normalizedCommunityId}:${objectId}:${userId}`;
}

function buildEventEnvelope(data, context = {}) {
  const type = String(data?.type || '').trim();
  if (!isSupportedEventType(type)) return null;

  const communityId = String(context.communityId || data?.group_id || 'default').trim() || 'default';
  const receivedAt = context.receivedAt || new Date().toISOString();
  const object = data?.object || {};

  return {
    eventId: buildEventId({ type, communityId, object }),
    eventType: type,
    profileId: String(context.profileId || '1'),
    communityId,
    userId: String(extractUserId(type, object) || ''),
    payload: data,
    createdAt: receivedAt,
    receivedAt,
    source: 'vk-callback',
    idempotencyKey: buildEventId({ type, communityId, object }),
    traceId: `evt_${Date.now().toString(36)}`,
    rawMeta: {
      hasSecret: Boolean(data?.secret),
      objectKeys: Object.keys(object)
    }
  };
}

module.exports = {
  SUPPORTED_EVENT_TYPES,
  buildEventId,
  buildEventEnvelope,
  extractUserId,
  isSupportedEventType
};
