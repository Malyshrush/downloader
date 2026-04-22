const { processStructuredTriggers } = require('./structured-triggers');
const { handleMessage } = require('./messages');
const { handleComment } = require('./comments');
const { processDelayed, processMailing } = require('./scheduler');
const {
  claimIncomingEvent,
  markProcessedEvent,
  releaseIncomingEventClaim
} = require('./event-queue');

async function processIncomingEvent(envelope, overrides = {}) {
  if (!envelope || !envelope.eventId || !envelope.eventType || !envelope.payload) {
    throw new Error('Invalid event envelope');
  }

  const claimIncomingEventImpl = overrides.claimIncomingEvent || claimIncomingEvent;
  const markProcessedEventImpl = overrides.markProcessedEvent || markProcessedEvent;
  const releaseIncomingEventClaimImpl = overrides.releaseIncomingEventClaim || releaseIncomingEventClaim;
  const processStructuredTriggersImpl = overrides.processStructuredTriggers || processStructuredTriggers;
  const handleMessageImpl = overrides.handleMessage || handleMessage;
  const handleCommentImpl = overrides.handleComment || handleComment;
  const processDelayedImpl = overrides.processDelayed || processDelayed;
  const processMailingImpl = overrides.processMailing || processMailing;

  const profileId = String(envelope.profileId || '1');
  const communityId = String(envelope.communityId || envelope.payload?.group_id || 'default');
  const data = envelope.payload;
  const claim = await claimIncomingEventImpl(envelope.eventId, {
    eventType: envelope.eventType,
    profileId,
    communityId,
    traceId: envelope.traceId || ''
  });

  if (!claim || claim.acquired === false) {
    return {
      skipped: true,
      reason: claim?.reason || 'duplicate',
      eventId: envelope.eventId
    };
  }

  try {
    await processStructuredTriggersImpl(data, profileId);

    if (envelope.eventType === 'message_new' || envelope.eventType === 'message_reply') {
      await handleMessageImpl(data, profileId);
    }

    if (envelope.eventType === 'wall_reply_new' || envelope.eventType === 'wall_reply_edit') {
      await handleCommentImpl(data, profileId);
    }

    await processDelayedImpl(communityId, profileId);
    await processMailingImpl(communityId, profileId);

    await markProcessedEventImpl(envelope.eventId, {
      eventType: envelope.eventType,
      profileId,
      communityId
    });
  } catch (error) {
    await releaseIncomingEventClaimImpl(envelope.eventId, {
      eventType: envelope.eventType,
      profileId,
      communityId,
      errorMessage: error.message
    });
    throw error;
  }

  return { ok: true, eventId: envelope.eventId };
}

module.exports = {
  processIncomingEvent
};
