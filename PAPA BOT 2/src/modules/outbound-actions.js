const {
  claimIncomingEvent,
  markProcessedEvent,
  releaseIncomingEventClaim
} = require('./event-queue');
const {
  sendMessageAndPerformActions,
  sendFallbackResponseFromRow
} = require('./messages');
const {
  sendCommentAndPerformActions,
  sendFallbackCommentFromRow
} = require('./comments');
const {
  processDelayedDeliveryAction,
  processMailingDeliveryAction
} = require('./scheduler');

async function dispatchOutboundAction(action, overrides = {}) {
  const sendMessageAndPerformActionsImpl = overrides.sendMessageAndPerformActions || sendMessageAndPerformActions;
  const sendFallbackResponseFromRowImpl = overrides.sendFallbackResponseFromRow || sendFallbackResponseFromRow;
  const sendCommentAndPerformActionsImpl = overrides.sendCommentAndPerformActions || sendCommentAndPerformActions;
  const sendFallbackCommentFromRowImpl = overrides.sendFallbackCommentFromRow || sendFallbackCommentFromRow;
  const processDelayedDeliveryActionImpl = overrides.processDelayedDeliveryAction || processDelayedDeliveryAction;
  const processMailingDeliveryActionImpl = overrides.processMailingDeliveryAction || processMailingDeliveryAction;
  const payload = action.payload || {};

  switch (action.actionType) {
    case 'send_message_response':
      return sendMessageAndPerformActionsImpl(
        payload.userId,
        payload.row,
        payload.originalMessage,
        false,
        payload.communityId,
        payload.profileId
      );

    case 'send_message_fallback':
      return sendFallbackResponseFromRowImpl(
        payload.userId,
        payload.row,
        payload.originalMessage,
        payload.communityId,
        payload.profileId
      );

    case 'send_comment_response':
      return sendCommentAndPerformActionsImpl(
        payload.comment,
        payload.groupId,
        payload.row,
        payload.communityId,
        payload.profileId
      );

    case 'send_comment_fallback':
      return sendFallbackCommentFromRowImpl(
        payload.comment,
        payload.groupId,
        payload.row,
        payload.communityId,
        payload.profileId
      );

    case 'send_delayed_delivery':
      return processDelayedDeliveryActionImpl(action, overrides);

    case 'send_mailing_delivery':
      return processMailingDeliveryActionImpl(action, overrides);

    default:
      throw new Error(`Unsupported outbound action type: ${action.actionType}`);
  }
}

async function processOutboundAction(action, overrides = {}) {
  if (!action || !action.actionId || !action.actionType) {
    throw new Error('Invalid outbound action');
  }

  const claimIncomingEventImpl = overrides.claimIncomingEvent || claimIncomingEvent;
  const markProcessedEventImpl = overrides.markProcessedEvent || markProcessedEvent;
  const releaseIncomingEventClaimImpl = overrides.releaseIncomingEventClaim || releaseIncomingEventClaim;
  const claim = await claimIncomingEventImpl(action.actionId, {
    eventType: `outbound:${action.actionType}`,
    profileId: String(action.profileId || action.payload?.profileId || ''),
    communityId: String(action.communityId || action.payload?.communityId || ''),
    traceId: String(action.traceId || '')
  });

  if (!claim || claim.acquired === false) {
    return {
      skipped: true,
      reason: claim?.reason || 'duplicate',
      actionId: action.actionId
    };
  }

  try {
    await dispatchOutboundAction(action, overrides);
    await markProcessedEventImpl(action.actionId, {
      eventType: `outbound:${action.actionType}`,
      profileId: String(action.profileId || action.payload?.profileId || ''),
      communityId: String(action.communityId || action.payload?.communityId || '')
    });
  } catch (error) {
    await releaseIncomingEventClaimImpl(action.actionId, {
      eventType: `outbound:${action.actionType}`,
      profileId: String(action.profileId || action.payload?.profileId || ''),
      communityId: String(action.communityId || action.payload?.communityId || ''),
      errorMessage: error.message
    });
    throw error;
  }

  return {
    ok: true,
    actionId: action.actionId
  };
}

module.exports = {
  dispatchOutboundAction,
  processOutboundAction
};
