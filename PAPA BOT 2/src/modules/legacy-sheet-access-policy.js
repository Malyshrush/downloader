const { buildEventRuntimeConfig, isCloudEventRuntimeEnabled } = require('./event-runtime-config');

function normalizeBoolean(value) {
    const normalized = String(value || '').trim().toLowerCase();
    if (!normalized) {
        return undefined;
    }
    return normalized === 'true';
}

function resolveRuntimeConfig(value) {
    if (
        value &&
        (Object.prototype.hasOwnProperty.call(value, 'mode')
            || Object.prototype.hasOwnProperty.call(value, 'incomingQueueUrl')
            || Object.prototype.hasOwnProperty.call(value, 'ydbDocApiEndpoint'))
    ) {
        return value;
    }
    return buildEventRuntimeConfig(value || process.env);
}

function buildLegacySheetAccessPolicy(config = buildEventRuntimeConfig(process.env)) {
    const runtimeConfig = resolveRuntimeConfig(config);
    return {
        allowAllLegacySheetFallback: normalizeBoolean(runtimeConfig.allowAllLegacySheetFallback),
        allowLegacySchedulerSheetAccess: normalizeBoolean(runtimeConfig.allowLegacySchedulerSheetAccess),
        allowLegacyVariablesSheetAccess: normalizeBoolean(runtimeConfig.allowLegacyVariablesSheetAccess),
        allowLegacyAppLogsSheetAccess: normalizeBoolean(runtimeConfig.allowLegacyAppLogsSheetAccess),
        cloudRuntimeEnabled: isCloudEventRuntimeEnabled(runtimeConfig)
    };
}

function isLegacySheetAccessAllowed(channel, policy = buildLegacySheetAccessPolicy()) {
    if (!policy || !policy.cloudRuntimeEnabled) {
        return true;
    }

    if (policy.allowAllLegacySheetFallback) {
        return true;
    }

    switch (String(channel || '').trim()) {
        case 'scheduler':
            return policy.allowLegacySchedulerSheetAccess || policy.allowLegacySchedulerSheetAccess !== false;
        case 'variables':
            return policy.allowLegacyVariablesSheetAccess === true;
        case 'app_logs':
            return policy.allowLegacyAppLogsSheetAccess === true;
        default:
            return false;
    }
}

function assertLegacySheetAccessAllowed(channel, policy = buildLegacySheetAccessPolicy()) {
    if (isLegacySheetAccessAllowed(channel, policy)) {
        return;
    }

    throw new Error(`Legacy sheet access is disabled for channel "${channel}" in cloud runtime`);
}

module.exports = {
    assertLegacySheetAccessAllowed,
    buildLegacySheetAccessPolicy,
    isLegacySheetAccessAllowed
};
