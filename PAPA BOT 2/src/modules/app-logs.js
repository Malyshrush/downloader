const { log } = require('../utils/logger');
const {
    getSheetData,
    invalidateCache,
    updateSheetData,
    getFileName,
    normalizeProfileId
} = require('./storage');
const { createHotStateStore } = require('./hot-state-store');
const { createAppLogsStore, buildAppLogsScope } = require('./app-logs-store');

const APP_LOG_SHEET = '\u041b\u041e\u0413\u0418 \u041f\u0420\u0418\u041b\u041e\u0416\u0415\u041d\u0418\u042f';
const DEFAULT_LOG_TITLE = '\u0421\u0438\u0441\u0442\u0435\u043c\u043d\u043e\u0435 \u0441\u043e\u0431\u044b\u0442\u0438\u0435';
const MAX_LOG_ROWS = 300;
const SETTINGS_TTL_MS = 5000;
const settingsCache = new Map();
const appLogsStore = createAppLogsStore();

function normalizeCommunityId(communityId) {
    const normalized = String(communityId || '').trim();
    return normalized || 'global';
}

function normalizeDetails(details) {
    if (!details) return [];
    if (Array.isArray(details)) {
        return details.map(item => String(item || '').trim()).filter(Boolean);
    }
    return [String(details).trim()].filter(Boolean);
}

function getAppLogFileName(communityId, profileId = '1') {
    return getFileName(APP_LOG_SHEET, normalizeCommunityId(communityId), normalizeProfileId(profileId));
}

function getAppLogSettingsFileName(communityId, profileId = '1') {
    const pid = normalizeProfileId(profileId);
    return 'app_logs_settings_profile_' + pid + '_' + normalizeCommunityId(communityId) + '.json';
}

function getSettingsCacheKey(communityId, profileId) {
    return normalizeProfileId(profileId) + ':' + normalizeCommunityId(communityId);
}

async function getAppLogSettingsWithDependencies(communityId, profileId = '1', overrides = {}) {
    const cacheKey = getSettingsCacheKey(communityId, profileId);
    const cached = settingsCache.get(cacheKey);
    if (cached && Date.now() - cached.updatedAt < SETTINGS_TTL_MS) {
        return cached.value;
    }

    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    const response = await hotStateStore.loadJsonObject(
        getAppLogSettingsFileName(communityId, profileId),
        { defaultValue: { enabled: true } }
    );
    const settings = response && response.value ? response.value : { enabled: true };
    const normalized = { enabled: settings.enabled !== false };
    settingsCache.set(cacheKey, { value: normalized, updatedAt: Date.now() });
    return normalized;
}

async function getAppLogSettings(communityId, profileId = '1') {
    return getAppLogSettingsWithDependencies(communityId, profileId);
}

async function saveAppLogSettingsWithDependencies(communityId, profileId = '1', enabled = true, overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    const value = { enabled: !!enabled };
    await hotStateStore.saveJsonObject(getAppLogSettingsFileName(communityId, profileId), value);
    settingsCache.set(getSettingsCacheKey(communityId, profileId), { value, updatedAt: Date.now() });
    return value;
}

async function saveAppLogSettings(communityId, profileId = '1', enabled = true) {
    return saveAppLogSettingsWithDependencies(communityId, profileId, enabled);
}

function buildLogRow(entry, communityId) {
    return {
        id: 'log_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8),
        createdAt: new Date().toISOString(),
        tab: String(entry?.tab || 'SYSTEM').trim() || 'SYSTEM',
        title: String(entry?.title || '').trim() || DEFAULT_LOG_TITLE,
        summary: String(entry?.summary || '').trim(),
        details: normalizeDetails(entry?.details),
        level: String(entry?.level || 'info').trim() || 'info',
        communityId,
        meta: entry?.meta && typeof entry.meta === 'object' ? entry.meta : {}
    };
}

async function addAppLogWithDependencies(entry, overrides = {}) {
    try {
        const profileId = normalizeProfileId(entry?.profileId || '1');
        const communityId = normalizeCommunityId(entry?.communityId);
        const settingsGetter = overrides.getAppLogSettings || getAppLogSettingsWithDependencies;
        const sheetUpdater = overrides.updateSheetData || updateSheetData;
        const structuredStore = overrides.appLogsStore || appLogsStore;
        const settings = await settingsGetter(communityId, profileId, overrides);
        if (!settings.enabled) return;

        const row = buildLogRow(entry, communityId);
        if (structuredStore && typeof structuredStore.isEnabled === 'function' && structuredStore.isEnabled()) {
            await structuredStore.addLog(buildAppLogsScope(communityId, profileId), row);
            return;
        }

        await sheetUpdater(APP_LOG_SHEET, communityId, profileId, rows => {
            const nextRows = Array.isArray(rows) ? rows : [];
            nextRows.unshift(row);
            return nextRows.slice(0, MAX_LOG_ROWS);
        });
    } catch (error) {
        log('warn', 'App log write skipped: ' + error.message);
    }
}

async function addAppLog(entry) {
    return addAppLogWithDependencies(entry);
}

async function getAppLogsWithDependencies(communityId, profileId = '1', limit = 150, overrides = {}) {
    const normalizedCommunityId = normalizeCommunityId(communityId);
    const structuredStore = overrides.appLogsStore || appLogsStore;
    if (structuredStore && typeof structuredStore.isEnabled === 'function' && structuredStore.isEnabled()) {
        return structuredStore.listLogs(
            buildAppLogsScope(normalizedCommunityId, profileId),
            limit
        );
    }

    const sheetGetter = overrides.getSheetData || getSheetData;
    const rows = await sheetGetter(APP_LOG_SHEET, normalizedCommunityId, profileId);
    return (Array.isArray(rows) ? rows : []).slice(0, limit);
}

async function getAppLogs(communityId, profileId = '1', limit = 150) {
    return getAppLogsWithDependencies(communityId, profileId, limit);
}

async function clearAppLogsWithDependencies(communityId, profileId = '1', overrides = {}) {
    const normalizedCommunityId = normalizeCommunityId(communityId);
    const structuredStore = overrides.appLogsStore || appLogsStore;
    if (structuredStore && typeof structuredStore.isEnabled === 'function' && structuredStore.isEnabled()) {
        await structuredStore.clearLogs(buildAppLogsScope(normalizedCommunityId, profileId));
        invalidateCache(APP_LOG_SHEET, normalizedCommunityId, profileId);
        return;
    }

    const sheetUpdater = overrides.updateSheetData || updateSheetData;
    await sheetUpdater(APP_LOG_SHEET, normalizedCommunityId, profileId, () => []);
}

async function clearAppLogs(communityId, profileId = '1') {
    return clearAppLogsWithDependencies(communityId, profileId);
}

async function deleteAppLogsFileWithDependencies(communityId, profileId = '1', overrides = {}) {
    const normalizedCommunityId = normalizeCommunityId(communityId);
    const fileName = getAppLogFileName(normalizedCommunityId, profileId);
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    const structuredStore = overrides.appLogsStore || appLogsStore;
    if (structuredStore && typeof structuredStore.isEnabled === 'function' && structuredStore.isEnabled()) {
        await structuredStore.clearLogs(buildAppLogsScope(normalizedCommunityId, profileId));
    }
    await hotStateStore.deleteJsonObject(fileName);
    invalidateCache(APP_LOG_SHEET, normalizedCommunityId, profileId);
    return { fileName };
}

async function deleteAppLogsFile(communityId, profileId = '1') {
    return deleteAppLogsFileWithDependencies(communityId, profileId);
}

module.exports = {
    addAppLog,
    getAppLogs,
    getAppLogFileName,
    getAppLogSettings,
    saveAppLogSettings,
    clearAppLogs,
    deleteAppLogsFile,
    __testOnly: {
        addAppLogWithDependencies,
        getAppLogsWithDependencies,
        getAppLogSettingsWithDependencies,
        saveAppLogSettingsWithDependencies,
        clearAppLogsWithDependencies,
        deleteAppLogsFileWithDependencies,
        buildLogRow
    }
};
