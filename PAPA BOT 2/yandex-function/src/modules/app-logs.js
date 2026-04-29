const { GetObjectCommand, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { log } = require('../utils/logger');
const {
    getSheetData,
    saveSheetData,
    invalidateCache,
    getS3Client,
    getBucketName,
    getFileName,
    normalizeProfileId
} = require('./storage');

const APP_LOG_SHEET = 'ЛОГИ ПРИЛОЖЕНИЯ';
const MAX_LOG_ROWS = 300;
const SETTINGS_TTL_MS = 5000;
const settingsCache = new Map();

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

async function readJsonFile(key, fallbackValue) {
    try {
        const response = await getS3Client().send(new GetObjectCommand({ Bucket: getBucketName(), Key: key }));
        const data = await response.Body.transformToString();
        return JSON.parse(data);
    } catch (error) {
        return fallbackValue;
    }
}

async function writeJsonFile(key, value) {
    await getS3Client().send(new PutObjectCommand({
        Bucket: getBucketName(),
        Key: key,
        Body: JSON.stringify(value, null, 2),
        ContentType: 'application/json'
    }));
}

function getSettingsCacheKey(communityId, profileId) {
    return normalizeProfileId(profileId) + ':' + normalizeCommunityId(communityId);
}

async function getAppLogSettings(communityId, profileId = '1') {
    const cacheKey = getSettingsCacheKey(communityId, profileId);
    const cached = settingsCache.get(cacheKey);
    if (cached && Date.now() - cached.updatedAt < SETTINGS_TTL_MS) {
        return cached.value;
    }

    const settings = await readJsonFile(getAppLogSettingsFileName(communityId, profileId), { enabled: true });
    const normalized = { enabled: settings?.enabled !== false };
    settingsCache.set(cacheKey, { value: normalized, updatedAt: Date.now() });
    return normalized;
}

async function saveAppLogSettings(communityId, profileId = '1', enabled = true) {
    const value = { enabled: !!enabled };
    await writeJsonFile(getAppLogSettingsFileName(communityId, profileId), value);
    settingsCache.set(getSettingsCacheKey(communityId, profileId), { value, updatedAt: Date.now() });
    return value;
}

async function addAppLog(entry) {
    try {
        const profileId = normalizeProfileId(entry?.profileId || '1');
        const communityId = normalizeCommunityId(entry?.communityId);
        const settings = await getAppLogSettings(communityId, profileId);
        if (!settings.enabled) return;

        const rows = await getSheetData(APP_LOG_SHEET, communityId, profileId);
        rows.unshift({
            id: 'log_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8),
            createdAt: new Date().toISOString(),
            tab: String(entry?.tab || 'SYSTEM').trim() || 'SYSTEM',
            title: String(entry?.title || '').trim() || 'Системное событие',
            summary: String(entry?.summary || '').trim(),
            details: normalizeDetails(entry?.details),
            level: String(entry?.level || 'info').trim() || 'info',
            communityId,
            meta: entry?.meta && typeof entry.meta === 'object' ? entry.meta : {}
        });

        await saveSheetData(APP_LOG_SHEET, rows.slice(0, MAX_LOG_ROWS), communityId, profileId);
        invalidateCache(APP_LOG_SHEET, communityId, profileId);
    } catch (error) {
        log('warn', '⚠️ App log write skipped: ' + error.message);
    }
}

async function getAppLogs(communityId, profileId = '1', limit = 150) {
    const rows = await getSheetData(APP_LOG_SHEET, normalizeCommunityId(communityId), profileId);
    return (Array.isArray(rows) ? rows : []).slice(0, limit);
}

async function clearAppLogs(communityId, profileId = '1') {
    const normalizedCommunityId = normalizeCommunityId(communityId);
    await saveSheetData(APP_LOG_SHEET, [], normalizedCommunityId, profileId);
    invalidateCache(APP_LOG_SHEET, normalizedCommunityId, profileId);
}

async function deleteAppLogsFile(communityId, profileId = '1') {
    const normalizedCommunityId = normalizeCommunityId(communityId);
    const fileName = getAppLogFileName(normalizedCommunityId, profileId);
    await getS3Client().send(new DeleteObjectCommand({ Bucket: getBucketName(), Key: fileName }));
    invalidateCache(APP_LOG_SHEET, normalizedCommunityId, profileId);
    return { fileName };
}

module.exports = {
    addAppLog,
    getAppLogs,
    getAppLogFileName,
    getAppLogSettings,
    saveAppLogSettings,
    clearAppLogs,
    deleteAppLogsFile
};
