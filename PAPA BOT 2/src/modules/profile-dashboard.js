const { getProfileById, isMainAdminProfile } = require('./admin-profiles');
const { getProfilePromoActivationStatus } = require('./admin-security');
const { loadBotConfig, getFullConfig } = require('./config');
const { createHotStateStore } = require('./hot-state-store');
const { listUsers } = require('./users');
const { log } = require('../utils/logger');

const DASHBOARD_FILE = 'profile_dashboard.json';
const DEFAULT_LIMIT = 1000;

function createDefaultData() {
    return {
        profiles: {},
        limitRequests: []
    };
}

function getTodayKey() {
    return new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Europe/Moscow',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).format(new Date());
}

async function loadDashboardData() {
    return loadDashboardDataWithDependencies();
}

async function loadDashboardDataWithDependencies(overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    try {
        const response = await hotStateStore.loadJsonObject(DASHBOARD_FILE, {
            defaultValue: createDefaultData()
        });
        const parsed = response && response.value ? response.value : createDefaultData();
        return {
            profiles: parsed.profiles || {},
            limitRequests: Array.isArray(parsed.limitRequests) ? parsed.limitRequests : []
        };
    } catch (error) {
        log('warn', `profile-dashboard load failed: ${error.message}`);
        return createDefaultData();
    }
}

async function saveDashboardData(data) {
    return saveDashboardDataWithDependencies(data);
}

async function saveDashboardDataWithDependencies(data, overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    const normalized = {
        profiles: data?.profiles || {},
        limitRequests: Array.isArray(data?.limitRequests) ? data.limitRequests : []
    };
    await hotStateStore.saveJsonObject(DASHBOARD_FILE, normalized);
    return normalized;
}

async function ensureProfileStatsContainer(data, profileId, overrides = {}) {
    const getProfileByIdImpl = overrides.getProfileById || getProfileById;
    const profile = await getProfileByIdImpl(profileId);
    const isMainAdmin = !!(profile && isMainAdminProfile(profile));
    const profileRequestsLimit = profile?.requestsLimit && Number(profile.requestsLimit) > 0
        ? Number(profile.requestsLimit)
        : DEFAULT_LIMIT;

    if (!data.profiles[profileId]) {
        data.profiles[profileId] = {
            profileId,
            profileName: profile?.name || `Профиль ${profileId}`,
            dailyLimit: isMainAdmin ? null : profileRequestsLimit,
            dailyUsed: 0,
            dailyUsageDay: getTodayKey(),
            totalPapaRequests: 0,
            totalMessages: 0,
            totalComments: 0,
            totalTriggers: 0,
            communities: {},
            communityFiles: {},
            limitHistory: []
        };
    }

    const container = data.profiles[profileId];
    container.profileName = profile?.name || container.profileName || `Профиль ${profileId}`;
    if (container.dailyUsageDay !== getTodayKey()) {
        container.dailyUsageDay = getTodayKey();
        container.dailyUsed = 0;
    }
    if (isMainAdmin) {
        container.dailyLimit = null;
    } else {
        container.dailyLimit = profileRequestsLimit;
    }
    if (!container.communities || typeof container.communities !== 'object') {
        container.communities = {};
    }
    if (!container.communityFiles || typeof container.communityFiles !== 'object') {
        container.communityFiles = {};
    }
    if (!Array.isArray(container.limitHistory)) {
        container.limitHistory = [];
    }
    return container;
}

function ensureCommunityStats(container, communityId) {
    const key = String(communityId || 'global').trim() || 'global';
    if (!container.communities[key]) {
        container.communities[key] = {
            communityId: key,
            papaRequests: 0,
            messages: 0,
            comments: 0,
            triggers: 0,
            lastEventAt: ''
        };
    }
    return container.communities[key];
}

function ensureCommunityFilesContainer(container, communityKey) {
    const key = String(communityKey || 'global').trim() || 'global';
    if (!Array.isArray(container.communityFiles[key])) {
        container.communityFiles[key] = [];
    }
    return container.communityFiles[key];
}

function normalizeFileEntry(entry = {}) {
    return {
        attachment: String(entry.attachment || '').trim(),
        fileName: String(entry.fileName || '').trim(),
        fileType: String(entry.fileType || '').trim(),
        fileSize: Number(entry.fileSize || 0),
        uploadedAt: String(entry.uploadedAt || '').trim(),
        communityId: String(entry.communityId || '').trim(),
        vkGroupId: String(entry.vkGroupId || '').trim(),
        groupName: String(entry.groupName || '').trim()
    };
}

function detectCounterType(eventType) {
    const normalized = String(eventType || '').trim().toLowerCase();
    if (normalized === 'message_new' || normalized === 'message_reply' || normalized === 'message_event') return 'messages';
    if (normalized === 'wall_reply_new' || normalized === 'wall_reply_delete' || normalized === 'wall_repost' || normalized === 'like_add') return 'comments';
    return null;
}

async function recordProfileEventUsage(profileId, communityId, eventType) {
    const data = await loadDashboardData();
    const container = await ensureProfileStatsContainer(data, profileId);
    const dailyLimit = Number(container.dailyLimit || 0);
    const hasLimit = Number.isFinite(dailyLimit) && dailyLimit > 0;

    if (hasLimit && container.dailyUsed >= dailyLimit) {
        return {
            allowed: false,
            dailyLimit,
            dailyUsed: container.dailyUsed,
            remaining: 0
        };
    }

    container.dailyUsed += 1;
    container.totalPapaRequests += 1;
    const communityStats = ensureCommunityStats(container, communityId);
    communityStats.papaRequests += 1;
    communityStats.lastEventAt = new Date().toISOString();

    const counterType = detectCounterType(eventType);
    if (counterType === 'messages') {
        container.totalMessages += 1;
        communityStats.messages += 1;
    } else if (counterType === 'comments') {
        container.totalComments += 1;
        communityStats.comments += 1;
    }

    await saveDashboardData(data);
    return {
        allowed: true,
        dailyLimit: hasLimit ? dailyLimit : null,
        dailyUsed: container.dailyUsed,
        remaining: hasLimit ? Math.max(dailyLimit - container.dailyUsed, 0) : null
    };
}

async function canProcessProfileEvents(profileId) {
    const data = await loadDashboardData();
    const container = await ensureProfileStatsContainer(data, profileId);
    const dailyLimit = Number(container.dailyLimit || 0);
    const hasLimit = Number.isFinite(dailyLimit) && dailyLimit > 0;
    if (!hasLimit) return true;
    return Number(container.dailyUsed || 0) < dailyLimit;
}

async function recordStructuredTriggerExecution(profileId, communityId) {
    const data = await loadDashboardData();
    const container = await ensureProfileStatsContainer(data, profileId);
    const communityStats = ensureCommunityStats(container, communityId);
    container.totalTriggers += 1;
    communityStats.triggers += 1;
    communityStats.lastEventAt = new Date().toISOString();
    await saveDashboardData(data);
}

async function recordUploadedCommunityFile(payload) {
    return recordUploadedCommunityFileWithDependencies(payload);
}

async function recordUploadedCommunityFileWithDependencies(payload, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const normalizedProfileId = String(payload?.profileId || '').trim();
    if (!normalizedProfileId) {
        throw new Error('profileId is required');
    }

    const container = await ensureProfileStatsContainer(data, normalizedProfileId, overrides);
    const entry = normalizeFileEntry({
        ...payload,
        uploadedAt: payload?.uploadedAt || new Date().toISOString()
    });
    if (!entry.attachment) {
        throw new Error('attachment is required');
    }

    const communityKeys = [entry.vkGroupId, entry.communityId].filter(Boolean);
    if (communityKeys.length === 0) {
        communityKeys.push('global');
    }

    for (const communityKey of communityKeys) {
        const files = ensureCommunityFilesContainer(container, communityKey);
        const existingIndex = files.findIndex(item => String(item.attachment || '').trim() === entry.attachment);
        if (existingIndex >= 0) {
            files[existingIndex] = {
                ...files[existingIndex],
                ...entry
            };
        } else {
            files.unshift(entry);
        }
        files.sort((a, b) => String(b.uploadedAt || '').localeCompare(String(a.uploadedAt || '')));
    }

    await saveDashboardDataWithDependencies(data, overrides);
    return entry;
}

async function createProfileLimitRequest(profileId, requestedLimit) {
    const limitValue = parseInt(requestedLimit, 10);
    if (!Number.isFinite(limitValue) || limitValue <= 0) {
        throw new Error('Некорректный лимит');
    }

    const data = await loadDashboardData();
    const container = await ensureProfileStatsContainer(data, profileId);
    const existingPending = data.limitRequests.find(function(request) {
        return String(request.profileId) === String(profileId) && request.status === 'pending';
    });
    if (existingPending) {
        throw new Error('У профиля уже есть необработанный запрос на увеличение лимита');
    }

    const request = {
        id: `limit_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        profileId: String(profileId),
        profileName: container.profileName,
        requestedLimit: limitValue,
        status: 'pending',
        createdAt: new Date().toISOString(),
        resolvedAt: '',
        resolvedBy: '',
        note: ''
    };
    data.limitRequests.unshift(request);
    await saveDashboardData(data);
    return request;
}

async function resolveProfileLimitRequest(requestId, status, actorProfileId, note = '') {
    const data = await loadDashboardData();
    const request = data.limitRequests.find(function(item) {
        return String(item.id) === String(requestId);
    });
    if (!request) throw new Error('Запрос на лимит не найден');
    if (request.status !== 'pending') throw new Error('Запрос уже обработан');

    const normalizedStatus = status === 'approved' ? 'approved' : 'rejected';
    request.status = normalizedStatus;
    request.resolvedAt = new Date().toISOString();
    request.resolvedBy = String(actorProfileId || '');
    request.note = String(note || '');

    const container = await ensureProfileStatsContainer(data, request.profileId);
    if (normalizedStatus === 'approved') {
        container.dailyLimit = parseInt(request.requestedLimit, 10);
        container.limitHistory.unshift({
            at: request.resolvedAt,
            limit: container.dailyLimit,
            resolvedBy: request.resolvedBy,
            requestId: request.id,
            note: request.note || ''
        });

        const { upsertAdminProfile } = require('./admin-profiles');
        const profile = await getProfileById(request.profileId);
        if (profile) {
            await upsertAdminProfile({
                id: profile.id,
                name: profile.name,
                username: profile.username,
                password: profile.password,
                recoveryEmail: profile.recoveryEmail,
                expiresAt: profile.expiresAt,
                active: profile.active,
                role: profile.role,
                promoCodeUsed: profile.promoCodeUsed,
                requestsLimit: parseInt(request.requestedLimit, 10)
            }, actorProfileId);
        }
    }

    await saveDashboardData(data);
    return request;
}

async function getAdminLimitRequests() {
    try {
        const parsed = await loadDashboardData();
        let limitRequests = Array.isArray(parsed.limitRequests) ? parsed.limitRequests : [];
        limitRequests = limitRequests.filter(function(request) {
            return request.status === 'pending';
        });
        limitRequests = limitRequests.sort(function(a, b) {
            return String(b.createdAt || '').localeCompare(String(a.createdAt || ''));
        });
        return limitRequests;
    } catch (error) {
        log('error', `getAdminLimitRequests failed: ${error.message}`);
        return [];
    }
}

async function getProfileDashboardOverview(profileId) {
    return getProfileDashboardOverviewWithDependencies(profileId);
}

async function getProfileDashboardOverviewWithDependencies(profileId, overrides = {}) {
    const getProfileByIdImpl = overrides.getProfileById || getProfileById;
    const getProfilePromoActivationStatusImpl = overrides.getProfilePromoActivationStatus || getProfilePromoActivationStatus;
    const loadBotConfigImpl = overrides.loadBotConfig || loadBotConfig;
    const getFullConfigImpl = overrides.getFullConfig || getFullConfig;
    const listUsersImpl = overrides.listUsers || listUsers;

    const data = await loadDashboardDataWithDependencies(overrides);
    const container = await ensureProfileStatsContainer(data, profileId, overrides);
    await saveDashboardDataWithDependencies(data, overrides);

    const profile = await getProfileByIdImpl(profileId);
    const promoActivationStatus = await getProfilePromoActivationStatusImpl(profileId);
    const profileRequestsLimit = profile?.requestsLimit && Number(profile.requestsLimit) > 0
        ? Number(profile.requestsLimit)
        : (container.dailyLimit || DEFAULT_LIMIT);

    await loadBotConfigImpl(profileId);
    const fullConfig = getFullConfigImpl(profileId);
    const communities = Object.entries(fullConfig?.communities || {});
    const communitySummaries = [];
    const communityFiles = {};

    for (const [internalCommunityId, config] of communities) {
        const vkGroupId = String(config?.vk_group_id || internalCommunityId || '').trim();
        let usersCount = 0;
        try {
            const users = await listUsersImpl(vkGroupId, profileId);
            usersCount = (users || []).filter(function(row) {
                return String(row['ID'] || '').trim();
            }).length;
        } catch (_error) {
            usersCount = 0;
        }

        const stats = container.communities[vkGroupId] || container.communities[internalCommunityId] || {
            papaRequests: 0,
            messages: 0,
            comments: 0,
            triggers: 0,
            lastEventAt: ''
        };

        communitySummaries.push({
            communityId: internalCommunityId,
            vkGroupId,
            groupName: config?.group_name || `Сообщество ${vkGroupId}`,
            usersCount,
            papaRequests: stats.papaRequests || 0,
            messages: stats.messages || 0,
            comments: stats.comments || 0,
            triggers: stats.triggers || 0,
            lastEventAt: stats.lastEventAt || ''
        });

        const files = container.communityFiles[vkGroupId] || container.communityFiles[internalCommunityId] || [];
        communityFiles[vkGroupId] = files
            .map(normalizeFileEntry)
            .filter(item => item.attachment)
            .sort((a, b) => String(b.uploadedAt || '').localeCompare(String(a.uploadedAt || '')));
    }

    const limitRequests = data.limitRequests.filter(function(request) {
        return String(request.profileId) === String(profileId);
    });
    const hasLimit = Number.isFinite(profileRequestsLimit) && profileRequestsLimit > 0;

    return {
        profileId: String(profileId),
        profileName: container.profileName,
        isMainAdmin: !!(profile && isMainAdminProfile(profile)),
        dailyLimit: hasLimit ? profileRequestsLimit : null,
        dailyUsed: Number(container.dailyUsed || 0),
        dailyRemaining: hasLimit ? Math.max(profileRequestsLimit - Number(container.dailyUsed || 0), 0) : null,
        totalPapaRequests: Number(container.totalPapaRequests || 0),
        totalMessages: Number(container.totalMessages || 0),
        totalComments: Number(container.totalComments || 0),
        totalTriggers: Number(container.totalTriggers || 0),
        dailyUsageDay: container.dailyUsageDay || getTodayKey(),
        communities: communitySummaries,
        communityFiles,
        limitHistory: container.limitHistory || [],
        limitRequests,
        promoActivationStatus,
        supportPackages: [1000, 2000, 5000, 10000, 30000, 50000]
    };
}

async function deleteProfileLimitRequest(requestId, profileId, isAdmin = false) {
    const data = await loadDashboardData();
    const request = data.limitRequests.find(function(item) {
        return String(item.id) === String(requestId);
    });

    if (!request) throw new Error('Запрос на лимит не найден');
    if (!isAdmin && String(request.profileId) !== String(profileId)) {
        throw new Error('Вы не можете удалить чужой запрос');
    }

    data.limitRequests = data.limitRequests.filter(function(item) {
        return String(item.id) !== String(requestId);
    });

    await saveDashboardData(data);
    log('info', `Limit request deleted: ${requestId}`);
    return { success: true, deletedRequestId: requestId };
}

module.exports = {
    DEFAULT_LIMIT,
    canProcessProfileEvents,
    recordProfileEventUsage,
    recordStructuredTriggerExecution,
    recordUploadedCommunityFile,
    createProfileLimitRequest,
    resolveProfileLimitRequest,
    getAdminLimitRequests,
    getProfileDashboardOverview,
    deleteProfileLimitRequest,
    __testOnly: {
        loadDashboardDataWithDependencies,
        saveDashboardDataWithDependencies,
        recordUploadedCommunityFileWithDependencies,
        getProfileDashboardOverviewWithDependencies
    }
};
