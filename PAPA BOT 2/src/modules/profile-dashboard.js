const { getProfileById, isMainAdminProfile } = require('./admin-profiles');
const { getProfilePromoActivationStatus } = require('./admin-security');
const { loadBotConfig, getFullConfig } = require('./config');
const { createHotStateStore } = require('./hot-state-store');
const { listUsers } = require('./users');
const {
    getConsentDocumentTypeMeta,
    getNextConsentDocumentVersion,
    normalizeDocumentType
} = require('./consent-documents');
const { PAYMENT_PROVIDER_PRESETS } = require('./payment-integrations');
const {
    DEFAULT_SERVICE_LIMITS,
    normalizeServiceLimits,
    loadServiceLimits,
    calculateBalanceCreditForLimits
} = require('./service-limits');
const { log } = require('../utils/logger');
const { getAttachmentUploadSettings } = require('./attachment-upload-settings');

const DASHBOARD_FILE = 'profile_dashboard.json';
const DEFAULT_LIMIT = 1000;
const BALANCE_TOP_UP_MIN = 50;
const BALANCE_TOP_UP_MAX = 50000;
const BUG_REPORT_DAILY_LIMIT = 5;
const BUG_REPORT_FIXED_REWARD = 1000;
const BUG_REPORT_STATUSES = {
    submitted: 'Отправлено',
    in_progress: 'В процессе исправления',
    not_bug: 'Это не ошибка',
    fixed: 'Исправлено'
};
const SUGGESTION_REPORT_DAILY_LIMIT = 5;
const SUGGESTION_REPORT_IMPLEMENTED_REWARD = 1000;
const SUGGESTION_REPORT_STATUSES = {
    submitted: 'Отправлено',
    in_development: 'В Разработке',
    rejected: 'Отклонено',
    implemented: 'Реализовано'
};
const DAILY_LIMIT_PACKAGES = [
    { cost: 100, requests: 500, days: 30 },
    { cost: 200, requests: 1050, days: 30 },
    { cost: 300, requests: 1650, days: 30 },
    { cost: 400, requests: 2300, days: 30 },
    { cost: 500, requests: 3000, days: 30 },
    { cost: 1000, requests: 7000, days: 30 },
    { cost: 2000, requests: 15000, days: 30 },
    { cost: 5000, requests: 35000, days: 30 }
];
const EXTRA_LIMIT_PACKAGES = [
    { cost: 100, requests: 1000 },
    { cost: 200, requests: 2100 },
    { cost: 300, requests: 3300 },
    { cost: 400, requests: 4600 },
    { cost: 500, requests: 6000 },
    { cost: 1000, requests: 13000 },
    { cost: 2000, requests: 28000 },
    { cost: 5000, requests: 70000 }
];
const ADMIN_FINANCIAL_OPERATION_TYPES = {
    top_up: { label: 'Пополнение баланса', source: 'Платежная система' },
    purchase_daily_limit: { label: 'Покупка подписки', source: 'Баланс профиля' },
    purchase_extra_limit: { label: 'Покупка пакета', source: 'Баланс профиля' },
    promo_credit: { label: 'Промокод', source: 'Промокод' },
    bug_report_fixed_reward: { label: 'Награда за ошибку', source: 'Исправленная ошибка' },
    suggestion_report_implemented_reward: { label: 'Награда за предложение', source: 'Реализованное предложение' },
    admin_balance_adjustment: { label: 'Ручная корректировка', source: 'Главный администратор' }
};
const ADMIN_FINANCIAL_STATUS_LABELS = {
    pending: 'Ожидает',
    succeeded: 'Успешно',
    canceled: 'Отменено',
    error: 'Ошибка'
};

async function getRuntimeServiceLimits(overrides = {}) {
    if (overrides.serviceLimits) return normalizeServiceLimits(overrides.serviceLimits);
    if (typeof overrides.getServiceLimits === 'function') return normalizeServiceLimits(await overrides.getServiceLimits());
    // Dependency-injected tests use an in-memory dashboard store and should
    // not require cloud storage for unrelated service-wide settings.
    if (overrides.hotStateStore) return normalizeServiceLimits(DEFAULT_SERVICE_LIMITS);
    return loadServiceLimits();
}

function createDefaultData() {
    return {
        profiles: {},
        limitRequests: [],
        balanceTopUps: [],
        errorReports: [],
        suggestionReports: []
    };
}

function getTodayKey(date = new Date()) {
    return new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Europe/Moscow',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).format(date);
}

async function loadDashboardData() {
    return loadDashboardDataWithDependencies();
}

async function loadDashboardDataWithDependencies(overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    try {
        const response = await hotStateStore.loadJsonObject(DASHBOARD_FILE, {
            defaultValue: createDefaultData(),
            preferS3Backup: true
        });
        const parsed = response && response.value ? response.value : createDefaultData();
        return {
            profiles: parsed.profiles || {},
            limitRequests: Array.isArray(parsed.limitRequests) ? parsed.limitRequests : [],
            balanceTopUps: Array.isArray(parsed.balanceTopUps) ? parsed.balanceTopUps : [],
            errorReports: Array.isArray(parsed.errorReports) ? parsed.errorReports : [],
            suggestionReports: Array.isArray(parsed.suggestionReports) ? parsed.suggestionReports : []
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
        limitRequests: Array.isArray(data?.limitRequests) ? data.limitRequests : [],
        balanceTopUps: Array.isArray(data?.balanceTopUps) ? data.balanceTopUps : [],
        errorReports: Array.isArray(data?.errorReports) ? data.errorReports : [],
        suggestionReports: Array.isArray(data?.suggestionReports) ? data.suggestionReports : []
    };
    await hotStateStore.saveJsonObject(DASHBOARD_FILE, normalized);
    return normalized;
}

async function ensureProfileStatsContainer(data, profileId, overrides = {}) {
    const getProfileByIdImpl = overrides.getProfileById || getProfileById;
    const profile = await getProfileByIdImpl(profileId);
    const isMainAdmin = !!(profile && isMainAdminProfile(profile));
    const serviceLimits = await getRuntimeServiceLimits(overrides);
    const getAttachmentUploadSettingsImpl = overrides.getAttachmentUploadSettings || getAttachmentUploadSettings;
    const profileRequestsLimit = profile?.requestsLimit && Number(profile.requestsLimit) > 0
        ? Number(profile.requestsLimit)
        : serviceLimits.freeDailyRequests;

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
            communityDocuments: {},
            limitHistory: [],
            balance: 0,
            extraRequestLimit: 0,
            communityDailyLimits: {},
            balanceOperations: [],
            paymentIntegrations: []
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
    if (!container.communityDocuments || typeof container.communityDocuments !== 'object') {
        container.communityDocuments = {};
    }
    if (!Array.isArray(container.limitHistory)) {
        container.limitHistory = [];
    }
    if (!Number.isFinite(Number(container.balance))) {
        container.balance = 0;
    } else {
        container.balance = Math.max(0, Math.floor(Number(container.balance)));
    }
    if (!Number.isFinite(Number(container.extraRequestLimit))) {
        container.extraRequestLimit = 0;
    } else {
        container.extraRequestLimit = Math.max(0, Math.floor(Number(container.extraRequestLimit)));
    }
    if (!container.communityDailyLimits || typeof container.communityDailyLimits !== 'object') {
        container.communityDailyLimits = {};
    }
    if (!Array.isArray(container.balanceOperations)) {
        container.balanceOperations = [];
    }
    if (!Array.isArray(container.paymentIntegrations)) {
        container.paymentIntegrations = [];
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
    const stats = container.communities[key];
    if (stats.dailyUsageDay !== getTodayKey()) {
        stats.dailyUsageDay = getTodayKey();
        stats.dailyUsed = 0;
    }
    return stats;
}

function ensureCommunityFilesContainer(container, communityKey) {
    const key = String(communityKey || 'global').trim() || 'global';
    if (!Array.isArray(container.communityFiles[key])) {
        container.communityFiles[key] = [];
    }
    return container.communityFiles[key];
}

function ensureCommunityDocumentsContainer(container, communityKey, documentType) {
    const key = String(communityKey || 'global').trim() || 'global';
    const type = normalizeDocumentType(documentType);
    if (!container.communityDocuments || typeof container.communityDocuments !== 'object') {
        container.communityDocuments = {};
    }
    if (!container.communityDocuments[key] || typeof container.communityDocuments[key] !== 'object') {
        container.communityDocuments[key] = {};
    }
    if (!Array.isArray(container.communityDocuments[key][type])) {
        container.communityDocuments[key][type] = [];
    }
    return container.communityDocuments[key][type];
}

async function getCommunityDocumentLookupKeys(profileId, communityId, overrides = {}) {
    const requested = String(communityId || 'global').trim() || 'global';
    const keys = [requested];
    const loadBotConfigImpl = overrides.loadBotConfig || loadBotConfig;
    const getFullConfigImpl = overrides.getFullConfig || getFullConfig;

    try {
        await loadBotConfigImpl(profileId);
        const fullConfig = getFullConfigImpl(profileId);
        Object.entries(fullConfig?.communities || {}).forEach(function([internalCommunityId, config]) {
            const internalKey = String(internalCommunityId || '').trim();
            const vkGroupId = String(config?.vk_group_id || internalKey || '').trim();
            if (requested === internalKey || requested === vkGroupId) {
                if (vkGroupId) keys.push(vkGroupId);
                if (internalKey) keys.push(internalKey);
            }
        });
    } catch (_error) {
        // Fallback to the requested key when config is unavailable.
    }

    return [...new Set(keys.filter(Boolean))];
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

function normalizeConsentDocumentEntry(entry = {}) {
    const type = normalizeDocumentType(entry.type || entry.documentType);
    const meta = getConsentDocumentTypeMeta(type);
    return {
        type,
        shortName: String(entry.shortName || meta.shortName || meta.label || type).trim(),
        label: String(entry.label || meta.label || type).trim(),
        version: String(entry.version || '').trim(),
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

function normalizeErrorReportScreenshot(entry = {}) {
    const previewDataUrl = String(entry.previewDataUrl || '').trim();
    return {
        attachment: String(entry.attachment || '').trim(),
        fileName: String(entry.fileName || '').trim(),
        fileType: String(entry.fileType || '').trim(),
        fileSize: Number(entry.fileSize || 0),
        previewDataUrl: previewDataUrl.startsWith('data:image/') && previewDataUrl.length <= 900000 ? previewDataUrl : ''
    };
}

function normalizeBugReportStatus(status) {
    const normalized = String(status || '').trim();
    if (normalized === 'new') return 'submitted';
    return Object.prototype.hasOwnProperty.call(BUG_REPORT_STATUSES, normalized) ? normalized : 'submitted';
}

function normalizeSuggestionReportStatus(status) {
    const normalized = String(status || '').trim();
    if (normalized === 'new') return 'submitted';
    return Object.prototype.hasOwnProperty.call(SUGGESTION_REPORT_STATUSES, normalized) ? normalized : 'submitted';
}

function normalizeErrorReport(entry = {}) {
    const status = normalizeBugReportStatus(entry.status);
    return {
        id: String(entry.id || '').trim(),
        profileId: String(entry.profileId || '').trim(),
        profileName: String(entry.profileName || '').trim(),
        principalProfileId: String(entry.principalProfileId || '').trim(),
        communityId: String(entry.communityId || '').trim(),
        pageUrl: String(entry.pageUrl || '').trim(),
        userAgent: String(entry.userAgent || '').trim(),
        description: String(entry.description || '').trim(),
        screenshots: (Array.isArray(entry.screenshots) ? entry.screenshots : [])
            .map(normalizeErrorReportScreenshot)
            .filter(item => item.attachment),
        status,
        statusLabel: BUG_REPORT_STATUSES[status],
        fixedRewardGrantedAt: String(entry.fixedRewardGrantedAt || '').trim(),
        fixedRewardAmount: Math.max(0, Math.floor(Number(entry.fixedRewardAmount || 0))),
        createdAt: String(entry.createdAt || '').trim(),
        updatedAt: String(entry.updatedAt || '').trim()
    };
}

function normalizeSuggestionReport(entry = {}) {
    const status = normalizeSuggestionReportStatus(entry.status);
    return {
        id: String(entry.id || '').trim(),
        profileId: String(entry.profileId || '').trim(),
        profileName: String(entry.profileName || '').trim(),
        principalProfileId: String(entry.principalProfileId || '').trim(),
        communityId: String(entry.communityId || '').trim(),
        pageUrl: String(entry.pageUrl || '').trim(),
        userAgent: String(entry.userAgent || '').trim(),
        description: String(entry.description || '').trim(),
        screenshots: (Array.isArray(entry.screenshots) ? entry.screenshots : [])
            .map(normalizeErrorReportScreenshot)
            .filter(item => item.attachment),
        status,
        statusLabel: SUGGESTION_REPORT_STATUSES[status],
        implementedRewardGrantedAt: String(entry.implementedRewardGrantedAt || '').trim(),
        implementedRewardAmount: Math.max(0, Math.floor(Number(entry.implementedRewardAmount || 0))),
        createdAt: String(entry.createdAt || '').trim(),
        updatedAt: String(entry.updatedAt || '').trim()
    };
}

function detectCounterType(eventType) {
    const normalized = String(eventType || '').trim().toLowerCase();
    if (normalized === 'message_new' || normalized === 'message_reply' || normalized === 'message_event') return 'messages';
    if (normalized === 'wall_reply_new' || normalized === 'wall_reply_delete' || normalized === 'wall_repost' || normalized === 'like_add') return 'comments';
    return null;
}

function calculateBalanceCredit(amountRub) {
    const amount = Math.floor(Number(amountRub || 0));
    if (!Number.isFinite(amount) || amount < BALANCE_TOP_UP_MIN || amount > BALANCE_TOP_UP_MAX) {
        throw new Error(`Сумма пополнения должна быть от ${BALANCE_TOP_UP_MIN} до ${BALANCE_TOP_UP_MAX} рублей`);
    }
    const bonusRate = amount >= 5000 ? 0.2 : (amount >= 1000 ? 0.1 : 0);
    const bonus = Math.floor(amount * bonusRate);
    return {
        amountRub: amount,
        bonus,
        credit: amount + bonus
    };
}

function findPackage(packages, cost) {
    const normalizedCost = Math.floor(Number(cost || 0));
    return packages.find(item => item.cost === normalizedCost) || null;
}

function addDays(date, days) {
    const next = new Date(date.getTime());
    next.setUTCDate(next.getUTCDate() + Number(days || 0));
    return next;
}

function addMinutes(date, minutes) {
    return new Date(date.getTime() + Number(minutes || 0) * 60 * 1000);
}

function getActiveCommunityDailyPlan(container, communityId, now = new Date()) {
    const key = String(communityId || '').trim();
    const plan = key && container.communityDailyLimits ? container.communityDailyLimits[key] : null;
    if (!plan) return null;
    const expiresAt = Date.parse(plan.expiresAt || '');
    if (!Number.isFinite(expiresAt) || expiresAt <= now.getTime()) return null;
    const limit = Math.floor(Number(plan.limit || 0));
    return limit > 0 ? Object.assign({}, plan, { limit }) : null;
}

async function recordProfileEventUsageWithDependencies(profileId, communityId, eventType, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const container = await ensureProfileStatsContainer(data, profileId, overrides);
    const communityStats = ensureCommunityStats(container, communityId);
    const activeCommunityPlan = getActiveCommunityDailyPlan(container, communityId);
    const dailyLimit = activeCommunityPlan ? Number(activeCommunityPlan.limit || 0) : Number(container.dailyLimit || 0);
    const hasLimit = Number.isFinite(dailyLimit) && dailyLimit > 0;
    const currentDailyUsed = activeCommunityPlan ? Number(communityStats.dailyUsed || 0) : Number(container.dailyUsed || 0);
    let usedExtraRequestLimit = false;

    if (hasLimit && currentDailyUsed >= dailyLimit) {
        container.extraRequestLimit = Math.max(0, Math.floor(Number(container.extraRequestLimit || 0)));
        if (container.extraRequestLimit <= 0) {
            await saveDashboardDataWithDependencies(data, overrides);
            return {
                allowed: false,
                dailyLimit,
                dailyUsed: currentDailyUsed,
                remaining: 0,
                extraRequestLimit: container.extraRequestLimit
            };
        }
        container.extraRequestLimit = Math.max(0, container.extraRequestLimit - 1);
        usedExtraRequestLimit = true;
    } else if (activeCommunityPlan) {
        communityStats.dailyUsed = currentDailyUsed + 1;
    } else {
        container.dailyUsed = currentDailyUsed + 1;
    }

    container.totalPapaRequests += 1;
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

    await saveDashboardDataWithDependencies(data, overrides);
    const nextDailyUsed = activeCommunityPlan ? Number(communityStats.dailyUsed || 0) : Number(container.dailyUsed || 0);
    return {
        allowed: true,
        dailyLimit: hasLimit ? dailyLimit : null,
        dailyUsed: nextDailyUsed,
        remaining: hasLimit ? Math.max(dailyLimit - nextDailyUsed, 0) : null,
        usedExtraRequestLimit,
        extraRequestLimit: container.extraRequestLimit
    };
}

async function recordProfileEventUsage(profileId, communityId, eventType) {
    return recordProfileEventUsageWithDependencies(profileId, communityId, eventType);
}

async function canProcessProfileEvents(profileId) {
    const data = await loadDashboardData();
    const container = await ensureProfileStatsContainer(data, profileId);
    const dailyLimit = Number(container.dailyLimit || 0);
    const hasLimit = Number.isFinite(dailyLimit) && dailyLimit > 0;
    if (!hasLimit) return true;
    return Number(container.dailyUsed || 0) < dailyLimit || Number(container.extraRequestLimit || 0) > 0;
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

async function recordConsentDocumentVersion(payload) {
    return recordConsentDocumentVersionWithDependencies(payload);
}

async function deleteProfileUploadedDocument(payload) {
    return deleteProfileUploadedDocumentWithDependencies(payload);
}

async function deleteProfilePaymentOperations(profileId, paymentIds) {
    return deleteProfilePaymentOperationsWithDependencies(profileId, paymentIds);
}

async function deleteProfilePaymentOperationsWithDependencies(profileId, paymentIds, overrides = {}) {
    const normalizedProfileId = String(profileId || '').trim();
    const normalizedIds = Array.from(new Set(
        (Array.isArray(paymentIds) ? paymentIds : [])
            .map(value => String(value || '').trim())
            .filter(Boolean)
    ));
    if (!normalizedProfileId) throw new Error('profileId is required');
    if (!normalizedIds.length) throw new Error('paymentIds are required');

    const data = await loadDashboardDataWithDependencies(overrides);
    const container = data.profiles[normalizedProfileId];
    const payments = Array.isArray(container?.paymentButtonPayments)
        ? container.paymentButtonPayments
        : [];
    const ids = new Set(normalizedIds);
    const remaining = payments.filter(item => {
        const paymentId = String(item?.paymentId || item?.id || item?.providerPaymentId || '').trim();
        return !ids.has(paymentId);
    });
    const removedCount = payments.length - remaining.length;

    if (container && removedCount > 0) {
        container.paymentButtonPayments = remaining;
        await saveDashboardDataWithDependencies(data, overrides);
    }

    return {
        removedCount,
        requestedCount: normalizedIds.length
    };
}

async function recordConsentDocumentVersionWithDependencies(payload, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const normalizedProfileId = String(payload?.profileId || '').trim();
    const communityKey = String(payload?.vkGroupId || payload?.communityId || '').trim();
    const attachment = String(payload?.attachment || '').trim();
    if (!normalizedProfileId) throw new Error('profileId is required');
    if (!communityKey) throw new Error('communityId is required');
    if (!attachment) throw new Error('attachment is required');

    const type = normalizeDocumentType(payload?.documentType || payload?.type);
    const container = await ensureProfileStatsContainer(data, normalizedProfileId, overrides);
    const versions = ensureCommunityDocumentsContainer(container, communityKey, type);
    const version = getNextConsentDocumentVersion(versions);
    const meta = getConsentDocumentTypeMeta(type);
    const entry = normalizeConsentDocumentEntry({
        ...payload,
        type,
        shortName: payload?.shortName || meta.shortName,
        label: payload?.label || meta.label,
        version,
        uploadedAt: payload?.uploadedAt || new Date().toISOString()
    });

    versions.unshift(entry);
    versions.sort((a, b) => String(b.uploadedAt || '').localeCompare(String(a.uploadedAt || '')));
    await saveDashboardDataWithDependencies(data, overrides);
    return entry;
}

async function getLatestConsentDocumentVersion(profileId, communityId, documentType, overrides = {}) {
    return getLatestConsentDocumentVersionWithDependencies(profileId, communityId, documentType, overrides);
}

async function getLatestConsentDocumentVersionWithDependencies(profileId, communityId, documentType, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const container = data.profiles && data.profiles[String(profileId || '').trim()];
    if (!container) return null;
    const type = normalizeDocumentType(documentType);
    const keys = await getCommunityDocumentLookupKeys(profileId, communityId, overrides);
    let versions = [];
    for (const key of keys) {
        versions = container.communityDocuments
            && container.communityDocuments[key]
            && Array.isArray(container.communityDocuments[key][type])
            ? container.communityDocuments[key][type]
            : [];
        if (versions.length) break;
    }
    return versions.length ? normalizeConsentDocumentEntry(versions[0]) : null;
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

async function deleteProfileUploadedDocumentWithDependencies(payload, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const normalizedProfileId = String(payload?.profileId || '').trim();
    const communityKey = String(payload?.vkGroupId || payload?.communityId || '').trim();
    const attachment = String(payload?.attachment || '').trim();
    const version = String(payload?.version || '').trim();
    const kind = String(payload?.kind || payload?.type || 'file').trim();

    if (!normalizedProfileId) throw new Error('profileId is required');
    if (!communityKey) throw new Error('communityId is required');
    if (!attachment) throw new Error('attachment is required');

    const container = await ensureProfileStatsContainer(data, normalizedProfileId, overrides);
    let deleted = null;

    if (kind === 'consent_document') {
        const documentType = normalizeDocumentType(payload?.documentType);
        const documentsByType = container.communityDocuments && container.communityDocuments[communityKey]
            ? container.communityDocuments[communityKey]
            : null;
        const versions = documentsByType && Array.isArray(documentsByType[documentType])
            ? documentsByType[documentType]
            : [];
        const nextVersions = versions.filter(function(item) {
            const sameAttachment = String(item?.attachment || '').trim() === attachment;
            const sameVersion = !version || String(item?.version || '').trim() === version;
            if (sameAttachment && sameVersion && !deleted) {
                deleted = normalizeConsentDocumentEntry(item);
                return false;
            }
            return true;
        });

        if (!deleted) {
            throw new Error('document not found');
        }

        documentsByType[documentType] = nextVersions;
    } else {
        const files = container.communityFiles && Array.isArray(container.communityFiles[communityKey])
            ? container.communityFiles[communityKey]
            : [];
        const nextFiles = files.filter(function(item) {
            if (String(item?.attachment || '').trim() === attachment && !deleted) {
                deleted = normalizeFileEntry(item);
                return false;
            }
            return true;
        });

        if (!deleted) {
            throw new Error('file not found');
        }

        container.communityFiles[communityKey] = nextFiles;
    }

    await saveDashboardDataWithDependencies(data, overrides);
    return {
        success: true,
        profileId: normalizedProfileId,
        communityId: communityKey,
        kind,
        deleted
    };
}

function buildVkCommunityUrl(communityId) {
    const normalized = String(communityId || '').trim().replace(/^club/i, '').replace(/^-/, '');
    return normalized ? `https://vk.com/club${normalized}` : '';
}

async function createProfileLimitRequest(profileId, requestedLimit, context = {}) {
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
        communityId: String(context.communityId || '').trim(),
        communityName: String(context.communityName || '').trim(),
        communityUrl: String(context.communityUrl || '').trim() || buildVkCommunityUrl(context.communityId),
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

async function createBalanceTopUp(profileId, amountRub, context = {}) {
    return createBalanceTopUpWithDependencies(profileId, amountRub, context);
}

async function createBalanceTopUpWithDependencies(profileId, amountRub, context = {}, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const container = await ensureProfileStatsContainer(data, profileId, overrides);
    const serviceLimits = await getRuntimeServiceLimits(overrides);
    const creditInfo = calculateBalanceCreditForLimits(amountRub, serviceLimits);
    const topUp = {
        id: context.id || `topup_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        profileId: String(profileId),
        profileName: container.profileName,
        amountRub: creditInfo.amountRub,
        bonus: creditInfo.bonus,
        credit: creditInfo.credit,
        provider: String(context.provider || 'manual').trim(),
        providerPaymentId: String(context.providerPaymentId || '').trim(),
        status: 'pending',
        confirmationUrl: '',
        createdAt: new Date().toISOString(),
        paidAt: '',
        creditedAt: '',
        rawStatus: '',
        error: ''
    };
    data.balanceTopUps = Array.isArray(data.balanceTopUps) ? data.balanceTopUps : [];
    data.balanceTopUps.unshift(topUp);
    await saveDashboardDataWithDependencies(data, overrides);
    return topUp;
}

async function attachProviderPaymentToTopUp(topUpId, payment = {}, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    data.balanceTopUps = Array.isArray(data.balanceTopUps) ? data.balanceTopUps : [];
    let topUp = data.balanceTopUps.find(item => String(item.id) === String(topUpId));
    if (!topUp && overrides.fallbackTopUp) {
        topUp = {
            ...overrides.fallbackTopUp,
            id: String(overrides.fallbackTopUp.id || topUpId),
            status: overrides.fallbackTopUp.status || 'pending'
        };
        data.balanceTopUps.unshift(topUp);
    }
    if (!topUp) throw new Error('Пополнение баланса не найдено');
    topUp.providerPaymentId = String(payment.id || payment.providerPaymentId || '').trim();
    topUp.confirmationUrl = String(payment.confirmationUrl || payment.confirmation?.confirmation_url || '').trim();
    topUp.rawStatus = String(payment.status || '').trim();
    await saveDashboardDataWithDependencies(data, overrides);
    return topUp;
}

async function listPendingBalanceTopUps(profileId = '', overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const normalizedProfileId = String(profileId || '').trim();
    return (Array.isArray(data.balanceTopUps) ? data.balanceTopUps : [])
        .filter(function(item) {
            if (normalizedProfileId && String(item.profileId) !== normalizedProfileId) return false;
            if (String(item.status || 'pending') !== 'pending') return false;
            return !!String(item.providerPaymentId || '').trim();
        })
        .map(item => Object.assign({}, item));
}

async function confirmBalanceTopUp(topUpId, payment = {}) {
    return confirmBalanceTopUpWithDependencies(topUpId, payment);
}

async function confirmBalanceTopUpWithDependencies(topUpId, payment = {}, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    data.balanceTopUps = Array.isArray(data.balanceTopUps) ? data.balanceTopUps : [];
    let topUp = data.balanceTopUps.find(item => String(item.id) === String(topUpId));
    if (!topUp && payment.metadata?.profileId) {
        const amountRub = Math.floor(Number(payment.amount?.value || payment.amountRub || 0));
        const credit = Math.floor(Number(payment.metadata?.credit || amountRub || 0));
        topUp = {
            id: String(topUpId),
            profileId: String(payment.metadata.profileId),
            profileName: '',
            amountRub: Math.max(0, amountRub),
            bonus: Math.max(0, credit - Math.max(0, amountRub)),
            credit: Math.max(0, credit),
            provider: 'yookassa',
            providerPaymentId: String(payment.providerPaymentId || payment.id || '').trim(),
            status: 'pending',
            confirmationUrl: '',
            createdAt: payment.created_at || new Date().toISOString(),
            paidAt: '',
            creditedAt: '',
            rawStatus: '',
            error: '',
            recoveredFromPayment: true
        };
        data.balanceTopUps.unshift(topUp);
    }
    if (!topUp) throw new Error('Пополнение баланса не найдено');
    const container = await ensureProfileStatsContainer(data, topUp.profileId, overrides);

    if (topUp.status === 'succeeded' && topUp.creditedAt) {
        return Object.assign({}, topUp, { alreadyCredited: true, balance: container.balance });
    }

    topUp.status = 'succeeded';
    topUp.providerPaymentId = String(payment.providerPaymentId || payment.id || topUp.providerPaymentId || '').trim();
    topUp.rawStatus = String(payment.rawStatus || payment.status || 'succeeded').trim();
    topUp.paidAt = payment.paidAt || new Date().toISOString();
    topUp.creditedAt = new Date().toISOString();
    container.balance = Math.max(0, Math.floor(Number(container.balance || 0))) + Math.max(0, Math.floor(Number(topUp.credit || 0)));
    container.balanceOperations.unshift({
        id: `balance_op_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        type: 'top_up',
        amount: Math.max(0, Math.floor(Number(topUp.credit || 0))),
        balanceAfter: container.balance,
        sourceId: topUp.id,
        providerPaymentId: topUp.providerPaymentId,
        createdAt: topUp.creditedAt
    });
    container.balanceOperations = container.balanceOperations.slice(0, 300);
    await saveDashboardDataWithDependencies(data, overrides);
    return Object.assign({}, topUp, { balance: container.balance });
}

async function purchaseDailyLimitPackage(profileId, communityId, cost) {
    return purchaseDailyLimitPackageWithDependencies(profileId, communityId, cost);
}

async function purchaseDailyLimitPackageWithDependencies(profileId, communityId, cost, overrides = {}) {
    const serviceLimits = await getRuntimeServiceLimits(overrides);
    const pkg = findPackage(serviceLimits.subscriptions, cost);
    if (!pkg) throw new Error('Пакет суточного лимита не найден');
    const normalizedCommunityId = String(communityId || '').trim();
    if (!normalizedCommunityId) throw new Error('Укажите сообщество для покупки суточного лимита');
    const data = await loadDashboardDataWithDependencies(overrides);
    const container = await ensureProfileStatsContainer(data, profileId, overrides);
    if (container.balance < pkg.cost) throw new Error('Недостаточно баланса');
    container.balance -= pkg.cost;
    const purchasedAt = new Date();
    container.communityDailyLimits[normalizedCommunityId] = {
        communityId: normalizedCommunityId,
        limit: pkg.requests,
        cost: pkg.cost,
        purchasedAt: purchasedAt.toISOString(),
        expiresAt: addMinutes(purchasedAt, pkg.durationMinutes).toISOString()
    };
    container.balanceOperations.unshift({
        id: `balance_op_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        type: 'purchase_daily_limit',
        amount: -pkg.cost,
        balanceAfter: container.balance,
        communityId: normalizedCommunityId,
        requests: pkg.requests,
        expiresAt: container.communityDailyLimits[normalizedCommunityId].expiresAt,
        createdAt: purchasedAt.toISOString()
    });
    await saveDashboardDataWithDependencies(data, overrides);
    return { success: true, balance: container.balance, package: pkg, communityPlan: container.communityDailyLimits[normalizedCommunityId] };
}

async function purchaseExtraLimitPackage(profileId, cost) {
    return purchaseExtraLimitPackageWithDependencies(profileId, cost);
}

async function purchaseExtraLimitPackageWithDependencies(profileId, cost, overrides = {}) {
    const serviceLimits = await getRuntimeServiceLimits(overrides);
    const pkg = findPackage(serviceLimits.extraPackages, cost);
    if (!pkg) throw new Error('Пакет вне суточного лимита не найден');
    const data = await loadDashboardDataWithDependencies(overrides);
    const container = await ensureProfileStatsContainer(data, profileId, overrides);
    if (container.balance < pkg.cost) throw new Error('Недостаточно баланса');
    container.balance -= pkg.cost;
    container.extraRequestLimit = Math.max(0, Math.floor(Number(container.extraRequestLimit || 0))) + pkg.requests;
    const createdAt = new Date().toISOString();
    container.balanceOperations.unshift({
        id: `balance_op_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        type: 'purchase_extra_limit',
        amount: -pkg.cost,
        balanceAfter: container.balance,
        requests: pkg.requests,
        extraRequestLimitAfter: container.extraRequestLimit,
        createdAt
    });
    await saveDashboardDataWithDependencies(data, overrides);
    return { success: true, balance: container.balance, package: pkg, extraRequestLimit: container.extraRequestLimit };
}

async function grantProfilePromoCredits(profileId, promo) {
    return grantProfilePromoCreditsWithDependencies(profileId, promo);
}

async function grantProfilePromoCreditsWithDependencies(profileId, promo, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const container = await ensureProfileStatsContainer(data, String(profileId), overrides);
    const balanceCredit = Math.max(0, Math.floor(Number(promo?.balanceCredit || 0)));
    const extraRequestLimitCredit = Math.max(0, Math.floor(Number(promo?.extraRequestLimitCredit || 0)));

    if (balanceCredit > 0) {
        container.balance = Math.max(0, Math.floor(Number(container.balance || 0))) + balanceCredit;
    }
    if (extraRequestLimitCredit > 0) {
        container.extraRequestLimit = Math.max(0, Math.floor(Number(container.extraRequestLimit || 0))) + extraRequestLimitCredit;
    }
    if (balanceCredit > 0 || extraRequestLimitCredit > 0) {
        container.balanceOperations.unshift({
            id: `balance_op_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            type: 'promo_credit',
            amount: balanceCredit,
            balanceAfter: container.balance,
            extraRequestLimitAfter: container.extraRequestLimit,
            extraRequestLimitCredit,
            promoCode: String(promo?.code || '').trim().toUpperCase(),
            createdAt: new Date().toISOString()
        });
        container.balanceOperations = container.balanceOperations.slice(0, 300);
    }

    await saveDashboardDataWithDependencies(data, overrides);
    return {
        success: true,
        balance: container.balance,
        extraRequestLimit: container.extraRequestLimit,
        balanceCredit,
        extraRequestLimitCredit
    };
}

async function setProfileBalanceFields(profileId, fields = {}) {
    return setProfileBalanceFieldsWithDependencies(profileId, fields);
}

async function setProfileBalanceFieldsWithDependencies(profileId, fields = {}, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const container = await ensureProfileStatsContainer(data, String(profileId), overrides);
    const hasBalance = Object.prototype.hasOwnProperty.call(fields, 'balance') && fields.balance !== null && fields.balance !== undefined && fields.balance !== '';
    const hasExtraRequestLimit = Object.prototype.hasOwnProperty.call(fields, 'extraRequestLimit') && fields.extraRequestLimit !== null && fields.extraRequestLimit !== undefined && fields.extraRequestLimit !== '';
    const beforeBalance = Math.max(0, Math.floor(Number(container.balance || 0)));
    const beforeExtraRequestLimit = Math.max(0, Math.floor(Number(container.extraRequestLimit || 0)));

    if (hasBalance) {
        const parsedBalance = Math.floor(Number(fields.balance || 0));
        if (!Number.isFinite(parsedBalance) || parsedBalance < 0) throw new Error('Баланс должен быть неотрицательным числом');
        container.balance = parsedBalance;
    }
    if (hasExtraRequestLimit) {
        const parsedExtraRequestLimit = Math.floor(Number(fields.extraRequestLimit || 0));
        if (!Number.isFinite(parsedExtraRequestLimit) || parsedExtraRequestLimit < 0) throw new Error('Вне суточный лимит должен быть неотрицательным числом');
        container.extraRequestLimit = parsedExtraRequestLimit;
    }

    if ((hasBalance && container.balance !== beforeBalance) || (hasExtraRequestLimit && container.extraRequestLimit !== beforeExtraRequestLimit)) {
        container.balanceOperations.unshift({
            id: `balance_op_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            type: 'admin_balance_adjustment',
            amount: Math.max(0, Math.floor(Number(container.balance || 0))) - beforeBalance,
            balanceAfter: Math.max(0, Math.floor(Number(container.balance || 0))),
            extraRequestLimitBefore: beforeExtraRequestLimit,
            extraRequestLimitAfter: Math.max(0, Math.floor(Number(container.extraRequestLimit || 0))),
            createdAt: new Date().toISOString()
        });
        container.balanceOperations = container.balanceOperations.slice(0, 300);
    }

    await saveDashboardDataWithDependencies(data, overrides);
    return {
        success: true,
        balance: Math.max(0, Math.floor(Number(container.balance || 0))),
        extraRequestLimit: Math.max(0, Math.floor(Number(container.extraRequestLimit || 0)))
    };
}

async function getAdminProfileBalanceSummaries() {
    return getAdminProfileBalanceSummariesWithDependencies();
}

async function getAdminProfileBalanceSummariesWithDependencies(overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const summaries = {};
    Object.entries(data.profiles || {}).forEach(function([profileId, container]) {
        summaries[String(profileId)] = {
            balance: Math.max(0, Math.floor(Number(container?.balance || 0))),
            extraRequestLimit: Math.max(0, Math.floor(Number(container?.extraRequestLimit || 0)))
        };
    });
    return summaries;
}

function normalizeAdminFinancialStatus(status, defaultStatus = 'succeeded') {
    const normalized = String(status || defaultStatus).trim().toLowerCase();
    if (normalized === 'success' || normalized === 'paid') return 'succeeded';
    if (normalized === 'cancelled' || normalized === 'failed') return 'canceled';
    if (ADMIN_FINANCIAL_STATUS_LABELS[normalized]) return normalized;
    return defaultStatus;
}

function toAdminFinancialNumber(value) {
    if (value === null || value === undefined || value === '') return null;
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
}

function getAdminFinancialOperationLimitDelta(operation, type) {
    if (type === 'purchase_daily_limit' || type === 'purchase_extra_limit') {
        return toAdminFinancialNumber(operation?.requests);
    }
    if (type === 'promo_credit') {
        return toAdminFinancialNumber(operation?.extraRequestLimitCredit);
    }
    if (type === 'admin_balance_adjustment') {
        const before = toAdminFinancialNumber(operation?.extraRequestLimitBefore);
        const after = toAdminFinancialNumber(operation?.extraRequestLimitAfter);
        return before !== null && after !== null ? after - before : null;
    }
    if (type === 'bug_report_fixed_reward') return BUG_REPORT_FIXED_REWARD;
    if (type === 'suggestion_report_implemented_reward') return SUGGESTION_REPORT_IMPLEMENTED_REWARD;
    return null;
}

function buildAdminFinancialOperationDescription(operation, originalType) {
    const details = [];
    if (operation?.promoCode) details.push(`Промокод: ${operation.promoCode}`);
    if (operation?.bugReportId) details.push(`Ошибка: ${operation.bugReportId}`);
    if (operation?.suggestionReportId) details.push(`Предложение: ${operation.suggestionReportId}`);
    if (operation?.note) details.push(String(operation.note));
    if (!ADMIN_FINANCIAL_OPERATION_TYPES[originalType]) details.push(`Исходный тип: ${originalType || 'не указан'}`);
    return details.join('. ');
}

function normalizeAdminFinancialOperation(profileId, container, operation = {}) {
    const originalType = String(operation.type || '').trim();
    const knownType = ADMIN_FINANCIAL_OPERATION_TYPES[originalType];
    const type = knownType ? originalType : 'other';
    const status = normalizeAdminFinancialStatus(operation.status);
    const amount = toAdminFinancialNumber(operation.amount);
    const isPurchase = type === 'purchase_daily_limit' || type === 'purchase_extra_limit';
    return {
        id: String(operation.id || '').trim(),
        createdAt: String(operation.createdAt || operation.updatedAt || '').trim(),
        profileId: String(profileId || container?.profileId || '').trim(),
        profileName: String(container?.profileName || `Профиль ${profileId}`).trim(),
        type,
        typeLabel: knownType?.label || 'Прочая операция',
        status,
        statusLabel: ADMIN_FINANCIAL_STATUS_LABELS[status] || 'Успешно',
        amountRub: isPurchase && amount !== null ? Math.abs(amount) : null,
        balanceDelta: amount,
        limitDelta: getAdminFinancialOperationLimitDelta(operation, originalType),
        communityId: String(operation.communityId || '').trim(),
        communityName: String(operation.communityName || '').trim(),
        source: knownType?.source || 'История баланса',
        description: buildAdminFinancialOperationDescription(operation, originalType),
        providerPaymentId: String(operation.providerPaymentId || '').trim()
    };
}

function normalizeAdminBalanceTopUp(topUp = {}, profiles = {}) {
    const profileId = String(topUp.profileId || '').trim();
    const container = profiles[profileId] || {};
    const status = normalizeAdminFinancialStatus(topUp.status, 'pending');
    const description = [
        topUp.provider ? `Провайдер: ${topUp.provider}` : '',
        topUp.bonus ? `Бонус: ${topUp.bonus}` : '',
        topUp.error ? String(topUp.error) : ''
    ].filter(Boolean).join('. ');
    return {
        id: String(topUp.id || '').trim(),
        createdAt: String(topUp.paidAt || topUp.creditedAt || topUp.createdAt || '').trim(),
        profileId,
        profileName: String(topUp.profileName || container.profileName || `Профиль ${profileId}`).trim(),
        type: 'top_up',
        typeLabel: ADMIN_FINANCIAL_OPERATION_TYPES.top_up.label,
        status,
        statusLabel: ADMIN_FINANCIAL_STATUS_LABELS[status] || ADMIN_FINANCIAL_STATUS_LABELS.pending,
        amountRub: toAdminFinancialNumber(topUp.amountRub),
        balanceDelta: status === 'succeeded' ? toAdminFinancialNumber(topUp.credit) : null,
        limitDelta: null,
        communityId: '',
        communityName: '',
        source: String(topUp.provider || ADMIN_FINANCIAL_OPERATION_TYPES.top_up.source).trim(),
        description,
        providerPaymentId: String(topUp.providerPaymentId || '').trim()
    };
}

function sortAdminFinancialOperations(rows) {
    return rows.sort(function(a, b) {
        const left = Date.parse(a.createdAt || '');
        const right = Date.parse(b.createdAt || '');
        if (Number.isFinite(left) && Number.isFinite(right)) return right - left;
        if (Number.isFinite(right)) return 1;
        if (Number.isFinite(left)) return -1;
        return 0;
    });
}

async function getAdminFinancialOperations() {
    return getAdminFinancialOperationsWithDependencies();
}

async function getAdminFinancialOperationsWithDependencies(overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const topUps = Array.isArray(data.balanceTopUps) ? data.balanceTopUps : [];
    const topUpIds = new Set(topUps.map(item => String(item?.id || '').trim()).filter(Boolean));
    const providerPaymentIds = new Set(topUps.map(item => String(item?.providerPaymentId || '').trim()).filter(Boolean));
    const rows = topUps.map(topUp => normalizeAdminBalanceTopUp(topUp, data.profiles || {}));

    Object.entries(data.profiles || {}).forEach(function([profileId, container]) {
        (Array.isArray(container?.balanceOperations) ? container.balanceOperations : []).forEach(function(operation) {
            const sourceId = String(operation?.sourceId || '').trim();
            const providerPaymentId = String(operation?.providerPaymentId || '').trim();
            if (
                String(operation?.type || '').trim() === 'top_up' &&
                ((sourceId && topUpIds.has(sourceId)) ||
                    (!sourceId && providerPaymentId && providerPaymentIds.has(providerPaymentId)))
            ) {
                return;
            }
            rows.push(normalizeAdminFinancialOperation(profileId, container, operation));
        });
    });

    return sortAdminFinancialOperations(rows);
}

async function getAdminLimitRequests() {
    try {
        const parsed = await loadDashboardData();
        let limitRequests = Array.isArray(parsed.limitRequests) ? parsed.limitRequests : [];
        limitRequests = limitRequests.sort(function(a, b) {
            return String(b.createdAt || '').localeCompare(String(a.createdAt || ''));
        });
        return limitRequests;
    } catch (error) {
        log('error', `getAdminLimitRequests failed: ${error.message}`);
        return [];
    }
}

async function getAdminBalanceTopUps() {
    try {
        const parsed = await loadDashboardData();
        return (Array.isArray(parsed.balanceTopUps) ? parsed.balanceTopUps : [])
            .sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
    } catch (error) {
        log('error', `getAdminBalanceTopUps failed: ${error.message}`);
        return [];
    }
}

async function recordProfileErrorReport(profileId, payload = {}) {
    return recordProfileErrorReportWithDependencies(profileId, payload);
}

async function recordProfileErrorReportWithDependencies(profileId, payload = {}, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const serviceLimits = await getRuntimeServiceLimits(overrides);
    const normalizedProfileId = String(profileId || payload.profileId || '').trim();
    if (!normalizedProfileId) throw new Error('profileId is required');
    const description = String(payload.description || '').trim();
    if (!description) throw new Error('Опишите обнаруженную ошибку');

    const container = await ensureProfileStatsContainer(data, normalizedProfileId, overrides);
    data.errorReports = Array.isArray(data.errorReports) ? data.errorReports : [];
    const createdAt = payload.createdAt || new Date().toISOString();
    const todayKey = getTodayKey(new Date(createdAt));
    const reportsToday = data.errorReports.filter(function(item) {
        if (String(item?.profileId || '').trim() !== normalizedProfileId) return false;
        const created = Date.parse(item?.createdAt || '');
        if (!Number.isFinite(created)) return false;
        return getTodayKey(new Date(created)) === todayKey;
    }).length;
    if (reportsToday >= serviceLimits.reports.bugDailyLimit) {
        throw new Error(`Лимит отправки ошибок на сегодня исчерпан: ${serviceLimits.reports.bugDailyLimit} из ${serviceLimits.reports.bugDailyLimit}`);
    }

    const report = normalizeErrorReport({
        id: payload.id || `error_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        profileId: normalizedProfileId,
        profileName: payload.profileName || container.profileName,
        principalProfileId: payload.principalProfileId,
        communityId: payload.communityId,
        pageUrl: payload.pageUrl,
        userAgent: payload.userAgent,
        description,
        screenshots: payload.screenshots,
        status: 'submitted',
        createdAt
    });

    data.errorReports.unshift(report);
    data.errorReports = data.errorReports.slice(0, 300);
    await saveDashboardDataWithDependencies(data, overrides);
    return report;
}

async function getAdminErrorReports() {
    try {
        const parsed = await loadDashboardData();
        return (Array.isArray(parsed.errorReports) ? parsed.errorReports : [])
            .map(normalizeErrorReport)
            .filter(item => item.id && item.description)
            .sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
    } catch (error) {
        log('error', `getAdminErrorReports failed: ${error.message}`);
        return [];
    }
}

async function recordProfileSuggestionReport(profileId, payload = {}) {
    return recordProfileSuggestionReportWithDependencies(profileId, payload);
}

async function recordProfileSuggestionReportWithDependencies(profileId, payload = {}, overrides = {}) {
    const data = await loadDashboardDataWithDependencies(overrides);
    const serviceLimits = await getRuntimeServiceLimits(overrides);
    const normalizedProfileId = String(profileId || payload.profileId || '').trim();
    if (!normalizedProfileId) throw new Error('profileId is required');
    const description = String(payload.description || '').trim();
    if (!description) throw new Error('Опишите предложение');

    const container = await ensureProfileStatsContainer(data, normalizedProfileId, overrides);
    data.suggestionReports = Array.isArray(data.suggestionReports) ? data.suggestionReports : [];
    const createdAt = payload.createdAt || new Date().toISOString();
    const todayKey = getTodayKey(new Date(createdAt));
    const reportsToday = data.suggestionReports.filter(function(item) {
        if (String(item?.profileId || '').trim() !== normalizedProfileId) return false;
        const created = Date.parse(item?.createdAt || '');
        if (!Number.isFinite(created)) return false;
        return getTodayKey(new Date(created)) === todayKey;
    }).length;
    if (reportsToday >= serviceLimits.reports.suggestionDailyLimit) {
        throw new Error(`Лимит отправки предложений на сегодня исчерпан: ${serviceLimits.reports.suggestionDailyLimit} из ${serviceLimits.reports.suggestionDailyLimit}`);
    }

    const report = normalizeSuggestionReport({
        id: payload.id || `suggestion_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        profileId: normalizedProfileId,
        profileName: payload.profileName || container.profileName,
        principalProfileId: payload.principalProfileId,
        communityId: payload.communityId,
        pageUrl: payload.pageUrl,
        userAgent: payload.userAgent,
        description,
        screenshots: payload.screenshots,
        status: 'submitted',
        createdAt
    });

    data.suggestionReports.unshift(report);
    data.suggestionReports = data.suggestionReports.slice(0, 300);
    await saveDashboardDataWithDependencies(data, overrides);
    return report;
}

async function getAdminSuggestionReports() {
    try {
        const parsed = await loadDashboardData();
        return (Array.isArray(parsed.suggestionReports) ? parsed.suggestionReports : [])
            .map(normalizeSuggestionReport)
            .filter(item => item.id && item.description)
            .sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
    } catch (error) {
        log('error', `getAdminSuggestionReports failed: ${error.message}`);
        return [];
    }
}

async function updateBugReportStatus(reportId, status, actorProfileId = '') {
    return updateBugReportStatusWithDependencies(reportId, status, actorProfileId);
}

async function updateBugReportStatusWithDependencies(reportId, status, actorProfileId = '', overrides = {}) {
    const normalizedReportId = String(reportId || '').trim();
    if (!normalizedReportId) throw new Error('reportId is required');
    const normalizedStatus = normalizeBugReportStatus(status);
    const data = await loadDashboardDataWithDependencies(overrides);
    const serviceLimits = await getRuntimeServiceLimits(overrides);
    data.errorReports = Array.isArray(data.errorReports) ? data.errorReports : [];
    const index = data.errorReports.findIndex(item => String(item?.id || '').trim() === normalizedReportId);
    if (index < 0) throw new Error('Сообщение об ошибке не найдено');

    const current = normalizeErrorReport(data.errorReports[index]);
    const now = new Date().toISOString();
    current.status = normalizedStatus;
    current.statusLabel = BUG_REPORT_STATUSES[normalizedStatus];
    current.updatedAt = now;
    current.updatedBy = String(actorProfileId || '').trim();

    let reward = null;
    if (normalizedStatus === 'fixed' && !current.fixedRewardGrantedAt) {
        const container = await ensureProfileStatsContainer(data, current.profileId, overrides);
        const rewardAmount = serviceLimits.reports.bugFixedReward;
        container.extraRequestLimit = Math.max(0, Math.floor(Number(container.extraRequestLimit || 0))) + rewardAmount;
        current.fixedRewardGrantedAt = now;
        current.fixedRewardAmount = rewardAmount;
        container.balanceOperations = Array.isArray(container.balanceOperations) ? container.balanceOperations : [];
        container.balanceOperations.unshift({
            id: `bug_reward_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            type: 'bug_report_fixed_reward',
            amount: 0,
            extraRequestLimitAfter: container.extraRequestLimit,
            bugReportId: current.id,
            createdAt: now
        });
        container.balanceOperations = container.balanceOperations.slice(0, 300);
        reward = {
            profileId: current.profileId,
            amount: rewardAmount,
            extraRequestLimit: container.extraRequestLimit
        };
    }

    data.errorReports[index] = current;
    await saveDashboardDataWithDependencies(data, overrides);
    return { report: current, reward };
}

async function updateSuggestionReportStatus(reportId, status, actorProfileId = '') {
    return updateSuggestionReportStatusWithDependencies(reportId, status, actorProfileId);
}

async function updateSuggestionReportStatusWithDependencies(reportId, status, actorProfileId = '', overrides = {}) {
    const normalizedReportId = String(reportId || '').trim();
    if (!normalizedReportId) throw new Error('reportId is required');
    const normalizedStatus = normalizeSuggestionReportStatus(status);
    const data = await loadDashboardDataWithDependencies(overrides);
    const serviceLimits = await getRuntimeServiceLimits(overrides);
    data.suggestionReports = Array.isArray(data.suggestionReports) ? data.suggestionReports : [];
    const index = data.suggestionReports.findIndex(item => String(item?.id || '').trim() === normalizedReportId);
    if (index < 0) throw new Error('Предложение не найдено');

    const current = normalizeSuggestionReport(data.suggestionReports[index]);
    const now = new Date().toISOString();
    current.status = normalizedStatus;
    current.statusLabel = SUGGESTION_REPORT_STATUSES[normalizedStatus];
    current.updatedAt = now;
    current.updatedBy = String(actorProfileId || '').trim();

    let reward = null;
    if (normalizedStatus === 'implemented' && !current.implementedRewardGrantedAt) {
        const container = await ensureProfileStatsContainer(data, current.profileId, overrides);
        const rewardAmount = serviceLimits.reports.suggestionImplementedReward;
        container.extraRequestLimit = Math.max(0, Math.floor(Number(container.extraRequestLimit || 0))) + rewardAmount;
        current.implementedRewardGrantedAt = now;
        current.implementedRewardAmount = rewardAmount;
        container.balanceOperations = Array.isArray(container.balanceOperations) ? container.balanceOperations : [];
        container.balanceOperations.unshift({
            id: `suggestion_reward_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            type: 'suggestion_report_implemented_reward',
            amount: 0,
            extraRequestLimitAfter: container.extraRequestLimit,
            suggestionReportId: current.id,
            createdAt: now
        });
        container.balanceOperations = container.balanceOperations.slice(0, 300);
        reward = {
            profileId: current.profileId,
            amount: rewardAmount,
            extraRequestLimit: container.extraRequestLimit
        };
    }

    data.suggestionReports[index] = current;
    await saveDashboardDataWithDependencies(data, overrides);
    return { report: current, reward };
}

async function getProfileDashboardOverview(profileId) {
    return getProfileDashboardOverviewWithDependencies(profileId);
}

async function getProfileDashboardOverviewWithDependencies(profileId, overrides = {}) {
    const getProfileByIdImpl = overrides.getProfileById || getProfileById;
    const getProfilePromoActivationStatusImpl = overrides.getProfilePromoActivationStatus || getProfilePromoActivationStatus;
    const getAttachmentUploadSettingsImpl = overrides.getAttachmentUploadSettings || getAttachmentUploadSettings;
    const loadBotConfigImpl = overrides.loadBotConfig || loadBotConfig;
    const getFullConfigImpl = overrides.getFullConfig || getFullConfig;
    const listUsersImpl = overrides.listUsers || listUsers;
    const serviceLimits = await getRuntimeServiceLimits(overrides);

    const data = await loadDashboardDataWithDependencies(overrides);
    const container = await ensureProfileStatsContainer(data, profileId, overrides);
    await saveDashboardDataWithDependencies(data, overrides);

    const profile = await getProfileByIdImpl(profileId);
    const attachmentUploadSettings = await getAttachmentUploadSettingsImpl(profileId);
    const promoActivationStatus = await getProfilePromoActivationStatusImpl(profileId);
    const profileRequestsLimit = profile?.requestsLimit && Number(profile.requestsLimit) > 0
        ? Number(profile.requestsLimit)
        : (container.dailyLimit || serviceLimits.freeDailyRequests);

    await loadBotConfigImpl(profileId);
    const fullConfig = getFullConfigImpl(profileId);
    const communities = Object.entries(fullConfig?.communities || {});
    const communitySummaries = [];
    const communityFiles = {};
    const communityDocuments = {};
    const paymentUserNames = new Map();
    const paymentCommunityNames = new Map();

    for (const [internalCommunityId, config] of communities) {
        const vkGroupId = String(config?.vk_group_id || internalCommunityId || '').trim();
        const groupName = String(config?.group_name || '').trim();
        paymentCommunityNames.set(String(internalCommunityId), groupName);
        paymentCommunityNames.set(vkGroupId, groupName);
        let usersCount = 0;
        try {
            const users = await listUsersImpl(vkGroupId, profileId);
            usersCount = (users || []).filter(function(row) {
                return String(row['ID'] || '').trim();
            }).length;
            (users || []).forEach(function(row) {
                const userId = String(row['ID'] || row.id || '').trim();
                const userName = String(row['ИМЯ'] || row['Имя'] || row.name || '').trim();
                if (!userId) return;
                paymentUserNames.set(`${internalCommunityId}:${userId}`, userName);
                paymentUserNames.set(`${vkGroupId}:${userId}`, userName);
            });
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
            dailyUsed: stats.dailyUsed || 0,
            dailyLimitPlan: getActiveCommunityDailyPlan(container, vkGroupId) || getActiveCommunityDailyPlan(container, internalCommunityId),
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

        const documentsByType = (container.communityDocuments && (container.communityDocuments[vkGroupId] || container.communityDocuments[internalCommunityId])) || {};
        communityDocuments[vkGroupId] = {};
        Object.keys(documentsByType).forEach(function(typeKey) {
            const type = normalizeDocumentType(typeKey);
            communityDocuments[vkGroupId][type] = (Array.isArray(documentsByType[typeKey]) ? documentsByType[typeKey] : [])
                .map(normalizeConsentDocumentEntry)
                .filter(item => item.attachment)
                .sort((a, b) => String(b.uploadedAt || '').localeCompare(String(a.uploadedAt || '')));
        });
    }

    const limitRequests = data.limitRequests.filter(function(request) {
        return String(request.profileId) === String(profileId);
    });
    const profileErrorReports = (Array.isArray(data.errorReports) ? data.errorReports : [])
        .map(normalizeErrorReport)
        .filter(item => String(item.profileId) === String(profileId))
        .sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
    const bugReportsToday = profileErrorReports.filter(function(item) {
        const created = Date.parse(item.createdAt || '');
        return Number.isFinite(created) && getTodayKey(new Date(created)) === getTodayKey();
    }).length;
    const profileSuggestionReports = (Array.isArray(data.suggestionReports) ? data.suggestionReports : [])
        .map(normalizeSuggestionReport)
        .filter(item => String(item.profileId) === String(profileId))
        .sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
    const suggestionReportsToday = profileSuggestionReports.filter(function(item) {
        const created = Date.parse(item.createdAt || '');
        return Number.isFinite(created) && getTodayKey(new Date(created)) === getTodayKey();
    }).length;
    const hasLimit = Number.isFinite(profileRequestsLimit) && profileRequestsLimit > 0;
    const paymentOperations = (Array.isArray(container.paymentButtonPayments)
        ? container.paymentButtonPayments
        : [])
        .slice()
        .sort((a, b) => String(b.createdAt || b.updatedAt || '').localeCompare(String(a.createdAt || a.updatedAt || '')))
        .slice(0, 500)
        .map(function(item) {
            const communityId = String(item.communityId || '').trim();
            const userId = String(item.userId || '').trim();
            return {
                ...item,
                sourceBot: String(item.sourceBot || '').trim(),
                userName: paymentUserNames.get(`${communityId}:${userId}`) || '',
                communityName: paymentCommunityNames.get(communityId) || ''
            };
        });

    return {
        profileId: String(profileId),
        profileName: container.profileName,
        attachmentUploadSettings,
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
        communityDocuments,
        aiIntegrations: Array.isArray(container.aiIntegrations) ? container.aiIntegrations : [],
        paymentIntegrations: Array.isArray(container.paymentIntegrations) ? container.paymentIntegrations : [],
        paymentOperations,
        paymentProviderPresets: PAYMENT_PROVIDER_PRESETS,
        aiSentAnswers: Array.isArray(container.aiSentAnswers) ? container.aiSentAnswers.slice(0, 100) : [],
        balance: Number(container.balance || 0),
        extraRequestLimit: Math.max(0, Math.floor(Number(container.extraRequestLimit || 0))),
        communityDailyLimits: container.communityDailyLimits || {},
        balanceOperations: Array.isArray(container.balanceOperations) ? container.balanceOperations.slice(0, 100) : [],
        balanceTopUps: (Array.isArray(data.balanceTopUps) ? data.balanceTopUps : []).filter(item => String(item.profileId) === String(profileId)).slice(0, 100),
        errorReports: profileErrorReports,
        bugReportDailyLimit: {
            limit: serviceLimits.reports.bugDailyLimit,
            used: Math.min(bugReportsToday, serviceLimits.reports.bugDailyLimit),
            remaining: Math.max(serviceLimits.reports.bugDailyLimit - bugReportsToday, 0)
        },
        suggestionReports: profileSuggestionReports,
        suggestionReportDailyLimit: {
            limit: serviceLimits.reports.suggestionDailyLimit,
            used: Math.min(suggestionReportsToday, serviceLimits.reports.suggestionDailyLimit),
            remaining: Math.max(serviceLimits.reports.suggestionDailyLimit - suggestionReportsToday, 0)
        },
        balanceTopUpLimits: { min: serviceLimits.balanceTopUp.min, max: serviceLimits.balanceTopUp.max },
        balanceTopUpBonuses: serviceLimits.balanceTopUp.bonuses,
        dailyLimitPackages: serviceLimits.subscriptions,
        extraLimitPackages: serviceLimits.extraPackages,
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
    BALANCE_TOP_UP_MIN,
    BALANCE_TOP_UP_MAX,
    DAILY_LIMIT_PACKAGES,
    EXTRA_LIMIT_PACKAGES,
    BUG_REPORT_DAILY_LIMIT,
    BUG_REPORT_FIXED_REWARD,
    BUG_REPORT_STATUSES,
    SUGGESTION_REPORT_DAILY_LIMIT,
    SUGGESTION_REPORT_IMPLEMENTED_REWARD,
    SUGGESTION_REPORT_STATUSES,
    calculateBalanceCredit,
    canProcessProfileEvents,
    recordProfileEventUsage,
    recordProfileEventUsageWithDependencies,
    recordStructuredTriggerExecution,
    recordUploadedCommunityFile,
    recordConsentDocumentVersion,
    deleteProfileUploadedDocument,
    deleteProfilePaymentOperations,
    getLatestConsentDocumentVersion,
    createProfileLimitRequest,
    resolveProfileLimitRequest,
    getAdminLimitRequests,
    getAdminBalanceTopUps,
    getAdminFinancialOperations,
    getAdminErrorReports,
    getAdminSuggestionReports,
    updateBugReportStatus,
    updateSuggestionReportStatus,
    getAdminProfileBalanceSummaries,
    getAdminProfileBalanceSummariesWithDependencies,
    getProfileDashboardOverview,
    deleteProfileLimitRequest,
    createBalanceTopUp,
    createBalanceTopUpWithDependencies,
    attachProviderPaymentToTopUp,
    listPendingBalanceTopUps,
    confirmBalanceTopUp,
    confirmBalanceTopUpWithDependencies,
    purchaseDailyLimitPackage,
    purchaseDailyLimitPackageWithDependencies,
    purchaseExtraLimitPackage,
    purchaseExtraLimitPackageWithDependencies,
    grantProfilePromoCredits,
    grantProfilePromoCreditsWithDependencies,
    setProfileBalanceFields,
    setProfileBalanceFieldsWithDependencies,
    recordProfileErrorReport,
    recordProfileSuggestionReport,
    buildVkCommunityUrl,
    __testOnly: {
        loadDashboardDataWithDependencies,
        saveDashboardDataWithDependencies,
        recordProfileEventUsageWithDependencies,
        createBalanceTopUpWithDependencies,
        listPendingBalanceTopUps,
        confirmBalanceTopUpWithDependencies,
        purchaseDailyLimitPackageWithDependencies,
        purchaseExtraLimitPackageWithDependencies,
        grantProfilePromoCreditsWithDependencies,
        setProfileBalanceFieldsWithDependencies,
        recordProfileErrorReportWithDependencies,
        updateBugReportStatusWithDependencies,
        recordProfileSuggestionReportWithDependencies,
        updateSuggestionReportStatusWithDependencies,
        getAdminProfileBalanceSummariesWithDependencies,
        getAdminFinancialOperationsWithDependencies,
        recordUploadedCommunityFileWithDependencies,
        recordConsentDocumentVersionWithDependencies,
        deleteProfileUploadedDocumentWithDependencies,
        deleteProfilePaymentOperationsWithDependencies,
        getLatestConsentDocumentVersionWithDependencies,
        getProfileDashboardOverviewWithDependencies
    }
};
