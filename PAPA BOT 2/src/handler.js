async function validateAdminSessionFromRequest(event = {}, query = {}, body = {}) {
    const sessionId = getAdminSessionIdFromEvent(event);
    const result = await validateAdminSessionRequest({
        sessionId,
        ip: getClientIpFromEvent(event),
        userAgent: getUserAgentFromEvent(event),
        now: new Date()
    });

    if (!result.ok) {
        return result;
    }

    const sessionState = {
        ok: true,
        principalProfile: result.profile,
        profile: result.profile,
        session: result.session,
        principalProfileId: result.profile.id,
        requestedProfileId: getRequestProfileId(query, body)
    };
    event.__adminSession = sessionState;
    return sessionState;
}

function buildAdminSessionErrorResponse(result) {
    const cookieHeaders = result.clearCookie ? buildCookieResponseMeta(buildClearSessionCookie()) : buildCookieResponseMeta();
    return {
        statusCode: result.statusCode || 403,
        ...cookieHeaders,
        body: JSON.stringify({
            success: false,
            sessionInvalid: result.sessionInvalid !== false,
            expired: !!result.expired,
            captchaRequired: !!result.captchaRequired,
            loginCaptchaRequired: !!result.loginCaptchaRequired,
            errorCode: result.errorCode || '',
            error: result.error || 'Session invalid'
        })
    };
}

async function reconcileProfileBalanceTopUps(profileId, context = 'profile balance') {
    try {
        return await reconcilePendingYooKassaTopUps(profileId);
    } catch (error) {
        log('warn', `YooKassa pending top-up reconciliation skipped for ${context}: ${error.message}`);
        return null;
    }
}

async function reconcileProfilesBalanceTopUps(profileIds = [], context = 'profile balances') {
    const uniqueIds = Array.from(new Set((Array.isArray(profileIds) ? profileIds : [])
        .map(profileId => String(profileId || '').trim())
        .filter(Boolean)
        .map(profileId => normalizeProfileId(profileId))));
    for (const profileId of uniqueIds) {
        await reconcileProfileBalanceTopUps(profileId, context);
    }
}

/**
 * Основной обработчик HTTP запросов (роутер)
 */

const crypto = require('crypto');
const { GetObjectCommand, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { log } = require('./utils/logger');
const { initializeStorage, getSheetData, saveSheetData, invalidateCache, getS3Client, getBucketName } = require('./modules/storage');
const {
    loadBotConfig, getFullConfig, getConfirmationToken, getSecretKey,
    saveBotConfig, saveAllCommunities, deleteCommunity,
    getActiveCommunityId, setActiveCommunity, getAllCommunityIds,
    resolveCommunityContext, getUserToken,
    resolveTelegramBotContext, saveTelegramBotConfig,
    getActiveTelegramBotId, deleteTelegramBot,
    saveTelegramChat, listTelegramChats, deleteTelegramChat,
    saveTelegramChatBinding, listTelegramChatBindings, deleteTelegramChatBinding,
    getTelegramFileCatalog, saveTelegramFileCatalogEntry, deleteTelegramFileCatalogEntry
} = require('./modules/config');
const {
    verifyAdminCredentials,
    getAllProfileIds,
    normalizeProfileId,
    getPublicProfiles,
    upsertAdminProfile,
    updateAdminProfileCredentials,
    deleteAdminProfile,
    getProfileById,
    findProfileByUsername,
    findProfileByRecoveryEmail,
    registerProfileFromPromo,
    reactivateExpiredProfile,
    activateProfileWithPromoCode,
    isMainAdminProfile,
    buildExpiresAt,
    isProfileExpired
} = require('./modules/admin-profiles');
const {
    registerLoginAttempt,
    getLoginStatus,
    clearLoginLock,
    appendSecurityEvent,
    checkCaptchaRateLimit,
    registerCaptchaRateLimitHit,
    loadSecurityData,
    saveSecurityData,
    requireLoginCaptcha,
    getLoginCaptchaStatus,
    clearLoginCaptcha,
    issueLoginCaptcha,
    verifyLoginCaptcha,
    registerPromoAttempt,
    getPromoStatus,
    getProfilePromoActivationStatus,
    registerProfilePromoActivationAttempt,
    listPromoCodes,
    savePromoCode,
    deletePromoCodeById,
    getPromoByCode,
    consumePromoCode,
    createRecoveryRequest,
    resolveRecoveryRequest,
    getAdminDashboardData
} = require('./modules/admin-security');
const { handleMessage } = require('./modules/messages');
const { handleComment } = require('./modules/comments');
const { processDelayed, processMailing } = require('./modules/scheduler');
const { processStructuredTriggers } = require('./modules/structured-triggers');
const { setupVkCallbackServer } = require('./modules/callback-setup');
const { uploadToVK } = require('./modules/attachments');
const { getTokenPermissions, getMarketProducts } = require('./modules/vk-api');
const {
    addAppLog,
    getAppLogs,
    getAppLogFileName,
    getAppLogSettings,
    saveAppLogSettings,
    clearAppLogs,
    deleteAppLogsFile
} = require('./modules/app-logs');
const { getBotVersionData, saveBotVersionData } = require('./modules/bot-version-store');
const { getClientIpFromHeaders } = require('./modules/client-ip');
const {
    canProcessProfileEvents,
    recordProfileEventUsage,
    recordUploadedCommunityFile,
    recordConsentDocumentVersion,
    getLatestConsentDocumentVersion,
    deleteProfileUploadedDocument,
    deleteProfilePaymentOperations,
    createProfileLimitRequest,
    resolveProfileLimitRequest,
    deleteProfileLimitRequest,
    getAdminLimitRequests,
    getProfileDashboardOverview,
    getAdminBalanceTopUps,
    getAdminFinancialOperations,
    getAdminErrorReports,
    getAdminSuggestionReports,
    getAdminProfileBalanceSummaries,
    setProfileBalanceFields,
    purchaseDailyLimitPackage,
    purchaseExtraLimitPackage,
    grantProfilePromoCredits,
    recordProfileErrorReport,
    updateBugReportStatus,
    recordProfileSuggestionReport,
    updateSuggestionReportStatus
} = require('./modules/profile-dashboard');
const { saveAiIntegrations, testAiIntegration } = require('./modules/ai-integrations');
const { savePaymentIntegrations, testPaymentIntegration } = require('./modules/payment-integrations');
const { sanitizeKeyboardForVk } = require('./modules/keyboard');
const { createEmailVerificationService } = require('./modules/email-verification');
const { resolvePaymentKeyboard } = require('./modules/payment-keyboards');
const { createYooKassaTopUpPayment, handleYooKassaWebhook, reconcilePendingYooKassaTopUps } = require('./modules/yookassa-balance');
const { handleProdamusWebhook, handleProdamusReturn } = require('./modules/prodamus-payments');
const { handleRobokassaResult, handleRobokassaReturn } = require('./modules/robokassa-payments');
const {
    buildAdminFinancialOperationsWorkbook,
    normalizeFinancialOperationsExportFilename,
    selectFinancialOperations
} = require('./modules/admin-financial-export');
const {
    buildCommentActivityStatsWorkbook,
    getCommentActivityStatsWithDependencies,
    normalizeCommentActivityStatsFilename,
    resetCommentActivityStatsWithDependencies
} = require('./modules/comment-activity-stats');
const { buildFaviconResponse } = require('./modules/favicon');
const {
    normalizeMiniAppGroupRows,
    listVisibleMiniAppGroups,
    findMiniAppGroupBySlug,
    toDetailDto
} = require('./modules/miniapp-groups');
const { verifyVkLaunchParams } = require('./modules/miniapp-auth');
const { saveMiniAppAssetWithDependencies, readMiniAppAssetWithDependencies } = require('./modules/miniapp-assets');
const { ensureMiniAppUser, getUserRow, updateUserGroups } = require('./modules/users');
const {
    createAdminSession,
    validateAdminSessionRequest,
    killAdminSession,
    issueSessionCaptcha,
    verifyAndRotateSessionCaptcha,
    getAdminSession,
    isSessionExpired
} = require('./modules/admin-sessions');
const { buildEventEnvelope, isSupportedEventType } = require('./modules/event-envelope');
const { buildTelegramEventEnvelope } = require('./modules/telegram-event-envelope');
const {
    getMe: getTelegramBotIdentity,
    setWebhook: setTelegramWebhook,
    getWebhookInfo: getTelegramWebhookInfo,
    deleteWebhook: deleteTelegramWebhook,
    sendTelegramResponse,
    getChat: getTelegramChat,
    getChatMember: getTelegramChatMember,
    uploadTelegramAttachment: uploadTelegramFile
} = require('./modules/telegram-api');
const {
    publishIncomingEvent,
    consumeIncomingEvent,
    consumeOutboundAction,
    setIncomingEventConsumer
} = require('./modules/event-queue');
const { processIncomingEvent } = require('./modules/event-worker');
const { processOutboundAction } = require('./modules/outbound-actions');
const {
    listDeliveryIncidents,
    markDeliveryIncidentsRead,
    cancelDeliveryIncident,
    retryDeliveryIncident,
    processDueDeliveryIncidents
} = require('./modules/delivery-incidents');
// Админ-панель (файл в корне dist/, на уровень выше от src/)
let adminHTML = '<h1>Admin panel loading...</h1>';
try {
    const { adminPanelHTML } = require('../adminPanelHTML');
    adminHTML = adminPanelHTML || adminHTML;
} catch(e) {
    // В Yandex Functions путь может отличаться
    try {
        const { adminPanelHTML } = require('./adminPanelHTML');
        adminHTML = adminPanelHTML || adminHTML;
    } catch(e2) {
        // Файл не найден — используем заглушку
    }
}

function getRequestProfileId(query = {}, body = {}) {
    return normalizeProfileId(query.profileId || body.profileId || '1');
}

function toPublicCommunityTokenStatus(config = {}) {
    const tokens = Array.from(new Set([
        ...(Array.isArray(config.vk_tokens) ? config.vk_tokens : []),
        config.vk_token || ''
    ].map(value => String(value || '').trim()).filter(Boolean)));
    return {
        user_token_set: Boolean(String(config.user_token || '').trim()),
        community_tokens_count: tokens.length
    };
}

function toPublicBotSettings(fullConfig = {}, profileId = '1') {
    const {
        vk_tokens: legacyVkTokens,
        vk_token: legacyVkToken,
        user_token: legacyUserToken,
        ...safeFullConfig
    } = fullConfig || {};
    const communities = {};
    for (const [communityId, config] of Object.entries(fullConfig.communities || {})) {
        const {
            vk_tokens,
            vk_token,
            user_token,
            ...safeConfig
        } = config || {};
        communities[communityId] = {
            ...safeConfig,
            ...toPublicCommunityTokenStatus(config)
        };
    }
    return {
        ...safeFullConfig,
        communities,
        profileId
    };
}

function getRequestPrincipalProfileId(query = {}, body = {}) {
    return normalizeProfileId(query.principalProfileId || body.principalProfileId || query.profileId || body.profileId || '1');
}

function mergeQueryObject(target, source) {
    if (!source || typeof source !== 'object') return;
    for (const [key, value] of Object.entries(source)) {
        if (!key || value === undefined || value === null) continue;
        target[key] = Array.isArray(value) ? value[0] : String(value);
    }
}

function mergeQueryString(target, rawQueryString) {
    const raw = String(rawQueryString || '').trim().replace(/^\?/, '');
    if (!raw) return;
    const params = new URLSearchParams(raw);
    for (const [key, value] of params.entries()) {
        if (key) target[key] = value;
    }
}

function getQueryParamsFromEvent(event = {}) {
    const query = {};
    for (const candidate of [event.rawQueryString, event.queryString]) {
        const raw = String(candidate || '');
        if (!raw) continue;
        mergeQueryString(query, raw);
    }

    for (const candidate of [event.url, event.path, event.requestContext?.http?.path]) {
        const raw = String(candidate || '');
        if (!raw || !raw.includes('?')) continue;
        mergeQueryString(query, raw.slice(raw.indexOf('?') + 1));
    }

    mergeQueryObject(query, event.params);
    mergeQueryObject(query, event.query);
    mergeQueryObject(query, event.queryStringParameters);
    return query;
}

function normalizeEventQuery(event = {}) {
    const query = getQueryParamsFromEvent(event);
    event.queryStringParameters = query;
    return query;
}

function parseCookies(event = {}) {
    const rawCookie = String(event.headers?.cookie || event.headers?.Cookie || '').trim();
    if (!rawCookie) return {};
    return rawCookie.split(';').reduce((acc, pair) => {
        const [rawKey, ...rest] = pair.split('=');
        const key = String(rawKey || '').trim();
        if (!key) return acc;
        acc[key] = rest.join('=').trim();
        return acc;
    }, {});
}

function getAdminSessionIdFromEvent(event = {}) {
    if (typeof event.__adminSessionId === 'string') {
        return event.__adminSessionId;
    }
    const headerSessionId = String(
        event.headers?.['x-admin-session'] ||
        event.headers?.['X-Admin-Session'] ||
        ''
    ).trim();
    if (headerSessionId) {
        event.__adminSessionId = headerSessionId;
        return headerSessionId;
    }
    const cookies = parseCookies(event);
    const cookieSessionId = String(cookies.adminSessionId || '').trim();
    event.__adminSessionId = cookieSessionId;
    return cookieSessionId;
}

function getClientIpFromEvent(event = {}) {
    return getClientIpFromHeaders(event.headers || {});
}

function getUserAgentFromEvent(event = {}) {
    return String(event.headers?.['user-agent'] || event.headers?.['User-Agent'] || '').trim();
}

function buildJsonHeaders(extra = {}) {
    return {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        ...extra
    };
}

function buildCookieResponseMeta(cookies = [], extraHeaders = {}) {
    const normalizedCookies = Array.isArray(cookies) ? cookies.filter(Boolean) : (cookies ? [cookies] : []);
    const headers = buildJsonHeaders({
        ...extraHeaders,
        ...(normalizedCookies.length ? { 'Set-Cookie': normalizedCookies[0] } : {})
    });
    return normalizedCookies.length
        ? { headers, multiValueHeaders: { 'Set-Cookie': normalizedCookies } }
        : { headers };
}

function buildSessionCookie(sessionId) {
    return `adminSessionId=${sessionId}; Path=/; HttpOnly; SameSite=Lax`;
}

function buildClearSessionCookie() {
    return 'adminSessionId=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0';
}

function getCaptchaMode(query = {}, body = {}) {
    return String(query.mode || body.mode || 'session').trim().toLowerCase() === 'login'
        ? 'login'
        : 'session';
}

function buildCaptchaRateLimitResponse(limit) {
    return {
        statusCode: 429,
        headers: buildJsonHeaders(),
        body: JSON.stringify({
            success: false,
            rateLimited: true,
            cooldownMs: limit.cooldownMs || 0,
            errorCode: 'captcha_rate_limited',
            error: 'РЎР»РёС€РєРѕРј РјРЅРѕРіРѕ Р·Р°РїСЂРѕСЃРѕРІ РєР°РїС‚С‡Рё. РџРѕРІС‚РѕСЂРёС‚Рµ РїРѕР·Р¶Рµ.'
        })
    };
}

async function reserveCaptchaRateLimit({ sessionId = '', ip = '', action = 'submit' } = {}) {
    const data = await loadSecurityData();
    const now = Date.now();
    const limit = checkCaptchaRateLimit({
        data,
        sessionId,
        ip,
        action,
        now
    });

    if (limit.blocked) {
        return { ok: false, response: buildCaptchaRateLimitResponse(limit) };
    }

    registerCaptchaRateLimitHit({
        data,
        key: limit.key,
        bucket: limit.bucket,
        now
    });
    await saveSecurityData(data);
    return { ok: true };
}

async function requireMainAdmin(subject = {}, body = {}) {
    const sessionPrincipal = subject && subject.__adminSession && subject.__adminSession.principalProfile
        ? subject.__adminSession.principalProfile
        : null;
    const query = subject && subject.httpMethod
        ? getQueryParamsFromEvent(subject)
        : subject;
    const principalProfileId = sessionPrincipal
        ? normalizeProfileId(sessionPrincipal.id)
        : getRequestPrincipalProfileId(query, body);
    const principalProfile = sessionPrincipal || await getProfileById(principalProfileId);
    if (!principalProfile || !isMainAdminProfile(principalProfile)) {
        throw new Error('Недостаточно прав: доступ только у главного администратора');
    }
    return principalProfile;
}

function getClientId(query = {}, body = {}) {
    return String(query.clientId || body.clientId || 'anonymous').trim() || 'anonymous';
}

function isProfileScopedSheet(sheetName) {
    return ['ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', 'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ'].includes(String(sheetName || '').trim());
}

async function validateAdminSession(query = {}, body = {}) {
    const principalProfileId = getRequestPrincipalProfileId(query, body);
    const principalProfile = await getProfileById(principalProfileId);

    if (!principalProfile) {
        return { ok: false, statusCode: 401, error: 'Профиль входа не найден' };
    }
    if (principalProfile.active === false) {
        return { ok: false, statusCode: 403, error: 'Профиль отключён' };
    }
    if (!isMainAdminProfile(principalProfile) && isProfileExpired(principalProfile)) {
        return { ok: false, statusCode: 403, error: 'Срок действия профиля истёк', expired: true };
    }

    return { ok: true, principalProfile };
}

function buildSessionErrorResponse(result) {
    return {
        statusCode: result.statusCode || 403,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({
            success: false,
            sessionInvalid: true,
            expired: !!result.expired,
            error: result.error || 'Сессия недействительна'
        })
    };
}

/**
 * Главный обработчик событий
 */
function miniAppJson(statusCode, payload) {
    return {
        statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
            Pragma: 'no-cache',
            Expires: '0'
        },
        body: JSON.stringify(payload)
    };
}

async function resolveMiniAppCommunity(c, profileId = '1', overrides = {}) {
    const requested = String(c || '').trim();
    if (!requested) return null;
    if (overrides.resolveCommunity) {
        return overrides.resolveCommunity(requested, profileId);
    }

    await loadBotConfig(profileId);
    const config = getFullConfig(profileId);
    const communities = config.communities || {};
    for (const [internalCommunityId, communityConfig] of Object.entries(communities)) {
        const vkGroupId = String(communityConfig?.vk_group_id || '').trim();
        if (String(internalCommunityId) === requested || vkGroupId === requested) {
            return {
                profileId,
                communityId: vkGroupId || requested,
                internalCommunityId,
                config: communityConfig
            };
        }
    }
    return null;
}

async function loadMiniAppGroupsForCommunity(resolved, overrides = {}) {
    const getSheetDataImpl = overrides.getSheetData || getSheetData;
    const rows = await getSheetDataImpl('ГРУППЫ', resolved.communityId, resolved.profileId);
    return normalizeMiniAppGroupRows(rows);
}

function parseJsonBody(event) {
    try {
        return JSON.parse(event.body || '{}');
    } catch (error) {
        return {};
    }
}

function verifyMiniAppRequest(body, overrides = {}) {
    return verifyVkLaunchParams(body.launchParams || {}, {
        secret: overrides.miniAppSecret || process.env.VK_MINIAPP_SECRET
    });
}

function parseMiniAppDataUrl(dataUrl, fallbackContentType = '') {
    const match = String(dataUrl || '').match(/^data:([^;]+);base64,(.+)$/);
    if (!match) {
        throw new Error('Некорректный формат изображения');
    }

    return {
        contentType: String(match[1] || fallbackContentType || '').trim(),
        buffer: Buffer.from(match[2], 'base64')
    };
}

async function handleMiniAppUploadAssetWithDependencies(event, overrides = {}) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const communityId = String(body.communityId || q.communityId || getActiveCommunityId(profileId) || '').trim();
        if (!communityId) {
            return miniAppJson(400, { success: false, error: 'community_required' });
        }

        const parsedImage = parseMiniAppDataUrl(body.dataUrl, body.contentType);
        const saveAssetImpl = overrides.saveMiniAppAsset || saveMiniAppAssetWithDependencies;
        const result = await saveAssetImpl({
            profileId,
            communityId,
            contentType: parsedImage.contentType,
            buffer: parsedImage.buffer,
            baseUrl: body.baseUrl || process.env.PAPA_BOT_PUBLIC_URL || process.env.VK_MINIAPP_APP_URL || ''
        }, overrides);

        return miniAppJson(200, { success: true, url: result.url, assetId: result.assetId, key: result.key });
    } catch (e) {
        return miniAppJson(400, { success: false, error: e.message });
    }
}

async function handleMiniAppAssetRequestWithDependencies(event, overrides = {}) {
    try {
        const q = getQueryParamsFromEvent(event);
        const readAssetImpl = overrides.readMiniAppAsset || readMiniAppAssetWithDependencies;
        const result = await readAssetImpl(q.miniappAsset, {
            ...overrides,
            profileId: q.assetProfile,
            communityId: q.assetCommunity,
            extension: q.assetExt
        });
        const contentType = String(result.contentType || '').toLowerCase();
        if (!['image/png', 'image/jpeg', 'image/webp'].includes(contentType)) {
            throw new Error('Unsupported Mini App asset content type');
        }
        return {
            statusCode: 200,
            headers: {
                'Content-Type': contentType,
                'Cache-Control': 'public, max-age=31536000, immutable',
                'Access-Control-Allow-Origin': '*',
                'X-Content-Type-Options': 'nosniff'
            },
            isBase64Encoded: true,
            body: result.buffer.toString('base64')
        };
    } catch (error) {
        log('warn', 'Mini App asset read failed:', error.message);
        return {
            statusCode: 404,
            headers: {
                'Content-Type': 'text/plain; charset=utf-8',
                'Cache-Control': 'no-store',
                'Access-Control-Allow-Origin': '*'
            },
            body: 'Mini App asset not found'
        };
    }
}

function extractMiniAppLaunchParamsFromQuery(query = {}) {
    const params = {};
    for (const [key, value] of Object.entries(query || {})) {
        if (key === 'sign' || key.startsWith('vk_')) {
            params[key] = value;
        }
    }
    return params;
}

function parseMiniAppSubscribedNames(row) {
    return String((row && row['ГРУППА']) || '')
        .split(/[\r\n,]+/)
        .map((item) => item.trim().toLowerCase())
        .filter(Boolean);
}

async function loadMiniAppSubscribedNames(query, resolved, overrides = {}) {
    const launchParams = extractMiniAppLaunchParamsFromQuery(query);
    if (!launchParams.sign || !launchParams.vk_user_id) {
        return [];
    }

    const auth = verifyVkLaunchParams(launchParams, {
        secret: overrides.miniAppSecret || process.env.VK_MINIAPP_SECRET
    });
    if (!auth.ok || (auth.groupId && String(auth.groupId) !== String(query.c))) {
        return [];
    }

    const getUserRowImpl = overrides.getUserRow || getUserRow;
    const row = await getUserRowImpl(auth.userId, resolved.communityId, resolved.profileId);
    return parseMiniAppSubscribedNames(row);
}

async function handleMiniAppRequestWithDependencies(event, overrides = {}) {
    const q = getQueryParamsFromEvent(event);
    const profileId = getRequestProfileId(q, {});
    if (overrides.initializeStorage) {
        await overrides.initializeStorage();
    } else if (!overrides.getSheetData && !overrides.resolveCommunity) {
        await initializeStorage();
    }

    const resolved = await resolveMiniAppCommunity(q.c, profileId, overrides);
    if (!resolved) {
        return miniAppJson(404, {
            success: false,
            error: 'community_not_found',
            message: 'Сообщество не найдено'
        });
    }

    const groups = await loadMiniAppGroupsForCommunity(resolved, overrides);
    if (event.httpMethod === 'POST' && (q.miniapp === 'subscribe' || q.miniapp === 'unsubscribe')) {
        const body = parseJsonBody(event);
        const auth = verifyMiniAppRequest(body, overrides);
        if (!auth.ok) {
            return miniAppJson(401, {
                success: false,
                error: auth.error,
                message: 'Не удалось подтвердить пользователя VK'
            });
        }
        if (auth.groupId && String(auth.groupId) !== String(q.c)) {
            return miniAppJson(403, {
                success: false,
                error: 'community_mismatch',
                message: 'Сообщество Mini App не совпадает со ссылкой'
            });
        }
        const group = findMiniAppGroupBySlug(groups, q.g);
        if (!group) {
            return miniAppJson(404, {
                success: false,
                error: 'group_not_found',
                message: 'Группа не найдена'
            });
        }
        const updateGroupsImpl = overrides.updateUserGroups || updateUserGroups;
        const groupName = group.groupName || group.name;
        const persistSubscription = async (addGroups, removeGroups) => {
            try {
                const result = await updateGroupsImpl(
                    auth.userId,
                    addGroups,
                    removeGroups,
                    resolved.communityId,
                    resolved.profileId
                );
                if (result && result.ok === false) {
                    return false;
                }
                const getUserRowImpl = overrides.getUserRow || getUserRow;
                const persistedGroups = parseMiniAppSubscribedNames(await getUserRowImpl(
                    auth.userId,
                    resolved.communityId,
                    resolved.profileId
                ));
                const shouldBeSubscribed = Boolean(String(addGroups || '').trim());
                return persistedGroups.includes(String(groupName || '').trim().toLowerCase()) === shouldBeSubscribed;
            } catch (error) {
                log('error', 'Mini App subscription state update failed:', error);
                return false;
            }
        };
        if (q.miniapp === 'subscribe') {
            const ensureUserImpl = overrides.ensureMiniAppUser || ensureMiniAppUser;
            await ensureUserImpl(auth.userId, resolved.communityId, resolved.profileId);
            if (!await persistSubscription(groupName, '')) {
                return miniAppJson(503, {
                    success: false,
                    error: 'subscription_update_failed',
                    message: 'Не удалось сохранить подписку. Повторите попытку.'
                });
            }
            return miniAppJson(200, { success: true, subscribed: true, group: toDetailDto(group, true) });
        }
        if (!await persistSubscription('', groupName)) {
            return miniAppJson(503, {
                success: false,
                error: 'subscription_update_failed',
                message: 'Не удалось сохранить изменение подписки. Повторите попытку.'
            });
        }
        return miniAppJson(200, { success: true, subscribed: false, group: toDetailDto(group, false) });
    }

    if (event.httpMethod === 'GET' && q.miniapp === 'groups') {
        const subscribedNames = await loadMiniAppSubscribedNames(q, resolved, overrides);
        return miniAppJson(200, {
            success: true,
            communityId: resolved.communityId,
            groups: listVisibleMiniAppGroups(groups, { subscribedNames })
        });
    }

    if (event.httpMethod === 'GET' && q.miniapp === 'group') {
        const group = findMiniAppGroupBySlug(groups, q.g);
        if (!group) {
            return miniAppJson(404, {
                success: false,
                error: 'group_not_found',
                message: 'Группа не найдена'
            });
        }
        return miniAppJson(200, {
            success: true,
            communityId: resolved.communityId,
            group: toDetailDto(group, (await loadMiniAppSubscribedNames(q, resolved, overrides)).includes(String(group.name || '').toLowerCase()))
        });
    }

    return miniAppJson(404, {
        success: false,
        error: 'miniapp_route_not_found',
        message: 'Mini App route not found'
    });
}

async function handler(event) {
    const q = normalizeEventQuery(event);
    log('info', '🔔 RAW REQUEST:', {
        method: event.httpMethod,
        path: event.path,
        query: q,
        bodyPreview: event.body?.substring(0, 200)
    });

    if (Array.isArray(event?.messages)) {
        return workerHandler(event);
    }

    // CORS preflight
    if (event.httpMethod === 'OPTIONS') {
        return {
            statusCode: 200,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type'
            },
            body: ''
        };
    }

    // ======== Timer trigger (проверяем ДО разделения GET/POST) ========
    if (q.source === 'timer' ||
        (event.event_metadata && event.event_metadata.event_type === 'yandex.cloud.events.serverless.triggers.TimerMessage')) {
        return handleTimerTrigger(event);
    }

    // ======== GET запросы ========
    if (event.httpMethod === 'GET') {
        return handleGetRequest(event);
    }

    // ======== POST запросы ========
    if (event.httpMethod === 'POST') {
        return handlePostRequest(event);
    }

    return { statusCode: 404, body: 'Not Found' };
}

/**
 * Обработка таймера (отложенные + рассылки)
 */
async function handleTimerTrigger(event) {
    log('info', '⏰ TIMER TRIGGER: Starting delayed message processing');
    await initializeStorage();

    try {
        const profileIds = await getAllProfileIds();

        for (const profileId of profileIds) {
            await loadBotConfig(profileId);
            const allIds = getAllCommunityIds(profileId);
            log('info', `⏰ TIMER TRIGGER: Profile ${profileId}, communities: ${allIds.join(', ') || 'none'}`);
            const profileCanProcessNewEvents = await canProcessProfileEvents(profileId);

            for (const cid of allIds) {
                log('info', `⏰ Processing community: ${cid} (profile ${profileId})`);
                await processDueDeliveryIncidents(cid, profileId);
                if (!profileCanProcessNewEvents) {
                    log('warn', `⛔ TIMER TRIGGER: new scheduled work for profile ${profileId} skipped because daily limit is reached`);
                    continue;
                }
                await processDelayed(cid, profileId);
                await processMailing(cid, profileId);
            }
        }

        log('info', '✅ TIMER TRIGGER completed');

        // Пинг Render сервиса чтобы он не засыпал
        try {
            const axios = require('axios');
            await axios.get('https://vk-uploader.onrender.com/healthz', { timeout: 5000 }).catch(() => {});
            log('debug', '🔔 Render service pinged');
        } catch (pingError) {
            log('debug', '⚠️ Render ping failed (non-critical):', pingError.message);
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
            body: 'timer-ok'
        };
    } catch (e) {
        log('error', '❌ TIMER TRIGGER error:', e);
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
            body: 'timer-error'
        };
    }
}

/**
 * Обработка GET запросов
 */
async function handleGetRequest(event) {
    const q = getQueryParamsFromEvent(event);
    const profileId = getRequestProfileId(q);

    if (q.favicon !== undefined) {
        return buildFaviconResponse();
    }

    if (q.miniappAsset !== undefined) {
        return handleMiniAppAssetRequestWithDependencies(event);
    }

    if (q.robokassaResult !== undefined) {
        return handleRobokassaResultRequest(event);
    }

    if (q.robokassaReturn !== undefined) {
        return handleRobokassaReturnRequest(event);
    }

    if (q.prodamusReturn !== undefined || (q._payform_order_id !== undefined && q._payform_sign !== undefined)) {
        return handleProdamusReturnRequest(event);
    }

    if (q.miniapp !== undefined) {
        return handleMiniAppRequestWithDependencies(event);
    }

    // Загрузка настроек для админ-панели
    if (q.getSettings) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        log('debug', '🔑 getSettings requested');
        await loadBotConfig(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                confirmation_code: getConfirmationToken(null, profileId) || '',
                secret_key: getSecretKey(null, profileId) || '',
                vk_token_set: !!getConfirmationToken(null, profileId),
                profileId
            })
        };
    }

    if (q.getCommentActivityStats !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        await initializeStorage();
        return handleGetCommentActivityStats(event);
    }

    // Health check
    if (q.health !== undefined) {
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ status: 'ok', timestamp: new Date().toISOString(), version: '2.0.0' })
        };
    }

    // Инициализация хранилища
    await initializeStorage();

    // Загрузка данных листа
    if (q.sheet) {
        try {
            const session = await validateAdminSessionFromRequest(event, q);
            if (!session.ok) return buildAdminSessionErrorResponse(session);
            await loadBotConfig(profileId);
            let communityId = isProfileScopedSheet(q.sheet) ? null : (q.communityId || getActiveCommunityId(profileId));
            if (!communityId && !isProfileScopedSheet(q.sheet)) {
                const ids = getAllCommunityIds(profileId);
                communityId = ids.length > 0 ? ids[0] : 'default';
            }
            log('debug', `getSheetData: ${q.sheet}, communityId: ${communityId}, profileId: ${profileId}`);
            const data = await getSheetData(q.sheet, communityId, profileId);
            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(data)
            };
        } catch (e) {
            log('error', 'Error getting sheet:', e);
            return {
                statusCode: 500,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: e.message })
            };
        }
    }

    // Настройки бота
    if (q.getBotSettings !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        await loadBotConfig(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(toPublicBotSettings(getFullConfig(profileId), profileId))
        };
    }

    if (q.getTelegramBots !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        await loadBotConfig(profileId);
        const fullConfig = getFullConfig(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                profileId,
                active_telegram_bot: fullConfig.active_telegram_bot || null,
                telegram_bots: Object.fromEntries(
                    Object.entries(fullConfig.telegram_bots || {}).map(([connectorId, config]) => [
                        connectorId,
                        toPublicTelegramBotConfig(connectorId, config)
                    ])
                ),
                file_catalog: Object.fromEntries(
                    await Promise.all(Object.keys(fullConfig.telegram_bots || {}).map(async connectorId => [
                        connectorId,
                        await getTelegramFileCatalog(connectorId, profileId)
                    ]))
                )
            })
        };
    }

    if (q.getTelegramChats !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        const kind = String(q.kind || '').trim();
        const chats = await listTelegramChats(profileId, kind || null);
        const bindings = await listTelegramChatBindings(profileId, kind ? { kind } : {});
        const fullConfig = getFullConfig(profileId);
        const fileCatalog = {};
        for (const connectorId of Object.keys(fullConfig.telegram_bots || {})) {
            fileCatalog[connectorId] = await getTelegramFileCatalog(connectorId, profileId);
        }
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                profileId,
                chats,
                bindings,
                file_catalog: fileCatalog
            })
        };
    }

    if (q.getBotVersion !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Cache-Control': 'no-store, no-cache, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0'
            },
            body: JSON.stringify(await getBotVersionData())
        };
    }

    if (q.saveBotVersion !== undefined) {
        return handleSaveBotVersion(event);
    }

    if (q.getAdminProfiles !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        await requireMainAdmin(event);
        const data = await getPublicProfiles(profileId);
        await reconcileProfilesBalanceTopUps((data.profiles || []).map(profile => profile.id), 'admin profiles');
        const balanceSummaries = await getAdminProfileBalanceSummaries();
        data.profiles = (data.profiles || []).map(profile => Object.assign({}, profile, balanceSummaries[String(profile.id)] || { balance: 0, extraRequestLimit: 0 }));
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(data)
        };
    }

    if (q.getAdminDashboard !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        await requireMainAdmin(event);
        log('info', `[getAdminDashboard] Loading admin dashboard data...`);
        const [profiles, dashboard, limitRequests] = await Promise.all([
            getPublicProfiles(profileId),
            getAdminDashboardData(),
            getAdminLimitRequests()
        ]);
        await reconcileProfilesBalanceTopUps((profiles.profiles || []).map(profile => profile.id), 'admin dashboard');
        const [balanceTopUps, financialOperations, balanceSummaries, errorReports, suggestionReports] = await Promise.all([
            getAdminBalanceTopUps(),
            getAdminFinancialOperations(),
            getAdminProfileBalanceSummaries(),
            getAdminErrorReports(),
            getAdminSuggestionReports()
        ]);
        const profilesWithBalances = (profiles.profiles || []).map(profile => Object.assign({}, profile, balanceSummaries[String(profile.id)] || { balance: 0, extraRequestLimit: 0 }));
        log('info', `[getAdminDashboard] Loaded ${limitRequests.length} limit requests`);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                profiles: profilesWithBalances,
                promoCodes: dashboard.promoCodes,
                recoveryRequests: dashboard.recoveryRequests,
                loginLogs: dashboard.loginLogs,
                limitRequests,
                balanceTopUps,
                financialOperations,
                errorReports,
                suggestionReports,
                yookassaConfigured: !!(process.env.YOOKASSA_SHOP_ID || process.env.YOOKASSA_SHOP_ID_TEST) && !!(process.env.YOOKASSA_SECRET_KEY || process.env.YOOKASSA_API_KEY || process.env.YOOKASSA_API || process.env.YOOKASSA_API_TEST)
            })
        };
    }

    if (q.getProfileDashboard !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        const reconciliationResult = await reconcileProfileBalanceTopUps(profileId, 'profile dashboard');
        const dashboard = await getProfileDashboardOverview(profileId);
        const creditedTopUps = Array.isArray(reconciliationResult?.credited) ? reconciliationResult.credited : [];
        const latestCreditedTopUp = creditedTopUps.find(item => Number.isFinite(Number(item?.balance)));
        if (latestCreditedTopUp) {
            dashboard.balance = Math.max(0, Math.floor(Number(latestCreditedTopUp.balance || 0)));
        }
        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
                Pragma: 'no-cache'
            },
            body: JSON.stringify({ success: true, dashboard })
        };
    }

    if (q.getCommunityProducts !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        await loadBotConfig(profileId);
        const context = await resolveCommunityContext(q.communityId || getActiveCommunityId(profileId), profileId);
        if (!context?.config) {
            return {
                statusCode: 404,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, products: [], error: 'Сообщество не найдено' })
            };
        }

        const token = await getUserToken(context.communityId, profileId);
        const vkGroupId = context.config.vk_group_id || context.communityId;
        if (!token || !vkGroupId) {
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, products: [], error: 'VK token или ID сообщества не настроены' })
            };
        }

        const parsedGroupId = parseInt(vkGroupId, 10);
        if (!Number.isFinite(parsedGroupId)) {
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, products: [], error: 'VK Group ID должен быть числовым' })
            };
        }

        const ownerId = -Math.abs(parsedGroupId);
        const result = await getMarketProducts(ownerId, token);
        if (result.error) {
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, products: [], error: result.error.error_msg || 'Не удалось загрузить товары/услуги VK' })
            };
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, products: result.response || [] })
        };
    }

    if (q.getAppLogs !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        const communityId = q.communityId || getActiveCommunityId(profileId) || 'global';
        const limit = Math.max(1, Math.min(200, Number(q.limit || 120) || 120));
        const [rows, settings] = await Promise.all([
            getAppLogs(communityId, profileId, limit),
            getAppLogSettings(communityId, profileId)
        ]);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                logs: rows,
                communityId,
                profileId,
                enabled: settings.enabled,
                fileName: getAppLogFileName(communityId, profileId)
            })
        };
    }

    if (q.getDeliveryIncidents !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        const communityId = String(q.communityId || getActiveCommunityId(profileId) || 'global').trim() || 'global';
        const incidents = await listDeliveryIncidents(communityId, profileId, {
            includeResolved: q.includeResolved !== '0',
            limit: Math.max(1, Math.min(300, Number(q.limit || 200) || 200))
        });
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                communityId,
                profileId,
                incidents,
                unreadCount: incidents.filter(item => item.unread).length,
                pendingCount: incidents.filter(item => item.status === 'pending' || item.status === 'retrying').length
            })
        };
    }

    if (q.checkPromoStatus !== undefined) {
        const status = await getPromoStatus(getClientId(q));
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(status)
        };
    }

    if (q.validateSession !== undefined) {
        const session = await validateAdminSessionFromRequest(event, q);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                principalProfileId: session.principalProfile.id,
                role: session.principalProfile.role,
                isMainAdmin: isMainAdminProfile(session.principalProfile)
            })
        };
    }

    if (q.getCaptcha !== undefined) {
        return handleGetCaptcha(event);
    }

    // Админ-панель
    return {
        statusCode: 200,
        headers: {
            'Content-Type': 'text/html; charset=utf-8',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
            'Pragma': 'no-cache',
            'Expires': '0'
        },
        body: adminHTML
    };
}

/**
 * Обработка POST запросов
 */
function isYooKassaWebhookPayload(body) {
    if (!body || typeof body !== 'object') return false;
    const eventName = String(body.event || '').trim();
    if (eventName !== 'payment.succeeded' && eventName !== 'payment.canceled') return false;
    return !!body.object && typeof body.object === 'object';
}

async function handlePostRequest(event) {
    const q = getQueryParamsFromEvent(event);
    let parsedPostBody = null;

    if (q.robokassaResult !== undefined) {
        return handleRobokassaResultRequest(event);
    }

    if (q.miniapp !== undefined) {
        return handleMiniAppRequestWithDependencies(event);
    }

    if (q.telegramWebhook !== undefined) {
        return handleTelegramWebhook(event);
    }

    // Обработка action из body (для загрузки вложений из админ-панели)
    if (event.body && event.httpMethod === 'POST') {
        try {
            parsedPostBody = JSON.parse(event.body);
            if (parsedPostBody.action === 'upload_attachment') {
                return handleUploadAttachment(event);
            }
            if (parsedPostBody.action === 'record_uploaded_file') {
                return handleRecordUploadedFile(event);
            }
            if (parsedPostBody.action === 'record_consent_document') {
                return handleRecordConsentDocument(event);
            }
            if (parsedPostBody.action === 'recover_render_upload') {
                return handleRecoverRenderUpload(event);
            }
            if (parsedPostBody.action === 'upload_attachment_chunk') {
                return handleUploadAttachmentChunk(event);
            }
            if (parsedPostBody.action === 'create_render_upload_grant') {
                return handleCreateRenderUploadGrant(event);
            }
        } catch (e) {
            // Не JSON body, продолжаем обычную обработку
        }
    }

    // Проверка VK токенов
    if (q.yookassaWebhook !== undefined || isYooKassaWebhookPayload(parsedPostBody)) {
        return handleYooKassaWebhookRequest(event);
    }

    if (q.prodamusWebhook !== undefined) {
        return handleProdamusWebhookRequest(event);
    }

    if (q.checkVkTokens !== undefined) {
        return handleCheckVkTokens(event);
    }

    // Проверка авторизации
    if (q.verifyAuth !== undefined || q.loginAdmin !== undefined) {
        return handleVerifyAuth(event);
    }

    if (q.verifyCaptcha !== undefined) {
        return handleVerifyCaptcha(event);
    }

    if (q.logoutAdmin !== undefined) {
        return handleLogoutAdmin(event);
    }

    // Запрос восстановления
    if (q.requestRecovery !== undefined) {
        return handleRecoveryRequest(event);
    }

    if (q.sendPasswordResetCode !== undefined) {
        return handleSendPasswordResetCode(event);
    }

    if (q.resetPasswordWithCode !== undefined) {
        return handleResetPasswordWithCode(event);
    }

    if (q.verifyPromoCode !== undefined) {
        return handleVerifyPromoCode(event);
    }

    if (q.sendRegistrationCode !== undefined) {
        return handleSendRegistrationCode(event);
    }

    if (q.sendOpenRegistrationCode !== undefined) {
        return handleSendOpenRegistrationCode(event);
    }

    if (q.registerAccount !== undefined) {
        return handleRegisterAccount(event);
    }

    if (q.reactivateExpiredProfile !== undefined) {
        return handleReactivateExpiredProfile(event);
    }

    // Инициализация хранилища
    await initializeStorage();

    const needsAdminSession =
        q.checkVkTokens !== undefined ||
        q.save ||
        q.setupCallback !== undefined ||
        q.saveBotSettings !== undefined ||
        q.saveAllCommunities !== undefined ||
        q.saveAdminProfile !== undefined ||
        q.saveProfileAiIntegrations !== undefined ||
        q.saveProfilePaymentIntegrations !== undefined ||
        q.testProfilePaymentIntegration !== undefined ||
        q.testProfileAiIntegration !== undefined ||
        q.deleteAdminProfile !== undefined ||
        q.savePromoCode !== undefined ||
        q.deletePromoCode !== undefined ||
        q.resolveRecovery !== undefined ||
        q.deleteCommunity !== undefined ||
        q.uploadAttachment !== undefined ||
        q.miniappUploadAsset !== undefined ||
        q.testSend !== undefined ||
        q.checkTokenPermissions !== undefined ||
        q.saveBotVersion !== undefined ||
        q.saveAppLogsSettings !== undefined ||
        q.clearAppLogs !== undefined ||
        q.deleteAppLogsFile !== undefined ||
        q.markDeliveryIncidentsRead !== undefined ||
        q.retryDeliveryIncident !== undefined ||
        q.cancelDeliveryIncident !== undefined ||
        q.requestProfileLimit !== undefined ||
        q.activateProfilePromoCode !== undefined ||
        q.createBalanceTopUp !== undefined ||
        q.purchaseDailyLimitPackage !== undefined ||
        q.purchaseExtraLimitPackage !== undefined ||
        q.submitBugReport !== undefined ||
        q.updateBugReportStatus !== undefined ||
        q.submitSuggestionReport !== undefined ||
        q.updateSuggestionReportStatus !== undefined ||
        q.resolveProfileLimitRequest !== undefined ||
        q.deleteProfileLimitRequest !== undefined ||
        q.exportAdminFinancialOperations !== undefined ||
        q.getCommentActivityStats !== undefined ||
        q.exportCommentActivityStats !== undefined ||
        q.resetCommentActivityStats !== undefined ||
        q.deleteProfilePaymentOperations !== undefined ||
        q.deleteProfileUploadedDocument !== undefined ||
        q.connectTelegramBot !== undefined ||
        q.refreshTelegramWebhook !== undefined ||
        q.testTelegramBot !== undefined ||
        q.deleteTelegramBot !== undefined ||
        q.saveTelegramChat !== undefined ||
        q.deleteTelegramChat !== undefined ||
        q.bindTelegramBotToChat !== undefined ||
        q.unbindTelegramBotFromChat !== undefined ||
        q.uploadTelegramAttachment !== undefined ||
        q.deleteTelegramAttachment !== undefined;

    let adminSession = null;
    if (needsAdminSession) {
        adminSession = await validateAdminSessionFromRequest(event, q);
        if (!adminSession.ok) return buildAdminSessionErrorResponse(adminSession);
    }

    // Сохранение данных листа
    if (q.save) {
        return handleSaveSheet(event);
    }

    // Автонастройка callback сервера
    if (q.setupCallback !== undefined) {
        return handleSetupCallback(event);
    }

    // Сохранение настроек сообщества
    if (q.saveBotSettings !== undefined) {
        return handleSaveBotSettings(event);
    }

    if (q.connectTelegramBot !== undefined) {
        return handleConnectTelegramBot(event);
    }

    if (q.refreshTelegramWebhook !== undefined) {
        return handleRefreshTelegramWebhook(event);
    }

    if (q.testTelegramBot !== undefined) {
        return handleTestTelegramBot(event);
    }

    if (q.deleteTelegramBot !== undefined) {
        return handleDeleteTelegramBot(event);
    }

    if (q.saveTelegramChat !== undefined) {
        return handleSaveTelegramChat(event);
    }

    if (q.deleteTelegramChat !== undefined) {
        return handleDeleteTelegramChat(event);
    }

    if (q.bindTelegramBotToChat !== undefined) {
        return handleBindTelegramBotToChat(event);
    }

    if (q.unbindTelegramBotFromChat !== undefined) {
        return handleUnbindTelegramBotFromChat(event);
    }

    if (q.uploadTelegramAttachment !== undefined) {
        return handleUploadTelegramAttachment(event);
    }

    if (q.deleteTelegramAttachment !== undefined) {
        return handleDeleteTelegramAttachment(event);
    }

    // Сохранение всех сообществ
    if (q.saveAllCommunities !== undefined) {
        return handleSaveAllCommunities(event);
    }

    if (q.saveAppLogsSettings !== undefined) {
        return handleSaveAppLogsSettings(event);
    }

    if (q.clearAppLogs !== undefined) {
        return handleClearAppLogs(event);
    }

    if (q.deleteAppLogsFile !== undefined) {
        return handleDeleteAppLogsFile(event);
    }

    if (q.markDeliveryIncidentsRead !== undefined) {
        return handleMarkDeliveryIncidentsRead(event);
    }

    if (q.retryDeliveryIncident !== undefined) {
        return handleRetryDeliveryIncident(event);
    }

    if (q.cancelDeliveryIncident !== undefined) {
        return handleCancelDeliveryIncident(event);
    }

    if (q.requestProfileLimit !== undefined) {
        return handleRequestProfileLimit(event);
    }

    if (q.activateProfilePromoCode !== undefined) {
        return handleActivateProfilePromoCode(event);
    }

    if (q.createBalanceTopUp !== undefined) {
        return handleCreateBalanceTopUp(event);
    }

    if (q.purchaseDailyLimitPackage !== undefined) {
        return handlePurchaseDailyLimitPackage(event);
    }

    if (q.purchaseExtraLimitPackage !== undefined) {
        return handlePurchaseExtraLimitPackage(event);
    }

    if (q.submitBugReport !== undefined) {
        return handleSubmitBugReport(event);
    }

    if (q.updateBugReportStatus !== undefined) {
        return handleUpdateBugReportStatus(event);
    }

    if (q.submitSuggestionReport !== undefined) {
        return handleSubmitSuggestionReport(event);
    }

    if (q.updateSuggestionReportStatus !== undefined) {
        return handleUpdateSuggestionReportStatus(event);
    }

    if (q.saveProfileAiIntegrations !== undefined) {
        return handleSaveProfileAiIntegrations(event);
    }

    if (q.saveProfilePaymentIntegrations !== undefined) {
        return handleSaveProfilePaymentIntegrations(event);
    }

    if (q.testProfilePaymentIntegration !== undefined) {
        return handleTestProfilePaymentIntegration(event);
    }

    if (q.testProfileAiIntegration !== undefined) {
        return handleTestProfileAiIntegration(event);
    }

    if (q.resolveProfileLimitRequest !== undefined) {
        return handleResolveProfileLimitRequest(event);
    }

    if (q.deleteProfileLimitRequest !== undefined) {
        return handleDeleteProfileLimitRequest(event);
    }

    if (q.exportAdminFinancialOperations !== undefined) {
        return handleExportAdminFinancialOperations(event);
    }

    if (q.getCommentActivityStats !== undefined) {
        return handleGetCommentActivityStats(event);
    }

    if (q.exportCommentActivityStats !== undefined) {
        return handleExportCommentActivityStats(event);
    }

    if (q.resetCommentActivityStats !== undefined) {
        return handleResetCommentActivityStats(event);
    }

    if (q.deleteProfilePaymentOperations !== undefined) {
        return handleDeleteProfilePaymentOperations(event);
    }

    if (q.deleteProfileUploadedDocument !== undefined) {
        return handleDeleteProfileUploadedDocument(event);
    }

    if (q.saveAdminProfile !== undefined) {
        return handleSaveAdminProfile(event);
    }

    if (q.deleteAdminProfile !== undefined) {
        return handleDeleteAdminProfile(event);
    }

    if (q.savePromoCode !== undefined) {
        return handleSavePromoCode(event);
    }

    if (q.deletePromoCode !== undefined) {
        return handleDeletePromoCode(event);
    }

    if (q.resolveRecovery !== undefined) {
        return handleResolveRecovery(event);
    }

    // Удаление сообщества
    if (q.deleteCommunity !== undefined) {
        return handleDeleteCommunity(event);
    }

    // Загрузка вложений
    if (q.uploadAttachment !== undefined) {
        return handleUploadAttachment(event);
    }

    // Тестовая отправка сообщения пользователю
    if (q.miniappUploadAsset !== undefined) {
        return handleMiniAppUploadAssetWithDependencies(event);
    }

    if (q.testSend !== undefined) {
        return handleTestSend(event);
    }

    // Проверка прав токена
    if (q.checkTokenPermissions !== undefined) {
        return handleCheckTokenPermissions(event);
    }

    // Вебхук от VK
    return handleVkWebhook(event);
}

/**
 * Проверка VK токенов
 */
async function handleCheckVkTokens(event) {
    try {
        const axios = require('axios');
        const body = JSON.parse(event.body || '{}');
        const { tokens } = body;

        if (!tokens || !Array.isArray(tokens)) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Tokens array is required' })
            };
        }

        const results = [];
        for (const token of tokens) {
            try {
                const checkRes = await axios.get('https://api.vk.com/method/users.get', {
                    params: { access_token: token, v: '5.199' }
                });
                results.push({
                    valid: !checkRes.data.error,
                    user: checkRes.data.response?.[0],
                    error: checkRes.data.error?.error_msg || null
                });
            } catch (e) {
                results.push({ valid: false, error: e.message });
            }
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, results })
        };
    } catch (error) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

/**
 * Проверка авторизации
 */
async function handleVerifyAuthLegacy(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const { username, password } = body;
        const ip = event.headers?.['x-forwarded-for'] || event.headers?.['X-Forwarded-For'] || '';

        const loginStatus = await getLoginStatus(username);
        if (loginStatus.lockUntil && loginStatus.lockUntil > Date.now()) {
            return {
                statusCode: 423,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: false,
                    locked: true,
                    recoveryRequired: true,
                    recoveryUsername: username,
                    lockUntil: loginStatus.lockUntil,
                    error: 'Профиль временно заблокирован после 3 неудачных попыток входа'
                })
            };
        }

        const authResult = await verifyAdminCredentials(username, password);

        if (authResult.success) {
            await registerLoginAttempt({
                username,
                success: true,
                profileId: authResult.profileId,
                ip
            });
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: true,
                    token: 'authenticated_' + Date.now(),
                    profileId: authResult.profileId,
                    principalProfileId: authResult.profileId,
                    profileName: authResult.profileName,
                    role: authResult.role,
                    isMainAdmin: authResult.isMainAdmin
                })
            };
        } else if (authResult.reason === 'expired') {
            const profile = await findProfileByUsername(username);
            return {
                statusCode: 403,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: false,
                    expired: true,
                    canReactivate: true,
                    error: authResult.error || 'Срок действия профиля истёк',
                    profileId: profile?.id || '',
                    profileName: profile?.name || username,
                    username
                })
            };
        } else {
            const lockInfo = await registerLoginAttempt({
                username,
                success: false,
                profileId: null,
                reason: authResult.error || authResult.reason || 'credentials',
                ip
            });
            return {
                statusCode: authResult.reason === 'expired' || authResult.reason === 'inactive' ? 403 : 401,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: false,
                    error: authResult.error || 'Неверный логин или пароль',
                    remainingAttempts: lockInfo.remainingAttempts,
                    lockUntil: lockInfo.lockUntil || 0,
                    locked: !!(lockInfo.lockUntil && lockInfo.lockUntil > Date.now())
                    ,recoveryRequired: !!(lockInfo.lockUntil && lockInfo.lockUntil > Date.now()),
                    recoveryUsername: username
                })
            };
        }
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

/**
 * Запрос восстановления
 */
async function handleRecoveryRequest(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const { email, username } = body;

        if (!email && !username) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Укажите email или логин' })
            };
        }

        const profile = email
            ? await findProfileByRecoveryEmail(email)
            : await findProfileByUsername(username);

        if (!profile) {
            return {
                statusCode: 404,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Профиль для восстановления не найден' })
            };
        }

        await createRecoveryRequest({
            profileId: profile.id,
            username: profile.username,
            recoveryEmail: profile.recoveryEmail,
            requestedByEmail: email || '',
            requestedByUsername: username || ''
        });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, message: 'Запрос на восстановление создан и передан главному админу' })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSendPasswordResetCode(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const username = String(body.username || '').trim();
        const email = String(body.recoveryEmail || body.email || '').trim();
        if (!username || !email) {
            return { statusCode: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Укажите логин и email' }) };
        }
        const profile = await findProfileByUsername(username);
        if (!profile || String(profile.recoveryEmail || '').trim().toLowerCase() !== email.toLowerCase()) {
            return { statusCode: 404, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Логин и email не совпадают' }) };
        }
        const result = await createEmailVerificationService().issueCode(email, 'password_reset');
        return { statusCode: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: true, expiresAt: result.expiresAt }) };
    } catch (error) {
        return { statusCode: 500, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: error.message }) };
    }
}

async function handleResetPasswordWithCode(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const username = String(body.username || '').trim();
        const email = String(body.recoveryEmail || body.email || '').trim();
        const emailCode = String(body.emailCode || body.code || '').trim();
        const newPassword = String(body.newPassword || body.password || '').trim();
        if (!username || !email || !emailCode || !newPassword) {
            return { statusCode: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Заполните логин, email, код и новый пароль' }) };
        }
        if (newPassword.length < 4) {
            return { statusCode: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Новый пароль должен содержать минимум 4 символа' }) };
        }
        const profile = await findProfileByUsername(username);
        if (!profile || String(profile.recoveryEmail || '').trim().toLowerCase() !== email.toLowerCase()) {
            return { statusCode: 404, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Логин и email не совпадают' }) };
        }
        const verification = await createEmailVerificationService().verifyCode(email, emailCode, 'password_reset');
        if (!verification.ok) {
            return { statusCode: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Неверный или просроченный код подтверждения' }) };
        }
        await updateAdminProfileCredentials(profile.id, { password: newPassword });
        const passwordCheck = await verifyAdminCredentials(username, newPassword);
        if (!passwordCheck.success) {
            throw new Error('Новый пароль не сохранился. Повторите восстановление.');
        }
        await clearLoginLock(username);
        await clearLoginCaptcha(getClientIpFromEvent(event));
        return { statusCode: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: true }) };
    } catch (error) {
        return { statusCode: 500, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: error.message }) };
    }
}

async function handleVerifyPromoCode(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const code = String(body.code || '').trim();
        const clientId = getClientId({}, body);

        const promoStatus = await getPromoStatus(clientId);
        if (promoStatus.lockUntil && promoStatus.lockUntil > Date.now()) {
            return {
                statusCode: 423,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: false,
                    locked: true,
                    lockUntil: promoStatus.lockUntil,
                    error: 'Ввод промокодов заблокирован на 24 часа'
                })
            };
        }

        const promo = code ? await getPromoByCode(code) : null;
        if (code && (!promo || promo.active === false || promo.usedCount >= promo.maxUses)) {
            const result = await registerPromoAttempt({ clientId, success: false, code, note: 'invalid_promo' });
            return {
                statusCode: 404,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: false,
                    error: 'Промокод не найден или уже недоступен',
                    remainingAttempts: result.remainingAttempts,
                    lockUntil: result.lockUntil || 0,
                    locked: !!(result.lockUntil && result.lockUntil > Date.now())
                })
            };
        }

        await registerPromoAttempt({ clientId, success: true, code, note: 'promo_verified' });
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                promo: {
                    code: promo.code,
                    label: promo.label,
                    durationMinutes: promo.durationMinutes,
                    maxUses: promo.maxUses,
                    usedCount: promo.usedCount
                }
            })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleRegisterAccount(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const clientId = getClientId({}, body);
        const code = String(body.code || '').trim();
        const username = String(body.username || '').trim();
        const password = String(body.password || '').trim();
        const name = String(body.name || '').trim();
        const recoveryEmail = String(body.recoveryEmail || '').trim();
        const emailCode = String(body.emailCode || '').trim();

        if (!recoveryEmail || !emailCode) {
            return { statusCode: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Укажите email и код подтверждения' }) };
        }
        const emailVerification = createEmailVerificationService();
        const verification = await emailVerification.verifyCode(recoveryEmail, emailCode, 'registration');
        if (!verification.ok) {
            return { statusCode: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Неверный или просроченный код подтверждения email' }) };
        }

        const promoStatus = await getPromoStatus(clientId);
        if (promoStatus.lockUntil && promoStatus.lockUntil > Date.now()) {
            return {
                statusCode: 423,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Ввод промокодов заблокирован на 24 часа', locked: true, lockUntil: promoStatus.lockUntil })
            };
        }

        const promo = code ? await getPromoByCode(code) : null;
        if (code && (!promo || promo.active === false || promo.usedCount >= promo.maxUses)) {
            const result = await registerPromoAttempt({ clientId, success: false, code, note: 'register_invalid_promo' });
            return {
                statusCode: 404,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Промокод не найден или уже недоступен', remainingAttempts: result.remainingAttempts, lockUntil: result.lockUntil || 0 })
            };
        }

        const profile = await registerProfileFromPromo({
            name,
            username,
            password,
            recoveryEmail,
            durationMinutes: promo ? promo.durationMinutes : null,
            requestsLimit: promo ? promo.dailyRequestsLimit : (process.env.DEFAULT_PROFILE_REQUESTS_LIMIT || 100)
        }, promo ? promo.code : '');

        if (promo) {
            await grantProfilePromoCredits(profile.id, promo);
            await consumePromoCode(promo.code, profile.id);
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, profile })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSendRegistrationCode(event) {
    // Email verification is allowed without a promo code; an optional promo is validated below.
    try {
        const body = JSON.parse(event.body || '{}');
        const email = String(body.recoveryEmail || '').trim();
        const code = String(body.code || '').trim();
        if (!email) return { statusCode: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Укажите email' }) };
        const promo = code ? await getPromoByCode(code) : null;
        if (!promo || promo.active === false || promo.usedCount >= promo.maxUses) return { statusCode: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Сначала укажите действующий промокод' }) };
        if (await findProfileByRecoveryEmail(email)) return { statusCode: 409, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Этот email уже используется' }) };
        const result = await createEmailVerificationService().issueCode(email, 'registration');
        return { statusCode: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: true, expiresAt: result.expiresAt }) };
    } catch (error) {
        return { statusCode: 500, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: error.message }) };
    }
}

async function handleSendOpenRegistrationCode(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const email = String(body.recoveryEmail || '').trim();
        if (!email) return { statusCode: 400, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Email is required' }) };
        if (await findProfileByRecoveryEmail(email)) return { statusCode: 409, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: 'Email is already used' }) };
        const result = await createEmailVerificationService().issueCode(email, 'registration');
        return { statusCode: 200, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: true, expiresAt: result.expiresAt }) };
    } catch (error) {
        return { statusCode: 500, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: false, error: error.message }) };
    }
}

async function handleReactivateExpiredProfile(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const username = String(body.username || '').trim();
        const password = String(body.password || '').trim();
        const code = String(body.code || '').trim();
        const attemptKey = `reactivate::${username.toLowerCase()}`;
        const ip = getClientIpFromEvent(event);
        const userAgent = getUserAgentFromEvent(event);

        const promoStatus = await getPromoStatus(null, attemptKey);
        if (promoStatus.lockUntil && promoStatus.lockUntil > Date.now()) {
            return {
                statusCode: 423,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Повторная активация по промокоду заблокирована на 24 часа', locked: true, lockUntil: promoStatus.lockUntil })
            };
        }

        const authResult = await verifyAdminCredentials(username, password);
        if (authResult.success) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Профиль уже активен, повторная активация не требуется' })
            };
        }
        if (authResult.reason !== 'expired') {
            return {
                statusCode: 401,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: authResult.error || 'Неверный логин или пароль' })
            };
        }

        const expiredProfile = await findProfileByUsername(username);
        if (!expiredProfile || expiredProfile.password !== password) {
            return {
                statusCode: 401,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Неверный логин или пароль' })
            };
        }

        const promo = await getPromoByCode(code);
        if (!promo || promo.active === false || promo.usedCount >= promo.maxUses) {
            const result = await registerPromoAttempt({ attemptKey, success: false, code, note: 'reactivate_invalid_promo' });
            return {
                statusCode: 404,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: false,
                    error: 'Промокод не найден или уже недоступен',
                    remainingAttempts: result.remainingAttempts,
                    lockUntil: result.lockUntil || 0,
                    locked: !!(result.lockUntil && result.lockUntil > Date.now())
                })
            };
        }

        const reactivatedProfile = await reactivateExpiredProfile(expiredProfile.id, promo.code, promo.durationMinutes, promo.dailyRequestsLimit);
        await grantProfilePromoCredits(expiredProfile.id, promo);
        await consumePromoCode(promo.code, expiredProfile.id);
        await clearLoginLock(username);
        await registerPromoAttempt({ attemptKey, success: true, code: promo.code, note: 'reactivate_success' });
        const session = await createAdminSession({
            profileId: expiredProfile.id,
            ip,
            userAgent,
            now: new Date().toISOString()
        });

        return {
            statusCode: 200,
            ...buildCookieResponseMeta(buildSessionCookie(session.sessionId)),
            body: JSON.stringify({
                success: true,
                sessionToken: session.sessionId,
                profileId: expiredProfile.id,
                principalProfileId: expiredProfile.id,
                profileName: reactivatedProfile.name,
                role: expiredProfile.role,
                isMainAdmin: false,
                message: 'Профиль повторно активирован'
            })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

/**
 * Сохранение данных листа
 */
async function handleSaveSheet(event) {
    return handleSaveSheetWithDependencies(event);
    try {
        const q = event.queryStringParameters || {};
        const body = JSON.parse(event.body || '{}');
        const sheetName = q.save;
        const profileId = getRequestProfileId(q, body);
        const communityId = isProfileScopedSheet(sheetName) ? null : (q.communityId || getActiveCommunityId(profileId));

        log('debug', `🔵 handleSaveSheet: q.communityId=${q.communityId}, getActiveCommunityId()=${getActiveCommunityId()}, resolved communityId=${communityId}`);

        // ✅ Используем vk_group_id для имени файла если он есть в конфиге
        let fileCommunityId = communityId;
        await loadBotConfig(profileId);
        const fullConfig = getFullConfig(profileId);
        log('debug', `🔵 handleSaveSheet: fullConfig keys: ${Object.keys(fullConfig?.communities || {}).join(', ')}`);
        if (!isProfileScopedSheet(sheetName) && fullConfig?.communities?.[communityId]?.vk_group_id) {
            fileCommunityId = fullConfig.communities[communityId].vk_group_id.toString();
            log('debug', `🔵 handleSaveSheet: Found vk_group_id for ${communityId} = ${fileCommunityId}`);
        } else if (!isProfileScopedSheet(sheetName)) {
            log('debug', `🔵 handleSaveSheet: No vk_group_id found for ${communityId}, using as-is: ${fileCommunityId}`);
        }

        log('debug', `saveSheetData: ${sheetName}, communityId: ${communityId}, fileCommunityId: ${fileCommunityId}`);

        if (sheetName === 'КОММЕНТАРИИ В ПОСТАХ') {
            const attachmentSnapshot = Array.isArray(body) ? body.map((row, idx) => ({
                idx,
                step: row['Шаг'] || '',
                trigger: row['Триггер'] || '',
                replyAttachments: row['Вложения к ответу'] || '',
                commentAttachments: row['Вложения'] || ''
            })) : [];
            log('debug', `📎 handleSaveSheet comments attachment snapshot: ${JSON.stringify(attachmentSnapshot)}`);
        }

        // Инвалидация кэша
        invalidateCache(sheetName, fileCommunityId, profileId);

        if (sheetName === 'РАССЫЛКА') {
            const currentData = await getSheetData(sheetName, fileCommunityId, profileId);
            const updatedData = body.map((newRow, idx) => ({
                ...newRow,
                'Статус': currentData[idx]?.['Статус'] || newRow['Статус'],
                'Фактическое время отправки': currentData[idx]?.['Фактическое время отправки'] || newRow['Фактическое время отправки'],
                'Ошибка': currentData[idx]?.['Ошибка'] || newRow['Ошибка']
            }));
            await saveSheetData(sheetName, updatedData, fileCommunityId, profileId);
        } else if (sheetName === 'ПОЛЬЗОВАТЕЛИ') {
            // ✅ Автоматически тримим поле ГРУППА чтобы убрать лишние пробелы
            const cleanedData = body.map(row => {
                if (row['ГРУППА'] && typeof row['ГРУППА'] === 'string') {
                    row['ГРУППА'] = row['ГРУППА'].trim();
                }
                return row;
            });
            await saveSheetData(sheetName, cleanedData, fileCommunityId, profileId);
        } else {
            await saveSheetData(sheetName, body, fileCommunityId, profileId);
        }

        log('info', `✅ Save completed for ${sheetName} community ${fileCommunityId}`);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

function collectSheetBotNames(rows) {
    const names = new Set();
    for (const row of Array.isArray(rows) ? rows : []) {
        const botName = String(row && row['Бот'] ? row['Бот'] : '').trim().toLowerCase();
        if (botName) names.add(botName);
    }
    return names;
}

async function validateUniqueBotNamesAcrossSheets(sheetName, nextRows, communityId, profileId, overrides = {}) {
    if (sheetName !== 'СООБЩЕНИЯ' && sheetName !== 'КОММЕНТАРИИ В ПОСТАХ') {
        return;
    }

    const getSheetDataImpl = overrides.getSheetData || getSheetData;
    const oppositeSheet = sheetName === 'СООБЩЕНИЯ' ? 'КОММЕНТАРИИ В ПОСТАХ' : 'СООБЩЕНИЯ';
    const nextBotNames = collectSheetBotNames(nextRows);
    const oppositeBotNames = collectSheetBotNames(await getSheetDataImpl(oppositeSheet, communityId, profileId));

    for (const botName of nextBotNames) {
        if (oppositeBotNames.has(botName)) {
            throw Object.assign(new Error(`Бот с именем "${botName}" уже существует во вкладке "${oppositeSheet}". Выберите другое имя.`), {
                statusCode: 400
            });
        }
    }
}

function buildExampleKeyboard(labels) {
    return {
        one_time: false,
        inline: true,
        buttons: labels.map((label, index) => ([{
            action: {
                type: 'text',
                label,
                payload: JSON.stringify({ button: index + 1, buttonLabel: label, source: 'example_template' })
            },
            color: index === 2 ? 'negative' : 'positive'
        }]))
    };
}

function buildConsentKeyboard(label, payload, color = 'positive') {
    if (Array.isArray(label)) {
        return {
            one_time: false,
            inline: true,
            buttons: label.map(button => ([{
                action: {
                    type: 'text',
                    label: button.label,
                    payload: {
                        ...(button.payload || {}),
                        buttonLabel: button.label
                    }
                },
                color: button.color || 'positive'
            }]))
        };
    }

    return {
        one_time: false,
        inline: true,
        buttons: [[{
            action: {
                type: 'text',
                label,
                payload: {
                    ...(payload || {}),
                    buttonLabel: label
                }
            },
            color
        }]]
    };
}

const CONSENT_TEMPLATE_PROTECTION_MARK = 'consents_default_v2';
const CONSENT_TEMPLATE_STEPS = [
    'Согласие с ОПД и Политикой ОПД',
    'Согласие с Публичной офертой',
    'Согласен с Согласие с ОПД',
    'Согласен с Офертой',
    'Удаление Согласие с ОПД и Политикой ОПД'
];
const CONSENT_TEMPLATE_STEP_SET = new Set(CONSENT_TEMPLATE_STEPS.map(step => step.toLowerCase()));

function isProtectedConsentTemplateRow(row = {}) {
    const botName = String(row['Бот'] || row['Р‘РѕС‚'] || '').trim().toLowerCase();
    const stepName = String(row['Шаг'] || row['РЁР°Рі'] || '').trim().toLowerCase();
    return row._templateProtected === CONSENT_TEMPLATE_PROTECTION_MARK
        && botName === 'согласия'
        && CONSENT_TEMPLATE_STEP_SET.has(stepName);
}

function assertProtectedConsentTemplateRowsPresent(sheetName, currentRows, nextRows, options = {}) {
    if (sheetName !== 'СООБЩЕНИЯ' && sheetName !== 'РЎРћРћР‘Р©Р•РќРРЇ') return;

    const nextKeys = new Set((Array.isArray(nextRows) ? nextRows : []).map(row => {
        const botName = String(row && (row['Бот'] || row['Р‘РѕС‚']) || '').trim().toLowerCase();
        const stepName = String(row && (row['Шаг'] || row['РЁР°Рі']) || '').trim().toLowerCase();
        return botName + '::' + stepName;
    }));

    for (const row of Array.isArray(currentRows) ? currentRows : []) {
        if (!isProtectedConsentTemplateRow(row)) continue;
        const botName = String(row['Бот'] || row['Р‘РѕС‚'] || '').trim().toLowerCase();
        const stepName = String(row['Шаг'] || row['РЁР°Рі'] || '').trim();
        if (!nextKeys.has(botName + '::' + stepName.toLowerCase())) {
            if (options.allowDelete === true) continue;
            throw Object.assign(new Error('Нельзя удалить шаблонный шаг "' + stepName + '" бота "Согласия".'), {
                statusCode: 400
            });
        }
    }
}

function canDeleteProtectedConsentTemplateRows(event = {}, body = {}, overrides = {}) {
    if (overrides.allowProtectedConsentTemplateDeletion === true) return true;
    const principalProfile = event && event.__adminSession && event.__adminSession.principalProfile
        ? event.__adminSession.principalProfile
        : null;
    if (principalProfile) return isMainAdminProfile(principalProfile);
    return false;
}

function buildDefaultConsentDocumentTemplates() {
    return [
        {
            documentType: 'personal_data_consent',
            fileName: 'Согласие с ОПД.txt',
            text: 'Тестовый шаблон согласия с обработкой персональных данных.\nЗамените этот документ на актуальный юридический текст перед публичным запуском.'
        },
        {
            documentType: 'personal_data_policy',
            fileName: 'Политика ОПД.txt',
            text: 'Тестовый шаблон политики обработки персональных данных.\nЗамените этот документ на актуальную политику перед публичным запуском.'
        },
        {
            documentType: 'public_offer',
            fileName: 'Публичная оферта.txt',
            text: 'Тестовый шаблон публичной оферты.\nЗамените этот документ на актуальную оферту перед публичным запуском.'
        }
    ];
}

function buildExampleBotTemplates() {
    return {
        messages: {
            sheetName: 'СООБЩЕНИЯ',
            row: {
                'Бот': 'Бот пример С-1',
                'Шаг': 'Тест С-1',
                'Триггер': 'Привет',
                'Ответ': 'Привет как дела?',
                'Вложения к ответу': '',
                'Точно/Не точно': 'ТОЧНО',
                'Регистр': 'не важно',
                '_keyboard': JSON.stringify(buildExampleKeyboard(['Нормально', 'Отлично', 'Ужасно']))
            }
        },
        comments: {
            sheetName: 'КОММЕНТАРИИ В ПОСТАХ',
            row: {
                'Бот': 'Бот пример К-1',
                'Шаг': 'Тест К-1',
                'Триггер': 'Привет',
                'Ответ': 'Привет как дела?',
                'Вложения к ответу': '',
                'Пост': 'ВСЕ',
                'Отметили': '',
                'Точно/Не точно': 'ТОЧНО',
                'Регистр': 'не важно'
            }
        },
        consentOpdRequest: {
            sheetName: 'СООБЩЕНИЯ',
            row: {
                'Р‘РѕС‚': 'Согласия',
                'Бот': 'Согласия',
                'РЁР°Рі': 'Согласие с ОПД и Политикой ОПД',
                'Шаг': 'Согласие с ОПД и Политикой ОПД',
                'РўСЂРёРіРіРµСЂ': '',
                'Триггер': '',
                'РћС‚РІРµС‚': 'Пожалуйста, ознакомьтесь с документом и нажмите «Согласен с ОПД».',
                'Ответ': 'Пожалуйста, ознакомьтесь с документом и нажмите «Согласен с ОПД».',
                'Р’Р»РѕР¶РµРЅРёСЏ Рє РѕС‚РІРµС‚Сѓ': '{{latest_document:personal_data_consent}},{{latest_document:personal_data_policy}}',
                'Вложения к ответу': '{{latest_document:personal_data_consent}},{{latest_document:personal_data_policy}}',
                '_keyboard': JSON.stringify(buildConsentKeyboard([
                    { label: 'Согласен с ОПД', payload: { consent: 'personal_data_consent' }, color: 'positive' },
                    { label: 'Удалить Согласие с ОПД', payload: { deleteConsent: 'personal_data_consent' }, color: 'negative' }
                ])),
                _templateProtected: CONSENT_TEMPLATE_PROTECTION_MARK
            }
        },
        consentOfferRequest: {
            sheetName: 'СООБЩЕНИЯ',
            row: {
                'Р‘РѕС‚': 'Согласия',
                'Бот': 'Согласия',
                'РЁР°Рі': 'Согласие с Публичной офертой',
                'Шаг': 'Согласие с Публичной офертой',
                'РўСЂРёРіРіРµСЂ': '',
                'Триггер': '',
                'РћС‚РІРµС‚': 'Пожалуйста, ознакомьтесь с документом и нажмите «Согласен с Офертой».',
                'Ответ': 'Пожалуйста, ознакомьтесь с документом и нажмите «Согласен с Офертой».',
                'Р’Р»РѕР¶РµРЅРёСЏ Рє РѕС‚РІРµС‚Сѓ': '{{latest_document:public_offer}}',
                'Вложения к ответу': '{{latest_document:public_offer}}',
                '_keyboard': JSON.stringify(buildConsentKeyboard('Согласен с Офертой', { consent: 'public_offer' }, 'positive')),
                _templateProtected: CONSENT_TEMPLATE_PROTECTION_MARK
            }
        },
        consentOpdAccepted: {
            sheetName: 'СООБЩЕНИЯ',
            row: {
                'Р‘РѕС‚': 'Согласия',
                'Бот': 'Согласия',
                'РЁР°Рі': 'Согласен с Согласие с ОПД',
                'Шаг': 'Согласен с Согласие с ОПД',
                'РўСЂРёРіРіРµСЂ': 'Согласен с ОПД',
                'Триггер': 'Согласен с ОПД',
                'РћС‚РІРµС‚': 'Согласие приняли. Теперь мы можем продолжить взаимодействие',
                'Ответ': 'Согласие приняли. Теперь мы можем продолжить взаимодействие',
                'Р’Р»РѕР¶РµРЅРёСЏ Рє РѕС‚РІРµС‚Сѓ': '',
                'Вложения к ответу': '',
                '_triggerMode': 'BUTTON',
                _templateProtected: CONSENT_TEMPLATE_PROTECTION_MARK
            }
        },
        consentOfferAccepted: {
            sheetName: 'СООБЩЕНИЯ',
            row: {
                'Р‘РѕС‚': 'Согласия',
                'Бот': 'Согласия',
                'РЁР°Рі': 'Согласен с Офертой',
                'Шаг': 'Согласен с Офертой',
                'РўСЂРёРіРіРµСЂ': 'Согласен с Офертой',
                'Триггер': 'Согласен с Офертой',
                'РћС‚РІРµС‚': 'Согласие с Офертой приняли.',
                'Ответ': 'Согласие с Офертой приняли.',
                'Р’Р»РѕР¶РµРЅРёСЏ Рє РѕС‚РІРµС‚Сѓ': '',
                'Вложения к ответу': '',
                '_triggerMode': 'BUTTON',
                _templateProtected: CONSENT_TEMPLATE_PROTECTION_MARK
            }
        },
        consentOpdDeleted: {
            sheetName: 'СООБЩЕНИЯ',
            row: {
                'Р‘РѕС‚': 'Согласия',
                'Бот': 'Согласия',
                'РЁР°Рі': 'Удаление Согласие с ОПД и Политикой ОПД',
                'Шаг': 'Удаление Согласие с ОПД и Политикой ОПД',
                'РўСЂРёРіРіРµСЂ': 'Удалить Согласие с ОПД',
                'Триггер': 'Удалить Согласие с ОПД',
                'РћС‚РІРµС‚': 'Согласие с ОПД - Удалено. Мы можем продолжить взаимодействие только с дальнейшего вашего Согласия.',
                'Ответ': 'Согласие с ОПД - Удалено. Мы можем продолжить взаимодействие только с дальнейшего вашего Согласия.',
                'Р’Р»РѕР¶РµРЅРёСЏ Рє РѕС‚РІРµС‚Сѓ': '',
                'Вложения к ответу': '',
                '_triggerMode': 'BUTTON',
                _templateProtected: CONSENT_TEMPLATE_PROTECTION_MARK
            }
        }
    };
}

async function ensureDefaultConsentDocumentsForCommunity(communityId, profileId, overrides = {}) {
    const getLatestConsentDocumentVersionImpl = overrides.getLatestConsentDocumentVersion || getLatestConsentDocumentVersion;
    const uploadToVKImpl = overrides.uploadToVK || uploadToVK;
    const recordConsentDocumentVersionImpl = overrides.recordConsentDocumentVersion || recordConsentDocumentVersion;
    const result = { createdCount: 0, created: [], skipped: [], failed: [] };

    for (const template of buildDefaultConsentDocumentTemplates()) {
        try {
            const latest = await getLatestConsentDocumentVersionImpl(profileId, communityId, template.documentType, overrides);
            if (latest && latest.attachment) {
                result.skipped.push(template.documentType);
                continue;
            }

            const buffer = Buffer.from(template.text, 'utf8');
            const attachment = await uploadToVKImpl(buffer, template.fileName, 'text/plain', 'messages', communityId);
            await recordConsentDocumentVersionImpl({
                profileId,
                communityId,
                vkGroupId: communityId,
                documentType: template.documentType,
                fileName: template.fileName,
                fileType: 'text/plain',
                fileSize: buffer.length,
                attachment
            });
            result.createdCount += 1;
            result.created.push(template.documentType);
        } catch (error) {
            result.failed.push({ documentType: template.documentType, error: error.message });
            log('warn', 'Default consent document was not created: ' + template.documentType + ': ' + error.message);
        }
    }

    return result;
}

async function ensureExampleBotsForCommunity(communityId, profileId, overrides = {}) {
    const getSheetDataImpl = overrides.getSheetData || getSheetData;
    const saveSheetDataImpl = overrides.saveSheetData || saveSheetData;
    const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
    const templates = buildExampleBotTemplates();
    const result = { createdCount: 0, created: [] };

    for (const template of Object.values(templates)) {
        const currentRows = await getSheetDataImpl(template.sheetName, communityId, profileId);
        const rows = Array.isArray(currentRows) ? currentRows : [];
        const templateBotName = String(template.row['Бот']).trim().toLowerCase();
        const templateStepName = String(template.row['Шаг'] || '').trim().toLowerCase();
        const exists = rows.some(row => {
            const rowBotName = String(row && row['Бот'] ? row['Бот'] : '').trim().toLowerCase();
            const rowStepName = String(row && row['Шаг'] ? row['Шаг'] : '').trim().toLowerCase();
            return rowBotName === templateBotName && (!templateStepName || rowStepName === templateStepName);
        });
        if (exists) continue;

        const updatedRows = rows.concat([{ ...template.row }]);
        await saveSheetDataImpl(template.sheetName, updatedRows, communityId, profileId);
        invalidateCacheImpl(template.sheetName, communityId, profileId);
        result.createdCount += 1;
        result.created.push({ sheetName: template.sheetName, bot: template.row['Бот'], step: template.row['Шаг'] });
    }

    return result;
}

async function handleSaveSheetWithDependencies(event, overrides = {}) {
    try {
        const q = event.queryStringParameters || {};
        const body = JSON.parse(event.body || '{}');
        const sheetName = q.save;
        const getRequestProfileIdImpl = overrides.getRequestProfileId || getRequestProfileId;
        const isProfileScopedSheetImpl = overrides.isProfileScopedSheet || isProfileScopedSheet;
        const getActiveCommunityIdImpl = overrides.getActiveCommunityId || getActiveCommunityId;
        const loadBotConfigImpl = overrides.loadBotConfig || loadBotConfig;
        const getFullConfigImpl = overrides.getFullConfig || getFullConfig;
        const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
        const getSheetDataImpl = overrides.getSheetData || getSheetData;
        const saveSheetDataImpl = overrides.saveSheetData || saveSheetData;
        const logImpl = overrides.log || log;
        const profileId = getRequestProfileIdImpl(q, body);
        const communityId = isProfileScopedSheetImpl(sheetName) ? null : (q.communityId || getActiveCommunityIdImpl(profileId));

        logImpl('debug', `handleSaveSheet community=${communityId} profile=${profileId}`);

        let fileCommunityId = communityId;
        await loadBotConfigImpl(profileId);
        const fullConfig = getFullConfigImpl(profileId);
        if (!isProfileScopedSheetImpl(sheetName) && fullConfig?.communities?.[communityId]?.vk_group_id) {
            fileCommunityId = fullConfig.communities[communityId].vk_group_id.toString();
        }

        const currentData = await getSheetDataImpl(sheetName, fileCommunityId, profileId);
        await validateUniqueBotNamesAcrossSheets(sheetName, body, fileCommunityId, profileId, overrides);
        assertProtectedConsentTemplateRowsPresent(sheetName, currentData, body, {
            allowDelete: canDeleteProtectedConsentTemplateRows(event, body, overrides)
        });
        invalidateCacheImpl(sheetName, fileCommunityId, profileId);

        if (sheetName === 'РАССЫЛКА') {
            const updatedData = body.map((newRow, idx) => ({
                ...newRow,
                'Статус': currentData[idx]?.['Статус'] || newRow['Статус'],
                'Фактическое время отправки': currentData[idx]?.['Фактическое время отправки'] || newRow['Фактическое время отправки'],
                'Ошибка': currentData[idx]?.['Ошибка'] || newRow['Ошибка']
            }));
            await saveSheetDataImpl(sheetName, updatedData, fileCommunityId, profileId);
        } else if (sheetName === 'ПОЛЬЗОВАТЕЛИ') {
            const cleanedData = body.map(row => {
                if (row['ГРУППА'] && typeof row['ГРУППА'] === 'string') {
                    row['ГРУППА'] = row['ГРУППА'].trim();
                }
                return row;
            });
            await saveSheetDataImpl(sheetName, cleanedData, fileCommunityId, profileId);
        } else {
            await saveSheetDataImpl(sheetName, body, fileCommunityId, profileId);
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true })
        };
    } catch (e) {
        return {
            statusCode: e.statusCode || 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

/**
 * Настройка callback сервера
 */
async function handleSetupCallback(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const { community_id, vk_token, vk_group_id, secret_key } = body;
        const profileId = getRequestProfileId(q, body);

        await loadBotConfig(profileId);
        const fullConfig = getFullConfig(profileId);

        // Сначала определяем выбранное сообщество. После сохранения токены скрыты
        // в браузере, поэтому автонастройка должна брать их из его конфигурации.
        let targetCommunityId = String(community_id || '').trim();
        if (fullConfig?.communities) {
            for (const [id, config] of Object.entries(fullConfig.communities)) {
                if (String(config?.vk_group_id || '') === String(vk_group_id || community_id || '')) {
                    log('info', `🔍 Найдено существующее сообщество ${id} по сохранённому VK Group ID, используем его`);
                    targetCommunityId = id;
                    break;
                }
            }
        }

        const storedConfig = fullConfig?.communities?.[String(targetCommunityId)] || {};
        const effectiveGroupId = String(vk_group_id || storedConfig.vk_group_id || '').trim();
        const effectiveToken = String(vk_token || storedConfig.vk_tokens?.[0] || storedConfig.vk_token || '').trim();
        const effectiveUserToken = String(storedConfig.user_token || '').trim();
        const missing = [];
        if (!effectiveGroupId) missing.push('VK Group ID');
        if (!effectiveToken) missing.push('токен сообщества');
        if (!effectiveUserToken) missing.push('User Token');
        if (missing.length) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Для автонастройки сначала сохраните: ' + missing.join(', ') + '.' })
            };
        }

        // Сохраняем значения, полученные из формы или защищённого хранилища.
        log('info', '🔧 handleSetupCallback: сохраняем конфиг для community_id=' + targetCommunityId);
        await saveBotConfig({
            vk_token: effectiveToken,
            vk_group_id: effectiveGroupId,
            vk_tokens: [effectiveToken],
            user_token: effectiveUserToken
        }, targetCommunityId.toString(), profileId);

        log('info', '🔧 handleSetupCallback: вызываем setupVkCallbackServer groupId=' + effectiveGroupId + ', communityId=' + targetCommunityId);
        const result = await setupVkCallbackServer(effectiveGroupId, targetCommunityId.toString(), profileId);
        const responsePayload = result && typeof result === 'object' ? { ...result } : { success: true, result };
        if (responsePayload.success !== false) {
            responsePayload.consentDocuments = await ensureDefaultConsentDocumentsForCommunity(effectiveGroupId, profileId);
            responsePayload.exampleBots = await ensureExampleBotsForCommunity(effectiveGroupId, profileId);
        }

        await addAppLog({
            tab: 'SETTINGS',
            title: 'Настроен Callback API',
            summary: 'Сообщество подключено к callback-серверу.',
            details: [
                'Сообщество: ' + targetCommunityId,
                'VK ID: ' + effectiveGroupId,
                'Статус: ' + (responsePayload?.success === false ? 'ошибка' : 'успешно'),
                'Шаблоны ботов: ' + (responsePayload.exampleBots?.createdCount || 0)
            ],
            communityId: effectiveGroupId,
            profileId
        });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(responsePayload)
        };
    } catch (error) {
        log('error', 'handleSetupCallback error:', error);
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

/**
 * Сохранение настроек сообщества
 */
async function handleSaveBotSettings(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const communityId = body.community_id || body.vk_group_id?.toString() || 'default';

        await loadBotConfig(profileId);
        const tokenAction = String(body.token_action || '').trim();
        if (tokenAction) {
            const fullConfig = getFullConfig(profileId);
            const existing = fullConfig.communities?.[String(communityId)] || {};
            const currentTokens = Array.from(new Set([
                ...(Array.isArray(existing.vk_tokens) ? existing.vk_tokens : []),
                existing.vk_token || ''
            ].map(value => String(value || '').trim()).filter(Boolean)));
            const tokenValue = String(body.token_value || '').trim();
            const tokenIndex = Number.parseInt(body.token_index, 10);
            const tokenPatch = {};

            if (tokenAction === 'save_user_token') {
                if (!tokenValue) throw new Error('Укажите User Token');
                tokenPatch.user_token = tokenValue;
            } else if (tokenAction === 'delete_user_token') {
                tokenPatch.user_token = '';
            } else if (tokenAction === 'add_community_token') {
                if (!tokenValue) throw new Error('Укажите Community Token');
                if (!currentTokens.includes(tokenValue)) currentTokens.push(tokenValue);
                if (currentTokens.length > 7) throw new Error('Можно сохранить не более 7 Community Token');
                tokenPatch.vk_tokens = currentTokens;
                tokenPatch.vk_token = currentTokens[0] || '';
            } else if (tokenAction === 'delete_community_token') {
                if (!Number.isInteger(tokenIndex) || tokenIndex < 0 || tokenIndex >= currentTokens.length) {
                    throw new Error('Community Token не найден');
                }
                currentTokens.splice(tokenIndex, 1);
                tokenPatch.vk_tokens = currentTokens;
                tokenPatch.vk_token = currentTokens[0] || '';
            } else {
                throw new Error('Неизвестная операция с токеном');
            }

            const updated = await saveBotConfig(tokenPatch, communityId, profileId);
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: true,
                    community_id: communityId,
                    tokenStatus: toPublicCommunityTokenStatus(updated.communities?.[String(communityId)] || {})
                })
            };
        }
        const updatedConfig = await saveBotConfig(body, communityId, profileId);

        await addAppLog({
            tab: 'SETTINGS',
            title: 'Сохранены настройки сообщества',
            summary: 'Обновлены токены и параметры текущего сообщества.',
            details: [
                'Сообщество: ' + communityId,
                'Название: ' + String(body.group_name || body.community_name || communityId),
                'VK ID: ' + String(body.vk_group_id || communityId)
            ],
            communityId: body.vk_group_id || communityId,
            profileId
        });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                community_id: communityId,
                active: updatedConfig.active_community,
                profileId
            })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

/**
 * Сохранение всех сообществ
 */
async function handleSaveAllCommunities(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        await saveAllCommunities(body, profileId);

        await addAppLog({
            tab: 'SETTINGS',
            title: 'Сохранены все сообщества',
            summary: 'Конфигурация сообществ обновлена целиком.',
            details: ['Профиль: ' + profileId],
            communityId: 'global',
            profileId
        });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSaveAppLogsSettings(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const communityId = body.communityId || q.communityId || getActiveCommunityId(profileId) || 'global';
        const settings = await saveAppLogSettings(communityId, profileId, body.enabled !== false);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, enabled: settings.enabled })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleClearAppLogs(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const communityId = body.communityId || q.communityId || getActiveCommunityId(profileId) || 'global';
        await clearAppLogs(communityId, profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleDeleteAppLogsFile(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const communityId = body.communityId || q.communityId || getActiveCommunityId(profileId) || 'global';
        const result = await deleteAppLogsFile(communityId, profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, fileName: result.fileName })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleMarkDeliveryIncidentsRead(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const communityId = String(body.communityId || q.communityId || getActiveCommunityId(profileId) || 'global').trim() || 'global';
        const incidents = await markDeliveryIncidentsRead(communityId, profileId, body.incidentIds || []);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, incidents })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleRetryDeliveryIncident(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const communityId = String(body.communityId || q.communityId || getActiveCommunityId(profileId) || 'global').trim() || 'global';
        const result = await retryDeliveryIncident(communityId, profileId, String(body.incidentId || '').trim());
        return {
            statusCode: result.ok ? 200 : 409,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: !!result.ok, ...result })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleCancelDeliveryIncident(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const communityId = String(body.communityId || q.communityId || getActiveCommunityId(profileId) || 'global').trim() || 'global';
        const incident = await cancelDeliveryIncident(communityId, profileId, String(body.incidentId || '').trim());
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, incident })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSaveBotVersion(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        await requireMainAdmin(event, body);
        const saved = await saveBotVersionData(body || {});
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, version: saved })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSaveAdminProfile(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const actor = await requireMainAdmin(event, body);
        let savedProfile = await upsertAdminProfile(body || {}, actor.id);
        if (
            Object.prototype.hasOwnProperty.call(body || {}, 'balance') ||
            Object.prototype.hasOwnProperty.call(body || {}, 'extraRequestLimit')
        ) {
            const balanceFields = await setProfileBalanceFields(savedProfile.id, {
                balance: body.balance,
                extraRequestLimit: body.extraRequestLimit
            });
            savedProfile = Object.assign({}, savedProfile, {
                balance: balanceFields.balance,
                extraRequestLimit: balanceFields.extraRequestLimit
            });
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, profile: savedProfile })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleDeleteAdminProfile(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        await requireMainAdmin(event, body);
        const result = await deleteAdminProfile(body.profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSavePromoCode(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const actor = await requireMainAdmin(event, body);
        const promo = await savePromoCode(body, actor.id);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, promo })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleDeletePromoCode(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const actor = await requireMainAdmin(event, body);
        const result = await deletePromoCodeById(body.id, actor.id);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleActivateProfilePromoCode(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const targetProfileId = getRequestProfileId({}, body);
        const principalProfile = event.__adminSession?.principalProfile || null;
        const principalProfileId = principalProfile ? normalizeProfileId(principalProfile.id) : '';
        const targetProfile = await getProfileById(targetProfileId);
        const code = String(body.code || '').trim().toUpperCase();

        if (!principalProfile) {
            return {
                statusCode: 401,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Сессия не найдена' })
            };
        }
        if (principalProfile.active === false) {
            return {
                statusCode: 403,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Профиль отключён' })
            };
        }
        if (!isMainAdminProfile(principalProfile) && isProfileExpired(principalProfile)) {
            return {
                statusCode: 403,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Срок действия профиля истёк', expired: true })
            };
        }
        if (!targetProfile) {
            return {
                statusCode: 404,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Профиль не найден' })
            };
        }
        if (!isMainAdminProfile(principalProfile) && String(targetProfileId) !== String(principalProfileId)) {
            return {
                statusCode: 403,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Недостаточно прав для активации промокода этого профиля' })
            };
        }
        if (false && isMainAdminProfile(targetProfile)) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Главному админу промокоды не требуются' })
            };
        }
        if (!code) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Введите промокод' })
            };
        }

        const promoStatus = await getProfilePromoActivationStatus(targetProfileId);
        if (promoStatus.blocked) {
            return {
                statusCode: 423,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: false,
                    locked: true,
                    error: 'Лимит попыток ввода промокода исчерпан до 00:00 МСК',
                    promoActivationStatus: promoStatus
                })
            };
        }

        const promo = await getPromoByCode(code);
        if (!promo || promo.active === false || promo.usedCount >= promo.maxUses) {
            const attemptStatus = await registerProfilePromoActivationAttempt(targetProfileId, {
                success: false,
                code,
                note: 'profile_invalid_promo'
            });
            const dashboard = await getProfileDashboardOverview(targetProfileId);
            return {
                statusCode: 404,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: false,
                    error: 'Промокод не найден или уже недоступен',
                    promoActivationStatus: attemptStatus,
                    dashboard
                })
            };
        }

        const updatedProfile = await activateProfileWithPromoCode(targetProfileId, promo);
        const creditResult = await grantProfilePromoCredits(targetProfileId, promo);
        await consumePromoCode(promo.code, targetProfileId);
        const attemptStatus = await registerProfilePromoActivationAttempt(targetProfileId, {
            success: true,
            code: promo.code,
            note: 'profile_promo_activated'
        });
        const dashboard = await getProfileDashboardOverview(targetProfileId);
        if (Number.isFinite(Number(creditResult?.balance))) {
            dashboard.balance = Math.max(0, Math.floor(Number(creditResult.balance || 0)));
        }
        if (Number.isFinite(Number(creditResult?.extraRequestLimit))) {
            dashboard.extraRequestLimit = Math.max(0, Math.floor(Number(creditResult.extraRequestLimit || 0)));
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                message: 'Промокод активирован',
                profile: updatedProfile,
                promo: {
                    code: promo.code,
                    label: promo.label,
                    durationMinutes: promo.durationMinutes,
                    dailyRequestsLimit: promo.dailyRequestsLimit,
                    balanceCredit: promo.balanceCredit,
                    extraRequestLimitCredit: promo.extraRequestLimitCredit
                },
                promoActivationStatus: attemptStatus,
                dashboard
            })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleResolveRecovery(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const actor = await requireMainAdmin(event, body);
        const targetProfile = await getProfileById(body.profileId);
        if (!targetProfile) {
            throw new Error('Профиль не найден');
        }

        let updatedProfile = null;
        if (body.tempPassword) {
            updatedProfile = await upsertAdminProfile({
                id: targetProfile.id,
                name: targetProfile.name,
                username: targetProfile.username,
                password: body.tempPassword,
                recoveryEmail: targetProfile.recoveryEmail,
                expiresAt: targetProfile.expiresAt,
                active: true,
                role: targetProfile.role,
                promoCodeUsed: targetProfile.promoCodeUsed,
                requestsLimit: targetProfile.requestsLimit
            }, actor.id);
            await clearLoginLock(targetProfile.username);
        }

        const request = await resolveRecoveryRequest(body.requestId, {
            status: body.status || 'resolved',
            tempPassword: body.tempPassword || '',
            note: body.note || '',
            resolvedByProfileId: actor.id
        });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, request, profile: updatedProfile })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleRequestProfileLimit(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const request = await createProfileLimitRequest(profileId, body.requestedLimit, {
            communityId: body.communityId || body.vkGroupId || '',
            communityName: body.communityName || '',
            communityUrl: body.communityUrl || ''
        });
        await addAppLog({
            tab: 'PROFILE',
            title: 'Запрошено увеличение лимита PAPA BOT',
            summary: `Профиль запросил ${request.requestedLimit} запросов в сутки`,
            details: ['Профиль: ' + request.profileName],
            communityId: 'global',
            profileId
        });
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, request })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleCreateBalanceTopUp(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const result = await createYooKassaTopUpPayment(profileId, body.amountRub, {
            returnUrl: body.returnUrl || ''
        });
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleYooKassaWebhookRequest(event) {
    try {
        const payload = JSON.parse(event.body || '{}');
        const result = await handleYooKassaWebhook(payload);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleProdamusWebhookRequest(event) {
    try {
        const result = await handleProdamusWebhook(event);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleProdamusReturnRequest(event) {
    try {
        const result = await handleProdamusReturn(event);
        const redirectUrl = String(result.redirectUrl || process.env.APP_URL || 'https://vk.com/im').trim();
        return {
            statusCode: 302,
            headers: {
                Location: redirectUrl,
                'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
                Pragma: 'no-cache',
                'Access-Control-Allow-Origin': '*'
            },
            body: ''
        };
    } catch (e) {
        log('error', 'Prodamus return handling failed:', e);
        return {
            statusCode: 302,
            headers: {
                Location: process.env.APP_URL || 'https://vk.com/im',
                'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
                Pragma: 'no-cache',
                'Access-Control-Allow-Origin': '*'
            },
            body: ''
        };
    }
}

async function handleRobokassaResultRequest(event) {
    try {
        const result = await handleRobokassaResult(event);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
            body: String(result.responseText || '')
        };
    } catch (e) {
        log('error', 'Robokassa ResultURL handling failed:', e);
        return {
            statusCode: 400,
            headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
            body: 'Robokassa ResultURL error'
        };
    }
}

async function handleRobokassaReturnRequest(event) {
    const fallbackUrl = process.env.APP_URL || 'https://vk.com/im';
    try {
        const result = await handleRobokassaReturn(event);
        const redirectUrl = String(result.redirectUrl || fallbackUrl).trim();
        return {
            statusCode: 302,
            headers: {
                Location: redirectUrl,
                'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
                Pragma: 'no-cache',
                'Access-Control-Allow-Origin': '*'
            },
            body: ''
        };
    } catch (e) {
        log('error', 'Robokassa return handling failed:', e);
        return {
            statusCode: 302,
            headers: {
                Location: fallbackUrl,
                'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
                Pragma: 'no-cache',
                'Access-Control-Allow-Origin': '*'
            },
            body: ''
        };
    }
}

async function handlePurchaseDailyLimitPackage(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        await reconcileProfileBalanceTopUps(profileId, 'daily limit purchase');
        const result = await purchaseDailyLimitPackage(profileId, body.communityId || body.vkGroupId || '', body.cost);
        const dashboard = await getProfileDashboardOverview(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, result, dashboard })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handlePurchaseExtraLimitPackage(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        await reconcileProfileBalanceTopUps(profileId, 'extra limit purchase');
        const result = await purchaseExtraLimitPackage(profileId, body.cost);
        const dashboard = await getProfileDashboardOverview(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, result, dashboard })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSubmitBugReport(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const session = event.__adminSession || {};
        const report = await recordProfileErrorReport(profileId, {
            principalProfileId: body.principalProfileId || session.principalProfileId || profileId,
            communityId: body.communityId || '',
            pageUrl: body.pageUrl || '',
            userAgent: body.userAgent || getUserAgentFromEvent(event),
            description: body.description || '',
            screenshots: Array.isArray(body.screenshots) ? body.screenshots : []
        });
        await addAppLog({
            tab: 'ADMIN',
            title: 'Получено сообщение об ошибке',
            summary: `Профиль ${report.profileName || report.profileId} отправил описание ошибки`,
            details: [
                'Профиль: ' + report.profileId,
                'Скриншотов: ' + report.screenshots.length,
                'Страница: ' + (report.pageUrl || 'не указана')
            ],
            communityId: report.communityId || 'global',
            profileId
        });
        const dashboard = await getProfileDashboardOverview(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, report, dashboard })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleUpdateBugReportStatus(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const actor = await requireMainAdmin(event, body);
        const result = await updateBugReportStatus(body.reportId, body.status, actor.id);
        await addAppLog({
            tab: 'ADMIN',
            title: 'Изменён статус сообщения об ошибке',
            summary: `Статус ошибки изменён на ${result.report.statusLabel || result.report.status}`,
            details: [
                'Ошибка: ' + result.report.id,
                'Профиль: ' + result.report.profileId,
                result.reward ? ('Начислено вне суточного лимита: +' + result.reward.amount) : 'Начисление не выполнялось'
            ],
            communityId: result.report.communityId || 'global',
            profileId: result.report.profileId
        });
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, report: result.report, reward: result.reward })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSubmitSuggestionReport(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const session = event.__adminSession || {};
        const report = await recordProfileSuggestionReport(profileId, {
            principalProfileId: body.principalProfileId || session.principalProfileId || profileId,
            communityId: body.communityId || '',
            pageUrl: body.pageUrl || '',
            userAgent: body.userAgent || getUserAgentFromEvent(event),
            description: body.description || '',
            screenshots: Array.isArray(body.screenshots) ? body.screenshots : []
        });
        await addAppLog({
            tab: 'ADMIN',
            title: 'Получено предложение',
            summary: `Профиль ${report.profileName || report.profileId} отправил предложение`,
            details: [
                'Профиль: ' + report.profileId,
                'Скриншотов: ' + report.screenshots.length,
                'Страница: ' + (report.pageUrl || 'не указана')
            ],
            communityId: report.communityId || 'global',
            profileId
        });
        const dashboard = await getProfileDashboardOverview(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, report, dashboard })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleUpdateSuggestionReportStatus(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const actor = await requireMainAdmin(event, body);
        const result = await updateSuggestionReportStatus(body.reportId, body.status, actor.id);
        await addAppLog({
            tab: 'ADMIN',
            title: 'Изменён статус предложения',
            summary: `Статус предложения изменён на ${result.report.statusLabel || result.report.status}`,
            details: [
                'Предложение: ' + result.report.id,
                'Профиль: ' + result.report.profileId,
                result.reward ? ('Начислено вне суточного лимита: +' + result.reward.amount) : 'Начисление не выполнялось'
            ],
            communityId: result.report.communityId || 'global',
            profileId: result.report.profileId
        });
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, report: result.report, reward: result.reward })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSaveProfileAiIntegrations(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const integrations = await saveAiIntegrations(profileId, body.integrations || []);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, integrations })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleSaveProfilePaymentIntegrations(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const integrations = await savePaymentIntegrations(profileId, body.integrations || []);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, integrations })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleTestProfilePaymentIntegration(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const result = await testPaymentIntegration(body.integration || {});
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleTestProfileAiIntegration(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const result = await testAiIntegration(body.integration || {});
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleResolveProfileLimitRequest(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const actor = await requireMainAdmin(event, body);
        const request = await resolveProfileLimitRequest(body.requestId, body.status, actor.id, body.note || '');
        await addAppLog({
            tab: 'ADMIN',
            title: request.status === 'approved' ? 'Одобрено увеличение лимита' : 'Отклонён запрос на лимит',
            summary: `${request.profileName}: ${request.requestedLimit} запросов в сутки`,
            details: [
                'Профиль: ' + request.profileId,
                'Статус: ' + request.status,
                request.note ? 'Комментарий: ' + request.note : ''
            ],
            communityId: 'global',
            profileId: actor.id
        });
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, request })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

// 🔥 НОВОЕ: Удаление запроса на лимит
async function handleDeleteProfileLimitRequest(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const requestId = body.requestId;

        if (!requestId) {
            throw new Error('requestId is required');
        }

        // Проверяем, что это запрос текущего профиля или админ удаляет
        let isAdmin = false;
        try {
            await requireMainAdmin(event, body);
            isAdmin = true;
        } catch (e) {
            isAdmin = false;
        }

        const result = await deleteProfileLimitRequest(requestId, profileId, isAdmin);

        await addAppLog({
            tab: 'PROFILE',
            title: 'Запрос на лимит удален',
            summary: `Профиль удалил свой запрос на увеличение лимита`,
            details: ['Request ID: ' + requestId],
            communityId: 'global',
            profileId: profileId
        });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, result })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

/**
 * Удаление сообщества
 */
async function handleDeleteCommunity(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const communityId = body.community_id;

        if (!communityId || communityId === 'default') {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Cannot delete default community' })
            };
        }

        const result = await deleteCommunity(communityId, profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

/**
 * Загрузка вложений
 */
async function handleUploadAttachment(event) {
    return handleUploadAttachmentWithDependencies(event);
}

const RENDER_RELAY_CHUNK_PREFIX = 'render-upload-chunks/';
const RENDER_RELAY_MAX_CHUNK_BYTES = 2 * 1024 * 1024;
const RENDER_RELAY_GRANT_TTL_MS = 5 * 60 * 1000;

function getRenderRelaySecret() {
    return String(process.env.RENDER_UPLOAD_SHARED_SECRET || process.env.CALLBACK_SECRET || '').trim();
}

function renderRelayChunkKey(uploadId, index) {
    return `${RENDER_RELAY_CHUNK_PREFIX}${uploadId}/${index}.bin`;
}

function createRenderRelayGrant(payload) {
    const secret = getRenderRelaySecret();
    if (!secret) throw new Error('Render relay secret is not configured');
    const encoded = Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
    const signature = crypto.createHmac('sha256', secret).update(encoded).digest('base64url');
    return `${encoded}.${signature}`;
}

function verifyRenderRelayGrant(grant) {
    const parts = String(grant || '').split('.');
    const secret = getRenderRelaySecret();
    if (!secret || parts.length !== 2 || !parts[0] || !parts[1] || !secretsMatch(parts[1], crypto.createHmac('sha256', secret).update(parts[0]).digest('base64url'))) return null;
    try {
        const payload = JSON.parse(Buffer.from(parts[0], 'base64url').toString('utf8'));
        return payload && Number(payload.exp || 0) >= Date.now() ? payload : null;
    } catch (_error) {
        return null;
    }
}

async function handleCreateRenderUploadGrant(event) {
    try {
        const body = typeof event?.body === 'string' ? JSON.parse(event.body || '{}') : (event?.body || {});
        const q = getQueryParamsFromEvent(event);
        const session = await validateAdminSessionFromRequest(event, q, body);
        if (!session.ok) return buildAdminSessionErrorResponse(session);
        const profileId = getRequestProfileId(q, body);
        const communityId = String(body.communityId || '').trim();
        const uploadId = String(body.upload_id || '').trim();
        const target = String(body.target || '').trim().toLowerCase();
        if (!communityId || !uploadId || !target) return { statusCode: 400, headers: buildJsonHeaders(), body: JSON.stringify({ success: false, error: 'communityId, upload_id и target обязательны' }) };
        await loadBotConfig(profileId);
        const config = getFullConfig(profileId)?.communities?.[communityId] || {};
        const groupId = String(config.vk_group_id || '').trim();
        const userToken = await getUserToken(communityId, profileId);
        if (!groupId || !userToken) return { statusCode: 400, headers: buildJsonHeaders(), body: JSON.stringify({ success: false, needsUserToken: true, error: 'Для загрузки требуется сохранённый User Token и VK Group ID.' }) };
        const grant = createRenderRelayGrant({ profileId: normalizeProfileId(profileId), communityId, groupId, target, uploadId, exp: Date.now() + RENDER_RELAY_GRANT_TTL_MS });
        return { statusCode: 200, headers: buildJsonHeaders({ 'Cache-Control': 'no-store' }), body: JSON.stringify({ success: true, grant }) };
    } catch (error) {
        return { statusCode: 500, headers: buildJsonHeaders(), body: JSON.stringify({ success: false, error: error.message }) };
    }
}

async function readRenderRelayObject(body) {
    if (body && typeof body.transformToByteArray === 'function') {
        return Buffer.from(await body.transformToByteArray());
    }
    const chunks = [];
    for await (const chunk of body || []) chunks.push(Buffer.from(chunk));
    return Buffer.concat(chunks);
}

async function handleUploadAttachmentChunk(event, overrides = {}) {
    const s3 = overrides.s3Client || getS3Client();
    const uploadToVKImpl = overrides.uploadToVK || uploadToVK;
    const persistUploadedCommunityFileRecordImpl = overrides.persistUploadedCommunityFileRecord || persistUploadedCommunityFileRecord;
    const loadBotConfigImpl = overrides.loadBotConfig || loadBotConfig;
    const getUserTokenImpl = overrides.getUserToken || getUserToken;
    const getBucketNameImpl = overrides.getBucketName || getBucketName;
    try {
        const body = typeof event?.body === 'string' ? JSON.parse(event.body || '{}') : (event?.body || {});
        const uploadId = String(body.upload_id || '').trim();
        const grant = verifyRenderRelayGrant(body.render_grant);
        const chunkIndex = Number(body.chunk_index);
        const totalChunks = Number(body.total_chunks);
        const chunkBase64 = String(body.chunk_base64 || '');
        if (!grant || grant.uploadId !== uploadId || !/^[a-zA-Z0-9_.:-]{8,128}$/.test(uploadId) || !Number.isInteger(chunkIndex) || !Number.isInteger(totalChunks) || totalChunks < 1 || chunkIndex < 0 || chunkIndex >= totalChunks || !chunkBase64) {
            return { statusCode: 400, headers: buildJsonHeaders(), body: JSON.stringify({ success: false, error: 'Некорректные параметры чанка загрузки' }) };
        }
        const chunk = Buffer.from(chunkBase64, 'base64');
        if (!chunk.length || chunk.length > RENDER_RELAY_MAX_CHUNK_BYTES) {
            return { statusCode: 413, headers: buildJsonHeaders(), body: JSON.stringify({ success: false, error: 'Чанк загрузки превышает допустимый размер' }) };
        }
        const bucket = getBucketNameImpl();
        const key = renderRelayChunkKey(uploadId, chunkIndex);
        await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: chunk, ContentType: 'application/octet-stream' }));

        if (chunkIndex !== totalChunks - 1) {
            return { statusCode: 200, headers: buildJsonHeaders(), body: JSON.stringify({ success: true, pending: true, chunk_index: chunkIndex }) };
        }

        const chunks = [];
        try {
            for (let index = 0; index < totalChunks; index += 1) {
                const object = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: renderRelayChunkKey(uploadId, index) }));
                chunks.push(await readRenderRelayObject(object.Body));
            }
            const buffer = Buffer.concat(chunks);
            const q = getQueryParamsFromEvent(event);
            const profileId = grant.profileId || getRequestProfileId(q, body);
            await loadBotConfigImpl(profileId);
            const userToken = await getUserTokenImpl(grant.communityId, profileId);
            if (!userToken) throw new Error('User Token не настроен для загрузки вложений.');
            const attachment = await uploadToVKImpl(buffer, body.fileName, body.fileType, grant.target, grant.groupId);
            await persistUploadedCommunityFileRecordImpl({ ...body, profileId, attachment, fileSize: Number(body.fileSize || buffer.length) });
            return { statusCode: 200, headers: buildJsonHeaders(), body: JSON.stringify({ success: true, attachment, fileName: body.fileName, fileType: body.fileType, fileSize: Number(body.fileSize || buffer.length) }) };
        } finally {
            await Promise.all(Array.from({ length: totalChunks }, (_, index) => s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: renderRelayChunkKey(uploadId, index) })).catch(() => {})));
        }
    } catch (error) {
        return { statusCode: 500, headers: buildJsonHeaders(), body: JSON.stringify({ success: false, error: error.message }) };
    }
}

async function handleUploadAttachmentWithDependencies(event, overrides = {}) {
    try {
        const loadBotConfigImpl = overrides.loadBotConfig || loadBotConfig;
        const getUserTokenImpl = overrides.getUserToken || getUserToken;
        const uploadToVKImpl = overrides.uploadToVK || uploadToVK;
        const persistUploadedCommunityFileRecordImpl = overrides.persistUploadedCommunityFileRecord || persistUploadedCommunityFileRecord;
        await loadBotConfigImpl();
        const body = JSON.parse(event.body);
        const { fileBase64, fileType, fileName, target, groupId, communityId } = body;
        const q = event.queryStringParameters || event.query || event.params || {};
        const profileId = getRequestProfileId(q, body);

        if (!fileBase64 || !target) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Не передан файл или target' })
            };
        }

        const tokenCommunityId = String(communityId || groupId || '').trim();
        const userToken = await getUserTokenImpl(tokenCommunityId || null, profileId);
        if (!userToken) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: false,
                    needsUserToken: true,
                    targetTab: 'Settings',
                    targetElementId: 'userTokenSettingsBlock',
                    error: 'User Token не настроен для загрузки вложений. Укажите User Token в настройках сообщества.'
                })
            };
        }

        const buffer = Buffer.from(fileBase64, 'base64');
        const attachment = await uploadToVKImpl(buffer, fileName, fileType, target, groupId || communityId || null);
        await persistUploadedCommunityFileRecordImpl({
            ...body,
            attachment,
            fileSize: Number(body.fileSize || buffer.length || 0)
        });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                attachment,
                fileName,
                fileType,
                fileSize: Number(body.fileSize || buffer.length || 0)
            })
        };
    } catch (err) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: err.message })
        };
    }
}

async function persistUploadedCommunityFileRecord(payload, overrides = {}) {
    const resolveCommunityContextImpl = overrides.resolveCommunityContext || resolveCommunityContext;
    const recordUploadedCommunityFileImpl = overrides.recordUploadedCommunityFile || recordUploadedCommunityFile;
    const profileId = payload.profileId ? normalizeProfileId(payload.profileId) : null;
    const requestedCommunityId = String(payload.communityId || payload.groupId || '').trim();
    const fallbackGroupId = String(payload.groupId || '').trim();
    const context =
        await resolveCommunityContextImpl(requestedCommunityId || null, profileId || null) ||
        await resolveCommunityContextImpl(fallbackGroupId || null, profileId || null);

    if (!context) {
        throw new Error('Не удалось определить сообщество для каталога файлов');
    }

    const vkGroupId = String(context.config?.vk_group_id || fallbackGroupId || context.communityId || '').trim();
    await recordUploadedCommunityFileImpl({
        profileId: context.profileId,
        communityId: context.communityId,
        vkGroupId,
        groupName: context.config?.group_name || '',
        fileName: payload.fileName,
        fileType: payload.fileType,
        fileSize: Number(payload.fileSize || 0),
        attachment: payload.attachment
    });

    return {
        success: true,
        profileId: context.profileId,
        communityId: context.communityId,
        vkGroupId
    };
}

async function handleRecordUploadedFile(event) {
    try {
        await loadBotConfig();
        const body = JSON.parse(event.body || '{}');
        const result = await persistUploadedCommunityFileRecord(body);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, ...result })
        };
    } catch (error) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

async function persistConsentDocumentRecord(payload, overrides = {}) {
    const resolveCommunityContextImpl = overrides.resolveCommunityContext || resolveCommunityContext;
    const recordConsentDocumentVersionImpl = overrides.recordConsentDocumentVersion || recordConsentDocumentVersion;
    const profileId = payload.profileId ? normalizeProfileId(payload.profileId) : null;
    const requestedCommunityId = String(payload.communityId || payload.groupId || '').trim();
    const fallbackGroupId = String(payload.groupId || '').trim();
    const context =
        await resolveCommunityContextImpl(requestedCommunityId || null, profileId || null) ||
        await resolveCommunityContextImpl(fallbackGroupId || null, profileId || null);

    if (!context) {
        throw new Error('Не удалось определить сообщество для каталога документов');
    }

    const vkGroupId = String(context.config?.vk_group_id || fallbackGroupId || context.communityId || '').trim();
    const document = await recordConsentDocumentVersionImpl({
        profileId: context.profileId,
        communityId: context.communityId,
        vkGroupId,
        groupName: context.config?.group_name || '',
        documentType: payload.documentType || payload.type,
        fileName: payload.fileName,
        fileType: payload.fileType,
        fileSize: Number(payload.fileSize || 0),
        attachment: payload.attachment
    });

    return {
        success: true,
        profileId: context.profileId,
        communityId: context.communityId,
        vkGroupId,
        document
    };
}

async function handleRecordConsentDocument(event) {
    try {
        await loadBotConfig();
        const body = JSON.parse(event.body || '{}');
        const result = await persistConsentDocumentRecord(body);
        const dashboard = await getProfileDashboardOverview(result.profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, ...result, dashboard })
        };
    } catch (error) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

/**
 * Проверка прав токена
 */
async function handleDeleteProfileUploadedDocument(event) {
    return handleDeleteProfileUploadedDocumentWithDependencies(event);
}

async function handleDeleteProfilePaymentOperations(event) {
    return handleDeleteProfilePaymentOperationsWithDependencies(event);
}

async function handleExportAdminFinancialOperations(event) {
    return handleExportAdminFinancialOperationsWithDependencies(event);
}

function assertProfileReadAccess(event, profileId) {
    const principalProfile = event.__adminSession?.principalProfile || null;
    const principalProfileId = principalProfile ? normalizeProfileId(principalProfile.id) : '';
    if (!principalProfile) {
        const error = new Error('Сессия не найдена');
        error.statusCode = 401;
        throw error;
    }
    if (!isMainAdminProfile(principalProfile) && String(profileId) !== String(principalProfileId)) {
        const error = new Error('Недостаточно прав для просмотра статистики этого профиля');
        error.statusCode = 403;
        throw error;
    }
}

async function handleGetCommentActivityStats(event) {
    return handleGetCommentActivityStatsWithDependencies(event);
}

async function handleGetCommentActivityStatsWithDependencies(event, overrides = {}) {
    const getStatsImpl = overrides.getCommentActivityStats || getCommentActivityStatsWithDependencies;
    try {
        const q = getQueryParamsFromEvent(event);
        const profileId = getRequestProfileId(q, {});
        assertProfileReadAccess(event, profileId);
        const activityId = String(q.activityId || '').trim();
        const communityId = String(q.communityId || getActiveCommunityId(profileId) || '').trim();
        const stats = await getStatsImpl({
            profileId,
            communityId,
            activityId,
            activityTitle: q.activityTitle || ''
        }, overrides);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' },
            body: JSON.stringify({ success: true, stats })
        };
    } catch (error) {
        return {
            statusCode: error.statusCode || 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

async function handleExportCommentActivityStats(event) {
    return handleExportCommentActivityStatsWithDependencies(event);
}

async function handleResetCommentActivityStats(event) {
    return handleResetCommentActivityStatsWithDependencies(event);
}

async function handleResetCommentActivityStatsWithDependencies(event, overrides = {}) {
    const resetStatsImpl = overrides.resetCommentActivityStats || resetCommentActivityStatsWithDependencies;
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        assertProfileReadAccess(event, profileId);
        const activityId = String(body.activityId || q.activityId || '').trim();
        const result = await resetStatsImpl({ profileId, activityId }, overrides);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' },
            body: JSON.stringify({ success: true, result })
        };
    } catch (error) {
        return {
            statusCode: error.statusCode || (error instanceof SyntaxError ? 400 : 500),
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

async function handleExportCommentActivityStatsWithDependencies(event, overrides = {}) {
    const getStatsImpl = overrides.getCommentActivityStats || getCommentActivityStatsWithDependencies;
    const buildWorkbookImpl = overrides.buildCommentActivityStatsWorkbook || buildCommentActivityStatsWorkbook;
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        assertProfileReadAccess(event, profileId);
        const activityId = String(body.activityId || q.activityId || '').trim();
        const communityId = String(body.communityId || q.communityId || getActiveCommunityId(profileId) || '').trim();
        const activityTitle = String(body.activityTitle || q.activityTitle || '').trim();
        const stats = await getStatsImpl({ profileId, communityId, activityId, activityTitle }, overrides);
        const workbookBuffer = await buildWorkbookImpl(stats, { activityTitle });
        const fileName = normalizeCommentActivityStatsFilename(body.fileName);
        const encodedFileName = encodeURIComponent(fileName);
        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'Content-Disposition': `attachment; filename="comment-activity-stats.xlsx"; filename*=UTF-8''${encodedFileName}`,
                'Access-Control-Allow-Origin': '*',
                'Cache-Control': 'no-store'
            },
            isBase64Encoded: true,
            body: Buffer.from(workbookBuffer).toString('base64')
        };
    } catch (error) {
        return {
            statusCode: error.statusCode || (error instanceof SyntaxError ? 400 : 500),
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

async function handleExportAdminFinancialOperationsWithDependencies(event, overrides = {}) {
    const requireMainAdminImpl = overrides.requireMainAdmin || requireMainAdmin;
    const getAdminFinancialOperationsImpl = overrides.getAdminFinancialOperations || getAdminFinancialOperations;
    const buildWorkbookImpl = overrides.buildAdminFinancialOperationsWorkbook || buildAdminFinancialOperationsWorkbook;

    try {
        if (!event?.__adminSession?.principalProfile) {
            return {
                statusCode: 401,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Сессия администратора не найдена' })
            };
        }

        const body = JSON.parse(event.body || '{}');
        await requireMainAdminImpl(event, body);
        const operations = await getAdminFinancialOperationsImpl();
        const selectedOperations = selectFinancialOperations(operations, body.operationIds);
        const workbookBuffer = await buildWorkbookImpl(selectedOperations);
        const fileName = normalizeFinancialOperationsExportFilename(body.fileName);
        const encodedFileName = encodeURIComponent(fileName);

        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'Content-Disposition': `attachment; filename="financial-operations.xlsx"; filename*=UTF-8''${encodedFileName}`,
                'Access-Control-Allow-Origin': '*',
                'Cache-Control': 'no-store'
            },
            isBase64Encoded: true,
            body: Buffer.from(workbookBuffer).toString('base64')
        };
    } catch (error) {
        const message = String(error?.message || 'Не удалось создать Excel-файл.');
        const isForbidden = /Недостаточно прав/i.test(message);
        const isValidation = error instanceof SyntaxError ||
            /операц|500|не найден/i.test(message);
        return {
            statusCode: isForbidden ? 403 : (isValidation ? 400 : 500),
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: message })
        };
    }
}

async function handleDeleteProfilePaymentOperationsWithDependencies(event, overrides = {}) {
    const deleteProfilePaymentOperationsImpl = overrides.deleteProfilePaymentOperations || deleteProfilePaymentOperations;
    const getProfileDashboardOverviewImpl = overrides.getProfileDashboardOverview || getProfileDashboardOverview;

    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const principalProfile = event.__adminSession?.principalProfile || null;
        const principalProfileId = principalProfile ? normalizeProfileId(principalProfile.id) : '';

        if (!principalProfile) {
            return {
                statusCode: 401,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Сессия не найдена' })
            };
        }
        if (!isMainAdminProfile(principalProfile) && String(profileId) !== String(principalProfileId)) {
            return {
                statusCode: 403,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Недостаточно прав для удаления платежных операций этого профиля' })
            };
        }

        const result = await deleteProfilePaymentOperationsImpl(profileId, body.paymentIds);
        const dashboard = await getProfileDashboardOverviewImpl(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                removedCount: Number(result?.removedCount || 0),
                dashboard
            })
        };
    } catch (error) {
        const isValidationError = /profileId is required|paymentIds are required/.test(String(error.message || ''));
        return {
            statusCode: isValidationError ? 400 : 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

async function handleDeleteProfileUploadedDocumentWithDependencies(event, overrides = {}) {
    const loadBotConfigImpl = overrides.loadBotConfig || loadBotConfig;
    const requireMainAdminImpl = overrides.requireMainAdmin || requireMainAdmin;
    const deleteProfileUploadedDocumentImpl = overrides.deleteProfileUploadedDocument || deleteProfileUploadedDocument;
    const getProfileDashboardOverviewImpl = overrides.getProfileDashboardOverview || getProfileDashboardOverview;

    try {
        await loadBotConfigImpl();
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        await requireMainAdminImpl(event, body);

        const profileId = getRequestProfileId(q, body);
        const payload = {
            profileId,
            communityId: String(body.communityId || body.groupId || '').trim(),
            kind: String(body.kind || 'file').trim(),
            documentType: String(body.documentType || '').trim(),
            attachment: String(body.attachment || '').trim(),
            version: String(body.version || '').trim()
        };
        const result = await deleteProfileUploadedDocumentImpl(payload);
        const dashboard = await getProfileDashboardOverviewImpl(profileId);

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, result, dashboard })
        };
    } catch (error) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

async function handleRecoverRenderUpload(event, overrides = {}) {
    const httpGet = overrides.httpGet || require('axios').get;
    const body = typeof event?.body === 'string' ? JSON.parse(event.body || '{}') : (event?.body || {});
    const uploadId = String(body.uploadId || body.upload_id || '').trim();

    if (!/^[a-zA-Z0-9_.:-]{8,128}$/.test(uploadId)) {
        return {
            statusCode: 400,
            headers: buildJsonHeaders(),
            body: JSON.stringify({ success: false, error: 'upload_id is required' })
        };
    }

    const renderUrl = String(process.env.RENDER_UPLOAD_URL || 'https://vk-uploader.onrender.com').replace(/\/+$/, '');
    try {
        const response = await httpGet(`${renderUrl}/upload-result`, {
            timeout: Number(overrides.timeoutMs || 10000),
            params: {
                upload_id: uploadId,
                t: Date.now()
            }
        });

        return {
            statusCode: response.status || 200,
            headers: buildJsonHeaders(),
            body: JSON.stringify(response.data || {})
        };
    } catch (error) {
        const status = error?.response?.status || 502;
        const payload = error?.response?.data || {
            success: false,
            error: error.message || 'Render recovery failed'
        };

        return {
            statusCode: status,
            headers: buildJsonHeaders(),
            body: JSON.stringify(payload)
        };
    }
}

async function handleCheckTokenPermissions(event) {
    try {
        const result = await getTokenPermissions();
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (error) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

const TELEGRAM_ALLOWED_UPDATES = [
    'message',
    'edited_message',
    'channel_post',
    'edited_channel_post',
    'callback_query',
    'my_chat_member'
];

function toPublicTelegramBotConfig(connectorId, config = {}) {
    const token = String(config.bot_token || '');
    return {
        connector_id: connectorId,
        bot_id: String(config.bot_id || ''),
        username: String(config.username || ''),
        first_name: String(config.first_name || ''),
        display_name: String(config.display_name || config.first_name || config.username || ''),
        can_join_groups: config.can_join_groups !== false,
        can_read_all_group_messages: config.can_read_all_group_messages === true,
        supports_inline_queries: config.supports_inline_queries === true,
        token_set: Boolean(token),
        token_hint: token ? `***${token.slice(-4)}` : '',
        webhook_url: String(config.webhook_url || ''),
        webhook_status: String(config.webhook_status || ''),
        webhook_error: String(config.webhook_error || ''),
        allowed_updates: Array.isArray(config.allowed_updates) ? config.allowed_updates : [],
        updated_at: String(config.updated_at || '')
    };
}

function getHeaderValue(event, headerName) {
    const expected = String(headerName || '').toLowerCase();
    for (const [key, value] of Object.entries(event?.headers || {})) {
        if (String(key).toLowerCase() === expected) {
            return Array.isArray(value) ? String(value[0] || '') : String(value || '');
        }
    }
    return '';
}

function secretsMatch(actual, expected) {
    const actualBuffer = Buffer.from(String(actual || ''), 'utf8');
    const expectedBuffer = Buffer.from(String(expected || ''), 'utf8');
    if (!actualBuffer.length || actualBuffer.length !== expectedBuffer.length) return false;
    return crypto.timingSafeEqual(actualBuffer, expectedBuffer);
}

function buildTelegramWebhookUrl(appUrl, botId) {
    const normalizedBotId = String(botId || '').trim();
    if (!/^\d+$/.test(normalizedBotId)) throw new Error('Telegram Bot ID должен быть числом');
    const url = new URL(String(appUrl || '').trim());
    url.search = '';
    url.hash = '';
    url.searchParams.set('telegramWebhook', '1');
    url.searchParams.set('botId', normalizedBotId);
    return url.toString();
}

async function configureTelegramWebhook(config, connectorId, profileId, overrides = {}) {
    const setTelegramWebhookImpl = overrides.setTelegramWebhook || setTelegramWebhook;
    const getTelegramWebhookInfoImpl = overrides.getTelegramWebhookInfo || getTelegramWebhookInfo;
    const saveTelegramBotConfigImpl = overrides.saveTelegramBotConfig || saveTelegramBotConfig;
    const appUrl = overrides.appUrl || process.env.APP_URL;
    if (!appUrl) throw new Error('APP_URL не задан');

    const botId = String(config.bot_id || '').trim();
    const webhookSecret = String(config.webhook_secret || crypto.randomBytes(24).toString('base64url'));
    const webhookUrl = buildTelegramWebhookUrl(appUrl, botId);
    try {
        await setTelegramWebhookImpl(config.bot_token, {
            url: webhookUrl,
            secretToken: webhookSecret,
            allowedUpdates: TELEGRAM_ALLOWED_UPDATES,
            dropPendingUpdates: false
        });
        const webhookInfo = await getTelegramWebhookInfoImpl(config.bot_token);

        const updatedConfig = {
            ...config,
            webhook_secret: webhookSecret,
            webhook_url: webhookUrl,
            webhook_status: webhookInfo?.url === webhookUrl ? 'active' : 'pending',
            webhook_error: String(webhookInfo?.last_error_message || ''),
            allowed_updates: TELEGRAM_ALLOWED_UPDATES
        };
        await saveTelegramBotConfigImpl(updatedConfig, connectorId, profileId);
        return updatedConfig;
    } catch (error) {
        try {
            await saveTelegramBotConfigImpl({
                ...config,
                webhook_secret: webhookSecret,
                webhook_url: webhookUrl,
                webhook_status: 'unknown',
                webhook_error: String(error?.message || 'Не удалось проверить webhook'),
                allowed_updates: TELEGRAM_ALLOWED_UPDATES
            }, connectorId, profileId);
        } catch (saveError) {
            log('warn', `Telegram webhook failure status was not saved for ${connectorId}: ${saveError.message}`);
        }
        throw error;
    }
}

async function handleConnectTelegramBotWithDependencies(event, overrides = {}) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const token = String(body.bot_token || body.token || '').trim();
        if (!token) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Укажите токен Telegram-бота из BotFather' })
            };
        }

        const getTelegramBotIdentityImpl = overrides.getTelegramBotIdentity || getTelegramBotIdentity;
        const saveTelegramBotConfigImpl = overrides.saveTelegramBotConfig || saveTelegramBotConfig;
        const identity = await getTelegramBotIdentityImpl(token);
        const botId = String(identity?.id || '').trim();
        if (!/^\d+$/.test(botId) || identity?.is_bot !== true) {
            throw new Error('Токен не принадлежит Telegram-боту');
        }

        const connectorId = `tg_${botId}`;
        const existing = await (overrides.resolveTelegramBotContext || resolveTelegramBotContext)(connectorId, profileId);
        const config = {
            bot_id: botId,
            bot_token: token,
            username: String(identity.username || ''),
            first_name: String(identity.first_name || ''),
            display_name: String(body.display_name || identity.first_name || identity.username || `Telegram ${botId}`),
            can_join_groups: identity.can_join_groups !== false,
            can_read_all_group_messages: identity.can_read_all_group_messages === true,
            supports_inline_queries: identity.supports_inline_queries === true,
            webhook_secret: existing?.config?.webhook_secret || crypto.randomBytes(24).toString('base64url'),
            webhook_status: 'pending',
            webhook_error: '',
            allowed_updates: TELEGRAM_ALLOWED_UPDATES
        };

        await saveTelegramBotConfigImpl(config, connectorId, profileId);
        const configured = await configureTelegramWebhook(config, connectorId, profileId, {
            ...overrides,
            saveTelegramBotConfig: saveTelegramBotConfigImpl
        });

        await (overrides.addAppLog || addAppLog)({
            tab: 'SETTINGS',
            title: 'Подключён Telegram-бот',
            summary: configured.username ? `@${configured.username}` : configured.display_name,
            details: [
                'Telegram Bot ID: ' + botId,
                'Webhook: ' + configured.webhook_status
            ],
            communityId: connectorId,
            profileId
        });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                profileId,
                active_telegram_bot: connectorId,
                bot: toPublicTelegramBotConfig(connectorId, configured)
            })
        };
    } catch (error) {
        log('error', 'handleConnectTelegramBot error:', error.message);
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

async function handleConnectTelegramBot(event) {
    return handleConnectTelegramBotWithDependencies(event);
}

async function handleRefreshTelegramWebhook(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const connectorId = String(body.connector_id || getActiveTelegramBotId(profileId) || '');
        const resolved = await resolveTelegramBotContext(connectorId, profileId);
        if (!resolved?.config?.bot_token) throw new Error('Telegram-бот не найден');
        const identity = await getTelegramBotIdentity(resolved.config.bot_token);
        const configured = await configureTelegramWebhook({
            ...resolved.config,
            username: String(identity?.username || resolved.config.username || ''),
            first_name: String(identity?.first_name || resolved.config.first_name || ''),
            can_join_groups: identity?.can_join_groups !== false,
            can_read_all_group_messages: identity?.can_read_all_group_messages === true,
            supports_inline_queries: identity?.supports_inline_queries === true
        }, resolved.connectorId, profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                bot: toPublicTelegramBotConfig(resolved.connectorId, configured)
            })
        };
    } catch (error) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

async function handleTestTelegramBot(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const connectorId = String(body.connector_id || getActiveTelegramBotId(profileId) || '');
        const chatId = String(body.chat_id || '').trim();
        const text = String(body.text || 'Тестовое сообщение PAPA BOT').trim();
        const resolved = await resolveTelegramBotContext(connectorId, profileId);
        if (!resolved?.config?.bot_token) throw new Error('Telegram-бот не найден');
        if (!chatId) throw new Error('Укажите Telegram Chat ID');
        await sendTelegramResponse(resolved.config.bot_token, chatId, text);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true })
        };
    } catch (error) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

async function handleDeleteTelegramBot(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const connectorId = String(body.connector_id || '');
        const resolved = await resolveTelegramBotContext(connectorId, profileId);
        if (!resolved?.config) throw new Error('Telegram-бот не найден');
        if (resolved.config.bot_token) {
            try {
                await deleteTelegramWebhook(resolved.config.bot_token);
            } catch (error) {
                log('warn', `Telegram deleteWebhook failed for ${connectorId}: ${error.message}`);
            }
        }
        const result = await deleteTelegramBot(connectorId, profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
        };
    } catch (error) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: error.message })
        };
    }
}

function buildTelegramAdminJson(statusCode, payload) {
    return {
        statusCode,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify(payload)
    };
}

async function handleSaveTelegramChat(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const chat = await saveTelegramChat({
            chat_id: body.chat_id,
            kind: body.kind,
            title: body.title,
            username: body.username,
            description: body.description
        }, profileId);
        return buildTelegramAdminJson(200, { success: true, chat });
    } catch (error) {
        return buildTelegramAdminJson(400, { success: false, error: error.message });
    }
}

async function handleDeleteTelegramChat(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const result = await deleteTelegramChat(body.telegram_chat_id || body.id, profileId);
        return buildTelegramAdminJson(200, result);
    } catch (error) {
        return buildTelegramAdminJson(400, { success: false, error: error.message });
    }
}

async function handleBindTelegramBotToChatWithDependencies(event, overrides = {}) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const telegramChatId = String(body.telegram_chat_id || '').trim();
        const connectorId = String(body.connector_id || '').trim();
        const listTelegramChatsImpl = overrides.listTelegramChats || listTelegramChats;
        const resolveTelegramBotContextImpl = overrides.resolveTelegramBotContext || resolveTelegramBotContext;
        const getTelegramChatImpl = overrides.getTelegramChat || getTelegramChat;
        const getTelegramChatMemberImpl = overrides.getTelegramChatMember || getTelegramChatMember;
        const getTelegramBotIdentityImpl = overrides.getTelegramBotIdentity || getTelegramBotIdentity;
        const saveTelegramChatImpl = overrides.saveTelegramChat || saveTelegramChat;
        const saveTelegramChatBindingImpl = overrides.saveTelegramChatBinding || saveTelegramChatBinding;
        const chats = await listTelegramChatsImpl(profileId);
        const chat = chats.find(item => String(item?.id || '') === telegramChatId);
        if (!chat) throw new Error('Сначала добавьте канал или группу');

        const bot = await resolveTelegramBotContextImpl(connectorId, profileId);
        if (!bot?.config?.bot_token) throw new Error('Выбранный Telegram-бот не найден');
        const telegramChat = await getTelegramChatImpl(bot.config.bot_token, chat.chat_id);
        const actualType = String(telegramChat?.type || '');
        const actualKind = actualType === 'channel'
            ? 'channel'
            : (['group', 'supergroup'].includes(actualType) ? 'group' : null);
        if (!actualKind || actualKind !== chat.kind) {
            throw new Error(`Telegram вернул тип чата «${actualType || 'неизвестно'}», который не соответствует разделу`);
        }

        const member = await getTelegramChatMemberImpl(bot.config.bot_token, telegramChat.id, bot.config.bot_id);
        const botStatus = String(member?.status || '');
        if (!['member', 'administrator', 'creator'].includes(botStatus)) {
            throw new Error('Добавьте выбранного бота в этот чат и повторите привязку');
        }
        if (chat.kind === 'channel') {
            if (!['administrator', 'creator'].includes(botStatus)) {
                throw new Error('Для работы в канале назначьте бота администратором');
            }
            if (member?.can_post_messages === false) {
                throw new Error('У бота нет права публиковать сообщения в канале');
            }
        }
        const identity = await getTelegramBotIdentityImpl(bot.config.bot_token);
        const canReadAllGroupMessages = identity?.can_read_all_group_messages === true;
        if (
            chat.kind === 'group' &&
            !['administrator', 'creator'].includes(botStatus) &&
            !canReadAllGroupMessages
        ) {
            throw new Error(
                'Бот добавлен в группу как обычный участник, а Privacy Mode включён: обычные сообщения и триггеры сценариев ему не передаются. ' +
                'Назначьте бота администратором группы или отключите /setprivacy в BotFather, затем повторите проверку.'
            );
        }

        const savedChat = await saveTelegramChatImpl({
            ...chat,
            chat_id: telegramChat.id,
            title: telegramChat.title || chat.title,
            username: telegramChat.username || chat.username
        }, profileId);
        const binding = await saveTelegramChatBindingImpl({
            connector_id: connectorId,
            chat_id: savedChat.chat_id,
            chat_type: actualType,
            kind: savedChat.kind,
            title: savedChat.title,
            username: savedChat.username,
            bot_status: botStatus,
            can_post_messages: member?.can_post_messages,
            can_delete_messages: member?.can_delete_messages,
            can_read_all_group_messages: canReadAllGroupMessages,
            enabled: true
        }, profileId);
        return buildTelegramAdminJson(200, { success: true, chat: savedChat, binding });
    } catch (error) {
        return buildTelegramAdminJson(400, { success: false, error: error.message });
    }
}

async function handleBindTelegramBotToChat(event) {
    return handleBindTelegramBotToChatWithDependencies(event);
}

async function handleUnbindTelegramBotFromChat(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const result = await deleteTelegramChatBinding(body.binding_id, profileId);
        return buildTelegramAdminJson(200, result);
    } catch (error) {
        return buildTelegramAdminJson(400, { success: false, error: error.message });
    }
}

async function handleUploadTelegramAttachment(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const bindingId = String(body.binding_id || '').trim();
        let connectorId = String(body.connector_id || '').trim();
        let uploadChatId = String(body.chat_id || '').trim();
        if (bindingId) {
            const bindings = await listTelegramChatBindings(profileId);
            const binding = bindings.find(item => String(item?.binding_id || '') === bindingId);
            if (!binding) throw new Error('Выбранная привязка TG-бота к чату не найдена');
            connectorId = String(binding.connector_id || '');
            uploadChatId = String(binding.chat_id || '');
        }
        if (!connectorId || !uploadChatId) {
            throw new Error('Для загрузки укажите TG-бота и Chat ID, куда он может отправить служебное сообщение');
        }
        const bot = await resolveTelegramBotContext(connectorId, profileId);
        if (!bot?.config?.bot_token) throw new Error('Токен выбранного Telegram-бота не найден');

        const uploaded = await uploadTelegramFile(bot.config.bot_token, {
            chatId: uploadChatId,
            filename: body.filename,
            mimeType: body.mime_type,
            base64: body.base64
        });
        const entry = await saveTelegramFileCatalogEntry(connectorId, {
            ...uploaded,
            name: body.name || body.filename,
            source_chat_id: uploadChatId
        }, profileId);
        return buildTelegramAdminJson(200, {
            success: true,
            attachment: {
                ...entry,
                value: entry.kind === 'photo'
                    ? `tg:photo:${entry.file_id}`
                    : (entry.kind === 'video' ? `tg:video:${entry.file_id}` : `tg:file:${entry.file_id}`)
            },
            warning: uploaded.delete_warning || ''
        });
    } catch (error) {
        return buildTelegramAdminJson(400, { success: false, error: error.message });
    }
}

async function handleDeleteTelegramAttachment(event) {
    try {
        const q = getQueryParamsFromEvent(event);
        const body = JSON.parse(event.body || '{}');
        const profileId = getRequestProfileId(q, body);
        const result = await deleteTelegramFileCatalogEntry(body.connector_id, body.entry_id, profileId);
        return buildTelegramAdminJson(200, result);
    } catch (error) {
        return buildTelegramAdminJson(400, { success: false, error: error.message });
    }
}

async function handleTelegramWebhookWithDependencies(event, overrides = {}) {
    const resolveTelegramBotContextImpl = overrides.resolveTelegramBotContext || resolveTelegramBotContext;
    const recordProfileEventUsageImpl = overrides.recordProfileEventUsage || recordProfileEventUsage;
    const buildTelegramEventEnvelopeImpl = overrides.buildTelegramEventEnvelope || buildTelegramEventEnvelope;
    const publishIncomingEventImpl = overrides.publishIncomingEvent || publishIncomingEvent;
    try {
        const q = getQueryParamsFromEvent(event);
        const botId = String(q.botId || '').trim();
        const resolved = await resolveTelegramBotContextImpl(botId);
        if (!resolved?.config) {
            return { statusCode: 404, body: 'telegram-bot-not-found' };
        }

        const secret = getHeaderValue(event, 'x-telegram-bot-api-secret-token');
        if (!secretsMatch(secret, resolved.config.webhook_secret)) {
            return { statusCode: 403, body: 'invalid-telegram-webhook-secret' };
        }

        const update = JSON.parse(event.body || '{}');
        const eventType = Object.keys(update).find(key => key !== 'update_id') || 'unknown';
        const usage = await recordProfileEventUsageImpl(
            resolved.profileId,
            resolved.connectorId,
            `telegram:${eventType}`
        );
        if (!usage.allowed) {
            log('warn', `⛔ Daily PAPA BOT limit reached for Telegram profile ${resolved.profileId}`);
            return { statusCode: 200, body: 'ok' };
        }

        const envelope = buildTelegramEventEnvelopeImpl(update, {
            botId: resolved.config.bot_id,
            connectorId: resolved.connectorId,
            profileId: resolved.profileId,
            receivedAt: new Date().toISOString()
        });
        if (envelope) {
            await publishIncomingEventImpl(envelope);
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'text/plain' },
            body: 'ok'
        };
    } catch (error) {
        log('error', 'Telegram webhook error:', error.message);
        return { statusCode: 500, body: 'telegram-webhook-error' };
    }
}

async function handleTelegramWebhook(event) {
    return handleTelegramWebhookWithDependencies(event);
}

/**
 * Обработка вебхука VK
 */
async function handleVkWebhookWithDependencies(event, overrides = {}) {
    const logImpl = overrides.log || log;
    const resolveCommunityContextImpl = overrides.resolveCommunityContext || resolveCommunityContext;
    const setActiveCommunityImpl = overrides.setActiveCommunity || setActiveCommunity;
    const recordProfileEventUsageImpl = overrides.recordProfileEventUsage || recordProfileEventUsage;
    const buildEventEnvelopeImpl = overrides.buildEventEnvelope || buildEventEnvelope;
    const publishIncomingEventImpl = overrides.publishIncomingEvent || publishIncomingEvent;
    try {
        const data = JSON.parse(event.body || '{}');

        if (data.type === 'confirmation') {
            const groupId = data.group_id?.toString() || null;
            logImpl('info', `🔑 Confirmation request from community: ${groupId}`);
            const resolved = await resolveCommunityContextImpl(groupId);
            let confirmationCode = resolved?.config?.confirmation_token || null;
            if (!confirmationCode) {
                confirmationCode = process.env.CONFIRMATION_TOKEN;
            }

            logImpl('info', `✅ Returning confirmation code: ${confirmationCode?.substring(0, 4)}...`);
            return {
                statusCode: 200,
                body: confirmationCode || 'error_no_token'
            };
        }

        if (!isSupportedEventType(data.type)) {
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
                body: 'ok'
            };
        }

        const groupId = data.group_id?.toString() || 'default';
        const resolved = await resolveCommunityContextImpl(groupId);
        if (resolved?.communityId) {
            setActiveCommunityImpl(resolved.communityId, resolved.profileId);
        }

        const profileId = resolved?.profileId || '1';
        const usage = await recordProfileEventUsageImpl(profileId, groupId, data.type);
        if (!usage.allowed) {
            logImpl('warn', `⛔ Daily PAPA BOT limit reached for profile ${profileId}`);
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
                body: 'ok'
            };
        }

        const envelope = buildEventEnvelopeImpl(data, {
            profileId,
            communityId: groupId,
            receivedAt: new Date().toISOString()
        });

        if (!envelope) {
            logImpl('warn', 'VK event skipped: envelope builder returned null');
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
                body: 'ok'
            };
        }

        await publishIncomingEventImpl(envelope);

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
            body: 'ok'
        };
    } catch (e) {
        logImpl('error', 'Handler error:', e.message);
        return {
            statusCode: 500,
            headers: { 'Access-Control-Allow-Origin': '*' },
            body: 'Internal error'
        };
    }
}

async function handleVkWebhook(event) {
    return handleVkWebhookWithDependencies(event);
}

/**
 * Тестовая отправка сообщения пользователю из админ-панели
 */
async function handleTestSend(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const { userId, text, attachments, keyboard, communityId, vkGroupId, stepActions, sourceBot } = body;
        const profileId = getRequestProfileId(q, body);

        log('info', '🧪 TEST SEND request:', { userId, textPreview: (text || '').substring(0, 50), communityId, vkGroupId, hasStepActions: !!stepActions });

        if (!userId) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'userId обязателен' })
            };
        }

        await initializeStorage();
        await loadBotConfig(profileId);

        const { sendMessage } = require('./modules/vk-api');
        const { getCommunityConfig, getVkToken, getAllCommunityIds } = require('./modules/config');

        const allCommunities = getAllCommunityIds(profileId);
        log('info', '🧪 TEST SEND: Available communities:', allCommunities);
        log('info', '🧪 TEST SEND: Requested communityId:', communityId);

        // Определяем сообщество для отправки
        // ПРИОРИТЕТ: vkGroupId > communityId > 'default'
        let targetCommunityId = communityId || 'default';

        // Если передан vkGroupId — ищем сообщество с таким vk_group_id
        if (vkGroupId) {
            const fullConfig = require('./modules/config').getFullConfig(profileId) || {};
            const communities = fullConfig.communities || {};

            for (const [cid, cfg] of Object.entries(communities)) {
                if (cfg.vk_group_id && cfg.vk_group_id.toString() === vkGroupId.toString()) {
                    targetCommunityId = cid;
                    log('info', '🧪 TEST SEND: Found community by vkGroupId: ' + cid + ' (vk_group_id=' + vkGroupId + ')');
                    break;
                }
            }
        } else if (communityId && !isNaN(parseInt(communityId))) {
            // Если vkGroupId не передан, но communityId числовой — ищем по vk_group_id
            const numId = parseInt(communityId).toString();
            const fullConfig = require('./modules/config').getFullConfig(profileId) || {};
            const communities = fullConfig.communities || {};

            for (const [cid, cfg] of Object.entries(communities)) {
                if (cfg.vk_group_id && cfg.vk_group_id.toString() === numId) {
                    targetCommunityId = cid;
                    log('info', '🧪 TEST SEND: Found community by numeric communityId: ' + cid);
                    break;
                }
            }
        }

        log('info', '🧪 TEST SEND: Using targetCommunityId:', targetCommunityId);

        const config = await getCommunityConfig(targetCommunityId, profileId);
        const token = await getVkToken(0, targetCommunityId, profileId);

        log('info', '🧪 TEST SEND: Config:', {
            targetCommunityId,
            vk_group_id: config?.vk_group_id,
            group_name: config?.group_name,
            hasToken: !!token,
            tokenStart: token ? token.substring(0, 15) + '...' : 'NONE'
        });

        if (!token) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'VK Token не настроен для сообщества ' + (config?.group_name || targetCommunityId) })
            };
        }

        const groupId = config.vk_group_id || targetCommunityId;

        // Подготавливаем keyboard если передан как строка
        let parsedKeyboard = null;
        if (keyboard) {
            try {
                parsedKeyboard = sanitizeKeyboardForVk(typeof keyboard === 'string' ? JSON.parse(keyboard) : keyboard);
                parsedKeyboard = await resolvePaymentKeyboard(parsedKeyboard, {
                    profileId,
                    communityId: targetCommunityId,
                    userId,
                    groupId,
                    sourceBot: String(sourceBot || '').trim(),
                    source: 'admin_test_send'
                });

                // ✅ Sanitize keyboard - удалить недопустимые поля для VK API
                // open_link кнопки НЕ поддерживают label и color
                if (parsedKeyboard && parsedKeyboard.buttons) {
                    for (const row of parsedKeyboard.buttons) {
                        for (const btn of row) {
                            if (btn.action) {
                                if (btn.action.type === 'open_link') {
                                    delete btn.action.color;
                                    delete btn.action.payload;
                                }
                                if (btn.action.type === 'text' && !btn.action.payload) {
                                    btn.action.payload = {};
                                }
                            }
                        }
                    }
                }
            } catch (e) {
                log('debug', '⚠️ Keyboard parse error:', e.message);
                parsedKeyboard = null;
            }
        }

        // Подготавливаем attachments
        let attachmentArray = attachments ? (typeof attachments === 'string' ? attachments.split(',').map(a => a.trim()).filter(a => a) : attachments) : [];

        // ✅ Обрабатываем вложения через модуль attachments (скачивание doc, re-upload в сообщество)
        if (attachmentArray.length > 0) {
            try {
                const { processAttachmentWithUserToken } = require('./modules/attachments');
                log('debug', '🧪 TEST SEND: Processing ' + attachmentArray.length + ' attachments...');

                const processedAttachments = [];
                for (const attachment of attachmentArray) {
                    if (!attachment || !attachment.trim()) continue;
                    const processed = await processAttachmentWithUserToken(attachment.trim(), groupId);
                    processedAttachments.push(processed || attachment);
                }
                attachmentArray = processedAttachments;
                log('debug', '🧪 TEST SEND: Processed attachments:', JSON.stringify(attachmentArray));
            } catch (attachError) {
                log('error', '🧪 TEST SEND: Attachment processing error:', attachError);
                // Не прерываем — пробуем отправить без вложений
                attachmentArray = [];
            }
        }

        // Реально выполняем действия шага если переданы (ОДИН раз!)
        const actionResults = [];
        if (stepActions) {
            try {
                const { performRowActions } = require('./modules/row-actions');
                const { getSheetData, saveSheetData, invalidateCache } = require('./modules/storage');

                // Убеждаемся что пользователь существует (для создания пользователя)
                const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', targetCommunityId, profileId);
                const existingUser = users.find(r => r['ID'] == userId);
                if (!existingUser) {
                    log('info', '🧪 TEST: Creating user ' + userId + '...');
                    const { getUserName } = require('./modules/vk-api');
                    const userName = await getUserName(userId, token) || ('User_' + userId);
                    users.push({
                        'ID': userId.toString(),
                        'ИМЯ': userName,
                        'ГРУППА': '',
                        'Пользовательская': '',
                        'Значения ПП': '',
                        'Переменная ПВС': '',
                        'Значение ПВС': '',
                        'Текущий Бот': '',
                        'Текущий Шаг': '',
                        'Отправленные Шаги': ''
                    });
                    await saveSheetData('ПОЛЬЗОВАТЕЛИ', users, targetCommunityId, profileId);
                    invalidateCache('ПОЛЬЗОВАТЕЛИ', targetCommunityId, profileId);
                }

                const fakeRow = {
                    'Бот': stepActions.bot || '',
                    'Шаг': stepActions.step || '',
                    'Задержка отправки на Шаг': stepActions.delay || '',
                    'ДОБАВИТЬ ГРУППУ': stepActions.addGroup || '',
                    'УДАЛИТЬ ГРУППУ': stepActions.removeGroup || '',
                    'Отправить на Шаг': stepActions.sendToStep || '',
                    'Действия с ПП': stepActions.ppActions || '',
                    'Действия с ГП': stepActions.gpActions || '',
                    'Действия с ПВС': stepActions.pvsActions || '',
                    'Действия с ПП/ГП/ПВК': stepActions.variableActions || ''
                };

                log('info', '🧪 TEST: Executing variable actions BEFORE text replacement');
                await performRowActions(fakeRow, userId, groupId, false, targetCommunityId, profileId);
                log('info', '🧪 TEST: Variable actions completed, variables are now saved');
            } catch (actionError) {
                log('error', '🧪 TEST: Variable actions error:', actionError);
            }
        }

        // Заменяем переменные в тексте (ПОСЛЕ выполнения действий)
        const { replaceVariables } = require('./modules/variables');
        const processedText = await replaceVariables(text || 'Тестовое сообщение', userId, groupId, targetCommunityId, profileId);

        log('info', '🧪 TEST SEND: After variable replacement:', { original: (text || '').substring(0, 80), processed: processedText.substring(0, 150) });

        // Отправляем сообщение
        const response = await sendMessage(userId, processedText, parsedKeyboard, groupId, attachmentArray, token);

        log('info', '🧪 TEST SEND: sendMessage response:', response);

        if (response.error) {
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: response.error.error_msg, errorCode: response.error.error_code })
            };
        }

        // Формируем отчёт о выполненных действиях (ДЛЯ UI, НЕ выполняем повторно)
        if (stepActions) {
            if (stepActions.delay) {
                actionResults.push('⏱️ <strong>Задержка отправки:</strong> ' + escapeHtml(stepActions.delay) + ' — ✅ запланировано');
            }
            if (stepActions.addGroup) {
                actionResults.push('➕ <strong>Добавить в группу:</strong> ' + escapeHtml(stepActions.addGroup) + ' — ✅ выполнено');
            }
            if (stepActions.removeGroup) {
                actionResults.push('➖ <strong>Удалить из группы:</strong> ' + escapeHtml(stepActions.removeGroup) + ' — ✅ выполнено');
            }
            if (stepActions.sendToStep) {
                if (stepActions.delay) {
                    actionResults.push('🔄 <strong>Перевести на шаг:</strong> ' + escapeHtml(stepActions.sendToStep) + ' (с задержкой ' + escapeHtml(stepActions.delay) + ') — ✅ запланировано');
                } else {
                    actionResults.push('🔄 <strong>Перевести на шаг:</strong> ' + escapeHtml(stepActions.sendToStep) + ' — ✅ выполнено');
                }
            }
            if (stepActions.variableActions) {
                actionResults.push('📊 <strong>Переменные:</strong> ' + escapeHtml(stepActions.variableActions) + ' — ✅ выполнено');
            }

            log('info', '🧪 TEST Step actions completed for user ' + userId);
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                messageId: response.response,
                actionResults: actionResults
            })
        };
    } catch (e) {
        log('error', 'handleTestSend error:', e);
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

async function handleVerifyAuth(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const { username, password } = body;
        const ip = getClientIpFromEvent(event);
        const userAgent = getUserAgentFromEvent(event);
        const captchaAnswer = String(body.captchaAnswer || body.answer || '').trim();
        const loginCaptchaStatus = await getLoginCaptchaStatus(ip);

        const loginStatus = await getLoginStatus(username);
        if (loginStatus.lockUntil && loginStatus.lockUntil > Date.now()) {
            return {
                statusCode: 401,
                headers: buildJsonHeaders(),
                body: JSON.stringify({
                    success: false,
                    locked: true,
                    recoveryRequired: true,
                    recoveryUsername: username,
                    lockUntil: loginStatus.lockUntil,
                    loginCaptchaRequired: false,
                    errorCode: 'password_recovery_required',
                    error: 'После трёх ошибок входа требуется восстановление пароля по email',
                    error: 'Профиль временно заблокирован после 3 неудачных попыток входа'
                })
            };
        }

        // Earlier releases could leave a login captcha challenge in storage.
        // Do not let that legacy state block a user forever or return its
        // broken legacy message. The current security flow requires email
        // password recovery instead of a login captcha.
        if (loginCaptchaStatus.required) {
            return {
                statusCode: 401,
                headers: buildJsonHeaders(),
                body: JSON.stringify({
                    success: false,
                    locked: true,
                    recoveryRequired: true,
                    recoveryUsername: username,
                    loginCaptchaRequired: false,
                    errorCode: 'password_recovery_required',
                    error: 'Требуется восстановление пароля по email'
                })
            };
        }

        if (loginCaptchaStatus.required) {
            if (!captchaAnswer) {
                return {
                    statusCode: 403,
                    headers: buildJsonHeaders(),
                    body: JSON.stringify({
                        success: false,
                        loginCaptchaRequired: true,
                        errorCode: 'login_captcha_required',
                        error: 'Р”Р»СЏ РІС…РѕРґР° С‚СЂРµР±СѓРµС‚СЃСЏ РєР°РїС‚С‡Р°'
                    })
                };
            }

            const captchaVerification = await verifyLoginCaptcha(ip, captchaAnswer, new Date());
            if (!captchaVerification.ok) {
                return {
                    statusCode: 403,
                    headers: buildJsonHeaders(),
                    body: JSON.stringify({
                        success: false,
                        loginCaptchaRequired: true,
                        errorCode: captchaVerification.errorCode || 'captcha_invalid',
                        remainingAttempts: captchaVerification.remainingAttempts,
                        error: 'РљР°РїС‚С‡Р° РЅРµ РїСЂРѕР№РґРµРЅР°'
                    })
                };
            }
        }

        const authResult = await verifyAdminCredentials(username, password);
        if (authResult.success) {
            await registerLoginAttempt({
                username,
                success: true,
                profileId: authResult.profileId,
                ip
            });
            const session = await createAdminSession({
                profileId: authResult.profileId,
                ip,
                userAgent,
                now: new Date().toISOString()
            });
            await clearLoginCaptcha(ip);
            return {
                statusCode: 200,
                ...buildCookieResponseMeta(buildSessionCookie(session.sessionId)),
                body: JSON.stringify({
                    success: true,
                    sessionToken: session.sessionId,
                    profileId: authResult.profileId,
                    principalProfileId: authResult.profileId,
                    profileName: authResult.profileName,
                    role: authResult.role,
                    isMainAdmin: authResult.isMainAdmin,
                    loginCaptchaRequired: false
                })
            };
        }

        if (authResult.reason === 'expired') {
            const profile = await findProfileByUsername(username);
            return {
                statusCode: 403,
                headers: buildJsonHeaders(),
                body: JSON.stringify({
                    success: false,
                    expired: true,
                    canReactivate: true,
                    error: authResult.error || 'Срок действия профиля истёк',
                    profileId: profile?.id || '',
                    profileName: profile?.name || username,
                    username
                })
            };
        }

        // Authentication providers may return implementation-specific text.
        // Do not expose it in the public login response: it can be encoded
        // inconsistently by an upstream storage/runtime layer.
        authResult.error = authResult.reason === 'inactive' ? 'Профиль отключён' : '';
        const lockInfo = await registerLoginAttempt({
            username,
            success: false,
            profileId: null,
            reason: authResult.error || authResult.reason || 'credentials',
            ip
        });
        return {
            statusCode: authResult.reason === 'expired' || authResult.reason === 'inactive' ? 403 : 401,
            headers: buildJsonHeaders(),
            body: JSON.stringify({
                success: false,
                    error: authResult.error || 'Неверный логин или пароль',
                remainingAttempts: lockInfo.remainingAttempts,
                lockUntil: lockInfo.lockUntil || 0,
                locked: !!(lockInfo.lockUntil && lockInfo.lockUntil > Date.now()),
                recoveryRequired: !!(lockInfo.lockUntil && lockInfo.lockUntil > Date.now()),
                recoveryUsername: username,
                loginCaptchaRequired: false
            })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: buildJsonHeaders(),
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleGetCaptcha(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const mode = getCaptchaMode(q);
        const ip = getClientIpFromEvent(event);

        if (mode === 'login') {
            const rateLimit = await reserveCaptchaRateLimit({ ip, action: 'refresh' });
            if (!rateLimit.ok) return rateLimit.response;

            const challenge = await issueLoginCaptcha(ip);
            return {
                statusCode: 200,
                headers: buildJsonHeaders(),
                body: JSON.stringify({
                    success: true,
                    mode: 'login',
                    loginCaptchaRequired: true,
                    captchaSvg: challenge.captchaSvg,
                    expiresAt: challenge.expiresAt
                })
            };
        }

        {
            const sessionId = getAdminSessionIdFromEvent(event);
            if (sessionId) await killAdminSession(sessionId, 'session_captcha_retired_reauth_required', new Date().toISOString());
            return buildAdminSessionErrorResponse({
                statusCode: 401,
                clearCookie: true,
                sessionInvalid: true,
                errorCode: 'session_reauth_required',
                error: 'Сессия завершена. Войдите снова.'
            });
        }

        /* c8 ignore next */ const sessionId = getAdminSessionIdFromEvent(event);
        if (!sessionId) {
            return buildAdminSessionErrorResponse({
                statusCode: 401,
                clearCookie: true,
                sessionInvalid: true,
                errorCode: 'session_missing',
                error: 'РЎРµСЃСЃРёСЏ РЅРµ РЅР°Р№РґРµРЅР°'
            });
        }

        const session = await getAdminSession(sessionId);
        if (!session) {
            return buildAdminSessionErrorResponse({
                statusCode: 401,
                clearCookie: true,
                sessionInvalid: true,
                errorCode: 'session_not_found',
                error: 'РЎРµСЃСЃРёСЏ РЅРµ РЅР°Р№РґРµРЅР°'
            });
        }
        if (session.terminatedAt) {
            return buildAdminSessionErrorResponse({
                statusCode: 401,
                clearCookie: true,
                sessionInvalid: true,
                errorCode: 'session_terminated',
                error: 'РЎРµСЃСЃРёСЏ Р·Р°РІРµСЂС€РµРЅР°'
            });
        }
        if (isSessionExpired(session, new Date())) {
            await killAdminSession(sessionId, 'session_expired', new Date().toISOString());
            return buildAdminSessionErrorResponse({
                statusCode: 401,
                clearCookie: true,
                sessionInvalid: true,
                expired: true,
                errorCode: 'session_expired',
                error: 'РЎРµСЃСЃРёСЏ РёСЃС‚РµРєР»Р°'
            });
        }

        const rateLimit = await reserveCaptchaRateLimit({ sessionId, ip, action: 'refresh' });
        if (!rateLimit.ok) return rateLimit.response;

        const challenge = await issueSessionCaptcha(sessionId);
        return {
            statusCode: 200,
            headers: buildJsonHeaders(),
            body: JSON.stringify({
                success: true,
                mode: 'session',
                captchaRequired: true,
                captchaSvg: challenge.captchaSvg,
                expiresAt: challenge.expiresAt
            })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: buildJsonHeaders(),
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleVerifyCaptcha(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const mode = getCaptchaMode(q, body);
        const answer = String(body.answer || body.captchaAnswer || '').trim();
        const ip = getClientIpFromEvent(event);
        const userAgent = getUserAgentFromEvent(event);

        if (!answer) {
            return {
                statusCode: 400,
                headers: buildJsonHeaders(),
                body: JSON.stringify({
                    success: false,
                    errorCode: 'captcha_answer_required',
                    error: 'Р’РІРµРґРёС‚Рµ РѕС‚РІРµС‚ РєР°РїС‚С‡Рё'
                })
            };
        }

        if (mode === 'login') {
            const rateLimit = await reserveCaptchaRateLimit({ ip, action: 'submit' });
            if (!rateLimit.ok) return rateLimit.response;

            const result = await verifyLoginCaptcha(ip, answer, new Date());
            if (result.ok) {
                return {
                    statusCode: 200,
                    headers: buildJsonHeaders(),
                    body: JSON.stringify({
                        success: true,
                        loginCaptchaRequired: false
                    })
                };
            }

            return {
                statusCode: 403,
                headers: buildJsonHeaders(),
                body: JSON.stringify({
                    success: false,
                    loginCaptchaRequired: true,
                    remainingAttempts: result.remainingAttempts,
                    errorCode: result.errorCode || 'captcha_invalid',
                    error: 'РљР°РїС‚С‡Р° РЅРµ РїСЂРѕР№РґРµРЅР°'
                })
            };
        }

        {
            const sessionId = getAdminSessionIdFromEvent(event);
            if (sessionId) await killAdminSession(sessionId, 'session_captcha_retired_reauth_required', new Date().toISOString());
            return buildAdminSessionErrorResponse({
                statusCode: 401,
                clearCookie: true,
                sessionInvalid: true,
                errorCode: 'session_reauth_required',
                error: 'Сессия завершена. Войдите снова.'
            });
        }

        /* c8 ignore next */ const sessionId = getAdminSessionIdFromEvent(event);
        if (!sessionId) {
            return buildAdminSessionErrorResponse({
                statusCode: 401,
                clearCookie: true,
                sessionInvalid: true,
                errorCode: 'session_missing',
                error: 'РЎРµСЃСЃРёСЏ РЅРµ РЅР°Р№РґРµРЅР°'
            });
        }

        const rateLimit = await reserveCaptchaRateLimit({ sessionId, ip, action: 'submit' });
        if (!rateLimit.ok) return rateLimit.response;

        const verifiedAt = new Date();
        const result = await verifyAndRotateSessionCaptcha(sessionId, answer, {
            ip,
            userAgent,
            now: verifiedAt
        });
        if (result.ok) {
            const activeSessionId = result.session?.sessionId || sessionId;
            return {
                statusCode: 200,
                ...buildCookieResponseMeta(buildSessionCookie(activeSessionId)),
                body: JSON.stringify({
                    success: true,
                    captchaRequired: false,
                    sessionInvalid: false,
                    sessionToken: activeSessionId
                })
            };
        }

        if (result.terminateSession) {
            await requireLoginCaptcha(ip, 'session_captcha_failed');
            return {
                statusCode: 403,
                ...buildCookieResponseMeta(buildClearSessionCookie()),
                body: JSON.stringify({
                    success: false,
                    sessionInvalid: true,
                    loginCaptchaRequired: true,
                    errorCode: result.errorCode || 'captcha_failed',
                    error: 'РЎРµСЃСЃРёСЏ Р·Р°РІРµСЂС€РµРЅР° РїРѕСЃР»Рµ 3 РЅРµСѓРґР°С‡РЅС‹С… РїРѕРїС‹С‚РѕРє РєР°РїС‚С‡Рё'
                })
            };
        }

        return {
            statusCode: 403,
            headers: buildJsonHeaders(),
            body: JSON.stringify({
                success: false,
                captchaRequired: true,
                sessionInvalid: false,
                remainingAttempts: result.remainingAttempts,
                errorCode: result.errorCode || 'captcha_invalid',
                error: 'РљР°РїС‚С‡Р° РЅРµ РїСЂРѕР№РґРµРЅР°'
            })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: buildJsonHeaders(),
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

async function handleLogoutAdmin(event) {
    try {
        const sessionId = getAdminSessionIdFromEvent(event);
        if (sessionId) {
            await killAdminSession(sessionId, 'manual_logout', new Date().toISOString());
        }
        return {
            statusCode: 200,
            ...buildCookieResponseMeta(buildClearSessionCookie()),
            body: JSON.stringify({ success: true })
        };
    } catch (e) {
        return {
            statusCode: 500,
            headers: buildJsonHeaders(),
            body: JSON.stringify({ success: false, error: e.message })
        };
    }
}

function extractQueuedPayloads(event) {
    const rawBody = typeof event?.body === 'string' ? JSON.parse(event.body || '{}') : (event?.body || event || {});
    if (!rawBody || (typeof rawBody === 'object' && Object.keys(rawBody).length === 0)) {
        return [];
    }

    if (Array.isArray(rawBody?.messages)) {
        return rawBody.messages
            .map(entry => entry?.details?.message?.body || '')
            .filter(Boolean)
            .map(body => typeof body === 'string' ? JSON.parse(body) : body);
    }

    return Array.isArray(rawBody?.events)
        ? rawBody.events
        : [rawBody?.envelope || rawBody];
}

function extractWorkerEnvelopes(event) {
    return extractQueuedPayloads(event);
}

async function workerHandlerWithDependencies(event, overrides = {}) {
    const consumeIncomingEventImpl = overrides.consumeIncomingEvent || consumeIncomingEvent;
    const processIncomingEventImpl = overrides.processIncomingEvent || processIncomingEvent;
    const processOutboundActionImpl = overrides.processOutboundAction || processOutboundAction;
    const envelopes = extractWorkerEnvelopes(event);

    if (envelopes.length === 0) {
        const processedCount = await consumeIncomingEventImpl(processIncomingEventImpl);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
            body: processedCount ? `worker-ok:${processedCount}` : 'worker-ok:0'
        };
    }

    for (const envelope of envelopes) {
        if (envelope && envelope.actionId && envelope.actionType && !envelope.eventType) {
            await processOutboundActionImpl(envelope);
            continue;
        }
        await processIncomingEventImpl(envelope);
    }

    return {
        statusCode: 200,
        headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
        body: `worker-ok:${envelopes.length}`
    };
}

async function workerHandler(event) {
    return workerHandlerWithDependencies(event);
}

async function senderHandlerWithDependencies(event, overrides = {}) {
    const consumeOutboundActionImpl = overrides.consumeOutboundAction || consumeOutboundAction;
    const processOutboundActionImpl = overrides.processOutboundAction || processOutboundAction;
    const actions = extractQueuedPayloads(event);

    if (actions.length === 0) {
        const processedCount = await consumeOutboundActionImpl(processOutboundActionImpl);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
            body: processedCount ? `sender-ok:${processedCount}` : 'sender-ok:0'
        };
    }

    for (const action of actions) {
        await processOutboundActionImpl(action);
    }

    return {
        statusCode: 200,
        headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
        body: `sender-ok:${actions.length}`
    };
}

async function senderHandler(event) {
    return senderHandlerWithDependencies(event);
}

setIncomingEventConsumer(processIncomingEvent);

module.exports = {
    handler,
    workerHandler,
    senderHandler,
    __testOnly: {
        handleVkWebhookWithDependencies,
        handleTelegramWebhookWithDependencies,
        handleConnectTelegramBotWithDependencies,
        handleBindTelegramBotToChatWithDependencies,
        buildTelegramWebhookUrl,
        secretsMatch,
        toPublicTelegramBotConfig,
        toPublicBotSettings,
        toPublicCommunityTokenStatus,
        handleSaveSheetWithDependencies,
        handleUploadAttachmentWithDependencies,
        handleUploadAttachmentChunk,
        createRenderRelayGrant,
        verifyRenderRelayGrant,
        handleGetCommentActivityStatsWithDependencies,
        handleExportCommentActivityStatsWithDependencies,
        handleResetCommentActivityStatsWithDependencies,
        handleExportAdminFinancialOperationsWithDependencies,
        handleDeleteProfilePaymentOperationsWithDependencies,
        handleDeleteProfileUploadedDocumentWithDependencies,
        ensureDefaultConsentDocumentsForCommunity,
        ensureExampleBotsForCommunity,
        handleRecoverRenderUpload,
        getClientIpFromEvent,
        workerHandlerWithDependencies,
        senderHandlerWithDependencies,
        handleMiniAppRequestWithDependencies,
        handleMiniAppUploadAssetWithDependencies,
        handleMiniAppAssetRequestWithDependencies
    }
};
