/**
 * Основной обработчик HTTP запросов (роутер)
 */

const { log } = require('./utils/logger');
const { initializeStorage, getSheetData, saveSheetData, invalidateCache } = require('./modules/storage');
const {
    loadBotConfig, getFullConfig, getConfirmationToken, getSecretKey,
    saveBotConfig, saveAllCommunities, deleteCommunity,
    getActiveCommunityId, setActiveCommunity, getAllCommunityIds,
    resolveCommunityContext
} = require('./modules/config');
const {
    verifyAdminCredentials,
    getAllProfileIds,
    normalizeProfileId,
    getPublicProfiles,
    upsertAdminProfile,
    deleteAdminProfile,
    getProfileById,
    findProfileByUsername,
    findProfileByRecoveryEmail,
    registerProfileFromPromo,
    reactivateExpiredProfile,
    isMainAdminProfile,
    buildExpiresAt,
    isProfileExpired
} = require('./modules/admin-profiles');
const {
    registerLoginAttempt,
    getLoginStatus,
    clearLoginLock,
    registerPromoAttempt,
    getPromoStatus,
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
const { getTokenPermissions } = require('./modules/vk-api');
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
const {
    canProcessProfileEvents,
    recordProfileEventUsage,
    createProfileLimitRequest,
    resolveProfileLimitRequest,
    deleteProfileLimitRequest,
    getAdminLimitRequests,
    getProfileDashboardOverview
} = require('./modules/profile-dashboard');
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

function getRequestPrincipalProfileId(query = {}, body = {}) {
    return normalizeProfileId(query.principalProfileId || body.principalProfileId || query.profileId || body.profileId || '1');
}

async function requireMainAdmin(query = {}, body = {}) {
    const principalProfileId = getRequestPrincipalProfileId(query, body);
    const principalProfile = await getProfileById(principalProfileId);
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
module.exports.handler = async function(event) {
    log('info', '🔔 RAW REQUEST:', {
        method: event.httpMethod,
        path: event.path,
        query: event.queryStringParameters,
        bodyPreview: event.body?.substring(0, 200)
    });

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
    const q = event.queryStringParameters || event.query || event.params || {};
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
};

/**
 * Обработка таймера (отложенные + рассылки)
 */
async function handleTimerTrigger(event) {
    log('info', '⏰ TIMER TRIGGER: Starting delayed message processing');
    await initializeStorage();

    try {
        const profileIds = await getAllProfileIds();

        for (const profileId of profileIds) {
            if (!(await canProcessProfileEvents(profileId))) {
                log('warn', `⛔ TIMER TRIGGER: profile ${profileId} skipped because daily limit is reached`);
                continue;
            }
            await loadBotConfig(profileId);
            const allIds = getAllCommunityIds(profileId);
            log('info', `⏰ TIMER TRIGGER: Profile ${profileId}, communities: ${allIds.join(', ') || 'none'}`);

            for (const cid of allIds) {
                log('info', `⏰ Processing community: ${cid} (profile ${profileId})`);
                await processDelayed(cid, profileId);
                await processMailing(cid, profileId);
            }
        }

        log('info', '✅ TIMER TRIGGER completed');
        
        // Пинг Render сервиса чтобы он не засыпал
        try {
            const axios = require('axios');
            await axios.get('https://vk-uploader.onrender.com/upload', { timeout: 5000 }).catch(() => {});
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
    const q = event.queryStringParameters || event.query || event.params || {};
    const profileId = getRequestProfileId(q);

    // Загрузка настроек для админ-панели
    if (q.getSettings) {
        const session = await validateAdminSession(q);
        if (!session.ok) return buildSessionErrorResponse(session);
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
            const session = await validateAdminSession(q);
            if (!session.ok) return buildSessionErrorResponse(session);
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
        const session = await validateAdminSession(q);
        if (!session.ok) return buildSessionErrorResponse(session);
        await loadBotConfig(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                ...getFullConfig(profileId),
                profileId
            })
        };
    }

    if (q.getBotVersion !== undefined) {
        const session = await validateAdminSession(q);
        if (!session.ok) return buildSessionErrorResponse(session);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(await getBotVersionData())
        };
    }

    if (q.saveBotVersion !== undefined) {
        return handleSaveBotVersion(event);
    }

    if (q.getAdminProfiles !== undefined) {
        const session = await validateAdminSession(q);
        if (!session.ok) return buildSessionErrorResponse(session);
        await requireMainAdmin(q);
        const data = await getPublicProfiles(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(data)
        };
    }

    if (q.getAdminDashboard !== undefined) {
        const session = await validateAdminSession(q);
        if (!session.ok) return buildSessionErrorResponse(session);
        await requireMainAdmin(q);
        log('info', `[getAdminDashboard] Loading admin dashboard data...`);
        const [profiles, dashboard, limitRequests] = await Promise.all([
            getPublicProfiles(profileId),
            getAdminDashboardData(),
            getAdminLimitRequests()
        ]);
        log('info', `[getAdminDashboard] Loaded ${limitRequests.length} limit requests`);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                profiles: profiles.profiles,
                promoCodes: dashboard.promoCodes,
                recoveryRequests: dashboard.recoveryRequests,
                loginLogs: dashboard.loginLogs,
                limitRequests
            })
        };
    }

    if (q.getProfileDashboard !== undefined) {
        const session = await validateAdminSession(q);
        if (!session.ok) return buildSessionErrorResponse(session);
        const dashboard = await getProfileDashboardOverview(profileId);
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, dashboard })
        };
    }

    if (q.getAppLogs !== undefined) {
        const session = await validateAdminSession(q);
        if (!session.ok) return buildSessionErrorResponse(session);
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

    if (q.checkPromoStatus !== undefined) {
        const status = await getPromoStatus(getClientId(q));
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(status)
        };
    }

    if (q.validateSession !== undefined) {
        const session = await validateAdminSession(q);
        if (!session.ok) return buildSessionErrorResponse(session);
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

    // Админ-панель
    return {
        statusCode: 200,
        headers: {
            'Content-Type': 'text/html; charset=utf-8',
            'Access-Control-Allow-Origin': '*'
        },
        body: adminHTML
    };
}

/**
 * Обработка POST запросов
 */
async function handlePostRequest(event) {
    const q = event.queryStringParameters || event.query || event.params || {};

    // Обработка action из body (для загрузки вложений из админ-панели)
    if (event.body && event.httpMethod === 'POST') {
        try {
            const body = JSON.parse(event.body);
            if (body.action === 'upload_attachment') {
                return handleUploadAttachment(event);
            }
        } catch (e) {
            // Не JSON body, продолжаем обычную обработку
        }
    }

    // Проверка VK токенов
    if (q.checkVkTokens !== undefined) {
        return handleCheckVkTokens(event);
    }

    // Проверка авторизации
    if (q.verifyAuth !== undefined) {
        return handleVerifyAuth(event);
    }

    // Запрос восстановления
    if (q.requestRecovery !== undefined) {
        return handleRecoveryRequest(event);
    }

    if (q.verifyPromoCode !== undefined) {
        return handleVerifyPromoCode(event);
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
        q.deleteAdminProfile !== undefined ||
        q.savePromoCode !== undefined ||
        q.deletePromoCode !== undefined ||
        q.resolveRecovery !== undefined ||
        q.deleteCommunity !== undefined ||
        q.uploadAttachment !== undefined ||
        q.testSend !== undefined ||
        q.checkTokenPermissions !== undefined ||
        q.saveBotVersion !== undefined ||
        q.saveAppLogsSettings !== undefined ||
        q.clearAppLogs !== undefined ||
        q.deleteAppLogsFile !== undefined ||
        q.requestProfileLimit !== undefined ||
        q.resolveProfileLimitRequest !== undefined;

    if (needsAdminSession) {
        const session = await validateAdminSession(q);
        if (!session.ok) return buildSessionErrorResponse(session);
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

    if (q.requestProfileLimit !== undefined) {
        return handleRequestProfileLimit(event);
    }

    if (q.resolveProfileLimitRequest !== undefined) {
        return handleResolveProfileLimitRequest(event);
    }

    if (q.deleteProfileLimitRequest !== undefined) {
        return handleDeleteProfileLimitRequest(event);
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
async function handleVerifyAuth(event) {
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

        const promo = await getPromoByCode(code);
        if (!promo || promo.active === false || promo.usedCount >= promo.maxUses) {
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

        const promoStatus = await getPromoStatus(clientId);
        if (promoStatus.lockUntil && promoStatus.lockUntil > Date.now()) {
            return {
                statusCode: 423,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Ввод промокодов заблокирован на 24 часа', locked: true, lockUntil: promoStatus.lockUntil })
            };
        }

        const promo = await getPromoByCode(code);
        if (!promo || promo.active === false || promo.usedCount >= promo.maxUses) {
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
            durationMinutes: promo.durationMinutes
        }, promo.code);

        await consumePromoCode(promo.code, profile.id);

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

async function handleReactivateExpiredProfile(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const username = String(body.username || '').trim();
        const password = String(body.password || '').trim();
        const code = String(body.code || '').trim();
        const attemptKey = `reactivate::${username.toLowerCase()}`;

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

        const reactivatedProfile = await reactivateExpiredProfile(expiredProfile.id, promo.code, promo.durationMinutes);
        await consumePromoCode(promo.code, expiredProfile.id);
        await clearLoginLock(username);
        await registerPromoAttempt({ attemptKey, success: true, code: promo.code, note: 'reactivate_success' });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({
                success: true,
                token: 'authenticated_' + Date.now(),
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

        await validateUniqueBotNamesAcrossSheets(sheetName, body, fileCommunityId, profileId, overrides);
        invalidateCacheImpl(sheetName, fileCommunityId, profileId);

        if (sheetName === 'РАССЫЛКА') {
            const currentData = await getSheetDataImpl(sheetName, fileCommunityId, profileId);
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

        if (!community_id || !vk_token || !vk_group_id) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'community_id, vk_token и vk_group_id обязательны' })
            };
        }

        await loadBotConfig(profileId);
        const fullConfig = getFullConfig(profileId);
        
        // ✅ Сначала ищем существующее сообщество по vk_group_id
        let targetCommunityId = community_id.toString();
        if (fullConfig?.communities) {
            for (const [id, config] of Object.entries(fullConfig.communities)) {
                if (config?.vk_group_id?.toString() === vk_group_id.toString()) {
                    log('info', `🔍 Найдено существующее сообщество ${id} с vk_group_id=${vk_group_id}, используем его`);
                    targetCommunityId = id;
                    break;
                }
            }
        }

        // Сохраняем ТОЛЬКО vk_token и vk_group_id (secret_key сгенерируется автоматически)
        log('info', '🔧 handleSetupCallback: сохраняем конфиг для community_id=' + targetCommunityId);
        await saveBotConfig({
            vk_token,
            vk_group_id: vk_group_id.toString(),
            vk_tokens: [vk_token]
            // НЕ передаём secret_key здесь - он будет сгенерирован в setupVkCallbackServer
        }, targetCommunityId.toString(), profileId);

        log('info', '🔧 handleSetupCallback: вызываем setupVkCallbackServer groupId=' + vk_group_id + ', communityId=' + targetCommunityId);
        const result = await setupVkCallbackServer(vk_group_id.toString(), targetCommunityId.toString(), profileId);

        await addAppLog({
            tab: 'SETTINGS',
            title: 'Настроен Callback API',
            summary: 'Сообщество подключено к callback-серверу.',
            details: [
                'Сообщество: ' + targetCommunityId,
                'VK ID: ' + vk_group_id,
                'Статус: ' + (result?.success === false ? 'ошибка' : 'успешно')
            ],
            communityId: vk_group_id,
            profileId
        });

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify(result)
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

async function handleSaveBotVersion(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        await requireMainAdmin(q, body);
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
        const actor = await requireMainAdmin({}, body);
        const savedProfile = await upsertAdminProfile(body || {}, actor.id);

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
        await requireMainAdmin({}, body);
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
        const actor = await requireMainAdmin({}, body);
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
        const actor = await requireMainAdmin({}, body);
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

async function handleResolveRecovery(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const actor = await requireMainAdmin({}, body);
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
        const request = await createProfileLimitRequest(profileId, body.requestedLimit);
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

async function handleResolveProfileLimitRequest(event) {
    try {
        const body = JSON.parse(event.body || '{}');
        const actor = await requireMainAdmin({}, body);
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
            await requireMainAdmin({}, body);
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
    try {
        await loadBotConfig();
        const body = JSON.parse(event.body);
        const { fileBase64, fileType, fileName, target, groupId, communityId } = body;

        if (!fileBase64 || !target) {
            return {
                statusCode: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: false, error: 'Не передан файл или target' })
            };
        }

        const buffer = Buffer.from(fileBase64, 'base64');
        const attachment = await uploadToVK(buffer, fileName, fileType, target, groupId || communityId || null);

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: true, attachment })
        };
    } catch (err) {
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
            body: JSON.stringify({ success: false, error: err.message })
        };
    }
}

/**
 * Проверка прав токена
 */
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

/**
 * Обработка вебхука VK
 */
async function handleVkWebhook(event) {
    try {
        const data = JSON.parse(event.body);

        // Подтверждение вебхука
        if (data.type === 'confirmation') {
            const groupId = data.group_id?.toString() || null;
            log('info', `🔑 Confirmation request from community: ${groupId}`);
            const resolved = await resolveCommunityContext(groupId);
            let confirmationCode = resolved?.config?.confirmation_token || null;
            // Fallback: ENV
            if (!confirmationCode) {
                confirmationCode = process.env.CONFIRMATION_TOKEN;
            }
            
            log('info', `✅ Returning confirmation code: ${confirmationCode?.substring(0, 4)}...`);
            return {
                statusCode: 200,
                body: confirmationCode || 'error_no_token'
            };
        }

        // Обработка событий
        if (data.type === 'message_new' || data.type === 'message_reply' || data.type === 'message_event') {
            const groupId = data.group_id?.toString() || 'default';
            const resolved = await resolveCommunityContext(groupId);
            if (resolved?.communityId) {
                setActiveCommunity(resolved.communityId, resolved.profileId);
            }
            const usage = await recordProfileEventUsage(resolved?.profileId || '1', groupId, data.type);
            if (!usage.allowed) {
                log('warn', `⛔ Daily PAPA BOT limit reached for profile ${resolved?.profileId || '1'}`);
                return {
                    statusCode: 200,
                    headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
                    body: 'ok'
                };
            }
            const structuredResult = await processStructuredTriggers(data, resolved?.profileId || '1');

            // handleMessage (классические триггеры из вкладки СООБЩЕНИЯ) вызывается ВСЕГДА,
            // даже если структурные триггеры уже сработали. Они независимы.
            if (data.type !== 'message_event') {
                await handleMessage(data, resolved?.profileId || '1');
            }
            // Обрабатываем отложенные и рассылки для ЭТОГО сообщества
            await processDelayed(groupId, resolved?.profileId || '1');
            await processMailing(groupId, resolved?.profileId || '1');
        } else if (data.type === 'wall_reply_new' || data.type === 'wall_reply_edit' || data.type === 'wall_reply_delete') {
            const groupId = data.group_id?.toString() || 'default';
            const resolved = await resolveCommunityContext(groupId);
            if (resolved?.communityId) {
                setActiveCommunity(resolved.communityId, resolved.profileId);
            }
            const usage = await recordProfileEventUsage(resolved?.profileId || '1', groupId, data.type);
            if (!usage.allowed) {
                log('warn', `⛔ Daily PAPA BOT limit reached for profile ${resolved?.profileId || '1'}`);
                return {
                    statusCode: 200,
                    headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
                    body: 'ok'
                };
            }
            await processStructuredTriggers(data, resolved?.profileId || '1');

            // handleComment (классические триггеры из вкладки КОММЕНТАРИИ В ПОСТАХ) вызывается ВСЕГДА,
            // даже если структурные триггеры уже сработали. Они независимы.
            if (data.type !== 'wall_reply_delete') {
                await handleComment(data, resolved?.profileId || '1');
            }
            // Обрабатываем отложенные и рассылки для ЭТОГО сообщества
            await processDelayed(groupId, resolved?.profileId || '1');
            await processMailing(groupId, resolved?.profileId || '1');
        } else if (['photo_new', 'video_new', 'group_join', 'group_leave', 'wall_repost', 'like_add'].includes(data.type)) {
            const groupId = data.group_id?.toString() || 'default';
            const resolved = await resolveCommunityContext(groupId);
            if (resolved?.communityId) {
                setActiveCommunity(resolved.communityId, resolved.profileId);
            }
            const usage = await recordProfileEventUsage(resolved?.profileId || '1', groupId, data.type);
            if (!usage.allowed) {
                log('warn', `⛔ Daily PAPA BOT limit reached for profile ${resolved?.profileId || '1'}`);
                return {
                    statusCode: 200,
                    headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
                    body: 'ok'
                };
            }
            await processStructuredTriggers(data, resolved?.profileId || '1');
            await processDelayed(groupId, resolved?.profileId || '1');
            await processMailing(groupId, resolved?.profileId || '1');
        } else {
            const profileIds = await getAllProfileIds();
            for (const profileId of profileIds) {
                if (!(await canProcessProfileEvents(profileId))) {
                    continue;
                }
                await loadBotConfig(profileId);
                for (const communityId of getAllCommunityIds(profileId)) {
                    await processDelayed(communityId, profileId);
                    await processMailing(communityId, profileId);
                }
            }
        }

        return {
            statusCode: 200,
            headers: { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' },
            body: 'ok'
        };
    } catch (e) {
        log('error', 'Handler error:', e.message);
        return {
            statusCode: 500,
            headers: { 'Access-Control-Allow-Origin': '*' },
            body: 'Internal error'
        };
    }
}

/**
 * Тестовая отправка сообщения пользователю из админ-панели
 */
async function handleTestSend(event) {
    try {
        const q = event.queryStringParameters || event.query || event.params || {};
        const body = JSON.parse(event.body || '{}');
        const { userId, text, attachments, keyboard, communityId, vkGroupId, stepActions } = body;
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
                parsedKeyboard = typeof keyboard === 'string' ? JSON.parse(keyboard) : keyboard;

                // ✅ Sanitize keyboard - удалить недопустимые поля для VK API
                // open_link кнопки НЕ поддерживают label и color
                if (parsedKeyboard && parsedKeyboard.buttons) {
                    for (const row of parsedKeyboard.buttons) {
                        for (const btn of row) {
                            if (btn.action) {
                                if (btn.action.type === 'open_link') {
                                    delete btn.action.label;
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
