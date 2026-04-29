const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const projectRoot = path.resolve(__dirname, '..');
const handlerPath = path.resolve(projectRoot, 'src/handler.js');

function buildBaseMocks(overrides = {}) {
    return {
        'src/utils/logger.js': { log() {} },
        'src/modules/storage.js': {
            initializeStorage: async () => {},
            getSheetData: async () => [],
            saveSheetData: async () => {},
            invalidateCache: () => {},
            getS3Client: () => ({
                send: async () => ({
                    Body: { transformToString: async () => JSON.stringify({}) }
                })
            }),
            getBucketName: () => 'test-bucket'
        },
        'src/modules/config.js': {
            loadBotConfig: async () => {},
            getFullConfig: () => ({}),
            getConfirmationToken: () => '',
            getSecretKey: () => '',
            saveBotConfig: async () => {},
            saveAllCommunities: async () => {},
            deleteCommunity: async () => ({ success: true }),
            getActiveCommunityId: () => 'community_1',
            setActiveCommunity: async () => {},
            getAllCommunityIds: () => ['community_1'],
            resolveCommunityContext: () => ({ communityId: 'community_1', profileId: '1' })
        },
        'src/modules/admin-profiles.js': {
            verifyAdminCredentials: async () => ({ success: false, error: 'invalid credentials' }),
            getAllProfileIds: async () => ['1'],
            normalizeProfileId: value => String(value || '1'),
            getPublicProfiles: async () => ({ profiles: [] }),
            upsertAdminProfile: async () => ({}),
            deleteAdminProfile: async () => ({ success: true }),
            getProfileById: async profileId => ({ id: String(profileId || '1'), role: 'admin', active: true }),
            findProfileByUsername: async username => ({ id: '2', username, name: username }),
            findProfileByRecoveryEmail: async () => null,
            registerProfileFromPromo: async () => ({}),
            reactivateExpiredProfile: async () => ({}),
            activateProfileWithPromoCode: async () => ({}),
            isMainAdminProfile: profile => String(profile?.id || '') === '1',
            buildExpiresAt: () => '',
            isProfileExpired: () => false
        },
        'src/modules/admin-security.js': {
            registerLoginAttempt: async () => ({ remainingAttempts: 3, lockUntil: 0 }),
            getLoginStatus: async () => ({ attempts: 0, lockUntil: 0, remainingAttempts: 3 }),
            clearLoginLock: async () => {},
            appendSecurityEvent: () => {},
            checkCaptchaRateLimit: () => ({ blocked: false, key: 'captcha:test', bucket: { count: 0, resetAt: Date.now() + 600000, lastAt: 0 }, cooldownMs: 0 }),
            registerCaptchaRateLimitHit: () => {},
            requireLoginCaptcha: async () => ({ required: true }),
            getLoginCaptchaStatus: async () => ({ required: false, failCount: 0, reason: '' }),
            clearLoginCaptcha: async () => {},
            issueLoginCaptcha: async () => ({ captchaSvg: '<svg>login</svg>', expiresAt: '2026-04-21T10:05:00.000Z', mode: 'login' }),
            verifyLoginCaptcha: async () => ({ ok: true, required: true }),
            loadSecurityData: async () => ({ captchaRateLimits: {} }),
            saveSecurityData: async data => data,
            registerPromoAttempt: async () => ({ remainingAttempts: 3, lockUntil: 0 }),
            getPromoStatus: async () => ({ attempts: 0, lockUntil: 0, remainingAttempts: 3 }),
            getProfilePromoActivationStatus: async () => ({ blocked: false, attempts: 0, remainingAttempts: 3 }),
            registerProfilePromoActivationAttempt: async () => ({ blocked: false, attempts: 1, remainingAttempts: 2 }),
            listPromoCodes: async () => [],
            savePromoCode: async () => ({}),
            deletePromoCodeById: async () => ({ success: true }),
            getPromoByCode: async () => null,
            consumePromoCode: async () => ({}),
            createRecoveryRequest: async () => ({}),
            resolveRecoveryRequest: async () => ({}),
            getAdminDashboardData: async () => ({ promoCodes: [], recoveryRequests: [], loginLogs: [] })
        },
        'src/modules/messages.js': { handleMessage: async () => ({ statusCode: 200, body: 'ok' }) },
        'src/modules/comments.js': { handleComment: async () => ({ statusCode: 200, body: 'ok' }) },
        'src/modules/scheduler.js': { processDelayed: async () => {}, processMailing: async () => {} },
        'src/modules/structured-triggers.js': { processStructuredTriggers: async () => ({ matched: false }) },
        'src/modules/callback-setup.js': { setupVkCallbackServer: async () => ({ success: true }) },
        'src/modules/attachments.js': { uploadToVK: async () => ({ success: true }) },
        'src/modules/vk-api.js': { getTokenPermissions: async () => ({ permissions: [] }) },
        'src/modules/app-logs.js': {
            addAppLog: async () => {},
            getAppLogs: async () => [],
            getAppLogFileName: () => 'app_logs.json',
            getAppLogSettings: async () => ({ enabled: true }),
            saveAppLogSettings: async () => ({ enabled: true }),
            clearAppLogs: async () => ({ success: true }),
            deleteAppLogsFile: async () => ({ success: true, fileName: 'app_logs.json' })
        },
        'src/modules/bot-version-store.js': {
            getBotVersionData: async () => ({}),
            saveBotVersionData: async () => ({})
        },
        'src/modules/profile-dashboard.js': {
            canProcessProfileEvents: async () => true,
            recordProfileEventUsage: async () => {},
            createProfileLimitRequest: async () => ({}),
            resolveProfileLimitRequest: async () => ({}),
            deleteProfileLimitRequest: async () => ({}),
            getAdminLimitRequests: async () => [],
            getProfileDashboardOverview: async () => ({})
        },
        'src/modules/admin-sessions.js': {
            createAdminSession: async () => ({ sessionId: 'sess_default', profileId: '2' }),
            validateAdminSessionRequest: async () => ({ ok: true, session: { sessionId: 'sess_default' }, profile: { id: '2', role: 'admin', active: true } }),
            killAdminSession: async () => ({ sessionId: 'sess_default' }),
            issueSessionCaptcha: async () => ({ captchaSvg: '<svg>session</svg>', expiresAt: '2026-04-21T10:05:00.000Z', mode: 'session' }),
            verifySessionCaptcha: async () => ({ ok: true, session: { sessionId: 'sess_default' } }),
            getAdminSession: async () => ({ sessionId: 'sess_default', profileId: '2', captchaRequired: true, terminatedAt: null, lastSeenAt: '2026-04-21T10:00:00.000Z' }),
            isSessionExpired: () => false
        },
        'adminPanelHTML.js': { adminPanelHTML: '<!doctype html><html></html>' },
        ...overrides
    };
}

function loadHandlerWithMocks(overrides = {}) {
    const mocks = buildBaseMocks(overrides);
    delete require.cache[handlerPath];

    for (const [relativePath, exports] of Object.entries(mocks)) {
        const absolutePath = path.resolve(projectRoot, relativePath);
        delete require.cache[absolutePath];
        require.cache[absolutePath] = {
            id: absolutePath,
            filename: absolutePath,
            loaded: true,
            exports
        };
    }

    return require(handlerPath).handler;
}

test('loginAdmin sets cookie-backed session instead of fake token', async () => {
    const handler = loadHandlerWithMocks({
        'src/modules/admin-profiles.js': {
            ...buildBaseMocks()['src/modules/admin-profiles.js'],
            verifyAdminCredentials: async () => ({
                success: true,
                profileId: '2',
                profileName: 'Profile Two',
                role: 'admin',
                isMainAdmin: false
            })
        },
        'src/modules/admin-sessions.js': {
            ...buildBaseMocks()['src/modules/admin-sessions.js'],
            createAdminSession: async () => ({ sessionId: 'sess_login_1', profileId: '2' })
        }
    });

    const response = await handler({
        httpMethod: 'POST',
        queryStringParameters: { loginAdmin: '1' },
        headers: {
            'user-agent': 'Mozilla/5.0',
            'x-forwarded-for': '203.0.113.10'
        },
        body: JSON.stringify({ username: 'admin', password: 'admin123' })
    });

    assert.equal(response.statusCode, 200);
    assert.match(String(response.headers['Set-Cookie'] || response.headers['set-cookie'] || ''), /adminSessionId=sess_login_1/);
    const payload = JSON.parse(response.body);
    assert.equal(payload.success, true);
    assert.equal('token' in payload, false);
});

test('loginAdmin exposes session cookie through multiValueHeaders for cloud http integration', async () => {
    const handler = loadHandlerWithMocks({
        'src/modules/admin-profiles.js': {
            ...buildBaseMocks()['src/modules/admin-profiles.js'],
            verifyAdminCredentials: async () => ({
                success: true,
                profileId: '1',
                profileName: 'Main Admin',
                role: 'main_admin',
                isMainAdmin: true
            })
        },
        'src/modules/admin-sessions.js': {
            ...buildBaseMocks()['src/modules/admin-sessions.js'],
            createAdminSession: async () => ({ sessionId: 'sess_cloud_cookie', profileId: '1' })
        }
    });

    const response = await handler({
        httpMethod: 'POST',
        queryStringParameters: { loginAdmin: '1' },
        headers: {
            'user-agent': 'Mozilla/5.0',
            'x-forwarded-for': '203.0.113.10'
        },
        body: JSON.stringify({ username: 'PAPA', password: 'secret' })
    });

    assert.equal(response.statusCode, 200);
    assert.match(String(response.multiValueHeaders?.['Set-Cookie']?.[0] || ''), /adminSessionId=sess_cloud_cookie/);
    const payload = JSON.parse(response.body);
    assert.equal(payload.sessionToken, 'sess_cloud_cookie');
});

test('validateSession accepts x-admin-session header when cookie transport is unavailable', async () => {
    const handler = loadHandlerWithMocks({
        'src/modules/admin-sessions.js': {
            ...buildBaseMocks()['src/modules/admin-sessions.js'],
            validateAdminSessionRequest: async ({ sessionId }) => ({
                ok: sessionId === 'sess_header_1',
                session: { sessionId: 'sess_header_1' },
                profile: { id: '1', role: 'main_admin', active: true }
            })
        }
    });

    const response = await handler({
        httpMethod: 'GET',
        queryStringParameters: { validateSession: '1' },
        headers: {
            'x-admin-session': 'sess_header_1'
        }
    });

    assert.equal(response.statusCode, 200);
    const payload = JSON.parse(response.body);
    assert.equal(payload.success, true);
});

test('reactivateExpiredProfile issues a real admin session for the renewed profile', async () => {
    const handler = loadHandlerWithMocks({
        'src/modules/admin-profiles.js': {
            ...buildBaseMocks()['src/modules/admin-profiles.js'],
            verifyAdminCredentials: async () => ({
                success: false,
                reason: 'expired',
                error: 'profile expired'
            }),
            findProfileByUsername: async username => ({
                id: '2',
                username,
                name: 'Profile Two',
                password: 'expired-pass',
                role: 'admin'
            }),
            reactivateExpiredProfile: async () => ({
                id: '2',
                name: 'Profile Two'
            })
        },
        'src/modules/admin-security.js': {
            ...buildBaseMocks()['src/modules/admin-security.js'],
            getPromoStatus: async () => ({ attempts: 0, lockUntil: 0, remainingAttempts: 3 }),
            getPromoByCode: async code => ({
                code,
                active: true,
                usedCount: 0,
                maxUses: 10,
                durationMinutes: 1440,
                dailyRequestsLimit: 100
            })
        },
        'src/modules/admin-sessions.js': {
            ...buildBaseMocks()['src/modules/admin-sessions.js'],
            createAdminSession: async () => ({ sessionId: 'sess_reactivated_1', profileId: '2' })
        }
    });

    const response = await handler({
        httpMethod: 'POST',
        queryStringParameters: { reactivateExpiredProfile: '1' },
        headers: {
            'user-agent': 'Mozilla/5.0',
            'x-forwarded-for': '203.0.113.11'
        },
        body: JSON.stringify({
            username: 'expired-user',
            password: 'expired-pass',
            code: 'PROMO123'
        })
    });

    assert.equal(response.statusCode, 200);
    assert.match(String(response.multiValueHeaders?.['Set-Cookie']?.[0] || ''), /adminSessionId=sess_reactivated_1/);
    const payload = JSON.parse(response.body);
    assert.equal(payload.success, true);
    assert.equal(payload.sessionToken, 'sess_reactivated_1');
    assert.equal('token' in payload, false);
});

test('getCaptcha in session mode returns server-issued session captcha', async () => {
    const handler = loadHandlerWithMocks({
        'src/modules/admin-sessions.js': {
            ...buildBaseMocks()['src/modules/admin-sessions.js'],
            getAdminSession: async () => ({
                sessionId: 'sess_login_1',
                profileId: '2',
                captchaRequired: true,
                terminatedAt: null,
                lastSeenAt: '2026-04-21T10:00:00.000Z'
            }),
            issueSessionCaptcha: async () => ({
                mode: 'session',
                captchaSvg: '<svg>session challenge</svg>',
                expiresAt: '2026-04-21T10:05:00.000Z'
            })
        }
    });

    const response = await handler({
        httpMethod: 'GET',
        queryStringParameters: { getCaptcha: '1', mode: 'session' },
        headers: {
            cookie: 'adminSessionId=sess_login_1',
            'x-forwarded-for': '203.0.113.10'
        }
    });

    assert.equal(response.statusCode, 200);
    const payload = JSON.parse(response.body);
    assert.equal(payload.success, true);
    assert.equal(payload.mode, 'session');
    assert.match(payload.captchaSvg, /<svg>/);
});

test('verifyCaptcha clears cookie and forces login captcha after session termination', async () => {
    const handler = loadHandlerWithMocks({
        'src/modules/admin-sessions.js': {
            ...buildBaseMocks()['src/modules/admin-sessions.js'],
            verifySessionCaptcha: async () => ({
                ok: false,
                terminateSession: true,
                errorCode: 'captcha_failed',
                remainingAttempts: 0,
                session: { sessionId: 'sess_login_1' }
            })
        }
    });

    const response = await handler({
        httpMethod: 'POST',
        queryStringParameters: { verifyCaptcha: '1' },
        headers: {
            cookie: 'adminSessionId=sess_login_1',
            'user-agent': 'Mozilla/5.0',
            'x-forwarded-for': '203.0.113.10'
        },
        body: JSON.stringify({ mode: 'session', answer: 'wrong' })
    });

    assert.equal(response.statusCode, 403);
    assert.match(String(response.headers['Set-Cookie'] || response.headers['set-cookie'] || ''), /adminSessionId=;/);
    const payload = JSON.parse(response.body);
    assert.equal(payload.success, false);
    assert.equal(payload.loginCaptchaRequired, true);
    assert.equal(payload.sessionInvalid, true);
});

test('logoutAdmin clears session cookie', async () => {
    const handler = loadHandlerWithMocks();
    const response = await handler({
        httpMethod: 'POST',
        queryStringParameters: { logoutAdmin: '1' },
        headers: { cookie: 'adminSessionId=sess_login_1' },
        body: '{}'
    });

    assert.equal(response.statusCode, 200);
    assert.match(String(response.headers['Set-Cookie'] || response.headers['set-cookie'] || ''), /adminSessionId=;/);
    const payload = JSON.parse(response.body);
    assert.equal(payload.success, true);
});
