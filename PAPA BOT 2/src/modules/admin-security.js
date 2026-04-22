const crypto = require('crypto');
const { createHotStateStore } = require('./hot-state-store');
const { log } = require('../utils/logger');

const SECURITY_FILE_KEY = 'admin_security.json';
const LOGIN_LOCK_MS = 30 * 60 * 1000;
const PROMO_LOCK_MS = 24 * 60 * 60 * 1000;
const MAX_LOGIN_ATTEMPTS = 3;
const MAX_PROMO_ATTEMPTS = 3;

function buildDefaultSecurityData() {
    return {
        loginAttempts: {},
        promoAttempts: {},
        profilePromoActivationAttempts: {},
        recoveryRequests: [],
        promoCodes: [],
        loginLogs: [],
        securityEvents: [],
        loginRateLimits: {},
        captchaRateLimits: {},
        loginCaptchaRequirements: {},
        loginCaptchaChallenges: {}
    };
}

function normalizeSecurityData(raw) {
    const fallback = buildDefaultSecurityData();
    if (!raw || typeof raw !== 'object') return fallback;
    return {
        loginAttempts: raw.loginAttempts && typeof raw.loginAttempts === 'object' ? raw.loginAttempts : {},
        promoAttempts: raw.promoAttempts && typeof raw.promoAttempts === 'object' ? raw.promoAttempts : {},
        profilePromoActivationAttempts: raw.profilePromoActivationAttempts && typeof raw.profilePromoActivationAttempts === 'object' ? raw.profilePromoActivationAttempts : {},
        recoveryRequests: Array.isArray(raw.recoveryRequests) ? raw.recoveryRequests : [],
        promoCodes: Array.isArray(raw.promoCodes) ? raw.promoCodes : [],
        loginLogs: Array.isArray(raw.loginLogs) ? raw.loginLogs : [],
        securityEvents: Array.isArray(raw.securityEvents) ? raw.securityEvents : [],
        loginRateLimits: raw.loginRateLimits && typeof raw.loginRateLimits === 'object' ? raw.loginRateLimits : {},
        captchaRateLimits: raw.captchaRateLimits && typeof raw.captchaRateLimits === 'object' ? raw.captchaRateLimits : {},
        loginCaptchaRequirements: raw.loginCaptchaRequirements && typeof raw.loginCaptchaRequirements === 'object' ? raw.loginCaptchaRequirements : {},
        loginCaptchaChallenges: raw.loginCaptchaChallenges && typeof raw.loginCaptchaChallenges === 'object' ? raw.loginCaptchaChallenges : {}
    };
}

async function loadSecurityData() {
    const s3Client = getS3Client();
    const bucket = getBucketName();

    try {
        const response = await s3Client.send(new GetObjectCommand({
            Bucket: bucket,
            Key: SECURITY_FILE_KEY
        }));
        const text = await response.Body.transformToString();
        return normalizeSecurityData(JSON.parse(text));
    } catch (error) {
        log('warn', `⚠️ admin_security.json not loaded, using defaults: ${error.message}`);
        return buildDefaultSecurityData();
    }
}

async function saveSecurityData(data) {
    const normalized = normalizeSecurityData(data);
    const s3Client = getS3Client();
    const bucket = getBucketName();

    await s3Client.send(new PutObjectCommand({
        Bucket: bucket,
        Key: SECURITY_FILE_KEY,
        Body: JSON.stringify(normalized, null, 2),
        ContentType: 'application/json'
    }));

    return normalized;
}

function appendLogEntry(data, entry) {
    data.loginLogs.unshift({
        id: `log_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        createdAt: new Date().toISOString(),
        ...entry
    });
    data.loginLogs = data.loginLogs.slice(0, 500);
}

function appendSecurityEvent(data, entry) {
    data.securityEvents = Array.isArray(data.securityEvents) ? data.securityEvents : [];
    data.securityEvents.unshift({
        id: `sec_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        createdAt: new Date().toISOString(),
        ...entry
    });
    data.securityEvents = data.securityEvents.slice(0, 1000);
}

function hashCaptchaAnswer(answer) {
    return crypto.createHash('sha256').update(String(answer || '').trim().toUpperCase()).digest('hex');
}

function buildCaptchaSvg(answer) {
    const safeAnswer = String(answer || '').trim().toUpperCase().slice(0, 8);
    return `<svg xmlns="http://www.w3.org/2000/svg" width="180" height="64" viewBox="0 0 180 64" role="img" aria-label="captcha"><rect width="180" height="64" rx="12" fill="#e2e8f0"/><path d="M10 40 Q 30 10 50 34 T 90 34 T 130 28 T 170 36" stroke="#94a3b8" stroke-width="3" fill="none" opacity="0.75"/><text x="18" y="42" font-size="28" font-family="monospace" letter-spacing="4" fill="#0f172a">${safeAnswer}</text></svg>`;
}

function createCaptchaChallenge({ mode = 'session', answer = '', now = new Date() } = {}) {
    const resolvedAnswer = String(answer || Math.random().toString(36).slice(2, 8)).trim().toUpperCase();
    return {
        mode: String(mode || 'session').trim() || 'session',
        hash: hashCaptchaAnswer(resolvedAnswer),
        expiresAt: new Date(now.getTime() + 5 * 60 * 1000).toISOString(),
        attempts: 0,
        captchaSvg: buildCaptchaSvg(resolvedAnswer)
    };
}

function verifyCaptchaAnswer(sessionState, answer, now = new Date()) {
    const challenge = sessionState?.captchaChallenge;
    if (!challenge) {
        return { ok: false, errorCode: 'captcha_missing' };
    }

    const expiresAtMs = new Date(challenge.expiresAt || 0).getTime();
    if (!expiresAtMs || expiresAtMs <= now.getTime()) {
        return { ok: false, errorCode: 'captcha_expired' };
    }

    challenge.attempts = Math.max(0, parseInt(challenge.attempts, 10) || 0) + 1;
    const ok = challenge.hash === hashCaptchaAnswer(answer);
    if (ok) {
        return { ok: true, attempts: challenge.attempts };
    }

    const nextFailCount = Math.max(0, parseInt(sessionState.captchaFailCount, 10) || 0) + 1;
    sessionState.captchaFailCount = nextFailCount;
    return {
        ok: false,
        terminateSession: nextFailCount >= 3,
        remainingAttempts: Math.max(0, 3 - nextFailCount),
        attempts: challenge.attempts
    };
}

function getRateLimitBucket(container, key, now = Date.now(), windowMs = 10 * 60 * 1000) {
    const current = container[key] || { count: 0, resetAt: now + windowMs, lastAt: 0 };
    if (current.resetAt <= now) {
        return { count: 0, resetAt: now + windowMs, lastAt: 0 };
    }
    return current;
}

function checkCaptchaRateLimit({ data, sessionId = '', ip = '', action = 'submit', now = Date.now() }) {
    data.captchaRateLimits = data.captchaRateLimits && typeof data.captchaRateLimits === 'object'
        ? data.captchaRateLimits
        : {};
    const key = `${action}:${sessionId || 'anonymous'}:${ip || 'no_ip'}`;
    const bucket = getRateLimitBucket(data.captchaRateLimits, key, now);
    const cooldownMs = bucket.lastAt ? (2000 - (now - bucket.lastAt)) : 0;
    return {
        key,
        bucket,
        blocked: bucket.count >= 10 || cooldownMs > 0,
        cooldownMs: Math.max(0, cooldownMs)
    };
}

function registerCaptchaRateLimitHit({ data, key, bucket, now = Date.now() }) {
    data.captchaRateLimits = data.captchaRateLimits && typeof data.captchaRateLimits === 'object'
        ? data.captchaRateLimits
        : {};
    data.captchaRateLimits[key] = {
        count: Math.max(0, parseInt(bucket?.count, 10) || 0) + 1,
        resetAt: bucket?.resetAt || (now + 10 * 60 * 1000),
        lastAt: now
    };
    return data.captchaRateLimits[key];
}

function getLoginCaptchaKey(ip = '') {
    return String(ip || '').trim() || 'anonymous';
}

function normalizeLoginCaptchaRequirement(raw = {}) {
    return {
        required: raw.required === true,
        failCount: Math.max(0, parseInt(raw.failCount, 10) || 0),
        reason: String(raw.reason || '').trim(),
        updatedAt: raw.updatedAt || null
    };
}

function getLoginCaptchaRequirement(data, ip = '') {
    data.loginCaptchaRequirements = data.loginCaptchaRequirements && typeof data.loginCaptchaRequirements === 'object'
        ? data.loginCaptchaRequirements
        : {};
    const key = getLoginCaptchaKey(ip);
    return {
        key,
        state: normalizeLoginCaptchaRequirement(data.loginCaptchaRequirements[key])
    };
}

function setLoginCaptchaRequirement(data, ip = '', required = true, reason = '') {
    data.loginCaptchaRequirements = data.loginCaptchaRequirements && typeof data.loginCaptchaRequirements === 'object'
        ? data.loginCaptchaRequirements
        : {};
    const key = getLoginCaptchaKey(ip);
    const current = normalizeLoginCaptchaRequirement(data.loginCaptchaRequirements[key]);
    data.loginCaptchaRequirements[key] = {
        required: !!required,
        failCount: required ? current.failCount : 0,
        reason: String(reason || current.reason || '').trim(),
        updatedAt: new Date().toISOString()
    };
    return { key, state: data.loginCaptchaRequirements[key] };
}

function clearLoginCaptchaRequirement(data, ip = '') {
    data.loginCaptchaRequirements = data.loginCaptchaRequirements && typeof data.loginCaptchaRequirements === 'object'
        ? data.loginCaptchaRequirements
        : {};
    const key = getLoginCaptchaKey(ip);
    delete data.loginCaptchaRequirements[key];
    return key;
}

function getLoginCaptchaChallenge(data, ip = '') {
    data.loginCaptchaChallenges = data.loginCaptchaChallenges && typeof data.loginCaptchaChallenges === 'object'
        ? data.loginCaptchaChallenges
        : {};
    const key = getLoginCaptchaKey(ip);
    return {
        key,
        challenge: data.loginCaptchaChallenges[key] || null
    };
}

function setLoginCaptchaChallenge(data, ip = '', challenge) {
    data.loginCaptchaChallenges = data.loginCaptchaChallenges && typeof data.loginCaptchaChallenges === 'object'
        ? data.loginCaptchaChallenges
        : {};
    const key = getLoginCaptchaKey(ip);
    data.loginCaptchaChallenges[key] = challenge || null;
    return { key, challenge: data.loginCaptchaChallenges[key] };
}

function clearLoginCaptchaChallenge(data, ip = '') {
    data.loginCaptchaChallenges = data.loginCaptchaChallenges && typeof data.loginCaptchaChallenges === 'object'
        ? data.loginCaptchaChallenges
        : {};
    const key = getLoginCaptchaKey(ip);
    delete data.loginCaptchaChallenges[key];
    return key;
}

function getLoginLockInfo(data, username) {
    const key = String(username || '').trim().toLowerCase();
    const current = data.loginAttempts[key] || { attempts: 0, lockUntil: 0 };
    const now = Date.now();
    if (current.lockUntil && current.lockUntil <= now) {
        current.attempts = 0;
        current.lockUntil = 0;
        data.loginAttempts[key] = current;
    }
    return current;
}

async function registerLoginAttempt({ username, success, reason = '', profileId = null, ip = '' }) {
    const data = await loadSecurityData();
    const key = String(username || '').trim().toLowerCase();
    const now = Date.now();
    const current = getLoginLockInfo(data, key);

    if (success) {
        data.loginAttempts[key] = { attempts: 0, lockUntil: 0, lastAttemptAt: now };
    } else {
        current.attempts = (current.attempts || 0) + 1;
        current.lastAttemptAt = now;
        if (current.attempts >= MAX_LOGIN_ATTEMPTS) {
            current.lockUntil = now + LOGIN_LOCK_MS;
        }
        data.loginAttempts[key] = current;
    }

    appendLogEntry(data, {
        type: success ? 'login_success' : 'login_failed',
        username,
        profileId,
        reason,
        ip,
        lockUntil: data.loginAttempts[key]?.lockUntil || 0
    });

    await saveSecurityData(data);

    return {
        attempts: data.loginAttempts[key]?.attempts || 0,
        lockUntil: data.loginAttempts[key]?.lockUntil || 0,
        remainingAttempts: Math.max(0, MAX_LOGIN_ATTEMPTS - (data.loginAttempts[key]?.attempts || 0))
    };
}

async function getLoginStatus(username) {
    const data = await loadSecurityData();
    const current = getLoginLockInfo(data, username);
    if (current.lockUntil && current.lockUntil <= Date.now()) {
        await saveSecurityData(data);
    }
    return {
        attempts: current.attempts || 0,
        lockUntil: current.lockUntil || 0,
        remainingAttempts: Math.max(0, MAX_LOGIN_ATTEMPTS - (current.attempts || 0))
    };
}

async function clearLoginLock(username) {
    const data = await loadSecurityData();
    const key = String(username || '').trim().toLowerCase();
    data.loginAttempts[key] = { attempts: 0, lockUntil: 0, lastAttemptAt: Date.now() };
    await saveSecurityData(data);
}

function getPromoAttemptInfo(data, clientId, attemptKey = '') {
    const key = String(attemptKey || clientId || '').trim() || 'anonymous';
    const current = data.promoAttempts[key] || { attempts: 0, lockUntil: 0 };
    const now = Date.now();
    if (current.lockUntil && current.lockUntil <= now) {
        current.attempts = 0;
        current.lockUntil = 0;
        data.promoAttempts[key] = current;
    }
    return { key, current };
}

async function registerPromoAttempt({ clientId, attemptKey = '', success, code = '', note = '' }) {
    const data = await loadSecurityData();
    const now = Date.now();
    const { key, current } = getPromoAttemptInfo(data, clientId, attemptKey);

    if (success) {
        data.promoAttempts[key] = { attempts: 0, lockUntil: 0, lastAttemptAt: now };
    } else {
        current.attempts = (current.attempts || 0) + 1;
        current.lastAttemptAt = now;
        if (current.attempts >= MAX_PROMO_ATTEMPTS) {
            current.lockUntil = now + PROMO_LOCK_MS;
        }
        data.promoAttempts[key] = current;
    }

    appendLogEntry(data, {
        type: success ? 'promo_success' : 'promo_failed',
        clientId: key,
        code,
        reason: note,
        lockUntil: data.promoAttempts[key]?.lockUntil || 0
    });

    await saveSecurityData(data);

    return {
        attempts: data.promoAttempts[key]?.attempts || 0,
        lockUntil: data.promoAttempts[key]?.lockUntil || 0,
        remainingAttempts: Math.max(0, MAX_PROMO_ATTEMPTS - (data.promoAttempts[key]?.attempts || 0))
    };
}

async function getPromoStatus(clientId, attemptKey = '') {
    const data = await loadSecurityData();
    const { current } = getPromoAttemptInfo(data, clientId, attemptKey);
    if (current.lockUntil && current.lockUntil <= Date.now()) {
        await saveSecurityData(data);
    }
    return {
        attempts: current.attempts || 0,
        lockUntil: current.lockUntil || 0,
        remainingAttempts: Math.max(0, MAX_PROMO_ATTEMPTS - (current.attempts || 0))
    };
}

function getMoscowDayKey(input = new Date()) {
    return new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Europe/Moscow',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).format(new Date(input));
}

function getNextMoscowMidnightTimestamp(input = new Date()) {
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Europe/Moscow',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
    }).formatToParts(new Date(input));
    const year = parts.find(part => part.type === 'year')?.value || '1970';
    const month = parts.find(part => part.type === 'month')?.value || '01';
    const day = parts.find(part => part.type === 'day')?.value || '01';
    return Date.parse(`${year}-${month}-${day}T00:00:00+03:00`) + (24 * 60 * 60 * 1000);
}

function normalizeProfilePromoActivationState(rawState = {}, input = new Date()) {
    const dayKey = getMoscowDayKey(input);
    const nextResetAt = getNextMoscowMidnightTimestamp(input);
    const current = rawState && typeof rawState === 'object' ? rawState : {};
    const sameDay = String(current.dayKey || '') === dayKey;
    const attempts = sameDay ? Math.max(0, parseInt(current.attempts, 10) || 0) : 0;
    const blocked = attempts >= MAX_PROMO_ATTEMPTS;

    return {
        attempts,
        dayKey,
        lastAttemptAt: sameDay ? (current.lastAttemptAt || 0) : 0,
        blocked,
        remainingAttempts: Math.max(0, MAX_PROMO_ATTEMPTS - attempts),
        nextResetAt
    };
}

async function getProfilePromoActivationStatus(profileId) {
    const data = await loadSecurityData();
    const key = String(profileId || '').trim() || 'anonymous';
    const normalized = normalizeProfilePromoActivationState(data.profilePromoActivationAttempts[key]);
    const changed = JSON.stringify(data.profilePromoActivationAttempts[key] || {}) !== JSON.stringify({
        attempts: normalized.attempts,
        dayKey: normalized.dayKey,
        lastAttemptAt: normalized.lastAttemptAt
    });

    data.profilePromoActivationAttempts[key] = {
        attempts: normalized.attempts,
        dayKey: normalized.dayKey,
        lastAttemptAt: normalized.lastAttemptAt
    };

    if (changed) {
        await saveSecurityData(data);
    }

    return normalized;
}

async function registerProfilePromoActivationAttempt(profileId, { success, code = '', note = '' } = {}) {
    const data = await loadSecurityData();
    const key = String(profileId || '').trim() || 'anonymous';
    const now = new Date();
    const normalized = normalizeProfilePromoActivationState(data.profilePromoActivationAttempts[key], now);
    const nextAttempts = Math.min(MAX_PROMO_ATTEMPTS, normalized.attempts + 1);

    data.profilePromoActivationAttempts[key] = {
        attempts: nextAttempts,
        dayKey: normalized.dayKey,
        lastAttemptAt: now.toISOString()
    };

    appendLogEntry(data, {
        type: success ? 'profile_promo_activation_success' : 'profile_promo_activation_failed',
        profileId: key,
        code,
        reason: note,
        lockUntil: nextAttempts >= MAX_PROMO_ATTEMPTS ? normalized.nextResetAt : 0
    });

    await saveSecurityData(data);

    return normalizeProfilePromoActivationState(data.profilePromoActivationAttempts[key], now);
}

function normalizePromoCode(promo) {
    const parsedDailyRequestsLimit = promo.dailyRequestsLimit === null || promo.dailyRequestsLimit === undefined || promo.dailyRequestsLimit === ''
        ? null
        : parseInt(promo.dailyRequestsLimit, 10);
    return {
        id: promo.id,
        code: String(promo.code || '').trim().toUpperCase(),
        label: String(promo.label || '').trim(),
        durationMinutes: promo.durationMinutes === null || promo.durationMinutes === undefined || promo.durationMinutes === ''
            ? null
            : Math.max(1, parseInt(promo.durationMinutes, 10) || 1),
        dailyRequestsLimit: Number.isFinite(parsedDailyRequestsLimit) && parsedDailyRequestsLimit > 0 ? parsedDailyRequestsLimit : null,
        maxUses: Math.max(1, parseInt(promo.maxUses, 10) || 1),
        usedCount: Math.max(0, parseInt(promo.usedCount, 10) || 0),
        active: promo.active !== false,
        createdAt: promo.createdAt || new Date().toISOString(),
        createdByProfileId: promo.createdByProfileId || '1',
        usedByProfileIds: Array.isArray(promo.usedByProfileIds) ? promo.usedByProfileIds : []
    };
}

async function listPromoCodes() {
    const data = await loadSecurityData();
    return data.promoCodes.map(normalizePromoCode).sort((a, b) => b.createdAt.localeCompare(a.createdAt));
}

async function savePromoCode(promoInput, createdByProfileId = '1') {
    const data = await loadSecurityData();
    const code = String(promoInput?.code || '').trim().toUpperCase();
    if (!code) throw new Error('Промокод обязателен');

    const existing = data.promoCodes.find(item => String(item.code || '').trim().toUpperCase() === code);
    if (existing && promoInput.id !== existing.id) {
        throw new Error('Такой промокод уже существует');
    }

    const promo = normalizePromoCode({
        ...existing,
        ...promoInput,
        id: promoInput?.id || existing?.id || `promo_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        createdByProfileId: existing?.createdByProfileId || createdByProfileId
    });

    const index = data.promoCodes.findIndex(item => item.id === promo.id);
    if (index >= 0) data.promoCodes[index] = promo;
    else data.promoCodes.unshift(promo);

    appendLogEntry(data, {
        type: 'promo_saved',
        code: promo.code,
        profileId: createdByProfileId,
        reason: promo.label || ''
    });

    await saveSecurityData(data);
    return promo;
}

async function deletePromoCodeById(id, deletedByProfileId = '1') {
    const data = await loadSecurityData();
    const index = data.promoCodes.findIndex(item => item.id === id);
    if (index < 0) throw new Error('Промокод не найден');
    const [removed] = data.promoCodes.splice(index, 1);
    appendLogEntry(data, {
        type: 'promo_deleted',
        code: removed.code,
        profileId: deletedByProfileId
    });
    await saveSecurityData(data);
    return { success: true };
}

async function getPromoByCode(code) {
    const codes = await listPromoCodes();
    return codes.find(item => item.code === String(code || '').trim().toUpperCase()) || null;
}

async function consumePromoCode(code, createdProfileId) {
    const data = await loadSecurityData();
    const normalizedCode = String(code || '').trim().toUpperCase();
    const promo = data.promoCodes.find(item => String(item.code || '').trim().toUpperCase() === normalizedCode);
    if (!promo || promo.active === false) {
        throw new Error('Промокод не найден');
    }
    const normalized = normalizePromoCode(promo);
    if (normalized.usedCount >= normalized.maxUses) {
        throw new Error('Промокод уже исчерпан');
    }

    normalized.usedCount += 1;
    normalized.usedByProfileIds = [...normalized.usedByProfileIds, createdProfileId];
    const index = data.promoCodes.findIndex(item => item.id === normalized.id);
    data.promoCodes[index] = normalized;

    appendLogEntry(data, {
        type: 'promo_consumed',
        code: normalized.code,
        profileId: createdProfileId
    });

    await saveSecurityData(data);
    return normalized;
}

async function createRecoveryRequest(request) {
    const data = await loadSecurityData();
    const entry = {
        id: `recovery_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        createdAt: new Date().toISOString(),
        status: 'pending',
        resolvedAt: null,
        tempPassword: '',
        ...request
    };
    data.recoveryRequests.unshift(entry);
    data.recoveryRequests = data.recoveryRequests.slice(0, 200);
    appendLogEntry(data, {
        type: 'recovery_requested',
        username: request.username || '',
        profileId: request.profileId || '',
        reason: request.recoveryEmail || ''
    });
    await saveSecurityData(data);
    return entry;
}

async function listRecoveryRequests() {
    const data = await loadSecurityData();
    return data.recoveryRequests.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
}

async function resolveRecoveryRequest(requestId, payload = {}) {
    const data = await loadSecurityData();
    const request = data.recoveryRequests.find(item => item.id === requestId);
    if (!request) throw new Error('Запрос восстановления не найден');
    request.status = payload.status || 'resolved';
    request.resolvedAt = new Date().toISOString();
    request.tempPassword = payload.tempPassword || '';
    request.resolvedByProfileId = payload.resolvedByProfileId || '1';
    request.note = payload.note || '';

    appendLogEntry(data, {
        type: 'recovery_resolved',
        profileId: request.profileId,
        username: request.username,
        reason: request.status
    });

    await saveSecurityData(data);
    return request;
}

async function getAdminDashboardData() {
    const data = await loadSecurityData();
    return {
        promoCodes: data.promoCodes.map(normalizePromoCode).sort((a, b) => b.createdAt.localeCompare(a.createdAt)),
        recoveryRequests: data.recoveryRequests.sort((a, b) => b.createdAt.localeCompare(a.createdAt)).slice(0, 100),
        loginLogs: data.loginLogs.slice(0, 200)
    };
}

async function requireLoginCaptcha(ip = '', reason = '') {
    const data = await loadSecurityData();
    const result = setLoginCaptchaRequirement(data, ip, true, reason);
    appendSecurityEvent(data, {
        type: 'login_captcha_required',
        ip: getLoginCaptchaKey(ip),
        reason: reason || 'security_incident'
    });
    await saveSecurityData(data);
    return result.state;
}

async function getLoginCaptchaStatus(ip = '') {
    const data = await loadSecurityData();
    return getLoginCaptchaRequirement(data, ip).state;
}

async function clearLoginCaptcha(ip = '') {
    const data = await loadSecurityData();
    clearLoginCaptchaRequirement(data, ip);
    clearLoginCaptchaChallenge(data, ip);
    await saveSecurityData(data);
}

async function issueLoginCaptcha(ip = '', answer = '') {
    const data = await loadSecurityData();
    const requirement = getLoginCaptchaRequirement(data, ip).state;
    if (!requirement.required) {
        setLoginCaptchaRequirement(data, ip, true, 'login_challenge_issued');
    }
    const challenge = createCaptchaChallenge({ mode: 'login', answer, now: new Date() });
    setLoginCaptchaChallenge(data, ip, challenge);
    await saveSecurityData(data);
    return challenge;
}

async function verifyLoginCaptcha(ip = '', answer = '', now = new Date()) {
    const data = await loadSecurityData();
    const requirementInfo = getLoginCaptchaRequirement(data, ip);
    const challengeInfo = getLoginCaptchaChallenge(data, ip);

    if (!requirementInfo.state.required) {
        return { ok: true, required: false };
    }

    const state = {
        captchaChallenge: challengeInfo.challenge,
        captchaFailCount: requirementInfo.state.failCount
    };
    const result = verifyCaptchaAnswer(state, answer, now);

    if (result.ok) {
        clearLoginCaptchaRequirement(data, ip);
        clearLoginCaptchaChallenge(data, ip);
        appendSecurityEvent(data, {
            type: 'captcha_passed',
            ip: getLoginCaptchaKey(ip),
            reason: 'login'
        });
    } else {
        setLoginCaptchaRequirement(data, ip, true, requirementInfo.state.reason || 'security_incident');
        data.loginCaptchaRequirements[getLoginCaptchaKey(ip)].failCount = state.captchaFailCount;
        if (result.terminateSession) {
            appendSecurityEvent(data, {
                type: 'captcha_failed',
                ip: getLoginCaptchaKey(ip),
                reason: 'login_failed_three_times'
            });
        } else {
            appendSecurityEvent(data, {
                type: 'captcha_failed',
                ip: getLoginCaptchaKey(ip),
                reason: 'login_failed'
            });
        }
    }

    await saveSecurityData(data);
    return {
        ...result,
        required: true
    };
}

async function loadSecurityDataWithDependencies(overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    try {
        const response = await hotStateStore.loadJsonObject(SECURITY_FILE_KEY, {
            defaultValue: buildDefaultSecurityData()
        });
        return normalizeSecurityData(response && response.value);
    } catch (error) {
        log('warn', `⚠️ admin_security.json not loaded, using defaults: ${error.message}`);
        return buildDefaultSecurityData();
    }
}

async function saveSecurityDataWithDependencies(data, overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    const normalized = normalizeSecurityData(data);
    await hotStateStore.saveJsonObject(SECURITY_FILE_KEY, normalized);
    return normalized;
}

async function loadSecurityData() {
    return loadSecurityDataWithDependencies();
}

async function saveSecurityData(data) {
    return saveSecurityDataWithDependencies(data);
}

module.exports = {
    SECURITY_FILE_KEY,
    LOGIN_LOCK_MS,
    PROMO_LOCK_MS,
    MAX_LOGIN_ATTEMPTS,
    MAX_PROMO_ATTEMPTS,
    buildDefaultSecurityData,
    loadSecurityData,
    saveSecurityData,
    appendSecurityEvent,
    createCaptchaChallenge,
    verifyCaptchaAnswer,
    checkCaptchaRateLimit,
    registerCaptchaRateLimitHit,
    requireLoginCaptcha,
    getLoginCaptchaStatus,
    clearLoginCaptcha,
    issueLoginCaptcha,
    verifyLoginCaptcha,
    registerLoginAttempt,
    getLoginStatus,
    clearLoginLock,
    registerPromoAttempt,
    getPromoStatus,
    getMoscowDayKey,
    getNextMoscowMidnightTimestamp,
    normalizeProfilePromoActivationState,
    getProfilePromoActivationStatus,
    registerProfilePromoActivationAttempt,
    listPromoCodes,
    savePromoCode,
    deletePromoCodeById,
    getPromoByCode,
    consumePromoCode,
    createRecoveryRequest,
    listRecoveryRequests,
    resolveRecoveryRequest,
    getAdminDashboardData,
    __testOnly: {
        loadSecurityDataWithDependencies,
        saveSecurityDataWithDependencies
    }
};
