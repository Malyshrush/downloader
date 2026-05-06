const crypto = require('crypto');
const { createHotStateStore } = require('./hot-state-store');
const { createCaptchaChallenge, verifyCaptchaAnswer } = require('./admin-security');
const { getProfileById, isMainAdminProfile, isProfileExpired } = require('./admin-profiles');
const { log } = require('../utils/logger');

const SESSIONS_FILE_KEY = 'admin_sessions.json';
const SESSION_TIMEOUT_MS = 12 * 60 * 60 * 1000;

function createEmptySessionStore() {
    return { sessions: {} };
}

function normalizeSessionRecord(raw = {}) {
    return {
        sessionId: String(raw.sessionId || '').trim(),
        profileId: String(raw.profileId || '').trim(),
        createdAt: raw.createdAt || new Date().toISOString(),
        lastSeenAt: raw.lastSeenAt || raw.createdAt || new Date().toISOString(),
        lastVerifiedIp: String(raw.lastVerifiedIp || '').trim(),
        lastUserAgent: String(raw.lastUserAgent || '').trim(),
        captchaRequired: raw.captchaRequired === true,
        captchaFailCount: Math.max(0, parseInt(raw.captchaFailCount, 10) || 0),
        suspiciousChangeCount: Math.max(0, parseInt(raw.suspiciousChangeCount, 10) || 0),
        captchaChallenge: raw.captchaChallenge && typeof raw.captchaChallenge === 'object'
            ? {
                hash: String(raw.captchaChallenge.hash || '').trim(),
                expiresAt: raw.captchaChallenge.expiresAt || '',
                attempts: Math.max(0, parseInt(raw.captchaChallenge.attempts, 10) || 0),
                mode: String(raw.captchaChallenge.mode || 'session').trim() || 'session',
                captchaSvg: raw.captchaChallenge.captchaSvg || ''
            }
            : null,
        loginCaptchaRequired: raw.loginCaptchaRequired === true,
        loginCaptchaFailCount: Math.max(0, parseInt(raw.loginCaptchaFailCount, 10) || 0),
        terminatedAt: raw.terminatedAt || null,
        terminateReason: String(raw.terminateReason || '').trim(),
        captchaReason: String(raw.captchaReason || '').trim()
    };
}

function normalizeSessionStore(raw) {
    if (!raw || typeof raw !== 'object' || !raw.sessions || typeof raw.sessions !== 'object') {
        return createEmptySessionStore();
    }

    const sessions = {};
    for (const [sessionId, session] of Object.entries(raw.sessions)) {
        const normalized = normalizeSessionRecord({ ...session, sessionId });
        if (!normalized.sessionId) continue;
        sessions[normalized.sessionId] = normalized;
    }

    return { sessions };
}

function createSessionRecord({ sessionId, profileId, ip, userAgent, now = new Date().toISOString() }) {
    return normalizeSessionRecord({
        sessionId,
        profileId,
        createdAt: now,
        lastSeenAt: now,
        lastVerifiedIp: ip,
        lastUserAgent: userAgent,
        captchaRequired: false,
        captchaFailCount: 0,
        suspiciousChangeCount: 0,
        captchaChallenge: null,
        loginCaptchaRequired: false,
        loginCaptchaFailCount: 0,
        terminatedAt: null,
        terminateReason: ''
    });
}

function generateSessionId() {
    return `sess_${crypto.randomBytes(24).toString('hex')}`;
}

function isSessionExpired(session, now = new Date()) {
    const lastSeenAtMs = new Date(session?.lastSeenAt || 0).getTime();
    if (!lastSeenAtMs) return true;
    return (now.getTime() - lastSeenAtMs) > SESSION_TIMEOUT_MS;
}

function computeSessionRisk(session, { ip, userAgent, now = new Date() }) {
    let total = 0;
    const currentIp = String(ip || '').trim();
    const currentUserAgent = String(userAgent || '').trim();
    const lastSeenAtMs = new Date(session?.lastSeenAt || 0).getTime();

    if (currentIp && session?.lastVerifiedIp && currentIp !== session.lastVerifiedIp) {
        total += 2;
    }
    if (currentUserAgent && session?.lastUserAgent && currentUserAgent !== session.lastUserAgent) {
        total += 2;
    }
    if (
        currentIp &&
        session?.lastVerifiedIp &&
        currentIp !== session.lastVerifiedIp &&
        lastSeenAtMs &&
        (now.getTime() - lastSeenAtMs) < (15 * 60 * 1000)
    ) {
        total += 2;
    }

    total += Math.min(3, Math.max(0, parseInt(session?.suspiciousChangeCount, 10) || 0));

    return {
        total,
        requiresCaptcha: total >= 3
    };
}

function touchSession(session, { ip, userAgent, now = new Date().toISOString(), verified = false } = {}) {
    session.lastSeenAt = now;
    if (ip !== undefined) session.lastVerifiedIp = String(ip || '').trim();
    if (userAgent !== undefined) session.lastUserAgent = String(userAgent || '').trim();
    if (verified) {
        session.captchaRequired = false;
        session.captchaFailCount = 0;
        session.suspiciousChangeCount = 0;
        session.captchaReason = '';
        session.captchaChallenge = null;
    }
    return session;
}

function markSessionCaptchaRequired(session, reason = '') {
    session.captchaRequired = true;
    session.suspiciousChangeCount = Math.min(99, (parseInt(session.suspiciousChangeCount, 10) || 0) + 1);
    if (reason) {
        session.captchaReason = String(reason).trim();
    }
    return session;
}

function killSession(session, reason = '', now = new Date().toISOString()) {
    session.terminatedAt = now;
    session.terminateReason = String(reason || '').trim();
    session.captchaRequired = false;
    session.captchaChallenge = null;
    return session;
}

function pruneExpiredSessions(store, now = new Date()) {
    const normalized = normalizeSessionStore(store);
    for (const [sessionId, session] of Object.entries(normalized.sessions)) {
        if (session.terminatedAt || isSessionExpired(session, now)) {
            delete normalized.sessions[sessionId];
        }
    }
    return normalized;
}

async function loadAdminSessions() {
    const s3Client = getS3Client();
    const bucket = getBucketName();

    try {
        const response = await s3Client.send(new GetObjectCommand({
            Bucket: bucket,
            Key: SESSIONS_FILE_KEY
        }));
        const text = await response.Body.transformToString();
        return normalizeSessionStore(JSON.parse(text));
    } catch (error) {
        log('warn', `admin_sessions.json not loaded, using defaults: ${error.message}`);
        return createEmptySessionStore();
    }
}

async function saveAdminSessions(data) {
    const normalized = normalizeSessionStore(data);
    const s3Client = getS3Client();
    const bucket = getBucketName();

    await s3Client.send(new PutObjectCommand({
        Bucket: bucket,
        Key: SESSIONS_FILE_KEY,
        Body: JSON.stringify(normalized, null, 2),
        ContentType: 'application/json'
    }));

    return normalized;
}

function upsertSession(store, session) {
    const normalizedStore = normalizeSessionStore(store);
    const normalizedSession = normalizeSessionRecord(session);
    if (!normalizedSession.sessionId) {
        throw new Error('Session id is required');
    }
    normalizedStore.sessions[normalizedSession.sessionId] = normalizedSession;
    return normalizedStore;
}

async function getAdminSession(sessionId) {
    const normalizedSessionId = String(sessionId || '').trim();
    if (!normalizedSessionId) return null;
    const store = await loadAdminSessions();
    return store.sessions[normalizedSessionId] || null;
}

async function createAdminSession({ profileId, ip, userAgent, now = new Date().toISOString() }) {
    const store = await loadAdminSessions();
    const session = createSessionRecord({
        sessionId: generateSessionId(),
        profileId,
        ip,
        userAgent,
        now
    });
    const nextStore = upsertSession(pruneExpiredSessions(store), session);
    await saveAdminSessions(nextStore);
    return session;
}

async function updateAdminSession(session) {
    const store = await loadAdminSessions();
    const nextStore = upsertSession(pruneExpiredSessions(store), session);
    await saveAdminSessions(nextStore);
    return nextStore.sessions[session.sessionId];
}

async function killAdminSession(sessionId, reason = '', now = new Date().toISOString()) {
    const store = await loadAdminSessions();
    const session = store.sessions[String(sessionId || '').trim()];
    if (!session) return null;
    killSession(session, reason, now);
    const nextStore = upsertSession(store, session);
    await saveAdminSessions(nextStore);
    return nextStore.sessions[session.sessionId];
}

async function issueSessionCaptcha(sessionId, answer = '') {
    const store = await loadAdminSessions();
    const session = store.sessions[String(sessionId || '').trim()];
    if (!session) return null;
    session.captchaChallenge = createCaptchaChallenge({ mode: 'session', answer, now: new Date() });
    const nextStore = upsertSession(store, session);
    await saveAdminSessions(nextStore);
    return nextStore.sessions[session.sessionId].captchaChallenge;
}

async function verifySessionCaptcha(sessionId, answer, { ip = '', userAgent = '', now = new Date() } = {}) {
    const store = await loadAdminSessions();
    const session = store.sessions[String(sessionId || '').trim()];
    if (!session) {
        return { ok: false, errorCode: 'session_not_found' };
    }

    const result = verifyCaptchaAnswer(session, answer, now);
    if (result.ok) {
        touchSession(session, {
            ip,
            userAgent,
            now: now.toISOString(),
            verified: true
        });
    }

    if (result.terminateSession) {
        killSession(session, 'captcha_failed_three_times', now.toISOString());
    }

    const nextStore = upsertSession(store, session);
    await saveAdminSessions(nextStore);
    return {
        ...result,
        session: nextStore.sessions[session.sessionId]
    };
}

async function validateAdminSessionRequest({ sessionId = '', ip = '', userAgent = '', now = new Date() } = {}) {
    const session = await getAdminSession(sessionId);
    if (!session) {
        return { ok: false, statusCode: 401, error: 'Сессия не найдена', clearCookie: true, sessionInvalid: true };
    }
    if (session.terminatedAt) {
        return { ok: false, statusCode: 401, error: 'Сессия завершена', clearCookie: true, sessionInvalid: true };
    }
    if (isSessionExpired(session, now)) {
        await killAdminSession(session.sessionId, 'session_expired', now.toISOString());
        return { ok: false, statusCode: 401, error: 'Сессия истекла', clearCookie: true, expired: true, sessionInvalid: true };
    }

    const profile = await getProfileById(session.profileId);
    if (!profile) {
        await killAdminSession(session.sessionId, 'profile_not_found', now.toISOString());
        return { ok: false, statusCode: 401, error: 'Профиль входа не найден', clearCookie: true, sessionInvalid: true };
    }
    if (profile.active === false) {
        await killAdminSession(session.sessionId, 'profile_inactive', now.toISOString());
        return { ok: false, statusCode: 403, error: 'Профиль отключён', clearCookie: true, sessionInvalid: true };
    }
    if (!isMainAdminProfile(profile) && isProfileExpired(profile)) {
        await killAdminSession(session.sessionId, 'profile_expired', now.toISOString());
        return { ok: false, statusCode: 403, error: 'Срок действия профиля истёк', clearCookie: true, expired: true, sessionInvalid: true };
    }

    if (session.captchaRequired) {
        return {
            ok: false,
            statusCode: 403,
            error: 'Требуется каптча',
            captchaRequired: true,
            sessionInvalid: false,
            clearCookie: false,
            session,
            profile
        };
    }

    const risk = computeSessionRisk(session, { ip, userAgent, now });
    if (risk.requiresCaptcha) {
        markSessionCaptchaRequired(session, 'ip_changed_suspicious');
        const store = await loadAdminSessions();
        const nextStore = upsertSession(store, session);
        await saveAdminSessions(nextStore);
        return {
            ok: false,
            statusCode: 403,
            error: 'Требуется каптча',
            captchaRequired: true,
            sessionInvalid: false,
            clearCookie: false,
            risk,
            session: nextStore.sessions[session.sessionId],
            profile
        };
    }

    touchSession(session, { now: now.toISOString() });
    const store = await loadAdminSessions();
    const nextStore = upsertSession(store, session);
    await saveAdminSessions(nextStore);

    return {
        ok: true,
        session: nextStore.sessions[session.sessionId],
        profile
    };
}

async function loadAdminSessionsWithDependencies(overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    try {
        const response = await hotStateStore.loadJsonObject(SESSIONS_FILE_KEY, {
            defaultValue: createEmptySessionStore()
        });
        return normalizeSessionStore(response && response.value);
    } catch (error) {
        log('warn', `⚠️ admin_sessions.json not loaded, using defaults: ${error.message}`);
        return createEmptySessionStore();
    }
}

async function saveAdminSessionsWithDependencies(data, overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    const normalized = normalizeSessionStore(data);
    await hotStateStore.saveJsonObject(SESSIONS_FILE_KEY, normalized);
    return normalized;
}

async function loadAdminSessions() {
    return loadAdminSessionsWithDependencies();
}

async function saveAdminSessions(data) {
    return saveAdminSessionsWithDependencies(data);
}

module.exports = {
    SESSIONS_FILE_KEY,
    SESSION_TIMEOUT_MS,
    createEmptySessionStore,
    normalizeSessionRecord,
    normalizeSessionStore,
    createSessionRecord,
    generateSessionId,
    isSessionExpired,
    computeSessionRisk,
    touchSession,
    markSessionCaptchaRequired,
    killSession,
    pruneExpiredSessions,
    loadAdminSessions,
    saveAdminSessions,
    upsertSession,
    getAdminSession,
    createAdminSession,
    updateAdminSession,
    killAdminSession,
    issueSessionCaptcha,
    verifySessionCaptcha,
    validateAdminSessionRequest,
    __testOnly: {
        loadAdminSessionsWithDependencies,
        saveAdminSessionsWithDependencies
    }
};
