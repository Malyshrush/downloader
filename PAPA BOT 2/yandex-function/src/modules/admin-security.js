const { GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getS3Client, getBucketName } = require('./storage');
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
        recoveryRequests: [],
        promoCodes: [],
        loginLogs: []
    };
}

function normalizeSecurityData(raw) {
    const fallback = buildDefaultSecurityData();
    if (!raw || typeof raw !== 'object') return fallback;
    return {
        loginAttempts: raw.loginAttempts && typeof raw.loginAttempts === 'object' ? raw.loginAttempts : {},
        promoAttempts: raw.promoAttempts && typeof raw.promoAttempts === 'object' ? raw.promoAttempts : {},
        recoveryRequests: Array.isArray(raw.recoveryRequests) ? raw.recoveryRequests : [],
        promoCodes: Array.isArray(raw.promoCodes) ? raw.promoCodes : [],
        loginLogs: Array.isArray(raw.loginLogs) ? raw.loginLogs : []
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

function normalizePromoCode(promo) {
    return {
        id: promo.id,
        code: String(promo.code || '').trim().toUpperCase(),
        label: String(promo.label || '').trim(),
        durationMinutes: promo.durationMinutes === null || promo.durationMinutes === undefined || promo.durationMinutes === ''
            ? null
            : Math.max(1, parseInt(promo.durationMinutes, 10) || 1),
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

module.exports = {
    SECURITY_FILE_KEY,
    LOGIN_LOCK_MS,
    PROMO_LOCK_MS,
    MAX_LOGIN_ATTEMPTS,
    MAX_PROMO_ATTEMPTS,
    buildDefaultSecurityData,
    loadSecurityData,
    saveSecurityData,
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
    listRecoveryRequests,
    resolveRecoveryRequest,
    getAdminDashboardData
};
