/**
 * Модуль профилей администраторов
 */

const { createHotStateStore } = require('./hot-state-store');
const { log } = require('../utils/logger');

const AUTH_FILE_KEY = 'admin_auth.json';
const MAIN_ADMIN_PROFILE_ID = '1';

function normalizeProfileId(profileId) {
    const normalized = String(profileId || MAIN_ADMIN_PROFILE_ID).trim();
    return normalized || MAIN_ADMIN_PROFILE_ID;
}

function buildDefaultAuthConfig() {
    return {
        defaultProfileId: MAIN_ADMIN_PROFILE_ID,
        profiles: {
            [MAIN_ADMIN_PROFILE_ID]: {
                id: MAIN_ADMIN_PROFILE_ID,
                name: 'Главный админ',
                username: process.env.ADMIN_USERNAME || 'admin',
                password: process.env.ADMIN_PASSWORD || 'admin123',
                recoveryEmail: process.env.ADMIN_EMAIL || 'admin@example.com',
                role: 'main_admin',
                active: true,
                expiresAt: null,
                createdAt: new Date().toISOString(),
                createdByProfileId: MAIN_ADMIN_PROFILE_ID,
                promoCodeUsed: ''
            }
        }
    };
}

function normalizeProfile(profileId, profile, fallbackProfile = null) {
    const normalizedId = normalizeProfileId(profileId);
    const fallback = fallbackProfile || {};
    const isMain = normalizedId === MAIN_ADMIN_PROFILE_ID;
    const resolvedRequestsLimit = profile?.requestsLimit !== undefined
        ? parseInt(profile.requestsLimit, 10)
        : (fallback?.requestsLimit !== undefined ? parseInt(fallback.requestsLimit, 10) : null);
    return {
        id: normalizedId,
        name: profile?.name || fallback.name || `Профиль ${normalizedId}`,
        username: profile?.username || fallback.username || '',
        password: profile?.password || fallback.password || '',
        recoveryEmail: profile?.recoveryEmail || fallback.recoveryEmail || '',
        role: isMain ? 'main_admin' : (profile?.role || 'admin'),
        active: isMain ? true : profile?.active !== false,
        expiresAt: isMain ? null : (profile?.expiresAt || null),
        createdAt: profile?.createdAt || fallback.createdAt || new Date().toISOString(),
        createdByProfileId: profile?.createdByProfileId || fallback.createdByProfileId || MAIN_ADMIN_PROFILE_ID,
        promoCodeUsed: profile?.promoCodeUsed || '',
        lastLoginAt: profile?.lastLoginAt || null,
        requestsLimit: Number.isFinite(resolvedRequestsLimit) && resolvedRequestsLimit > 0 ? resolvedRequestsLimit : null
    };
}

function normalizeAdminAuth(raw) {
    const fallback = buildDefaultAuthConfig();

    if (!raw || typeof raw !== 'object') {
        return fallback;
    }

    if (!raw.profiles || typeof raw.profiles !== 'object') {
        return {
            defaultProfileId: MAIN_ADMIN_PROFILE_ID,
            profiles: {
                [MAIN_ADMIN_PROFILE_ID]: normalizeProfile(MAIN_ADMIN_PROFILE_ID, raw, fallback.profiles[MAIN_ADMIN_PROFILE_ID])
            }
        };
    }

    const profiles = {};
    for (const [profileId, profile] of Object.entries(raw.profiles)) {
        profiles[normalizeProfileId(profileId)] = normalizeProfile(profileId, profile, fallback.profiles[profileId]);
    }

    profiles[MAIN_ADMIN_PROFILE_ID] = normalizeProfile(
        MAIN_ADMIN_PROFILE_ID,
        profiles[MAIN_ADMIN_PROFILE_ID] || fallback.profiles[MAIN_ADMIN_PROFILE_ID],
        fallback.profiles[MAIN_ADMIN_PROFILE_ID]
    );

    const defaultProfileId = profiles[normalizeProfileId(raw.defaultProfileId)]
        ? normalizeProfileId(raw.defaultProfileId)
        : MAIN_ADMIN_PROFILE_ID;

    return {
        defaultProfileId,
        profiles
    };
}

async function loadAdminAuth() {
    const s3Client = getS3Client();
    const bucket = getBucketName();

    try {
        const response = await s3Client.send(new GetObjectCommand({
            Bucket: bucket,
            Key: AUTH_FILE_KEY
        }));
        const text = await response.Body.transformToString();
        return normalizeAdminAuth(JSON.parse(text));
    } catch (error) {
        log('warn', `⚠️ admin_auth.json not loaded, using defaults: ${error.message}`);
        return buildDefaultAuthConfig();
    }
}

async function saveAdminAuth(config) {
    const normalized = normalizeAdminAuth(config);
    const s3Client = getS3Client();
    const bucket = getBucketName();

    await s3Client.send(new PutObjectCommand({
        Bucket: bucket,
        Key: AUTH_FILE_KEY,
        Body: JSON.stringify(normalized, null, 2),
        ContentType: 'application/json'
    }));

    return normalized;
}

async function getAllProfileIds() {
    const auth = await loadAdminAuth();
    return Object.keys(auth.profiles || {}).sort((a, b) => Number(a) - Number(b));
}

function isProfileExpired(profile) {
    if (!profile?.expiresAt) return false;
    return new Date(profile.expiresAt).getTime() <= Date.now();
}

function isMainAdminProfile(profile) {
    return profile?.role === 'main_admin' || normalizeProfileId(profile?.id) === MAIN_ADMIN_PROFILE_ID;
}

function toPublicProfile(profile, currentProfileId = null) {
    const id = normalizeProfileId(profile?.id);
    const expired = isProfileExpired(profile);
    const expiresAt = profile?.expiresAt || null;
    const remainingMinutes = expiresAt && !expired
        ? Math.max(Math.ceil((new Date(expiresAt).getTime() - Date.now()) / 60000), 0)
        : null;
    return {
        id,
        name: profile?.name || `Профиль ${id}`,
        username: profile?.username || '',
        recoveryEmail: profile?.recoveryEmail || '',
        role: isMainAdminProfile(profile) ? 'main_admin' : 'admin',
        active: profile?.active !== false,
        expiresAt,
        isExpired: expired,
        remainingMinutes,
        isCurrent: normalizeProfileId(currentProfileId || MAIN_ADMIN_PROFILE_ID) === id,
        createdAt: profile?.createdAt || null,
        createdByProfileId: profile?.createdByProfileId || MAIN_ADMIN_PROFILE_ID,
        promoCodeUsed: profile?.promoCodeUsed || '',
        lastLoginAt: profile?.lastLoginAt || null,
        requestsLimit: profile?.requestsLimit || null
    };
}

async function getPublicProfiles(currentProfileId = null) {
    const auth = await loadAdminAuth();
    const profiles = Object.values(auth.profiles || {})
        .map(profile => toPublicProfile(profile, currentProfileId))
        .sort((a, b) => Number(a.id) - Number(b.id));

    return {
        defaultProfileId: normalizeProfileId(auth.defaultProfileId || MAIN_ADMIN_PROFILE_ID),
        profiles
    };
}

function getNextProfileId(profiles) {
    const ids = Object.keys(profiles || {}).map(id => parseInt(id, 10)).filter(Number.isFinite);
    const maxId = ids.length ? Math.max(...ids) : 0;
    return String(maxId + 1);
}

async function getProfileById(profileId) {
    const auth = await loadAdminAuth();
    return auth.profiles[normalizeProfileId(profileId)] || null;
}

async function findProfileByUsername(username) {
    const auth = await loadAdminAuth();
    const normalizedUsername = String(username || '').trim();
    return Object.values(auth.profiles || {}).find(profile => profile.username === normalizedUsername) || null;
}

async function findProfileByRecoveryEmail(email) {
    const auth = await loadAdminAuth();
    const normalizedEmail = String(email || '').trim().toLowerCase();
    return Object.values(auth.profiles || {}).find(profile => String(profile.recoveryEmail || '').trim().toLowerCase() === normalizedEmail) || null;
}

function buildExpiresAt(durationMinutes) {
    if (durationMinutes === null || durationMinutes === undefined || durationMinutes === '' || String(durationMinutes).toLowerCase() === 'infinite') {
        return null;
    }
    const minutes = Math.max(1, parseInt(durationMinutes, 10) || 1);
    return new Date(Date.now() + minutes * 60 * 1000).toISOString();
}

function buildProfilePromoActivationUpdate(profile, promo, inputNow = new Date()) {
    const now = new Date(inputNow);
    const durationMinutes = promo?.durationMinutes === null || promo?.durationMinutes === undefined || promo?.durationMinutes === ''
        ? null
        : Math.max(1, parseInt(promo.durationMinutes, 10) || 1);
    const currentExpiresAtMs = profile?.expiresAt ? new Date(profile.expiresAt).getTime() : 0;
    const baseTime = currentExpiresAtMs && currentExpiresAtMs > now.getTime() ? currentExpiresAtMs : now.getTime();
    const expiresAt = durationMinutes ? new Date(baseTime + durationMinutes * 60 * 1000).toISOString() : (profile?.expiresAt || null);
    const currentRequestsLimit = profile?.requestsLimit === null || profile?.requestsLimit === undefined || profile?.requestsLimit === ''
        ? null
        : Math.max(1, parseInt(profile.requestsLimit, 10) || 1);
    const promoRequestsLimit = promo?.dailyRequestsLimit === null || promo?.dailyRequestsLimit === undefined || promo?.dailyRequestsLimit === ''
        ? null
        : Math.max(1, parseInt(promo.dailyRequestsLimit, 10) || 1);
    const requestsLimit = promoRequestsLimit
        ? ((currentRequestsLimit || 0) + promoRequestsLimit)
        : currentRequestsLimit;

    return {
        active: true,
        expiresAt,
        promoCodeUsed: String(promo?.code || '').trim().toUpperCase(),
        lastLoginAt: now.toISOString(),
        requestsLimit: requestsLimit || null
    };
}

async function upsertAdminProfile(profileData, actorProfileId = MAIN_ADMIN_PROFILE_ID) {
    const auth = await loadAdminAuth();
    const profileId = profileData?.id ? normalizeProfileId(profileData.id) : getNextProfileId(auth.profiles);
    const existingProfile = auth.profiles[profileId] || null;
    const username = String(profileData?.username || '').trim();
    const providedPassword = String(profileData?.password || '').trim();
    const password = providedPassword || existingProfile?.password || '';
    const name = String(profileData?.name || `Профиль ${profileId}`).trim();
    const recoveryEmail = String(profileData?.recoveryEmail || '').trim();
    const expiresAt = profileData?.hasOwnProperty('expiresAt')
        ? (profileData.expiresAt || null)
        : buildExpiresAt(profileData?.durationMinutes);
    
    // Обработка лимита запросов
    const parsedRequestsLimit = profileData?.requestsLimit === null || profileData?.requestsLimit === undefined || profileData?.requestsLimit === ''
        ? null
        : parseInt(profileData.requestsLimit, 10);
    const requestsLimit = Number.isFinite(parsedRequestsLimit) && parsedRequestsLimit > 0
        ? parsedRequestsLimit
        : (existingProfile?.requestsLimit || null);

    if (!username) throw new Error('Логин обязателен');
    if (!password) throw new Error('Пароль обязателен');

    const duplicate = Object.values(auth.profiles || {}).find(profile => {
        return profile.username === username && normalizeProfileId(profile.id) !== profileId;
    });
    if (duplicate) {
        throw new Error('Такой логин уже используется в другом профиле');
    }

    const isMain = profileId === MAIN_ADMIN_PROFILE_ID;
    const role = isMain ? 'main_admin' : (profileData?.role || existingProfile?.role || 'admin');

    auth.profiles[profileId] = normalizeProfile(profileId, {
        ...existingProfile,
        ...profileData,
        id: profileId,
        name,
        username,
        password,
        recoveryEmail,
        role,
        expiresAt: isMain ? null : expiresAt,
        active: isMain ? true : profileData?.active !== false,
        createdByProfileId: existingProfile?.createdByProfileId || normalizeProfileId(actorProfileId),
        createdAt: existingProfile?.createdAt || new Date().toISOString(),
        promoCodeUsed: profileData?.promoCodeUsed || existingProfile?.promoCodeUsed || '',
        requestsLimit: requestsLimit
    });

    if (!auth.defaultProfileId) {
        auth.defaultProfileId = profileId;
    }

    const saved = await saveAdminAuth(auth);
    return toPublicProfile(saved.profiles[profileId], profileId);
}

async function registerProfileFromPromo(profileData, promoCode) {
    const publicProfile = await upsertAdminProfile({
        ...profileData,
        role: 'admin',
        promoCodeUsed: String(promoCode || '').trim().toUpperCase()
    }, MAIN_ADMIN_PROFILE_ID);
    return publicProfile;
}

async function reactivateExpiredProfile(profileId, promoCode, durationMinutes, requestsLimit = null) {
    const auth = await loadAdminAuth();
    const normalizedId = normalizeProfileId(profileId);
    const existingProfile = auth.profiles[normalizedId];

    if (!existingProfile) {
        throw new Error('Профиль не найден');
    }
    if (isMainAdminProfile(existingProfile)) {
        throw new Error('Главный админ не требует повторной активации');
    }

    const updatedProfile = normalizeProfile(normalizedId, {
        ...existingProfile,
        expiresAt: buildExpiresAt(durationMinutes),
        active: true,
        promoCodeUsed: String(promoCode || '').trim().toUpperCase(),
        lastLoginAt: new Date().toISOString(),
        requestsLimit: requestsLimit === null || requestsLimit === undefined || requestsLimit === ''
            ? (existingProfile?.requestsLimit || null)
            : Math.max(1, parseInt(requestsLimit, 10) || 1)
    }, existingProfile);

    auth.profiles[normalizedId] = updatedProfile;
    await saveAdminAuth(auth);
    return toPublicProfile(updatedProfile, normalizedId);
}

async function activateProfileWithPromoCode(profileId, promo) {
    const auth = await loadAdminAuth();
    const normalizedId = normalizeProfileId(profileId);
    const existingProfile = auth.profiles[normalizedId];

    if (!existingProfile) {
        throw new Error('РџСЂРѕС„РёР»СЊ РЅРµ РЅР°Р№РґРµРЅ');
    }
    if (isMainAdminProfile(existingProfile)) {
        throw new Error('Р“Р»Р°РІРЅС‹Р№ Р°РґРјРёРЅ РЅРµ РёСЃРїРѕР»СЊР·СѓРµС‚ РїСЂРѕРјРѕРєРѕРґС‹');
    }

    const promoUpdate = buildProfilePromoActivationUpdate(existingProfile, promo, new Date());
    const updatedProfile = normalizeProfile(normalizedId, {
        ...existingProfile,
        ...promoUpdate
    }, existingProfile);

    auth.profiles[normalizedId] = updatedProfile;
    await saveAdminAuth(auth);
    return toPublicProfile(updatedProfile, normalizedId);
}

async function deleteAdminProfile(profileId) {
    const auth = await loadAdminAuth();
    const normalizedId = normalizeProfileId(profileId);

    if (normalizedId === MAIN_ADMIN_PROFILE_ID) {
        throw new Error('Главный профиль нельзя удалить');
    }
    if (!auth.profiles[normalizedId]) {
        throw new Error('Профиль не найден');
    }

    const ids = Object.keys(auth.profiles || {});
    if (ids.length <= 1) {
        throw new Error('Нельзя удалить последний профиль');
    }

    delete auth.profiles[normalizedId];

    if (normalizeProfileId(auth.defaultProfileId) === normalizedId) {
        auth.defaultProfileId = Object.keys(auth.profiles).sort((a, b) => Number(a) - Number(b))[0] || MAIN_ADMIN_PROFILE_ID;
    }

    await saveAdminAuth(auth);
    return { success: true, defaultProfileId: auth.defaultProfileId };
}

async function verifyAdminCredentials(username, password) {
    const auth = await loadAdminAuth();
    const normalizedUsername = String(username || '').trim();
    const normalizedPassword = String(password || '').trim();

    for (const [profileId, profile] of Object.entries(auth.profiles || {})) {
        if (normalizedUsername === profile.username && normalizedPassword === profile.password) {
            if (profile.active === false) {
                return { success: false, reason: 'inactive', error: 'Профиль отключён' };
            }
            if (isProfileExpired(profile)) {
                return { success: false, reason: 'expired', error: 'Срок действия профиля истёк' };
            }

            profile.lastLoginAt = new Date().toISOString();
            await saveAdminAuth(auth);

            return {
                success: true,
                profileId,
                profileName: profile.name || `Профиль ${profileId}`,
                recoveryEmail: profile.recoveryEmail || '',
                role: isMainAdminProfile(profile) ? 'main_admin' : 'admin',
                isMainAdmin: isMainAdminProfile(profile)
            };
        }
    }

    return { success: false, reason: 'credentials', error: 'Неверный логин или пароль' };
}

async function loadAdminAuthWithDependencies(overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();

    try {
        const response = await hotStateStore.loadJsonObject(AUTH_FILE_KEY, {
            defaultValue: buildDefaultAuthConfig()
        });
        return normalizeAdminAuth(response && response.value);
    } catch (error) {
        log('warn', `⚠️ admin_auth.json not loaded, using defaults: ${error.message}`);
        return buildDefaultAuthConfig();
    }
}

async function saveAdminAuthWithDependencies(config, overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    const normalized = normalizeAdminAuth(config);
    await hotStateStore.saveJsonObject(AUTH_FILE_KEY, normalized);
    return normalized;
}

async function loadAdminAuth() {
    return loadAdminAuthWithDependencies();
}

async function saveAdminAuth(config) {
    return saveAdminAuthWithDependencies(config);
}

module.exports = {
    AUTH_FILE_KEY,
    MAIN_ADMIN_PROFILE_ID,
    normalizeProfileId,
    normalizeAdminAuth,
    buildDefaultAuthConfig,
    loadAdminAuth,
    saveAdminAuth,
    getAllProfileIds,
    getPublicProfiles,
    getProfileById,
    findProfileByUsername,
    findProfileByRecoveryEmail,
    upsertAdminProfile,
    registerProfileFromPromo,
    reactivateExpiredProfile,
    activateProfileWithPromoCode,
    deleteAdminProfile,
    verifyAdminCredentials,
    isMainAdminProfile,
    isProfileExpired,
    buildExpiresAt,
    buildProfilePromoActivationUpdate,
    toPublicProfile,
    __testOnly: {
        loadAdminAuthWithDependencies,
        saveAdminAuthWithDependencies
    }
};
