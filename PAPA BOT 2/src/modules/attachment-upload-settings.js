const { createHotStateStore } = require('./hot-state-store');

const ATTACHMENT_UPLOAD_SETTINGS_FILE = 'attachment_upload_settings.json';
const TYPES = ['image', 'document', 'video'];
const SOURCES = ['user', 'community'];
const USER_VIDEO_PRIVACY_VALUES = ['all', 'friends', 'friends_of_friends', 'nobody'];
const DEFAULT_GLOBAL_SOURCES = Object.freeze({
    image: 'community',
    document: 'user',
    video: 'community'
});
const DEFAULT_GLOBAL_USER_VIDEO_PRIVACY = 'all';

function clone(value) {
    return JSON.parse(JSON.stringify(value));
}

function normalizeSource(value, fallback) {
    return SOURCES.includes(String(value || '').trim().toLowerCase())
        ? String(value).trim().toLowerCase()
        : fallback;
}

function normalizeSources(raw, fallback = DEFAULT_GLOBAL_SOURCES) {
    const source = raw && typeof raw === 'object' ? raw : {};
    return TYPES.reduce((result, type) => {
        result[type] = normalizeSource(source[type], fallback[type] || DEFAULT_GLOBAL_SOURCES[type]);
        return result;
    }, {});
}

function normalizeOverrides(raw) {
    const source = raw && typeof raw === 'object' ? raw : {};
    return TYPES.reduce((result, type) => {
        const value = String(source[type] || '').trim().toLowerCase();
        if (SOURCES.includes(value)) result[type] = value;
        return result;
    }, {});
}

function normalizeUserVideoPrivacy(value, fallback = DEFAULT_GLOBAL_USER_VIDEO_PRIVACY) {
    const normalized = String(value || '').trim().toLowerCase();
    return USER_VIDEO_PRIVACY_VALUES.includes(normalized) ? normalized : fallback;
}

function normalizeUserVideoPrivacyOverrides(raw) {
    const source = raw && typeof raw === 'object' ? raw : {};
    const result = {};
    Object.entries(source).forEach(([profileId, value]) => {
        const id = String(profileId || '').trim();
        const normalized = String(value || '').trim().toLowerCase();
        if (id && USER_VIDEO_PRIVACY_VALUES.includes(normalized)) result[id] = normalized;
    });
    return result;
}

function normalizeState(raw) {
    const source = raw && typeof raw === 'object' ? raw : {};
    const profiles = source.profileOverrides && typeof source.profileOverrides === 'object'
        ? source.profileOverrides
        : {};
    const profileOverrides = {};
    Object.entries(profiles).forEach(([profileId, overrides]) => {
        const id = String(profileId || '').trim();
        const normalized = normalizeOverrides(overrides);
        if (id && Object.keys(normalized).length) profileOverrides[id] = normalized;
    });
    return {
        global: normalizeSources(source.global, DEFAULT_GLOBAL_SOURCES),
        profileOverrides,
        globalUserVideoPrivacy: normalizeUserVideoPrivacy(source.globalUserVideoPrivacy),
        profileUserVideoPrivacyOverrides: normalizeUserVideoPrivacyOverrides(source.profileUserVideoPrivacyOverrides),
        updatedAt: String(source.updatedAt || ''),
        updatedBy: String(source.updatedBy || '')
    };
}

async function loadAttachmentUploadSettingsWithDependencies(overrides = {}) {
    const store = overrides.hotStateStore || createHotStateStore();
    try {
        const response = await store.loadJsonObject(ATTACHMENT_UPLOAD_SETTINGS_FILE, {
            defaultValue: { global: clone(DEFAULT_GLOBAL_SOURCES), profileOverrides: {}, globalUserVideoPrivacy: DEFAULT_GLOBAL_USER_VIDEO_PRIVACY, profileUserVideoPrivacyOverrides: {} },
            preferS3Backup: true
        });
        return normalizeState(response?.value);
    } catch (_) {
        // Settings are optional infrastructure state. A transient storage error must
        // never make the Profile page or an attachment delivery unavailable.
        return normalizeState({ global: clone(DEFAULT_GLOBAL_SOURCES), profileOverrides: {}, globalUserVideoPrivacy: DEFAULT_GLOBAL_USER_VIDEO_PRIVACY, profileUserVideoPrivacyOverrides: {} });
    }
}

async function saveAttachmentUploadSettingsWithDependencies(next, overrides = {}) {
    const store = overrides.hotStateStore || createHotStateStore();
    const normalized = normalizeState(next);
    await store.saveJsonObject(ATTACHMENT_UPLOAD_SETTINGS_FILE, normalized);
    return normalized;
}

function publicSettings(state, profileId = '') {
    const id = String(profileId || '').trim();
    const overrides = normalizeOverrides(state.profileOverrides?.[id]);
    const effective = TYPES.reduce((result, type) => {
        result[type] = overrides[type] || state.global[type];
        return result;
    }, {});
    const userVideoPrivacyOverride = state.profileUserVideoPrivacyOverrides?.[id] || '';
    return {
        global: clone(state.global),
        overrides,
        effective,
        userVideoPrivacy: {
            global: state.globalUserVideoPrivacy,
            override: userVideoPrivacyOverride,
            effective: userVideoPrivacyOverride || state.globalUserVideoPrivacy
        },
        updatedAt: state.updatedAt,
        updatedBy: state.updatedBy
    };
}

async function getAttachmentUploadSettings(profileId = '', overrides = {}) {
    return publicSettings(await loadAttachmentUploadSettingsWithDependencies(overrides), profileId);
}

async function saveGlobalAttachmentUploadSettings(values, options = {}, overrides = {}) {
    const state = await loadAttachmentUploadSettingsWithDependencies(overrides);
    state.global = normalizeSources(values, state.global);
    state.globalUserVideoPrivacy = normalizeUserVideoPrivacy(values?.userVideoPrivacy, state.globalUserVideoPrivacy);
    state.profileOverrides = {};
    state.profileUserVideoPrivacyOverrides = {};
    state.updatedAt = new Date().toISOString();
    state.updatedBy = String(options.updatedBy || 'main-admin');
    const saved = await saveAttachmentUploadSettingsWithDependencies(state, overrides);
    return publicSettings(saved);
}

async function saveProfileAttachmentUploadOverrides(profileId, values, options = {}, overrides = {}) {
    const id = String(profileId || '').trim();
    if (!id) throw new Error('Profile ID is required');
    const state = await loadAttachmentUploadSettingsWithDependencies(overrides);
    const next = normalizeOverrides(values);
    if (Object.keys(next).length) state.profileOverrides[id] = next;
    else delete state.profileOverrides[id];
    const videoPrivacy = String(values?.userVideoPrivacy || '').trim().toLowerCase();
    if (USER_VIDEO_PRIVACY_VALUES.includes(videoPrivacy)) state.profileUserVideoPrivacyOverrides[id] = videoPrivacy;
    else delete state.profileUserVideoPrivacyOverrides[id];
    state.updatedAt = new Date().toISOString();
    state.updatedBy = String(options.updatedBy || id);
    const saved = await saveAttachmentUploadSettingsWithDependencies(state, overrides);
    return publicSettings(saved, id);
}

async function resolveAttachmentUploadSource(profileId, type, overrides = {}) {
    const normalizedType = TYPES.includes(type) ? type : 'document';
    const settings = await getAttachmentUploadSettings(profileId, overrides);
    return { source: settings.effective[normalizedType], isProfileOverride: !!settings.overrides[normalizedType], settings };
}

module.exports = {
    ATTACHMENT_UPLOAD_SETTINGS_FILE,
    TYPES,
    SOURCES,
    USER_VIDEO_PRIVACY_VALUES,
    DEFAULT_GLOBAL_SOURCES,
    DEFAULT_GLOBAL_USER_VIDEO_PRIVACY,
    normalizeSources,
    normalizeOverrides,
    normalizeUserVideoPrivacy,
    normalizeState,
    getAttachmentUploadSettings,
    saveGlobalAttachmentUploadSettings,
    saveProfileAttachmentUploadOverrides,
    resolveAttachmentUploadSource,
    __testOnly: { loadAttachmentUploadSettingsWithDependencies, saveAttachmentUploadSettingsWithDependencies, publicSettings }
};
