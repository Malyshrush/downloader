/**
 * Модуль конфигурации бота (мульти-сообщества + профили админов)
 */

const { PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getS3Client, getBucketName } = require('./storage');
const { createHotStateStore } = require('./hot-state-store');
const { getAllProfileIds, normalizeProfileId } = require('./admin-profiles');
const { log } = require('../utils/logger');

const BOT_CONFIGS = {};
const LAST_USED_PROFILE = { id: '1' };
const hotStateStore = createHotStateStore();

function createEmptyConfig() {
    return {
        communities: {},
        active_community: null
    };
}

function normalizeLoadedConfig(parsed) {
    if (parsed && parsed.communities) {
        return parsed;
    }

    if (parsed && typeof parsed === 'object' && Object.keys(parsed).length > 0) {
        return {
            communities: { default: parsed },
            active_community: 'default'
        };
    }

    return createEmptyConfig();
}

function getBotConfigKey(profileId) {
    return `bot_config_profile_${normalizeProfileId(profileId)}.json`;
}

function getLegacyBotConfigKey() {
    return 'bot_config.json';
}

function getCachedConfig(profileId = '1') {
    const pid = normalizeProfileId(profileId);
    return BOT_CONFIGS[pid] || createEmptyConfig();
}

function normalizeVkGroupId(vkGroupId) {
    const normalized = String(vkGroupId || '').trim();
    return normalized || null;
}

function searchCommunityInConfig(fullConfig, communityId) {
    if (!fullConfig?.communities || !communityId) return null;

    const requested = String(communityId).trim();
    if (!requested) return null;

    if (fullConfig.communities[requested]) {
        return {
            communityId: requested,
            config: fullConfig.communities[requested]
        };
    }

    for (const [internalId, config] of Object.entries(fullConfig.communities)) {
        if (config?.vk_group_id?.toString() === requested) {
            return {
                communityId: internalId,
                config
            };
        }
    }

    return null;
}

async function loadBotConfig(profileId = '1') {
    const pid = normalizeProfileId(profileId);
    const s3Client = getS3Client();
    const bucket = getBucketName();
    const profileKey = getBotConfigKey(pid);

    LAST_USED_PROFILE.id = pid;

    try {
        log('debug', `📥 loadBotConfig: Loading ${profileKey} from S3...`);
        const result = await hotStateStore.loadJsonObject(profileKey, {
            defaultValue: createEmptyConfig(),
            legacyKeys: pid === '1' ? [getLegacyBotConfigKey()] : []
        });
        BOT_CONFIGS[pid] = normalizeLoadedConfig(result.value);
    } catch (error) {
        if (pid === '1') {
            try {
                log('debug', `📥 loadBotConfig: Fallback to legacy ${getLegacyBotConfigKey()}...`);
                const legacyResponse = await s3Client.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: getLegacyBotConfigKey()
                }));
                const legacyData = await legacyResponse.Body.transformToString();
                BOT_CONFIGS[pid] = normalizeLoadedConfig(JSON.parse(legacyData));
            } catch (legacyError) {
                log('warn', `⚠️ loadBotConfig: legacy fallback failed: ${legacyError.message}`);
                BOT_CONFIGS[pid] = createEmptyConfig();
            }
        } else {
            log('warn', `⚠️ loadBotConfig failed for ${profileKey}: ${error.message}`);
            BOT_CONFIGS[pid] = createEmptyConfig();
        }
    }

    const loaded = BOT_CONFIGS[pid] || createEmptyConfig();
    const communityKeys = Object.keys(loaded.communities || {});
    log('info', `✅ Bot config loaded for profile ${pid}, communities: ${communityKeys.join(', ') || 'none'}`);
    return loaded;
}

async function resolveCommunityContext(communityId = null, profileId = null) {
    const requestedProfileId = profileId ? normalizeProfileId(profileId) : null;

    if (requestedProfileId) {
        const fullConfig = await loadBotConfig(requestedProfileId);
        if (!communityId) {
            const activeId = fullConfig.active_community;
            return activeId && fullConfig.communities[activeId]
                ? { profileId: requestedProfileId, communityId: activeId, config: fullConfig.communities[activeId], fullConfig }
                : null;
        }

        const match = searchCommunityInConfig(fullConfig, communityId);
        return match ? { profileId: requestedProfileId, ...match, fullConfig } : null;
    }

    if (!communityId) {
        const pid = LAST_USED_PROFILE.id || '1';
        const fullConfig = await loadBotConfig(pid);
        const activeId = fullConfig.active_community;
        return activeId && fullConfig.communities[activeId]
            ? { profileId: pid, communityId: activeId, config: fullConfig.communities[activeId], fullConfig }
            : null;
    }

    const profileIds = await getAllProfileIds();
    for (const pid of profileIds) {
        const fullConfig = await loadBotConfig(pid);
        const match = searchCommunityInConfig(fullConfig, communityId);
        if (match) {
            return { profileId: pid, ...match, fullConfig };
        }
    }

    return null;
}

async function findVkGroupUsage(vkGroupId, excludeProfileId = null, excludeCommunityId = null) {
    const normalizedVkGroupId = normalizeVkGroupId(vkGroupId);
    if (!normalizedVkGroupId) return null;

    const previousProfileId = LAST_USED_PROFILE.id;
    const profileIds = await getAllProfileIds();

    try {
        for (const pid of profileIds) {
            const fullConfig = await loadBotConfig(pid);
            for (const [communityId, config] of Object.entries(fullConfig.communities || {})) {
                if (normalizeVkGroupId(config?.vk_group_id) !== normalizedVkGroupId) continue;
                if (normalizeProfileId(pid) === normalizeProfileId(excludeProfileId) && String(communityId) === String(excludeCommunityId)) {
                    continue;
                }
                return {
                    profileId: normalizeProfileId(pid),
                    communityId,
                    vkGroupId: normalizedVkGroupId,
                    groupName: config?.group_name || `Сообщество ${normalizedVkGroupId}`
                };
            }
        }
        return null;
    } finally {
        LAST_USED_PROFILE.id = previousProfileId;
    }
}

async function ensureVkGroupIdNotDuplicated(vkGroupId, profileId, communityId) {
    const normalizedVkGroupId = normalizeVkGroupId(vkGroupId);
    if (!normalizedVkGroupId) return;

    const existingUsage = await findVkGroupUsage(normalizedVkGroupId, profileId, communityId);
    if (existingUsage) {
        throw new Error(
            `vk_group_id ${normalizedVkGroupId} уже используется в профиле ${existingUsage.profileId} ` +
            `(сообщество ${existingUsage.communityId}${existingUsage.groupName ? `, ${existingUsage.groupName}` : ''})`
        );
    }
}

async function validateCommunitiesForProfile(fullConfig, profileId) {
    const seenGroupIds = new Map();

    for (const [communityId, config] of Object.entries(fullConfig.communities || {})) {
        const normalizedVkGroupId = normalizeVkGroupId(config?.vk_group_id);
        if (!normalizedVkGroupId) continue;

        if (seenGroupIds.has(normalizedVkGroupId)) {
            throw new Error(
                `vk_group_id ${normalizedVkGroupId} повторяется внутри профиля ` +
                `в сообществах ${seenGroupIds.get(normalizedVkGroupId)} и ${communityId}`
            );
        }
        seenGroupIds.set(normalizedVkGroupId, communityId);

        await ensureVkGroupIdNotDuplicated(normalizedVkGroupId, profileId, communityId);
    }
}

async function saveFullConfig(fullConfig, profileId = '1') {
    const pid = normalizeProfileId(profileId);
    return saveFullConfigWithDependencies(fullConfig, pid);
}

async function saveFullConfigWithDependencies(fullConfig, profileId = '1', overrides = {}) {
    const pid = normalizeProfileId(profileId);
    const targetStore = overrides.hotStateStore || hotStateStore;
    await targetStore.saveJsonObject(getBotConfigKey(pid), fullConfig);
    BOT_CONFIGS[pid] = fullConfig;
    LAST_USED_PROFILE.id = pid;
    return fullConfig;
}

async function saveBotConfig(config, communityId = null, profileId = '1') {
    const pid = normalizeProfileId(profileId);
    const fullConfig = await loadBotConfig(pid);

    if (!fullConfig.communities) fullConfig.communities = {};

    const targetId = String(communityId || config.vk_group_id || 'default');
    const existingConfig = fullConfig.communities[targetId] || {};
    const targetVkGroupId = config.vk_group_id ? parseInt(config.vk_group_id, 10) : existingConfig.vk_group_id;

    await ensureVkGroupIdNotDuplicated(targetVkGroupId, pid, targetId);

    fullConfig.communities[targetId] = {
        vk_tokens: config.vk_tokens || existingConfig.vk_tokens || (config.vk_token ? [config.vk_token] : []),
        vk_token: config.vk_token !== undefined ? config.vk_token : existingConfig.vk_token,
        confirmation_token: config.confirmation_token !== undefined ? config.confirmation_token : existingConfig.confirmation_token,
        secret_key: config.secret_key !== undefined ? config.secret_key : existingConfig.secret_key,
        vk_group_id: targetVkGroupId,
        user_token: config.user_token !== undefined ? config.user_token : existingConfig.user_token,
        group_name: config.group_name !== undefined ? config.group_name : existingConfig.group_name,
        updated_at: new Date().toISOString()
    };

    if (!fullConfig.active_community) {
        fullConfig.active_community = targetId;
    }

    await saveFullConfig(fullConfig, pid);
    log('info', `✅ Bot config saved for community ${targetId} in profile ${pid}`);
    return fullConfig;
}

function getActiveCommunityId(profileId = null) {
    const pid = normalizeProfileId(profileId || LAST_USED_PROFILE.id || '1');
    const active = getCachedConfig(pid).active_community;
    return (active && active !== 'default') ? active : null;
}

function setActiveCommunity(communityId, profileId = null) {
    const pid = normalizeProfileId(profileId || LAST_USED_PROFILE.id || '1');
    const fullConfig = getCachedConfig(pid);
    if (fullConfig?.communities?.[communityId]) {
        fullConfig.active_community = communityId;
        BOT_CONFIGS[pid] = fullConfig;
        LAST_USED_PROFILE.id = pid;
        log('info', `🔄 Active community switched to: ${communityId} (profile ${pid})`);
        return true;
    }
    return false;
}

async function getCommunityConfig(communityId = null, profileId = null) {
    const resolved = await resolveCommunityContext(communityId, profileId);
    return resolved?.config || null;
}

function getAllCommunityIds(profileId = null) {
    const pid = normalizeProfileId(profileId || LAST_USED_PROFILE.id || '1');
    return Object.keys(getCachedConfig(pid).communities || {});
}

async function getVkToken(index = 0, communityId = null, profileId = null) {
    const config = await getCommunityConfig(communityId, profileId);
    if (!config) {
        return process.env.VK_TOKEN || null;
    }

    if (Array.isArray(config.vk_tokens) && index >= 0 && index < config.vk_tokens.length) {
        return config.vk_tokens[index];
    }

    return config.vk_token || null;
}

async function getAllVkTokens(communityId = null, profileId = null) {
    const config = await getCommunityConfig(communityId, profileId);
    if (!config) return [process.env.VK_TOKEN].filter(Boolean);
    if (Array.isArray(config.vk_tokens) && config.vk_tokens.length > 0) return config.vk_tokens;
    return config.vk_token ? [config.vk_token] : [];
}

async function getVkTokensCount(communityId = null, profileId = null) {
    const tokens = await getAllVkTokens(communityId, profileId);
    return tokens.length;
}

function getVkGroupId(communityId = null, profileId = null) {
    const pid = normalizeProfileId(profileId || LAST_USED_PROFILE.id || '1');
    const fullConfig = getCachedConfig(pid);
    const targetCommunityId = communityId || fullConfig.active_community;
    const match = targetCommunityId ? searchCommunityInConfig(fullConfig, targetCommunityId) : null;
    return match?.config?.vk_group_id || parseInt(process.env.VK_GROUP_ID, 10) || null;
}

function getConfirmationToken(communityId = null, profileId = null) {
    const pid = normalizeProfileId(profileId || LAST_USED_PROFILE.id || '1');
    const fullConfig = getCachedConfig(pid);
    const targetCommunityId = communityId || fullConfig.active_community;
    const match = targetCommunityId ? searchCommunityInConfig(fullConfig, targetCommunityId) : null;
    return match?.config?.confirmation_token || process.env.CONFIRMATION_TOKEN;
}

function getSecretKey(communityId = null, profileId = null) {
    const pid = normalizeProfileId(profileId || LAST_USED_PROFILE.id || '1');
    const fullConfig = getCachedConfig(pid);
    const targetCommunityId = communityId || fullConfig.active_community;
    const match = targetCommunityId ? searchCommunityInConfig(fullConfig, targetCommunityId) : null;
    return match?.config?.secret_key || process.env.SECRET_KEY;
}

async function getUserToken(communityId = null, profileId = null) {
    const config = await getCommunityConfig(communityId, profileId);
    
    // ✅ ВАЖНО: Используем ТОЛЬКО user_token из настроек сообщества, НЕ из .env
    let userToken = config?.user_token || null;
    
    // ✅ ПРОВЕРКА: Если user_token совпадает с Community Token, то считаем что User Token не настроен
    const communityToken = config?.vk_tokens?.[0] || config?.vk_token;
    if (userToken && userToken === communityToken) {
        log('warn', '⚠️ user_token совпадает с Community Token - User Token не настроен правильно');
        userToken = null;
    }
    
    log('debug', '🔑 getUserToken: communityId=' + communityId + ', profileId=' + profileId + ', config_user_token=' + (config?.user_token ? 'SET (' + String(config.user_token).substring(0, 10) + '...)' : 'NOT SET') + ', result=' + (userToken ? 'SET (' + String(userToken).substring(0, 10) + '...)' : 'NONE'));
    return userToken;
}

function getFullConfig(profileId = null) {
    const pid = normalizeProfileId(profileId || LAST_USED_PROFILE.id || '1');
    return getCachedConfig(pid);
}

async function saveAllCommunities(config, profileId = '1') {
    const pid = normalizeProfileId(profileId);
    await validateCommunitiesForProfile(config, pid);
    await saveFullConfig(config, pid);
    log('info', `✅ All communities config saved for profile ${pid}`);
}

async function deleteCommunity(communityId, profileId = '1') {
    const pid = normalizeProfileId(profileId);

    if (!communityId || communityId === 'default') {
        throw new Error('Cannot delete default community');
    }

    const fullConfig = await loadBotConfig(pid);
    if (!fullConfig.communities[communityId]) {
        throw new Error('Community not found');
    }

    delete fullConfig.communities[communityId];

    if (fullConfig.active_community === communityId) {
        const keys = Object.keys(fullConfig.communities);
        fullConfig.active_community = keys[0] || null;
    }

    await saveFullConfig(fullConfig, pid);
    log('info', `✅ Community deleted: ${communityId} (profile ${pid})`);

    return { success: true, active: fullConfig.active_community };
}

module.exports = {
    loadBotConfig,
    saveBotConfig,
    getActiveCommunityId,
    setActiveCommunity,
    getCommunityConfig,
    getAllCommunityIds,
    getVkToken,
    getAllVkTokens,
    getVkTokensCount,
    getVkGroupId,
    getConfirmationToken,
    getSecretKey,
    getUserToken,
    getFullConfig,
    saveAllCommunities,
    deleteCommunity,
    resolveCommunityContext,
    getBotConfigKey,
    findVkGroupUsage,
    ensureVkGroupIdNotDuplicated,
    __testOnly: {
        saveFullConfigWithDependencies
    }
};
