/**
 * Модуль настройки callback сервера VK
 */

const { log } = require('../utils/logger');
const { saveBotConfig, getCommunityConfig, getVkToken, getVkGroupId } = require('./config');
const {
    getCallbackConfirmationCode,
    getCallbackServers,
    addCallbackServer,
    deleteCallbackServer,
    setCallbackServer,
    setCallbackSettings
} = require('./vk-api');

/**
 * Генерация случайного пароля
 */
function generateRandomPassword(length = 16) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

/**
 * Задержка
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function isServerConfirmed(server) {
    const status = server?.status;
    return status === 1 || status === true || status === 'ok' || status === 'confirmed' || status === 'active';
}

async function waitForServerConfirmation(groupId, serverId, token, maxAttempts = 12, delayMs = 3000) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        await sleep(delayMs);
        let servers = [];
        try {
            servers = await getCallbackServers(groupId, token);
        } catch (e) {
            log('warn', `⚠️ Не удалось получить callback servers на попытке ${attempt}: ${e.message}`);
            continue;
        }

        const server = servers.find(item => String(item.id) === String(serverId));
        log('info', `⏳ Проверка подтверждения сервера ${serverId}: попытка ${attempt}/${maxAttempts}, status=${server?.status ?? 'unknown'}`);

        if (server && isServerConfirmed(server)) {
            return server;
        }
    }

    throw new Error('VK не подтвердил callback-сервер вовремя. Проверьте, что URL функции доступен и сообщество может получить confirmation-код.');
}

/**
 * Автоматически настроить callback сервер
 */
async function setupVkCallbackServer(providedGroupId = null, communityId = null, profileId = '1') {
    const { loadBotConfig } = require('./config');
    await loadBotConfig(profileId);

    const targetCommunityId = communityId;
    const config = await getCommunityConfig(targetCommunityId, profileId);
    
    if (!config) {
        throw new Error('Сообщество не найдено: ' + targetCommunityId);
    }

    let token = config?.vk_tokens?.[0] || config?.vk_token;
    let groupId = providedGroupId || config?.vk_group_id;

    if (!groupId || !token) {
        throw new Error('VK Group ID или VK Token не настроены для сообщества ' + targetCommunityId);
    }

    const url = process.env.APP_URL;
    if (!url) throw new Error('APP_URL не задан');

    // ✅ ВСЕГДА генерируем новый secret_key если он пустой или не задан
    const secretKey = (config?.secret_key && config.secret_key.trim()) 
        ? config.secret_key 
        : generateRandomPassword(16);

    log('info', `🛠️ Настройка сервера для сообщества ${targetCommunityId}, группа ${groupId}, URL: ${url}`);
    log('info', `🔑 Secret key: '${secretKey}' (длина: ${secretKey.length}, из конфига: ${!!config?.secret_key})`);
    log('info', `📋 Полный конфиг: ${JSON.stringify({ has_vk_tokens: !!(config?.vk_tokens?.length || config?.vk_token), vk_group_id: config?.vk_group_id, group_name: config?.group_name })}`);

    // 1. Получаем список существующих серверов
    let servers = [];
    try {
        servers = await getCallbackServers(groupId, token);
        log('info', `📋 Найдено серверов: ${servers.length}`);
    } catch (err) {
        log('error', 'Ошибка получения списка серверов:', err.message);
    }

    // 2. ✅ Ищем и удаляем ВСЕ серверы PAPA_BOT* перед созданием нового
    const baseTitle = 'PAPA_BOT';
    const papaServers = servers.filter(s => s.title.startsWith(baseTitle));
    
    if (papaServers.length > 0) {
        log('info', `🗑️ Удаляем ${papaServers.length} старых серверов PAPA_BOT...`);
        for (const server of papaServers) {
            try {
                await deleteCallbackServer(groupId, server.id, token);
                log('info', `   ✅ Удалён: ${server.title} (ID: ${server.id})`);
            } catch (e) {
                log('warn', `   ⚠️ Не удалось удалить ${server.title}: ${e.message}`);
            }
        }
    }

    // 3. Создаём НОВЫЙ сервер с именем PAPA_BOT (без суффиксов)
    const title = baseTitle;
    let serverId = null;
    try {
        serverId = await addCallbackServer(groupId, url, title, secretKey, token);
        log('info', `✅ Сервер создан: ${title} (ID: ${serverId})`);
    } catch (err) {
        throw new Error(`Ошибка создания сервера: ${err.message}`);
    }

    // 4. Получаем confirmation code и сохраняем его ДО запуска подтверждения
    const confirmationCode = await getCallbackConfirmationCode(groupId, token);
    if (!confirmationCode) throw new Error('Не удалось получить confirmation code');

    await saveBotConfig({
        vk_tokens: config.vk_tokens || [token],
        vk_token: token,
        confirmation_token: confirmationCode,
        secret_key: secretKey,
        vk_group_id: groupId,
        user_token: config.user_token,
        group_name: config.group_name || `Сообщество ${groupId}`
    }, targetCommunityId, profileId);

    // 5. Явно запускаем подтверждение сервера и ждём статуса от VK
    log('info', '⏳ Запускаем подтверждение callback-сервера через VK API...');
    const confirmStarted = await setCallbackServer(groupId, serverId, token, url, title, secretKey);
    if (!confirmStarted) {
        throw new Error('Не удалось запустить подтверждение callback-сервера через groups.editCallbackServer');
    }

    log('info', '⏳ Ожидаем, пока VK подтвердит сервер...');
    await waitForServerConfirmation(groupId, serverId, token);
    log('info', `✅ VK подтвердил callback-сервер ${serverId}`);

    // 6. Настраиваем события только после подтверждения
    const events = {
        message_new: 1,
        message_reply: 1,
        message_event: 1,
        wall_reply_new: 1,
        wall_reply_edit: 1,
        wall_reply_delete: 1,
        photo_comment_new: 1,
        photo_comment_edit: 1,
        photo_comment_delete: 1,
        video_comment_new: 1,
        video_comment_edit: 1,
        video_comment_delete: 1,
        wall_post_new: 1,
        wall_repost: 1,
        like_add: 1,
        group_join: 1,
        group_leave: 1
    };
    
    try {
        await setCallbackSettings(groupId, serverId, events, token);
    } catch (e) {
        log('warn', '⚠️ Ошибка настройки событий:', e.message);
    }

    // 7. Финально сохраняем настройки ещё раз для гарантии актуальности
    const fullConfig = await saveBotConfig({
        vk_tokens: config.vk_tokens || [token],
        vk_token: token,
        confirmation_token: confirmationCode,
        secret_key: secretKey,
        vk_group_id: groupId,
        user_token: config.user_token,
        group_name: config.group_name || `Сообщество ${groupId}`  // ✅ Имя если не задано
    }, targetCommunityId, profileId);

    log('info', `✅ ✅ ✅ АВТОНАСТРОЙКА УСПЕШНА для сообщества ${targetCommunityId}:`);
    log('info', `   • Сервер: ${title} (ID: ${serverId})`);
    log('info', `   • Confirmation code: ${confirmationCode}`);
    log('info', `   • Secret key: ${secretKey}`);
    log('info', `   • Group ID: ${groupId}`);

    return {
        success: true,
        confirmation_code: confirmationCode,
        secret_key: secretKey,
        server_id: serverId,
        community_id: targetCommunityId,
        server_name: title
    };
}

module.exports = {
    setupVkCallbackServer,
    generateRandomPassword
};
