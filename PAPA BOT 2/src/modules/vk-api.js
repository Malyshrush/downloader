/**
 * Модуль работы с VK API (базовые запросы)
 */

const axios = require('axios');
const { log } = require('../utils/logger');

const VK_API_VERSION = '5.199';
const VK_API_BASE = 'https://api.vk.com/method';

/**
 * Выполнить GET запрос к VK API
 */
async function vkGet(method, params) {
    try {
        const response = await axios.get(`${VK_API_BASE}/${method}`, {
            params: { ...params, v: VK_API_VERSION }
        });
        
        if (response.data.error) {
            log('warn', `⚠️ VK API error [${method}]:`, response.data.error.error_msg);
            return { error: response.data.error };
        }
        
        return { response: response.data.response };
    } catch (error) {
        log('error', `❌ VK API network error [${method}]:`, error.message);
        return { error: { error_msg: error.message } };
    }
}

/**
 * Выполнить POST запрос к VK API
 */
async function vkPost(method, params) {
    try {
        const response = await axios.post(`${VK_API_BASE}/${method}`, null, {
            params: { ...params, v: VK_API_VERSION }
        });
        
        if (response.data.error) {
            log('warn', `⚠️ VK API error [${method}]:`, response.data.error.error_msg);
            return { error: response.data.error };
        }
        
        return { response: response.data.response };
    } catch (error) {
        log('error', `❌ VK API network error [${method}]:`, error.message);
        return { error: { error_msg: error.message } };
    }
}

/**
 * Отправить сообщение пользователю
 */
async function sendMessage(userId, text, keyboard, groupId, attachments, accessToken) {
    const absGroupId = groupId ? Math.abs(parseInt(groupId)) : null;
    
    const params = {
        user_id: userId,
        message: text,
        random_id: Math.floor(Math.random() * 1e7),
        access_token: accessToken
    };

    // ✅ Обязательно указываем group_id чтобы ответ был от правильного сообщества
    if (absGroupId) {
        params.group_id = absGroupId;
    }

    if (keyboard) {
        params.keyboard = JSON.stringify(keyboard);
    }

    if (attachments && attachments.length > 0) {
        params.attachment = attachments.filter(a => a && a.trim()).join(',');
    }

    log('debug', `📤 messages.send: user_id=${params.user_id}, group_id=${params.group_id || 'NOT SET'}, token_start=${accessToken?.substring(0, 10)}...`);

    const response = await vkPost('messages.send', params);
    
    if (response.error) {
        log('error', `❌ messages.send error: ${response.error.error_msg} (code: ${response.error.error_code})`);
    }
    
    return response;
}

/**
 * Отправить комментарий к посту
 */
async function sendComment(ownerId, postId, text, replyTo, attachments, accessToken) {
    const params = {
        owner_id: ownerId,
        post_id: postId,
        message: text,
        access_token: accessToken
    };

    if (replyTo) {
        params.reply_to_comment = replyTo.toString();
    }

    if (attachments && attachments.length > 0) {
        params.attachments = attachments.filter(a => a && a.trim()).join(',');
    }

    log('debug', `📤 wall.createComment request: owner_id=${params.owner_id}, post_id=${params.post_id}, reply_to=${params.reply_to_comment || '-'}, attachments=${params.attachments || '-'}`);

    const response = await vkPost('wall.createComment', params);
    if (response?.error) {
        log('error', `❌ wall.createComment error: ${response.error.error_msg} (code: ${response.error.error_code})`);
    } else {
        log('debug', `✅ wall.createComment response: ${JSON.stringify(response.response || response)}`);
    }
    return response;
}

/**
 * Получить имя пользователя VK
 */
async function getUserName(userId, accessToken) {
    const response = await vkGet('users.get', {
        user_ids: userId,
        access_token: accessToken
    });

    if (response.error || !response.response?.length) {
        return null;
    }

    const user = response.response[0];
    return `${user.first_name} ${user.last_name}`;
}

/**
 * Получить ID группы по токену
 */
async function getGroupIdFromToken(token) {
    const response = await vkGet('groups.getById', {
        access_token: token
    });

    if (response.error || !response.response?.length) {
        throw new Error(response.error?.error_msg || 'Не удалось получить group_id');
    }

    return -response.response[0].id;
}

/**
 * Получить confirmation code от VK
 */
async function getCallbackConfirmationCode(groupId, accessToken) {
    const response = await vkGet('groups.getCallbackConfirmationCode', {
        group_id: Math.abs(parseInt(groupId)),
        access_token: accessToken
    });

    if (response.error) {
        log('error', '❌ VK API error getting confirmation code:', response.error);
        return null;
    }

    return response.response?.code || null;
}

/**
 * Проверить валидность токена
 */
async function checkToken(token) {
    const response = await vkGet('users.get', {
        access_token: token
    });

    return {
        valid: !response.error,
        user: response.response?.[0],
        error: response.error?.error_msg || null
    };
}

/**
 * Получить маску прав токена
 */
async function getTokenPermissions(accessToken) {
    const response = await vkGet('account.getAppPermissions', {
        access_token: accessToken
    });

    if (response.error) {
        return null;
    }

    const mask = response.response;
    
    return {
        mask,
        permissions: {
            messages: (mask & 4096) !== 0,
            docs: (mask & 131072) !== 0,
            wall: (mask & 8192) !== 0,
            video: (mask & 128) !== 0,
            photos: (mask & 4) !== 0,
            groups: (mask & 2) !== 0,
            friends: (mask & 2) !== 0,
            status: (mask & 1024) !== 0,
            notes: (mask & 2048) !== 0,
            pages: (mask & 16384) !== 0,
            menu: (mask & 524288) !== 0
        }
    };
}

/**
 * Получить существующие callback серверы
 */
async function getCallbackServers(groupId, accessToken) {
    const response = await vkGet('groups.getCallbackServers', {
        group_id: groupId,
        access_token: accessToken
    });

    if (response.error) {
        log('error', 'Ошибка получения списка серверов:', response.error.error_msg);
        return [];
    }

    return response.response?.items || [];
}

/**
 * Добавить callback сервер
 */
async function addCallbackServer(groupId, url, title, secretKey, accessToken) {
    const response = await vkPost('groups.addCallbackServer', {
        group_id: groupId,
        url: url,
        title: title,
        secret_key: secretKey,
        access_token: accessToken
    });

    if (response.error) {
        throw new Error(response.error.error_msg);
    }

    return response.response?.server_id;
}

/**
 * Удалить callback сервер
 */
async function deleteCallbackServer(groupId, serverId, accessToken) {
    const response = await vkPost('groups.deleteCallbackServer', {
        group_id: groupId,
        server_id: serverId,
        access_token: accessToken
    });

    if (response.error) {
        throw new Error(response.error.error_msg);
    }

    return response.response?.success || true;
}

/**
 * Установить callback сервер как активный
 */
async function setCallbackServer(groupId, serverId, accessToken, url, title, secretKey) {
    const response = await vkPost('groups.editCallbackServer', {
        group_id: groupId,
        server_id: serverId,
        url: url,
        title: title,
        secret_key: secretKey,
        confirm: 1,
        access_token: accessToken
    });

    return !response.error;
}

/**
 * Настроить callback события
 */
async function setCallbackSettings(groupId, serverId, events, accessToken) {
    const params = {
        group_id: groupId,
        server_id: serverId,
        ...events,
        access_token: accessToken
    };

    const response = await vkPost('groups.setCallbackSettings', params);
    return !response.error;
}

async function approveGroupJoinRequest(groupId, userId, accessToken) {
    const response = await vkPost('groups.approveRequest', {
        group_id: Math.abs(parseInt(groupId, 10)),
        user_id: parseInt(userId, 10),
        access_token: accessToken
    });
    if (response.error) {
        throw new Error(response.error.error_msg || 'Не удалось одобрить заявку в сообщество');
    }
    return response.response;
}

async function removeUserFromCommunity(groupId, userId, accessToken) {
    const response = await vkPost('groups.removeUser', {
        group_id: Math.abs(parseInt(groupId, 10)),
        user_id: parseInt(userId, 10),
        access_token: accessToken
    });
    if (response.error) {
        throw new Error(response.error.error_msg || 'Не удалось удалить пользователя из сообщества');
    }
    return response.response;
}

async function deleteConversationWithUser(userId, accessToken) {
    const attempts = [
        { peer_id: parseInt(userId, 10) },
        { user_id: parseInt(userId, 10) }
    ];

    let lastError = null;
    for (const params of attempts) {
        const response = await vkPost('messages.deleteConversation', {
            ...params,
            access_token: accessToken
        });
        if (!response.error) {
            return response.response;
        }
        lastError = response.error;
    }

    throw new Error(lastError?.error_msg || 'Не удалось удалить переписку с пользователем');
}

module.exports = {
    vkGet,
    vkPost,
    sendMessage,
    sendComment,
    getUserName,
    getGroupIdFromToken,
    getCallbackConfirmationCode,
    checkToken,
    getTokenPermissions,
    getCallbackServers,
    addCallbackServer,
    deleteCallbackServer,
    setCallbackServer,
    setCallbackSettings,
    approveGroupJoinRequest,
    removeUserFromCommunity,
    deleteConversationWithUser
};
