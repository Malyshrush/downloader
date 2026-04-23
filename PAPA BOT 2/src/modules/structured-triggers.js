const { log } = require('../utils/logger');
const { getSheetData } = require('./storage');
const { sendMessageAndPerformActions } = require('./messages');
const { sendCommentAndPerformActions } = require('./comments');
const {
    checkUserGroups,
    updateUserGroups,
    updateUserBotAndStep,
    removeUserBotAndStep,
    getUserVariables,
    updateUserVariables,
    deleteUserData
} = require('./users');
const { addAppLog } = require('./app-logs');
const { getGlobalVariables, updateGlobalVariables, getProfileUserSharedVariables, updateProfileUserSharedVariables } = require('./variables');
const { recordStructuredTriggerExecution } = require('./profile-dashboard');
const { createStructuredTriggerStore } = require('./structured-trigger-store');

// ==========================================
// Callback Proxy — для методов, требующих User Token
// ==========================================
const CALLBACK_PROXY_URL = process.env.CALLBACK_PROXY_URL || 'https://vk-callback-proxy.onrender.com';
const CALLBACK_SECRET = process.env.CALLBACK_SECRET || 'papa-bot-callback-secret-2026';
const axios = require('axios');
const structuredTriggerStore = createStructuredTriggerStore();

/**
 * Отправляет запрос на callback-proxy в режиме fire-and-forget.
 * НЕ ждёт ответа — Render на free тарифе может спать >30 сек.
 */
function callCallbackProxy(action, groupId, userId, userToken) {
    const url = CALLBACK_PROXY_URL + '/webhook';
    log('info', '🌐 callCallbackProxy (fire-and-forget): action=' + action + ', group=' + groupId + ', user=' + userId);

    // Отправляем и НЕ ждём
    axios.post(url, {
        secret: CALLBACK_SECRET,
        action,
        groupId: String(groupId),
        userId: String(userId),
        userToken
    }, {
        timeout: 60000
    }).then(response => {
        if (response.data.success) {
            log('info', '✅ callCallbackProxy success: ' + action + ' for user=' + userId);
        } else {
            log('error', '❌ callCallbackProxy error: ' + (response.data.error || 'Unknown'), response.data);
        }
    }).catch(err => {
        log('error', '❌ callCallbackProxy failed: ' + err.message);
    });

    // Сразу возвращаем — функция не блокирует основной поток
    return Promise.resolve();
}

const SUPPORTED_TYPES = [
    'message_new',
    'message_reply',
    'message_event',
    'photo_new',
    'video_new',
    'wall_reply_new',
    'wall_reply_delete',
    'wall_repost',
    'like_add',
    'group_join',
    'group_leave'
];

function getStructuredTriggerStore(overrides = {}) {
    return overrides.structuredTriggerStore || structuredTriggerStore;
}

function isStructuredTriggerStoreEnabled(overrides = {}) {
    const store = getStructuredTriggerStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

async function loadStructuredTriggerRows(communityId, profileId = '1', overrides = {}) {
    if (!isStructuredTriggerStoreEnabled(overrides)) {
        return (overrides.getSheetData || getSheetData)('ТРИГГЕРЫ', communityId, profileId);
    }

    const snapshot = await getStructuredTriggerStore(overrides).listTriggerRows(communityId, profileId);
    if (snapshot && snapshot.initialized) {
        return Array.isArray(snapshot.rows) ? snapshot.rows : [];
    }

    return (overrides.getSheetData || getSheetData)('ТРИГГЕРЫ', communityId, profileId);
}

function normalizeValue(value) {
    return String(value || '').trim().toLowerCase();
}

function isYes(value, defaultValue = false) {
    const normalized = normalizeValue(value);
    if (!normalized) return defaultValue;
    return ['да', 'true', '1', 'yes', 'on', 'active'].includes(normalized);
}

function parsePayload(payload) {
    if (!payload) return null;
    if (typeof payload === 'string') {
        try {
            return JSON.parse(payload);
        } catch (e) {
            return null;
        }
    }
    return typeof payload === 'object' ? payload : null;
}

function getButtonLabel(payload, fallbackText = '') {
    if (!payload) return String(fallbackText || '').trim();
    return String(payload.buttonLabel || payload.label || fallbackText || '').trim();
}

function buildWallShort(ownerId, postId) {
    if (ownerId === undefined || ownerId === null || postId === undefined || postId === null) return '';
    return 'wall' + String(ownerId) + '_' + String(postId);
}

function buildWallLink(ownerId, postId) {
    const short = buildWallShort(ownerId, postId);
    return short ? 'https://vk.com/' + short : '';
}

function normalizeWallLink(value) {
    const raw = String(value || '').trim();
    if (!raw) return '';
    const match = raw.match(/wall-?\d+_\d+/i);
    return match ? match[0].toLowerCase() : raw.toLowerCase();
}

function splitLines(value) {
    return String(value || '')
        .split(/[\r\n,]+/)
        .map(item => item.trim())
        .filter(Boolean);
}

function parseNumber(value) {
    const normalized = String(value || '').trim().replace(',', '.');
    if (!normalized) return null;
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
}

function extractUserId(eventType, object) {
    if (!object) return null;
    if (eventType === 'group_join' || eventType === 'group_leave') {
        return object.user_id || object.joined?.user_id || null;
    }
    if (eventType === 'message_new' || eventType === 'message_reply') {
        return object.message?.from_id || null;
    }
    if (eventType === 'message_event') {
        return object.user_id || null;
    }
    if (eventType === 'wall_reply_new' || eventType === 'wall_reply_delete') {
        return object.from_id || object.deleter_id || null;
    }
    if (eventType === 'wall_repost') {
        return object.from_id || object.owner_id || null;
    }
    if (eventType === 'like_add') {
        return object.liker_id || object.user_id || object.from_id || null;
    }
    if (eventType === 'photo_new' || eventType === 'video_new') {
        return object.user_id || object.owner_id || object.from_id || null;
    }
    return null;
}

function extractText(eventType, object, payload) {
    if (!object) return '';
    if (eventType === 'message_new' || eventType === 'message_reply') {
        return String(object.message?.text || '').trim();
    }
    if (eventType === 'message_event') {
        return String(getButtonLabel(payload) || object.text || '').trim();
    }
    if (eventType === 'wall_reply_new' || eventType === 'wall_reply_delete') {
        return String(object.text || '').trim();
    }
    if (eventType === 'video_new') {
        return String(object.description || object.text || '').trim();
    }
    return '';
}

function extractAttachments(eventType, object) {
    if (!object) return [];
    if (eventType === 'message_new' || eventType === 'message_reply') {
        return Array.isArray(object.message?.attachments) ? object.message.attachments : [];
    }
    if (eventType === 'wall_reply_new' || eventType === 'wall_reply_delete') {
        return Array.isArray(object.attachments) ? object.attachments : [];
    }
    if (eventType === 'photo_new') {
        return [{ type: 'photo', photo: object }];
    }
    if (eventType === 'video_new') {
        return [{ type: 'video', video: object }];
    }
    return [];
}

function getPostInfo(eventType, data) {
    const object = data.object || {};
    const groupId = data.group_id;

    if (eventType === 'wall_reply_new' || eventType === 'wall_reply_delete') {
        const ownerId = object.post_owner_id || object.owner_id || (groupId ? -Math.abs(Number(groupId)) : null);
        return {
            postId: object.post_id || null,
            ownerId,
            postShort: buildWallShort(ownerId, object.post_id),
            postLink: buildWallLink(ownerId, object.post_id)
        };
    }

    if (eventType === 'wall_repost') {
        const source = Array.isArray(object.copy_history) && object.copy_history[0]
            ? object.copy_history[0]
            : object;
        const ownerId = source.owner_id || source.from_id || null;
        const postId = source.id || source.post_id || null;
        return {
            postId,
            ownerId,
            postShort: buildWallShort(ownerId, postId),
            postLink: buildWallLink(ownerId, postId)
        };
    }

    if (eventType === 'like_add') {
        const ownerId = object.object_owner_id || object.owner_id || null;
        const postId = object.object_id || object.post_id || null;
        return {
            postId,
            ownerId,
            postShort: buildWallShort(ownerId, postId),
            postLink: buildWallLink(ownerId, postId),
            objectType: normalizeValue(object.object_type)
        };
    }

    return {
        postId: null,
        ownerId: null,
        postShort: '',
        postLink: ''
    };
}

function hasAttachmentType(attachments, types) {
    if (!Array.isArray(attachments) || !attachments.length) return false;
    const list = Array.isArray(types) ? types : [types];
    return attachments.some(item => list.includes(normalizeValue(item?.type)));
}

async function matchesMessageCondition(conditionCode, conditionValue, conditionParam, details) {
    const text = String(details.text || '').trim();
    const lowerText = text.toLowerCase();
    const lowerValue = String(conditionValue || '').trim().toLowerCase();
    const numericText = parseNumber(text);
    const numericValue = parseNumber(conditionValue);

    if (!conditionCode || conditionCode === 'any_message') return true;
    if (conditionCode === 'text_equals') return lowerText === lowerValue;
    if (conditionCode === 'text_not_equals') return lowerText !== lowerValue;
    if (conditionCode === 'text_contains') return !!lowerValue && lowerText.includes(lowerValue);
    if (conditionCode === 'text_not_contains') return !!lowerValue && !lowerText.includes(lowerValue);
    if (conditionCode === 'text_regex') {
        try {
            return !!conditionValue && new RegExp(String(conditionValue), 'i').test(text);
        } catch (e) {
            return false;
        }
    }
    if (conditionCode === 'phone_ru') return /^(\+7|7|8)[\s\-()]*\d[\d\s\-()]{8,}$/.test(text);
    if (conditionCode === 'email') return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text);
    if (conditionCode === 'number') return numericText !== null;
    if (conditionCode === 'number_less_than') return numericText !== null && numericValue !== null && numericText < numericValue;
    if (conditionCode === 'number_greater_than') return numericText !== null && numericValue !== null && numericText > numericValue;
    if (conditionCode === 'message_has_photo') return hasAttachmentType(details.attachments, 'photo');
    if (conditionCode === 'message_has_video') return hasAttachmentType(details.attachments, 'video');
    if (conditionCode === 'message_has_audio') return hasAttachmentType(details.attachments, 'audio');
    if (conditionCode === 'message_has_document') return hasAttachmentType(details.attachments, 'doc');
    if (conditionCode === 'message_has_voice') return hasAttachmentType(details.attachments, 'audio_message');
    if (conditionCode === 'message_has_product') return hasAttachmentType(details.attachments, ['market', 'market_album']);
    if (conditionCode === 'message_has_attachment') return Array.isArray(details.attachments) && details.attachments.length > 0;
    if (conditionCode === 'text_contains_user_var' || conditionCode === 'text_contains_global_var' || conditionCode === 'text_contains_shared_var') {
        const targetName = String(conditionParam || '').trim().toLowerCase();
        if (!targetName) return false;

        let expectedValue = '';
        if (conditionCode === 'text_contains_user_var') {
            if (!details.userId) return false;
            const userVars = await getUserVariables(details.userId, details.communityId, details.profileId);
            expectedValue = userVars[targetName] || '';
        } else if (conditionCode === 'text_contains_global_var') {
            const globalData = await getGlobalVariables(details.communityId, details.profileId);
            expectedValue = globalData.globalVars?.[targetName] || '';
        } else {
            const sharedVars = await getProfileUserSharedVariables(details.userId, details.profileId);
            expectedValue = sharedVars[targetName] || '';
        }

        return !!expectedValue && lowerText.includes(String(expectedValue).trim().toLowerCase());
    }
    return false;
}

function matchesPostCondition(conditionCode, conditionValue, details) {
    if (!conditionCode || conditionCode === 'any_post') return true;
    if (!details.postShort && !details.postLink) return false;
    const currentShort = normalizeWallLink(details.postShort);
    const currentLink = normalizeWallLink(details.postLink);
    return splitLines(conditionValue).some(link => {
        const normalized = normalizeWallLink(link);
        return !!normalized && (normalized === currentShort || normalized === currentLink);
    });
}

function matchesButtonCondition(conditionCode, conditionValue, details) {
    if (!details.buttonLabel) return false;
    if (!conditionCode || conditionCode === 'any_button') return true;
    if (conditionCode === 'button_label_equals') {
        return details.buttonLabel.toLowerCase() === String(conditionValue || '').trim().toLowerCase();
    }
    return false;
}

function matchesCommentTextCondition(extraConditionCode, extraValue, details) {
    if (!extraConditionCode || extraConditionCode === 'any_comment_text') return true;
    if (extraConditionCode === 'comment_text_contains') {
        const lowerText = String(details.text || '').toLowerCase();
        const needle = String(extraValue || '').trim().toLowerCase();
        return !!needle && lowerText.includes(needle);
    }
    return false;
}

async function matchesJoinRequestCondition(conditionCode, conditionValue, conditionParam, details) {
    if (!conditionCode || conditionCode === 'any_request_condition') return true;
    const targetName = String(conditionParam || '').trim().toLowerCase();
    log('debug', '🔍 matchesJoinRequestCondition: code=' + conditionCode + ', param=' + conditionParam + ', targetName=' + targetName + ', expectedValue=' + conditionValue);
    if (!targetName) return false;
    const sharedVars = await getProfileUserSharedVariables(details.userId, details.profileId);
    log('debug', '🔍 matchesJoinRequestCondition: sharedVars for user ' + details.userId + ' = ' + JSON.stringify(sharedVars));
    const actualValue = String(sharedVars[targetName] || '').trim().toLowerCase();
    const expectedValue = String(conditionValue || '').trim().toLowerCase();
    log('debug', '🔍 matchesJoinRequestCondition: actualValue=' + actualValue + ', expectedValue=' + expectedValue + ', match=' + (actualValue === expectedValue));

    if (conditionCode === 'shared_var_equals') return actualValue === expectedValue;
    if (conditionCode === 'shared_var_not_equals') return actualValue !== expectedValue;
    return false;
}

function mapLegacyEventCode(rowEventType, legacyCheckType) {
    if (rowEventType === 'message_new') return 'incoming_message';
    if (rowEventType === 'message_reply') return 'outgoing_message';
    if (rowEventType === 'wall_reply_new') return 'wall_comment_add';
    if (rowEventType === 'wall_reply_delete') return 'wall_comment_delete';
    if (rowEventType === 'wall_repost') return 'wall_repost';
    if (rowEventType === 'like_add') return 'wall_like';
    if (rowEventType === 'group_join') return 'user_group_join';
    if (rowEventType === 'group_leave') return 'user_group_leave';
    if (rowEventType === 'photo_new' && legacyCheckType === 'all') return 'photo_new';
    if (rowEventType === 'video_new' && legacyCheckType === 'all') return 'video_new';
    return rowEventType;
}

function mapLegacyCondition(eventCode, legacyCheckType) {
    if (!legacyCheckType || legacyCheckType === 'all') {
        if (eventCode === 'wall_repost' || eventCode === 'wall_like' || eventCode === 'wall_comment_add' || eventCode === 'wall_comment_delete') {
            return 'any_post';
        }
        return 'any_message';
    }

    if (legacyCheckType === 'equals') return 'text_equals';
    if (legacyCheckType === 'contains') return 'text_contains';
    if (legacyCheckType === 'regex') return 'text_regex';
    if (legacyCheckType === 'photo') return 'message_has_photo';
    if (legacyCheckType === 'video') return 'message_has_video';
    if (legacyCheckType === 'doc') return 'message_has_document';
    if (legacyCheckType === 'attach') return 'message_has_attachment';
    return legacyCheckType;
}

function normalizeActionCode(value) {
    const normalized = normalizeValue(value);
    if (normalized === 'add_group' || normalized === 'добавить в группу') return 'add_group';
    if (normalized === 'remove_group' || normalized === 'исключить из группы') return 'remove_group';
    if (normalized === 'add_to_bot' || normalized === 'добавить в бота') return 'add_to_bot';
    if (normalized === 'remove_from_bot' || normalized === 'исключить из бота') return 'remove_from_bot';
    if (normalized === 'approve_group_request' || normalized === 'одобрить заявку в сообщество') return 'approve_group_request';
    if (normalized === 'remove_from_community' || normalized === 'удалить пользователя из сообщества') return 'remove_from_community';
    if (normalized === 'delete_user_data' || normalized === 'удалить данные пользователя') return 'delete_user_data';
    if (normalized === 'delete_user_conversation' || normalized === 'удалить переписку с пользователем') return 'delete_user_conversation';
    if (normalized === 'delete_user_data_and_conversation' || normalized === 'удалить данные пользователя и переписку') return 'delete_user_data_and_conversation';
    if (normalized === 'user_var_add') return 'user_var_add';
    if (normalized === 'user_var_update') return 'user_var_update';
    if (normalized === 'user_var_delete') return 'user_var_delete';
    if (normalized === 'global_var_add') return 'global_var_add';
    if (normalized === 'global_var_update') return 'global_var_update';
    if (normalized === 'global_var_delete') return 'global_var_delete';
    if (normalized === 'shared_var_add') return 'shared_var_add';
    if (normalized === 'shared_var_update') return 'shared_var_update';
    if (normalized === 'shared_var_delete') return 'shared_var_delete';
    return normalized;
}

function normalizeRow(row) {
    const legacyEventType = normalizeValue(row['Тип события']);
    const legacyCheckType = normalizeValue(row['Проверка']);
    const eventCode = normalizeValue(row['Код события']) || mapLegacyEventCode(legacyEventType, legacyCheckType);
    const conditionCode = normalizeValue(row['Код условия']) || mapLegacyCondition(eventCode, legacyCheckType || normalizeValue(row['Условие']));
    const extraConditionCode = normalizeValue(row['Код доп. условия'] || row['Доп. условие']);

    let actions = [];
    try {
        const parsed = JSON.parse(String(row['Действия JSON'] || '').trim() || '[]');
        if (Array.isArray(parsed)) actions = parsed;
    } catch (e) {
        actions = [];
    }
    if (!actions.length) {
        actions = [{
            action: normalizeActionCode(row['Код действия'] || row['Действие']),
            actionGroup: String(row['Группа'] || row['ДОБАВИТЬ ГРУППУ'] || row['УДАЛИТЬ ГРУППУ'] || '').trim(),
            actionBot: String(row['Бот'] || '').trim(),
            actionStep: String(row['Шаг'] || '').trim(),
            actionVarName: String(row['Название переменной'] || '').trim(),
            actionVarValue: String(row['Значение переменной'] || '').trim(),
            actionCommunityId: String(row['ID сообщества действия'] || row['ID сообщества'] || '').trim()
        }];
    }

    return {
        eventCode,
        legacyEventType,
        conditionCode,
        conditionParam: String(row['Параметр условия'] || row['Имя переменной условия'] || '').trim(),
        conditionValue: String(row['Значение'] || '').trim(),
        extraConditionCode,
        extraValue: String(row['Доп. значение'] || '').trim(),
        actionCode: normalizeActionCode(row['Код действия'] || row['Действие']),
        targetGroup: String(row['Группа'] || row['ДОБАВИТЬ ГРУППУ'] || row['УДАЛИТЬ ГРУППУ'] || '').trim(),
        targetBot: String(row['Бот'] || '').trim(),
        targetStep: String(row['Шаг'] || '').trim(),
        targetVariableName: String(row['Название переменной'] || '').trim(),
        targetVariableValue: String(row['Значение переменной'] || '').trim(),
        targetCommunityId: String(row['ID сообщества действия'] || row['ID сообщества'] || '').trim(),
        actions,
        active: row['Активен'] ? isYes(row['Активен']) : true,
        stopFurther: isYes(row['Не применять остальные правила'])
    };
}

function matchesEvent(eventCode, details) {
    const eventType = details.eventType;

    if (eventCode === 'incoming_message') return eventType === 'message_new';
    if (eventCode === 'outgoing_message') return eventType === 'message_reply';
    if (eventCode === 'message_button_click') {
        return (eventType === 'message_new' || eventType === 'message_event') && !!details.buttonLabel;
    }
    if (eventCode === 'wall_repost') return eventType === 'wall_repost';
    if (eventCode === 'wall_like') {
        return eventType === 'like_add' && (!details.objectType || details.objectType === 'post');
    }
    if (eventCode === 'wall_comment_add') return eventType === 'wall_reply_new';
    if (eventCode === 'wall_comment_delete') return eventType === 'wall_reply_delete';
    if (eventCode === 'user_group_request') return eventType === 'group_join' && details.joinType === 'request';
    if (eventCode === 'user_group_join') return eventType === 'group_join' && details.joinType !== 'request';
    if (eventCode === 'user_group_leave') return eventType === 'group_leave';
    // Legacy support: если триггер создан для group_join, но не различает request/join
    if (eventCode === 'group_join') return eventType === 'group_join';
    return eventType === eventCode;
}

async function matchesNormalizedRow(normalizedRow, details) {
    if (!normalizedRow.active) return { matched: false, reason: 'inactive' };
    if (!matchesEvent(normalizedRow.eventCode, details)) {
        return { matched: false, reason: 'event_type_mismatch' };
    }

    if (normalizedRow.eventCode === 'message_button_click') {
        return {
            matched: matchesButtonCondition(normalizedRow.conditionCode, normalizedRow.conditionValue, details),
            reason: normalizedRow.conditionCode || 'any_button'
        };
    }

    if (normalizedRow.eventCode === 'wall_repost' || normalizedRow.eventCode === 'wall_like') {
        return {
            matched: matchesPostCondition(normalizedRow.conditionCode, normalizedRow.conditionValue, details),
            reason: normalizedRow.conditionCode || 'any_post'
        };
    }

    if (normalizedRow.eventCode === 'wall_comment_add' || normalizedRow.eventCode === 'wall_comment_delete') {
        const postMatched = matchesPostCondition(normalizedRow.conditionCode, normalizedRow.conditionValue, details);
        if (!postMatched) return { matched: false, reason: 'post_condition_failed' };
        return {
            matched: matchesCommentTextCondition(normalizedRow.extraConditionCode, normalizedRow.extraValue, details),
            reason: normalizedRow.extraConditionCode || 'any_comment_text'
        };
    }

    if (normalizedRow.eventCode === 'user_group_request') {
        return {
            matched: await matchesJoinRequestCondition(normalizedRow.conditionCode, normalizedRow.conditionValue, normalizedRow.conditionParam, details),
            reason: normalizedRow.conditionCode || 'any_request_condition'
        };
    }

    // Legacy support: group_join без различия request/join
    if (normalizedRow.eventCode === 'group_join') {
        // Если это request, проверяем условия ПВС
        if (details.joinType === 'request' && normalizedRow.conditionCode && normalizedRow.conditionCode !== 'any_message') {
            return {
                matched: await matchesJoinRequestCondition(normalizedRow.conditionCode, normalizedRow.conditionValue, normalizedRow.conditionParam, details),
                reason: normalizedRow.conditionCode
            };
        }
        return { matched: true, reason: 'group_join' };
    }

    if (normalizedRow.eventCode === 'user_group_join' || normalizedRow.eventCode === 'user_group_leave') {
        return { matched: true, reason: normalizedRow.eventCode };
    }

    return {
        matched: await matchesMessageCondition(normalizedRow.conditionCode, normalizedRow.conditionValue, normalizedRow.conditionParam, details),
        reason: normalizedRow.conditionCode || 'any_message'
    };
}

function resolveActionCommunityId(actionCommunityId, details) {
    const raw = String(actionCommunityId || '').trim();
    if (raw) return raw;
    if (details.groupId !== undefined && details.groupId !== null) return String(details.groupId);
    return '';
}

async function executeStructuredAction(row, normalizedRow, details, communityId, profileId) {
    const answer = String(row['Ответ'] || '').trim();
    if (answer) {
        if (normalizedRow.eventCode === 'wall_comment_add') {
            await sendCommentAndPerformActions(details.object, details.groupId, row, communityId, profileId);
            return;
        }

        if (details.userId) {
            await sendMessageAndPerformActions(details.userId, row, { group_id: details.groupId }, false, communityId, profileId);
            return;
        }
    }

    if (!details.userId) return;

    for (const action of normalizedRow.actions || []) {
        const actionCode = normalizeActionCode(action.action || action.actionCode || '');
        const actionGroup = String(action.actionGroup || '').trim();
        const actionBot = String(action.actionBot || '').trim();
        const actionStep = String(action.actionStep || '').trim();
        const actionVarName = String(action.actionVarName || '').trim();
        const actionVarValue = String(action.actionVarValue || '').trim();
        const actionCommunityId = resolveActionCommunityId(action.actionCommunityId, details);

        if (actionCode === 'add_group' && actionGroup) {
            await updateUserGroups(details.userId, actionGroup, '', communityId, profileId);
            continue;
        }

        if (actionCode === 'remove_group' && actionGroup) {
            await updateUserGroups(details.userId, '', actionGroup, communityId, profileId);
            continue;
        }

        if (actionCode === 'add_to_bot' && actionBot && actionStep) {
            await updateUserBotAndStep(details.userId, actionBot, actionStep, communityId, profileId);
            continue;
        }

        if (actionCode === 'remove_from_bot' && actionBot) {
            await removeUserBotAndStep(details.userId, actionBot, communityId, profileId);
            continue;
        }

        if (actionCode === 'approve_group_request' && actionCommunityId) {
            // WORKAROUND: VK заблокировал Standalone-приложения, поэтому получить User Token для groups.approveRequest
            // невозможно (токен привязан к IP клиента, а бот на сервере).
            // Решение: используем метод groups.add с Community Token (токеном сообщества).
            // Community Token не привязан к IP и работает с сервера.
            // Вызов groups.add для пользователя с заявкой автоматически одобряет её.
            
            const { getVkToken } = require('./config');
            const communityToken = await getVkToken(0, actionCommunityId, profileId);
            
            if (!communityToken) {
                log('error', '❌ approve_group_request: Не найден токен сообщества для ' + actionCommunityId);
            } else {
                try {
                    const res = await axios.post('https://api.vk.com/method/groups.add', null, {
                        params: {
                            group_id: Math.abs(parseInt(actionCommunityId)),
                            user_id: parseInt(details.userId),
                            access_token: communityToken,
                            v: '5.199'
                        }
                    });
                    
                    if (res.data.error) {
                        log('warn', '⚠️ groups.add error (возможно, у токена нет прав manage_community): ' + res.data.error.error_msg);
                    } else {
                        log('info', '✅ Заявка одобрена через groups.add (user=' + details.userId + ')');
                    }
                } catch (e) {
                    log('error', '❌ groups.add failed: ' + e.message);
                }
            }
            continue;
        }

        if (actionCode === 'remove_from_community' && actionCommunityId) {
            // groups.removeUser требует User Token — вызываем через callback-proxy
            const { getUserToken } = require('./config');
            const userToken = await getUserToken(actionCommunityId, profileId);
            log('debug', '🔑 remove_from_community: using userToken for community ' + actionCommunityId + ', token_start=' + (userToken ? String(userToken).substring(0, 10) : 'NONE'));
            if (!userToken) throw new Error('Не найден User Token для удаления пользователя из сообщества');
            await callCallbackProxy('remove_user', actionCommunityId, details.userId, userToken);
            continue;
        }

        if (actionCode === 'delete_user_data' || actionCode === 'delete_user_data_and_conversation') {
            await deleteUserData(details.userId, communityId, profileId);
            if (actionCode === 'delete_user_data') continue;
        }

        if (actionCode === 'delete_user_conversation' || actionCode === 'delete_user_data_and_conversation') {
            // messages.deleteConversation требует User Token — вызываем через callback-proxy
            const { getUserToken } = require('./config');
            const userToken = await getUserToken(communityId, profileId);
            log('debug', '🔑 delete_conversation: using userToken for community ' + communityId + ', token_start=' + (userToken ? String(userToken).substring(0, 10) : 'NONE'));
            if (!userToken) throw new Error('Не найден User Token для удаления переписки');
            await callCallbackProxy('delete_conversation', communityId, details.userId, userToken);
            if (actionCode === 'delete_user_data') continue;
        }

        if (actionCode === 'user_var_add' || actionCode === 'user_var_update' || actionCode === 'user_var_delete') {
            const variables = await getUserVariables(details.userId, communityId, profileId);
            const nextVars = Object.assign({}, variables);
            if (actionCode === 'user_var_delete') delete nextVars[actionVarName.toLowerCase()];
            else nextVars[actionVarName.toLowerCase()] = actionVarValue;
            await updateUserVariables(details.userId, nextVars, true, communityId, profileId);
            continue;
        }

        if (actionCode === 'global_var_add' || actionCode === 'global_var_update' || actionCode === 'global_var_delete') {
            const { globalVars } = await getGlobalVariables(communityId, profileId);
            const nextVars = Object.assign({}, globalVars);
            if (actionCode === 'global_var_delete') delete nextVars[actionVarName.toLowerCase()];
            else nextVars[actionVarName.toLowerCase()] = actionVarValue;
            await updateGlobalVariables(nextVars, communityId, profileId);
            continue;
        }

        if (actionCode === 'shared_var_add' || actionCode === 'shared_var_update' || actionCode === 'shared_var_delete') {
            const sharedVars = await getProfileUserSharedVariables(details.userId, profileId);
            const nextVars = Object.assign({}, sharedVars);
            if (actionCode === 'shared_var_delete') delete nextVars[actionVarName.toLowerCase()];
            else nextVars[actionVarName.toLowerCase()] = actionVarValue;
            await updateProfileUserSharedVariables(details.userId, nextVars, profileId);
            continue;
        }
    }
}

function buildEventDetails(data) {
    const eventType = normalizeValue(data.type);
    const object = data.object || {};
    const payload = parsePayload(
        eventType === 'message_event'
            ? object.payload
            : object.message?.payload
    );
    const attachments = extractAttachments(eventType, object);
    const postInfo = getPostInfo(eventType, data);

    return {
        data,
        eventType,
        groupId: data.group_id,
        communityId: data.group_id?.toString(),
        object,
        payload,
        userId: extractUserId(eventType, object),
        text: extractText(eventType, object, payload),
        attachments,
        buttonLabel: getButtonLabel(payload, object.message?.text || object.text || ''),
        postId: postInfo.postId,
        postOwnerId: postInfo.ownerId,
        postShort: postInfo.postShort,
        postLink: postInfo.postLink,
        objectType: postInfo.objectType || '',
        joinType: (function() {
            const joinedType = object.join_type || object.joined?.join_type || object.joined?.type || '';
            const normalizedJoinType = normalizeValue(joinedType);
            const result = normalizedJoinType.includes('request') || object.joined_by_request ? 'request' : (normalizedJoinType || 'join');
            log('debug', '🎯 buildEventDetails: joinType=' + result + ', raw join_type=' + joinedType + ', joined_by_request=' + (object.joined_by_request || 'undefined'));
            return result;
        })()
    };
}

async function processStructuredTriggersWithDependencies(data, profileId = '1', overrides = {}) {
    const details = Object.assign({}, buildEventDetails(data), { profileId });
    if (!SUPPORTED_TYPES.includes(details.eventType)) {
        return { matched: false, handled: false };
    }

    if (!details.communityId) {
        log('debug', `🎯 Structured triggers skipped: no communityId for event ${details.eventType}`);
        return { matched: false, handled: false };
    }

    const rows = await loadStructuredTriggerRows(details.communityId, profileId, overrides);
    if (!rows || !rows.length) {
        return { matched: false, handled: false };
    }

    log('debug', `🎯 Structured triggers: event=${details.eventType}, rows=${rows.length}, text="${details.text}", button="${details.buttonLabel}", post="${details.postShort}"`);

    let matchedAny = false;
    let handledAny = false;

    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const normalizedRow = normalizeRow(row);
        const title = String(row['Название'] || `Trigger #${i + 1}`);

        if ((row['Ответить если в Группе'] || '').trim() && details.userId) {
            const groupsMatched = await (overrides.checkUserGroups || checkUserGroups)(details.userId, row['Ответить если в Группе'], details.communityId, profileId);
            if (!groupsMatched) {
                log('debug', `🎯 Structured trigger skipped [${title}]: group condition failed`);
                continue;
            }
        }

        const match = await matchesNormalizedRow(normalizedRow, details);
        log('debug', `🎯 Structured trigger check [${title}]: event=${details.eventType}, code=${normalizedRow.eventCode}, matched=${match.matched}, reason=${match.reason}`);

        if (!match.matched) continue;
        matchedAny = true;

        await (overrides.addAppLog || addAppLog)({
            tab: 'TRIGGERS',
            title: 'Сработал триггер',
            summary: title,
            details: [
                'Событие: ' + details.eventType,
                normalizedRow.conditionCode ? 'Условие: ' + normalizedRow.conditionCode : '',
                normalizedRow.actionCode ? 'Действие: ' + normalizedRow.actionCode : ''
            ],
            communityId: details.communityId,
            profileId
        });

        await executeStructuredAction(row, normalizedRow, details, details.communityId, profileId);
        await (overrides.recordStructuredTriggerExecution || recordStructuredTriggerExecution)(profileId, details.communityId);
        handledAny = true;

        if (normalizedRow.stopFurther) {
            return { matched: true, handled: handledAny, stopFurther: true };
        }
    }

    return { matched: matchedAny, handled: handledAny };
}

async function processStructuredTriggers(data, profileId = '1') {
    return processStructuredTriggersWithDependencies(data, profileId);
}

module.exports = {
    processStructuredTriggers,
    __testOnly: {
        loadStructuredTriggerRows,
        processStructuredTriggersWithDependencies
    }
};
