/**
 * Модуль обработки сообщений
 */

const crypto = require('crypto');
const { log } = require('../utils/logger');
const { getSheetData } = require('./storage');
const { getVkToken, getAllVkTokens, getUserToken } = require('./config');
const { sendMessage } = require('./vk-api');
const { createKeyboard } = require('./keyboard');
const { getAttachmentsFromRow, processAttachmentWithUserToken } = require('./attachments');
const { replaceVariables } = require('./variables');
const { checkTriggerExists, checkTriggerMatch, checkAllConditions, normalizeTriggerMode } = require('./triggers');
const { updateUserData } = require('./users');
const { addAppLog } = require('./app-logs');
const { publishOutboundAction } = require('./event-queue');

// Кэш обработанных сообщений (защита от дублей)
const processedMessages = new Map();
const MESSAGE_TTL = 5 * 60 * 1000;

// Очистка старых записей
setInterval(() => {
    const now = Date.now();
    for (const [key, timestamp] of processedMessages.entries()) {
        if (now - timestamp > MESSAGE_TTL) {
            processedMessages.delete(key);
        }
    }
}, 60000);

function buildMessageRowFingerprint(row = {}) {
    const normalized = {
        answer: row['РћС‚РІРµС‚'] || '',
        fallbackAnswer: row['Р—Р°РіРѕС‚РѕРІР»РµРЅРЅС‹Р№ РѕС‚РІРµС‚'] || '',
        step: row['РЁР°Рі'] || '',
        bot: row['Р‘РѕС‚'] || '',
        trigger: row['РўСЂРёРіРіРµСЂ'] || ''
    };

    return crypto
        .createHash('sha1')
        .update(JSON.stringify(normalized))
        .digest('hex')
        .slice(0, 12);
}

function buildMessageOutboundAction(actionType, { userId, row, originalMessage, communityId, profileId }) {
    const messageId = originalMessage?.conversation_message_id || originalMessage?.id || 'no_message_id';

    return {
        actionId: [
            'vk',
            'outbound',
            actionType,
            String(communityId || 'default'),
            String(profileId || '1'),
            String(messageId),
            String(userId || '0'),
            buildMessageRowFingerprint(row)
        ].join(':'),
        actionType,
        communityId: String(communityId || 'default'),
        profileId: String(profileId || '1'),
        createdAt: new Date().toISOString(),
        source: 'message-handler',
        payload: {
            userId,
            row,
            originalMessage,
            communityId: String(communityId || 'default'),
            profileId: String(profileId || '1')
        }
    };
}

/**
 * Обработать входящее сообщение
 */
async function handleMessage(data, profileId = '1') {
    try {
        const message = data.object.message;
        const text = (message.text || '').trim();
        const userId = message.from_id;
        const groupId = data.group_id;
        const peerId = message.peer_id;
        const communityId = groupId?.toString();
        let parsedPayload = null;
        if (message.payload) {
            if (typeof message.payload === 'string') {
                try {
                    parsedPayload = JSON.parse(message.payload);
                } catch (e) {
                    parsedPayload = null;
                }
            } else if (typeof message.payload === 'object') {
                parsedPayload = message.payload;
            }
        }
        const eventContext = {
            payload: parsedPayload,
            attachments: Array.isArray(message.attachments) ? message.attachments : []
        };

        // ✅ Добавляем group_id в message чтобы sendMessageAndPerformActions мог его использовать
        message.group_id = groupId;

        log('info', '📨 ========== NEW MESSAGE ==========' );
        log('info', `📨 From: ${userId}, group_id: ${groupId}, peer_id: ${peerId}, Text: "${text}"`);
        await addAppLog({
            tab: 'MESSAGES',
            title: data.type === 'message_reply' ? 'Исходящее сообщение' : 'Новое сообщение',
            summary: text ? 'Текст: "' + text + '"' : 'Сообщение без текста',
            details: [
                'Пользователь: ' + userId,
                Array.isArray(eventContext.attachments) && eventContext.attachments.length ? 'Вложений: ' + eventContext.attachments.length : ''
            ],
            communityId,
            profileId
        });

        // Проверка дублирования
        const messageKey = `msg_${userId}_${message.conversation_message_id || message.id}`;
        if (processedMessages.has(messageKey)) {
            const lastTime = processedMessages.get(messageKey);
            const now = Date.now();
            if (now - lastTime < 10000) {
                log('debug', `⚠️ Message already processed ${Math.round((now-lastTime)/1000)}s ago`);
                return;
            }
        }
        processedMessages.set(messageKey, Date.now());

        if (!await getVkToken(0, communityId, profileId)) {
            log('error', '❌ getVkToken() is not set!');
            return;
        }

        // ШАГ 1: Обновляем данные пользователя
        log('debug', '📝 Step 1: Updating user data...');
        const userUpdated = await updateUserData(userId, communityId, profileId);
        log('debug', '📝 Step 1 done: userUpdated=' + userUpdated);
        if (!userUpdated) {
            log('debug', `⚠️ User ${userId} not in database, but continuing...`);
        }

        // ШАГ 2: Загружаем сообщения
        log('debug', '📝 Step 2: Loading messages for community ' + communityId);
        const messages = await getSheetData('СООБЩЕНИЯ', communityId, profileId);
        log('debug', '📝 Step 2 done: messages=' + (messages ? messages.length : 'null'));
        if (!messages) {
            log('error', `❌ No messages data loaded`);
            return;
        }
        if (messages.length > 0) {
            log('debug', `📝 First message row: ${JSON.stringify(messages[0]).substring(0, 200)}`);
        }
        log('debug', `✅ Loaded ${messages.length} message rules`);

        // Поиск совпадений
        let matchedRowWithConditions = null;
        let matchedRowWithoutConditions = null;
        let fallbackRow = null;

        for (let i = 0; i < messages.length; i++) {
            const row = messages[i];
            const trigger = (row['Триггер'] || '').trim();
            const fallbackAnswer = (row['Заготовленный ответ'] || '').trim();
            const triggerMode = normalizeTriggerMode(row._triggerMode);

            // Запоминаем fallback
            if (!trigger && fallbackAnswer && triggerMode === 'TEXT') {
                fallbackRow = row;
                continue;
            }
            if (!trigger && triggerMode !== 'FILE') continue;

            // Проверка триггера
            const triggerExists = await checkTriggerExists(text, trigger, triggerMode, eventContext);
            if (!triggerExists) continue;

            // Проверка условий
            const otherConditionsMet = await checkAllConditions(row, {
                userId, groupId, text,
                eventType: 'message',
                communityId,
                profileId
            });
            if (!otherConditionsMet) continue;

            // Проверка точности
            const matchType = (row['Точно/Не точно'] || '').trim().toUpperCase() || 'НЕ ТОЧНО';
            const caseSensitiveStr = (row['Регистр'] || '').trim();
            const exactMatch = await checkTriggerMatch(text, trigger, matchType, caseSensitiveStr, userId, groupId, communityId, profileId, triggerMode, eventContext);

            if (exactMatch) {
                matchedRowWithConditions = row;
                break;
            } else {
                if (!matchedRowWithoutConditions) {
                    matchedRowWithoutConditions = row;
                }
            }
        }

        // Принятие решения
        if (matchedRowWithConditions) {
            log('debug', '🚀 STEP 4.1: SENDING MAIN RESPONSE');
            await publishOutboundAction(buildMessageOutboundAction('send_message_response', {
                userId,
                row: matchedRowWithConditions,
                originalMessage: message,
                communityId,
                profileId
            }));
        } else if (matchedRowWithoutConditions) {
            log('debug', '🚀 STEP 4.2: SENDING FALLBACK FROM TRIGGER ROW');
            await publishOutboundAction(buildMessageOutboundAction('send_message_fallback', {
                userId,
                row: matchedRowWithoutConditions,
                originalMessage: message,
                communityId,
                profileId
            }));
        } else if (fallbackRow) {
            log('debug', '🚀 STEP 4.3: SENDING GENERAL FALLBACK');
            await publishOutboundAction(buildMessageOutboundAction('send_message_fallback', {
                userId,
                row: fallbackRow,
                originalMessage: message,
                communityId,
                profileId
            }));
        } else {
            // ✅ Автоответчик отключён - бот молчит если триггер не найден
            log('debug', '🚫 STEP 4.4: NO TRIGGER MATCHED - Bot stays silent');
            await addAppLog({
                tab: 'MESSAGES',
                title: 'Сообщение без ответа',
                summary: text ? 'Не найден подходящий шаг для текста "' + text + '"' : 'Не найден подходящий шаг',
                details: ['Пользователь: ' + userId],
                communityId,
                profileId,
                level: 'warn'
            });
        }

        log('info', '✅ ========== MESSAGE PROCESSING COMPLETE ==========');
    } catch (error) {
        log('error', '❌ CRITICAL ERROR in handleMessage:', error);
        log('error', error.stack);
    }
}

/**
 * Отправить сообщение и выполнить действия
 */
async function sendMessageAndPerformActions(userId, row, originalMessage, isComment, communityId, profileId = '1') {
    try {
        const answer = (row['Ответ'] || '').trim();
        const groupId = originalMessage.group_id;
        const bot = (row['Бот'] || 'main').trim();
        const step = (row['Шаг'] || '').trim();

        log('debug', `📤 Original answer: "${answer}"`);

        // Обработка вложений
        let attachments = getAttachmentsFromRow(row, 'MESSAGES');
        const userToken = await getUserToken(communityId, profileId);
        const vkToken = await getVkToken(0, communityId, profileId);
        log('debug', `🔑 Token info: userToken=${userToken ? 'SET' : 'NONE'}, vkToken=${vkToken ? vkToken.substring(0, 15) + '...' : 'NONE'}, communityId=${communityId}`);
        
        if (attachments.length > 0 && userToken) {
            const processedAttachments = [];
            for (const attachment of attachments) {
                if (!attachment || !attachment.trim()) continue;
                const processed = await processAttachmentWithUserToken(attachment.trim(), groupId);
                processedAttachments.push(processed || attachment);
            }
            attachments = processedAttachments;
        }

        const processedAnswer = await replaceVariables(answer, userId, groupId, communityId, profileId);
        log('debug', `📤 Processed answer: "${processedAnswer}"`);

        const keyboard = !isComment ? createKeyboard(row, 'Кнопка Ответа', 'Цвет/Ссылка Ответа') : null;
        if (keyboard) {
            log('debug', '⌨️ Keyboard created: ' + JSON.stringify(keyboard).substring(0, 500));
        }
        
        // Отправка с перебором токенов
        const success = await sendMessageWithTokenRetry(userId, processedAnswer, keyboard, groupId, attachments, communityId, profileId);

        if (success && step && bot) {
            const { markStepAsSent } = require('./users');
            await markStepAsSent(userId, bot, step, communityId, profileId);
            
            const { performRowActions } = require('./row-actions');
            await performRowActions(row, userId, groupId, isComment, communityId, profileId);
        }

        if (success) {
            const details = [
                'Бот: ' + (bot || '-'),
                'Шаг: ' + (step || '-'),
                processedAnswer ? 'Ответ: "' + processedAnswer + '"' : 'Ответ без текста'
            ];
            if ((row['ДОБАВИТЬ ГРУППУ'] || row['УДАЛИТЬ ГРУППУ'] || row['Отправить на Шаг'] || row['Действия с ПП'] || row['Действия с ГП'] || row['Действия с ПВС'])) {
                details.push('Доп. действия строки выполнены');
            }
            await addAppLog({
                tab: 'MESSAGES',
                title: 'Отправлен ответ',
                summary: 'Сработал бот ' + (bot || '-') + ' - шаг ' + (step || '-'),
                details,
                communityId,
                profileId
            });
        }
        
        log('debug', `✅ sendMessageAndPerformActions completed`);
    } catch (error) {
        log('error', '❌ Error in sendMessageAndPerformActions:', error);
        throw error;
    }
}

/**
 * Отправить сообщение с перебором токенов
 */
async function sendMessageWithTokenRetry(userId, text, keyboard, groupId, attachments, communityId, profileId = '1') {
    const tokens = await getAllVkTokens(communityId, profileId);
    let lastError = null;

    for (let tokenIndex = 0; tokenIndex < tokens.length; tokenIndex++) {
        const token = tokens[tokenIndex];
        if (!token) continue;

        log('debug', `🔑 Trying token ${tokenIndex + 1}/${tokens.length}`);

        try {
            const response = await sendMessage(userId, text, keyboard, groupId, attachments, token);

            if (response.error) {
                log('warn', `⚠️ Token ${tokenIndex + 1} failed: ${response.error.error_msg}`);
                lastError = response.error;

                // Пробуем следующий токен при ошибках 912 или 6
                if ((response.error.error_code === 912 || response.error.error_code === 6) && 
                    tokenIndex < tokens.length - 1) {
                    log('info', '🔄 Switching to next token...');
                    continue;
                }
                return false;
            } else {
                log('info', `✅ MESSAGE SENT (token ${tokenIndex + 1}/${tokens.length})`);
                return true;
            }
        } catch (axiosError) {
            log('warn', `⚠️ Token ${tokenIndex + 1} network error: ${axiosError.message}`);
            lastError = axiosError;

            if (tokenIndex < tokens.length - 1) {
                log('info', '🔄 Network error, switching to next token...');
                continue;
            }
        }
    }

    log('error', `❌ All ${tokens.length} tokens failed. Last error:`, lastError?.error_msg || lastError?.message);
    return false;
}

/**
 * Отправить fallback ответ
 */
async function sendFallbackResponseFromRow(userId, row, originalMessage, communityId, profileId = '1') {
    try {
        const fallbackAnswer = (row['Заготовленный ответ'] || '').trim();
        if (!fallbackAnswer) {
            log('debug', `❌ No fallback answer, sending default`);
            await sendDefaultResponse(userId, originalMessage);
            return;
        }

        const groupId = originalMessage.group_id;
        const processedAnswer = await replaceVariables(fallbackAnswer, userId, groupId, communityId, profileId);

        let attachments = getAttachmentsFromRow(row, 'MESSAGES');
        const keyboard = createKeyboard(row, 'Кнопка ЗО', 'Цвет/Ссылка ЗО');
        
        await sendMessageWithTokenRetry(userId, processedAnswer, keyboard, groupId, attachments, communityId, profileId);
        log('debug', `✅ Fallback sent successfully to ${userId}`);
    } catch (error) {
        log('error', '❌ Error sending fallback:', error);
    }
}

/**
 * Отправить ответ по умолчанию
 */
async function sendDefaultResponse(userId, originalMessage) {
    // ✅ Автоответчик отключён - бот молчит если триггер не найден
    log('debug', `🚫 Default response DISABLED for ${userId}`);
    // Если нужно включить обратно:
    // const defaultText = "Извините, я не понял ваш запрос. Обратитесь к администратору.";
    // await sendMessageWithTokenRetry(userId, defaultText, null, originalMessage.group_id, null, originalMessage.group_id?.toString());
}

module.exports = {
    handleMessage,
    sendMessageAndPerformActions,
    sendFallbackResponseFromRow,
    sendMessageWithTokenRetry,
    __testOnly: {
        buildMessageOutboundAction
    }
};
