/**
 * Модуль обработки отложенных сообщений и рассылок
 */

const { log } = require('../utils/logger');
const { getSheetData, saveSheetData, invalidateCache } = require('./storage');
const { getVkToken, getVkGroupId, getActiveCommunityId } = require('./config');
const { sendMessage: sendVkMessage } = require('./vk-api');
const { createKeyboard, createMailingKeyboard } = require('./keyboard');
const { getAttachmentsFromRow } = require('./attachments');
const { replaceVariables } = require('./variables');
const { addAppLog } = require('./app-logs');

// Per-community блокировки
const isProcessingDelayed = {};
const lastProcessTime = {};

const processedDelayedMessages = new Map();
const DELAYED_TTL = 5 * 60 * 1000;

const processedMailings = new Map();
const MAILING_TTL = 10 * 60 * 1000;

function formatMskDateTime(date) {
    const mskOffset = 3 * 60 * 60 * 1000;
    return new Date(date.getTime() + mskOffset).toISOString().replace('T', ' ').substring(0, 19);
}

// Очистка старых записей
setInterval(() => {
    const now = Date.now();

    for (const [key, ts] of processedDelayedMessages.entries()) {
        if (now - ts > DELAYED_TTL) processedDelayedMessages.delete(key);
    }

    for (const [key, ts] of processedMailings.entries()) {
        if (now - ts > MAILING_TTL) processedMailings.delete(key);
    }
}, 60000);

/**
 * Обработать отложенные сообщения
 */
async function processDelayed(communityId = null, profileId = '1') {
    const cid = communityId || getActiveCommunityId(profileId) || 'default';
    try {
        const now = Date.now();

        // Per-community throttling
        if (isProcessingDelayed[cid] || (now - (lastProcessTime[cid] || 0)) < 3000) {
            log('debug', `⏰ [TIMER] Skipping ${cid} - throttling`);
            return;
        }
        isProcessingDelayed[cid] = true;
        lastProcessTime[cid] = now;

        log('info', '⏰ [TIMER] Starting processDelayed for community: ' + cid);

        // Получаем vk_group_id для корректного имени файла
        let fileCommunityId = cid;
        try {
            const { getCommunityConfig } = require('./config');
            const config = await getCommunityConfig(cid, profileId);
            if (config && config.vk_group_id) {
                fileCommunityId = config.vk_group_id.toString();
            }
        } catch(e) {}

        log('debug', '⏰ [TIMER] fileCommunityId: ' + fileCommunityId);

        const delayed = await getSheetData('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);
        const messages = await getSheetData('СООБЩЕНИЯ', fileCommunityId, profileId);
        const comments = await getSheetData('КОММЕНТАРИИ В ПОСТАХ', fileCommunityId, profileId);

        // Получаем vk_group_id и токен для отправки
        const { getCommunityConfig, getVkToken: getCommunityToken } = require('./config');
        const communityConfig = await getCommunityConfig(cid, profileId);
        const actualGroupId = (communityConfig && communityConfig.vk_group_id) ? communityConfig.vk_group_id.toString() : fileCommunityId;

        log('debug', `⏰ [TIMER] actualGroupId: ${actualGroupId}`);

        const currentTime = new Date();
        const currentMskStr = formatMskDateTime(currentTime);
        let hasChanges = false;

        for (const item of delayed) {
            if (item['Статус'] !== 'Ожидает') continue;

            // Парсим запланированное время как московское (UTC+3)
            const scheduledTimeStr = item['Дата и время отправки'];
            if (!scheduledTimeStr || typeof scheduledTimeStr !== 'string') {
                item['Статус'] = 'Ошибка';
                item['Ошибка'] = 'Не указана дата и время отправки';
                hasChanges = true;
                continue;
            }
            const scheduledTime = new Date(scheduledTimeStr.replace(' ', 'T') + '+03:00');
            if (Number.isNaN(scheduledTime.getTime())) {
                item['Статус'] = 'Ошибка';
                item['Ошибка'] = 'Некорректная дата и время отправки';
                hasChanges = true;
                continue;
            }
            if (scheduledTime > currentTime) {
                log('debug', `⏰ [TIMER] Not yet: scheduled ${scheduledTimeStr}, now MSK ${currentMskStr}`);
                continue;
            }

            const userId = item['ID Пользователя'];
            const stepName = item['Шаг'];
            const type = item['Тип'] || 'message';

            // Проверка дублирования
            const uniqueKey = `${item['ID Пользователя']}_${item['Шаг']}_${scheduledTimeStr}`;
            if (processedDelayedMessages.has(uniqueKey)) {
                continue;
            }

            // Блокировка
            processedDelayedMessages.set(uniqueKey, Date.now());
            item['Статус'] = 'В обработке';

            const rows = type === 'comment' ? comments : messages;
            const row = rows.find(r => (r['Шаг'] || '').trim() === stepName);

            if (row) {
                const answer = row['Ответ'] || '';
                const processedAnswer = await replaceVariables(answer, userId, actualGroupId, cid, profileId);

                // Получаем вложения из строки СООБЩЕНИЙ (где они реально хранятся)
                let attachments = [];
                try {
                    attachments = getAttachmentsFromRow(row, 'MESSAGES') || [];
                } catch(e) {}
                if (!attachments || attachments.length === 0) {
                    // Если в отложенном нет вложений — ищем в сообщениях строку с таким шагом
                    for (const msg of messages) {
                        if ((msg['Шаг'] || '').trim() === stepName) {
                            attachments = getAttachmentsFromRow(msg, 'MESSAGES') || [];
                            break;
                        }
                    }
                }
                log('debug', `⏰ [DELAYED] Attachments for step ${stepName}: ${JSON.stringify(attachments)}`);

                try {
                    const keyboard = createKeyboard(row, 'Кнопка Ответа', 'Цвет/Ссылка Ответа');
                    const token = await getCommunityToken(0, cid, profileId);
                    const sendSuccess = await sendVkMessage(userId, processedAnswer, keyboard, actualGroupId, attachments, token);

                    if (sendSuccess && !sendSuccess.error) {
                        item['Статус'] = 'Отправлено';
                        // Сохраняем московское время
                        const mskStr = currentMskStr;
                        item['Факт. время отправки (по мск.)'] = mskStr;
                        item['Фактическое время отправки'] = mskStr; // совместимость
                        hasChanges = true;

                        log('debug', `🛠️ Executing actions from step ${stepName}`);
                        const { performRowActions } = require('./row-actions');
                        const actionGroupId = type === 'comment' ? `-${actualGroupId}` : actualGroupId;
                        await performRowActions(row, userId, actionGroupId, type === 'comment', cid, profileId);

                        log('info', `✅ Delayed message #${item['№']} sent to ${userId} at ${mskStr} мск.`);
                        await addAppLog({
                            tab: 'DELAYED',
                            title: 'Отправлено отложенное сообщение',
                            summary: 'Шаг ' + stepName + ' отправлен пользователю ' + userId,
                            details: ['Время: ' + mskStr, 'Тип: ' + type],
                            communityId: fileCommunityId,
                            profileId
                        });
                    } else {
                        item['Статус'] = 'Ошибка';
                        item['Ошибка'] = sendSuccess.error?.error_msg || 'VK API returned false';
                        hasChanges = true;
                    }
                } catch (sendError) {
                    item['Статус'] = 'Ошибка';
                    item['Ошибка'] = sendError.message;
                    hasChanges = true;
                }
            }
        }

        if (hasChanges) {
            await saveSheetData('ОТЛОЖЕННЫЕ', delayed, fileCommunityId, profileId);
            invalidateCache('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);
        }
    } catch (error) {
        log('error', '❌ Error in processDelayed:', error);
    } finally {
        isProcessingDelayed[cid] = false;
    }
}

/**
 * Обработать рассылки
 */
async function processMailing(communityId = null, profileId = '1') {
    try {
        const now = Date.now();
        const cid = communityId || getActiveCommunityId(profileId) || 'default';

        // Per-community throttling для рассылок
        const mailingKey = 'mailing_' + cid;
        if ((now - (lastProcessTime[mailingKey] || 0)) < 3000) {
            log('debug', `📢 [MAILING] Skipping ${cid} - throttling`);
            return;
        }
        lastProcessTime[mailingKey] = now;

        log('info', '📢 [MAILING] Starting processMailing for community: ' + cid);

        // Получаем vk_group_id для корректного имени файла
        let fileCommunityId = cid;
        try {
            const { getCommunityConfig } = require('./config');
            const config = await getCommunityConfig(cid, profileId);
            if (config && config.vk_group_id) {
                fileCommunityId = config.vk_group_id.toString();
            }
        } catch(e) {}

        invalidateCache('РАССЫЛКА', fileCommunityId, profileId);
        const mailing = await getSheetData('РАССЫЛКА', fileCommunityId, profileId);
        if (!mailing || mailing.length === 0) {
            log('debug', '📢 [MAILING] No mailing rows found');
            return;
        }

        // Получаем vk_group_id и токен для отправки
        const { getCommunityConfig, getVkToken: getCommunityToken } = require('./config');
        const communityConfig = await getCommunityConfig(cid, profileId);
        const actualGroupId = (communityConfig && communityConfig.vk_group_id) ? communityConfig.vk_group_id.toString() : fileCommunityId;

        log('debug', `📢 [MAILING] actualGroupId: ${actualGroupId}`);

        const currentTime = new Date();
        const currentMskStr = formatMskDateTime(currentTime);

        log('debug', `📢 [MAILING] Current MSK time: ${currentMskStr}`);

        let changed = false;

        for (let i = 0; i < mailing.length; i++) {
            const row = mailing[i];
            // Пробуем оба варианта имени поля
            const scheduledVal = row['Дата и время отправки (по мск.)'] || row['Дата и время отправки'] || row['Фактическое время отправки (по мск.)'];
            log('debug', `📢 [MAILING] Row ${i} status: "${row['Статус']}", scheduled: "${scheduledVal}"`);
            log('debug', `📢 [MAILING] Row ${i} keys: ${Object.keys(row).join(', ')}`);
            if (row['Статус'] !== 'Ожидает') {
                log('debug', `📢 [MAILING] Row ${i} skipped: status="${row['Статус']}"`);
                continue;
            }

            const mailingKey = `mail_${row['№'] || i}_${scheduledVal}`;

            if (processedMailings.has(mailingKey)) continue;

            const scheduledTimeStr = scheduledVal;
            if (!scheduledTimeStr?.trim()) {
                log('warn', `📢 [MAILING] Row ${i} skipped: scheduled time is empty/undefined. Set "Дата и время отправки (по мск.)" field!`);
                continue;
            }

            // Парсим время как московское (UTC+3)
            let scheduledMskTime;
            try {
                // Добавляем +03:00 чтобы JS понял что это МСК
                scheduledMskTime = new Date(scheduledTimeStr.replace(' ', 'T') + '+03:00');
                if (isNaN(scheduledMskTime.getTime())) throw new Error('Invalid date');
            } catch (e) {
                log('error', `❌ Mailing ${mailingKey}: invalid date "${scheduledTimeStr}"`);
                continue;
            }

            // Сравниваем с текущим МСК временем
            if (scheduledMskTime > currentTime) {
                log('debug', `📢 [MAILING] Row ${i} not yet: scheduled ${scheduledTimeStr}, now MSK ${currentMskStr}`);
                continue;
            }

            log('info', `📢 [MAILING] Processing row ${i}, scheduled: ${scheduledTimeStr}, now MSK: ${currentMskStr}`);

            // Блокировка
            processedMailings.set(mailingKey, Date.now());
            row['Статус'] = 'В обработке';
            row['Фактическое время отправки'] = currentMskStr;
            row['Факт. время отправки (по мск.)'] = row['Фактическое время отправки'];

            await saveSheetData('РАССЫЛКА', mailing, fileCommunityId, profileId);
            invalidateCache('РАССЫЛКА', fileCommunityId, profileId);

            // Сбор получателей
            const userIds = await collectMailingRecipients(row, fileCommunityId, profileId);

            log('info', `📢 [MAILING] Raw recipients: ${JSON.stringify(userIds)}`);

            // Если recipients empty — возможно нет пользователей с указанной группой
            if (userIds.length === 0) {
                row['Статус'] = 'Ошибка';
                row['Ошибка'] = 'Нет получателей (проверьте ID/Группу в настройках рассылки)';
                await saveSheetData('РАССЫЛКА', mailing, fileCommunityId, profileId);
                invalidateCache('РАССЫЛКА', fileCommunityId, profileId);
                continue;
            }

            // Отправка
            const messageText = row['Сообщение Рассылки'] || '';
            const attachments = getAttachmentsFromRow(row, 'MAILING');
            const keyboard = createMailingKeyboard(row);

            log('info', `📢 [MAILING] Sending to ${userIds.length} users: "${messageText.substring(0, 50)}"`);

            let successCount = 0;
            let errorCount = 0;

            for (const userId of userIds) {
                try {
                    const token = await getCommunityToken(0, cid, profileId);
                    const success = await sendVkMessage(userId, messageText, keyboard, actualGroupId, attachments, token);

                    if (success && !success.error) {
                        successCount++;
                        log('debug', `📢 Sent to ${userId}`);
                    } else {
                        errorCount++;
                        log('error', `❌ Mailing send error to ${userId}: ${success?.error?.error_msg}`);
                    }
                } catch (err) {
                    errorCount++;
                    log('error', `❌ Error sending to ${userId}:`, err.message);
                }
            }

            // Обновление статуса
            if (errorCount === 0) {
                row['Статус'] = 'Отправлено';
                row['Ошибка'] = '';
            } else if (successCount === 0) {
                row['Статус'] = 'Ошибка';
                row['Ошибка'] = `Не удалось отправить ни одному (${errorCount} ошибок)`;
            } else {
                row['Статус'] = 'Отправлено (с ошибками)';
                row['Ошибка'] = `Отправлено: ${successCount}, ошибок: ${errorCount}`;
            }

            row['Факт. время отправки (по мск.)'] = currentMskStr;
            row['Фактическое время отправки'] = row['Факт. время отправки (по мск.)'];
            changed = true;

            log('info', `📢 Mailing row ${i} processed: sent to ${successCount}/${userIds.length}`);
            await addAppLog({
                tab: 'MAILING',
                title: 'Выполнена рассылка',
                summary: 'Отправлено ' + successCount + ' из ' + userIds.length,
                details: [
                    messageText ? 'Текст: "' + String(messageText).substring(0, 120) + '"' : 'Сообщение без текста',
                    errorCount ? 'Ошибок: ' + errorCount : ''
                ],
                communityId: fileCommunityId,
                profileId
            });
        }

        if (changed) {
            await saveSheetData('РАССЫЛКА', mailing, fileCommunityId, profileId);
            invalidateCache('РАССЫЛКА', fileCommunityId, profileId);
        }
    } catch (error) {
        log('error', '❌ Error in processMailing:', error);
    }
}

/**
 * Собрать получателей рассылки
 */
async function collectMailingRecipients(row, communityId, profileId = '1') {
    let userIds = [];

    log('debug', `📢 [MAILING] collectMailingRecipients: communityId=${communityId}`);
    log('debug', `📢 [MAILING] ID Получателей: "${row['ID Получателей']}"`);
    log('debug', `📢 [MAILING] ГРУППА Получателей: "${row['ГРУППА Получателей']}"`);

    // По ID
    const idsRaw = row['ID Получателей'] || '';
    if (idsRaw) {
        userIds.push(...idsRaw.split(/[\r\n,]+/).map(id => id.trim()).filter(id => id && /^\d+$/.test(id)));
        log('debug', `📢 [MAILING] Found ${userIds.length} recipients by ID`);
    }

    // По группам
    const groupsRaw = row['ГРУППА Получателей'] || '';
    if (groupsRaw) {
        const requiredGroups = groupsRaw.split(/[\r\n,]+/).map(g => g.trim().toLowerCase()).filter(g => g);
        log('debug', `📢 [MAILING] Required groups: ${JSON.stringify(requiredGroups)}`);

        if (requiredGroups.length) {
            const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
            log('debug', `📢 [MAILING] Loaded ${users.length} users`);

            for (const user of users) {
                const userGroups = (user['ГРУППА'] || '').split(/[\r\n,]+/).map(g => g.trim().toLowerCase()).filter(g => g);

                if (requiredGroups.some(req => userGroups.includes(req)) && !userIds.includes(user['ID'])) {
                    userIds.push(user['ID']);
                    log('debug', `📢 [MAILING] Matched user ${user['ID']} with groups: ${JSON.stringify(userGroups)}`);
                }
            }
        }
    }

    log('debug', `📢 [MAILING] Total recipients: ${userIds.length}`);
    return [...new Set(userIds)];
}

module.exports = {
    processDelayed,
    processMailing
};
