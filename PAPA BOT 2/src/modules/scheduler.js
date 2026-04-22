/**
 * Queue-driven processing for delayed messages and mailings.
 * The timer scans due rows and enqueues outbound actions; the sender worker performs delivery.
 */

const { log } = require('../utils/logger');
const { getSheetData, saveSheetData, invalidateCache } = require('./storage');
const { getCommunityConfig, getActiveCommunityId } = require('./config');
const { createKeyboard, createMailingKeyboard } = require('./keyboard');
const { getAttachmentsFromRow } = require('./attachments');
const { replaceVariables } = require('./variables');
const { addAppLog } = require('./app-logs');
const { publishOutboundAction } = require('./event-queue');
const { sendMessageWithTokenRetry } = require('./messages');
const { performRowActions } = require('./row-actions');

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

setInterval(() => {
    const now = Date.now();

    for (const [key, ts] of processedDelayedMessages.entries()) {
        if (now - ts > DELAYED_TTL) processedDelayedMessages.delete(key);
    }

    for (const [key, ts] of processedMailings.entries()) {
        if (now - ts > MAILING_TTL) processedMailings.delete(key);
    }
}, 60000);

function sanitizeActionIdPart(value) {
    return String(value || '')
        .trim()
        .replace(/\s+/g, '_')
        .replace(/[^a-zA-Z0-9:_-]/g, '_')
        .slice(0, 120);
}

function buildSchedulerActionId(prefix, parts) {
    const normalizedParts = Array.isArray(parts)
        ? parts.map(part => sanitizeActionIdPart(part)).filter(Boolean)
        : [];
    return [prefix, ...normalizedParts].join(':');
}

function findRowByNumber(rows, rowNumber) {
    const expected = String(rowNumber || '').trim();
    return rows.find(row => String(row['№'] || '').trim() === expected);
}

function parseScheduledTime(rawValue) {
    const value = String(rawValue || '').trim();
    if (!value) {
        return { ok: false, error: 'Не указана дата и время отправки' };
    }

    const parsed = new Date(value.replace(' ', 'T') + '+03:00');
    if (Number.isNaN(parsed.getTime())) {
        return { ok: false, error: 'Некорректная дата и время отправки' };
    }

    return { ok: true, value, date: parsed };
}

function safeGetAttachments(getAttachmentsFromRowImpl, row, scope) {
    try {
        return getAttachmentsFromRowImpl(row, scope) || [];
    } catch (error) {
        log('debug', `Attachment extraction failed for scope ${scope}: ${error.message}`);
        return [];
    }
}

async function getCommunityFileContext(communityId, profileId = '1', overrides = {}) {
    if (typeof overrides.getCommunityFileContext === 'function') {
        return overrides.getCommunityFileContext(communityId, profileId);
    }

    const getCommunityConfigImpl = overrides.getCommunityConfig || getCommunityConfig;
    let fileCommunityId = communityId;

    try {
        const config = await getCommunityConfigImpl(communityId, profileId);
        if (config && config.vk_group_id) {
            fileCommunityId = config.vk_group_id.toString();
        }
    } catch (error) {
        log('debug', `Community config fallback for ${communityId}: ${error.message}`);
    }

    return {
        fileCommunityId,
        actualGroupId: String(fileCommunityId || communityId || 'default')
    };
}

function buildDelayedDeliveryAction({
    item,
    communityId,
    profileId,
    fileCommunityId,
    actualGroupId
}) {
    const rowNumber = String(item['№'] || '').trim();
    const scheduledTimeStr = String(item['Дата и время отправки'] || '').trim();
    const userId = String(item['ID Пользователя'] || '').trim();
    const stepName = String(item['Шаг'] || '').trim();
    const type = String(item['Тип'] || 'message').trim() || 'message';
    const actionId = buildSchedulerActionId('scheduler_delayed', [
        profileId,
        fileCommunityId,
        rowNumber,
        scheduledTimeStr,
        userId,
        stepName,
        type
    ]);

    return {
        actionId,
        actionType: 'send_delayed_delivery',
        profileId: String(profileId || '1'),
        communityId: String(communityId || ''),
        traceId: actionId,
        payload: {
            delayedRowNumber: rowNumber,
            scheduledTimeStr,
            userId,
            stepName,
            type,
            fileCommunityId,
            actualGroupId,
            communityId: String(communityId || ''),
            profileId: String(profileId || '1')
        }
    };
}

function buildMailingDeliveryAction({
    row,
    rowIndex,
    communityId,
    profileId,
    fileCommunityId,
    actualGroupId
}) {
    const rowNumber = String(row['№'] || rowIndex + 1).trim();
    const scheduledTimeStr = String(
        row['Дата и время отправки (по мск.)'] ||
        row['Дата и время отправки'] ||
        row['Фактическое время отправки (по мск.)'] ||
        ''
    ).trim();
    const actionId = buildSchedulerActionId('scheduler_mailing', [
        profileId,
        fileCommunityId,
        rowNumber,
        scheduledTimeStr
    ]);

    return {
        actionId,
        actionType: 'send_mailing_delivery',
        profileId: String(profileId || '1'),
        communityId: String(communityId || ''),
        traceId: actionId,
        payload: {
            mailingRowNumber: rowNumber,
            scheduledTimeStr,
            fileCommunityId,
            actualGroupId,
            communityId: String(communityId || ''),
            profileId: String(profileId || '1')
        }
    };
}

async function processDelayedWithDependencies(communityId = null, profileId = '1', overrides = {}) {
    const getActiveCommunityIdImpl = overrides.getActiveCommunityId || getActiveCommunityId;
    const getSheetDataImpl = overrides.getSheetData || getSheetData;
    const saveSheetDataImpl = overrides.saveSheetData || saveSheetData;
    const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
    const publishOutboundActionImpl = overrides.publishOutboundAction || publishOutboundAction;
    const cid = communityId || getActiveCommunityIdImpl(profileId) || 'default';

    try {
        const now = overrides.now instanceof Date ? overrides.now : new Date();
        const nowTs = now.getTime();

        if (isProcessingDelayed[cid] || (nowTs - (lastProcessTime[cid] || 0)) < 3000) {
            log('debug', `⏰ [TIMER] Skipping ${cid} - throttling`);
            return { ok: true, queuedCount: 0, throttled: true };
        }

        isProcessingDelayed[cid] = true;
        lastProcessTime[cid] = nowTs;

        log('info', '⏰ [TIMER] Starting processDelayed for community: ' + cid);

        const { fileCommunityId, actualGroupId } = await getCommunityFileContext(cid, profileId, overrides);
        const delayed = await getSheetDataImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);
        const messages = await getSheetDataImpl('СООБЩЕНИЯ', fileCommunityId, profileId);
        const comments = await getSheetDataImpl('КОММЕНТАРИИ В ПОСТАХ', fileCommunityId, profileId);

        let hasChanges = false;
        let queuedCount = 0;

        for (const item of delayed) {
            if (item['Статус'] !== 'Ожидает') continue;

            const schedule = parseScheduledTime(item['Дата и время отправки']);
            if (!schedule.ok) {
                item['Статус'] = 'Ошибка';
                item['Ошибка'] = schedule.error;
                hasChanges = true;
                continue;
            }

            if (schedule.date > now) {
                continue;
            }

            const userId = String(item['ID Пользователя'] || '').trim();
            const stepName = String(item['Шаг'] || '').trim();
            const type = String(item['Тип'] || 'message').trim() || 'message';
            const rowSet = type === 'comment' ? comments : messages;
            const row = rowSet.find(entry => String(entry['Шаг'] || '').trim() === stepName);

            if (!row) {
                item['Статус'] = 'Ошибка';
                item['Ошибка'] = `Не найден шаг ${stepName || '(пусто)'}`;
                hasChanges = true;
                continue;
            }

            const uniqueKey = `${userId}_${stepName}_${schedule.value}`;
            if (processedDelayedMessages.has(uniqueKey)) {
                continue;
            }

            processedDelayedMessages.set(uniqueKey, nowTs);
            item['Статус'] = 'В обработке';
            item['Ошибка'] = '';

            try {
                const action = buildDelayedDeliveryAction({
                    item,
                    communityId: cid,
                    profileId,
                    fileCommunityId,
                    actualGroupId
                });
                await publishOutboundActionImpl(action);
                queuedCount += 1;
                hasChanges = true;
            } catch (error) {
                item['Статус'] = 'Ошибка';
                item['Ошибка'] = error.message;
                hasChanges = true;
            }
        }

        if (hasChanges) {
            await saveSheetDataImpl('ОТЛОЖЕННЫЕ', delayed, fileCommunityId, profileId);
            invalidateCacheImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);
        }

        return {
            ok: true,
            queuedCount,
            fileCommunityId,
            actualGroupId
        };
    } catch (error) {
        log('error', '❌ Error in processDelayed:', error);
        return {
            ok: false,
            queuedCount: 0,
            error: error.message
        };
    } finally {
        isProcessingDelayed[cid] = false;
    }
}

async function processMailingWithDependencies(communityId = null, profileId = '1', overrides = {}) {
    const getActiveCommunityIdImpl = overrides.getActiveCommunityId || getActiveCommunityId;
    const getSheetDataImpl = overrides.getSheetData || getSheetData;
    const saveSheetDataImpl = overrides.saveSheetData || saveSheetData;
    const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
    const publishOutboundActionImpl = overrides.publishOutboundAction || publishOutboundAction;
    const cid = communityId || getActiveCommunityIdImpl(profileId) || 'default';

    try {
        const now = overrides.now instanceof Date ? overrides.now : new Date();
        const nowTs = now.getTime();
        const mailingThrottleKey = 'mailing_' + cid;

        if ((nowTs - (lastProcessTime[mailingThrottleKey] || 0)) < 3000) {
            log('debug', `📢 [MAILING] Skipping ${cid} - throttling`);
            return { ok: true, queuedCount: 0, throttled: true };
        }

        lastProcessTime[mailingThrottleKey] = nowTs;

        const { fileCommunityId, actualGroupId } = await getCommunityFileContext(cid, profileId, overrides);
        invalidateCacheImpl('РАССЫЛКА', fileCommunityId, profileId);
        const mailing = await getSheetDataImpl('РАССЫЛКА', fileCommunityId, profileId);
        if (!mailing || mailing.length === 0) {
            return { ok: true, queuedCount: 0, fileCommunityId, actualGroupId };
        }

        let hasChanges = false;
        let queuedCount = 0;

        for (let i = 0; i < mailing.length; i++) {
            const row = mailing[i];
            if (row['Статус'] !== 'Ожидает') {
                continue;
            }

            const scheduledValue =
                row['Дата и время отправки (по мск.)'] ||
                row['Дата и время отправки'] ||
                row['Фактическое время отправки (по мск.)'];
            const schedule = parseScheduledTime(scheduledValue);
            if (!schedule.ok) {
                row['Статус'] = 'Ошибка';
                row['Ошибка'] = schedule.error;
                hasChanges = true;
                continue;
            }

            if (schedule.date > now) {
                continue;
            }

            const mailingKey = `mail_${row['№'] || i}_${schedule.value}`;
            if (processedMailings.has(mailingKey)) {
                continue;
            }

            processedMailings.set(mailingKey, nowTs);
            row['Статус'] = 'В обработке';
            row['Ошибка'] = '';

            try {
                const action = buildMailingDeliveryAction({
                    row,
                    rowIndex: i,
                    communityId: cid,
                    profileId,
                    fileCommunityId,
                    actualGroupId
                });
                await publishOutboundActionImpl(action);
                queuedCount += 1;
                hasChanges = true;
            } catch (error) {
                row['Статус'] = 'Ошибка';
                row['Ошибка'] = error.message;
                hasChanges = true;
            }
        }

        if (hasChanges) {
            await saveSheetDataImpl('РАССЫЛКА', mailing, fileCommunityId, profileId);
            invalidateCacheImpl('РАССЫЛКА', fileCommunityId, profileId);
        }

        return {
            ok: true,
            queuedCount,
            fileCommunityId,
            actualGroupId
        };
    } catch (error) {
        log('error', '❌ Error in processMailing:', error);
        return {
            ok: false,
            queuedCount: 0,
            error: error.message
        };
    }
}

async function markDelayedDeliveryError(item, delayedRows, fileCommunityId, profileId, message, overrides = {}) {
    const saveSheetDataImpl = overrides.saveSheetData || saveSheetData;
    const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
    item['Статус'] = 'Ошибка';
    item['Ошибка'] = String(message || 'Unknown scheduler delivery error');
    await saveSheetDataImpl('ОТЛОЖЕННЫЕ', delayedRows, fileCommunityId, profileId);
    invalidateCacheImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);
}

async function processDelayedDeliveryActionWithDependencies(action, overrides = {}) {
    const payload = action && action.payload ? action.payload : {};
    const rowNumber = String(payload.delayedRowNumber || '').trim();
    const fileCommunityId = String(payload.fileCommunityId || payload.communityId || '').trim();
    const actualGroupId = String(payload.actualGroupId || fileCommunityId || '').trim();
    const communityId = String(payload.communityId || '').trim();
    const profileId = String(payload.profileId || '1').trim() || '1';

    if (!rowNumber || !fileCommunityId) {
        throw new Error('Invalid delayed delivery payload');
    }

    const now = overrides.now instanceof Date ? overrides.now : new Date();
    const getSheetDataImpl = overrides.getSheetData || getSheetData;
    const saveSheetDataImpl = overrides.saveSheetData || saveSheetData;
    const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
    const replaceVariablesImpl = overrides.replaceVariables || replaceVariables;
    const getAttachmentsFromRowImpl = overrides.getAttachmentsFromRow || getAttachmentsFromRow;
    const createKeyboardImpl = overrides.createKeyboard || createKeyboard;
    const sendMessageWithTokenRetryImpl = overrides.sendMessageWithTokenRetry || sendMessageWithTokenRetry;
    const performRowActionsImpl = overrides.performRowActions || performRowActions;
    const addAppLogImpl = overrides.addAppLog || addAppLog;

    const delayed = await getSheetDataImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);
    const messages = await getSheetDataImpl('СООБЩЕНИЯ', fileCommunityId, profileId);
    const comments = await getSheetDataImpl('КОММЕНТАРИИ В ПОСТАХ', fileCommunityId, profileId);
    const item = findRowByNumber(delayed, rowNumber);

    if (!item) {
        return { skipped: true, reason: 'row_missing', delayedRowNumber: rowNumber };
    }

    if (String(item['Статус'] || '').trim() === 'Отправлено') {
        return { skipped: true, reason: 'already_sent', delayedRowNumber: rowNumber };
    }

    const stepName = String(item['Шаг'] || '').trim();
    const type = String(item['Тип'] || 'message').trim() || 'message';
    const userId = String(item['ID Пользователя'] || '').trim();
    const rowSet = type === 'comment' ? comments : messages;
    const row = rowSet.find(entry => String(entry['Шаг'] || '').trim() === stepName);

    if (!row) {
        await markDelayedDeliveryError(item, delayed, fileCommunityId, profileId, `Не найден шаг ${stepName || '(пусто)'}`, overrides);
        return { ok: false, delayedRowNumber: rowNumber, reason: 'missing_step' };
    }

    const answer = String(row['Ответ'] || '').trim();
    const processedAnswer = await replaceVariablesImpl(answer, userId, actualGroupId, communityId, profileId);
    let attachments = safeGetAttachments(getAttachmentsFromRowImpl, row, 'MESSAGES');

    if (attachments.length === 0) {
        const messageRow = messages.find(entry => String(entry['Шаг'] || '').trim() === stepName);
        if (messageRow) {
            attachments = safeGetAttachments(getAttachmentsFromRowImpl, messageRow, 'MESSAGES');
        }
    }

    try {
        const keyboard = createKeyboardImpl(row, 'Кнопка Ответа', 'Цвет/Ссылка Ответа');
        const sendSuccess = await sendMessageWithTokenRetryImpl(
            userId,
            processedAnswer,
            keyboard,
            actualGroupId,
            attachments,
            communityId,
            profileId
        );

        if (!sendSuccess) {
            await markDelayedDeliveryError(item, delayed, fileCommunityId, profileId, 'VK API returned false', overrides);
            return { ok: false, delayedRowNumber: rowNumber, reason: 'send_failed' };
        }

        const currentMskStr = formatMskDateTime(now);
        item['Статус'] = 'Отправлено';
        item['Ошибка'] = '';
        item['Факт. время отправки (по мск.)'] = currentMskStr;
        item['Фактическое время отправки'] = currentMskStr;
        await saveSheetDataImpl('ОТЛОЖЕННЫЕ', delayed, fileCommunityId, profileId);
        invalidateCacheImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);

        const actionGroupId = type === 'comment' ? `-${actualGroupId}` : actualGroupId;
        await performRowActionsImpl(row, userId, actionGroupId, type === 'comment', communityId, profileId);

        await addAppLogImpl({
            tab: 'DELAYED',
            title: 'Отправлено отложенное сообщение',
            summary: 'Шаг ' + stepName + ' отправлен пользователю ' + userId,
            details: ['Время: ' + currentMskStr, 'Тип: ' + type],
            communityId: fileCommunityId,
            profileId
        });

        return {
            ok: true,
            delayedRowNumber: rowNumber
        };
    } catch (error) {
        await markDelayedDeliveryError(item, delayed, fileCommunityId, profileId, error.message, overrides);
        return {
            ok: false,
            delayedRowNumber: rowNumber,
            reason: 'exception',
            error: error.message
        };
    }
}

async function processMailingDeliveryActionWithDependencies(action, overrides = {}) {
    const payload = action && action.payload ? action.payload : {};
    const rowNumber = String(payload.mailingRowNumber || '').trim();
    const fileCommunityId = String(payload.fileCommunityId || payload.communityId || '').trim();
    const actualGroupId = String(payload.actualGroupId || fileCommunityId || '').trim();
    const communityId = String(payload.communityId || '').trim();
    const profileId = String(payload.profileId || '1').trim() || '1';

    if (!rowNumber || !fileCommunityId) {
        throw new Error('Invalid mailing delivery payload');
    }

    const now = overrides.now instanceof Date ? overrides.now : new Date();
    const getSheetDataImpl = overrides.getSheetData || getSheetData;
    const saveSheetDataImpl = overrides.saveSheetData || saveSheetData;
    const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
    const collectMailingRecipientsImpl = overrides.collectMailingRecipients || collectMailingRecipients;
    const createMailingKeyboardImpl = overrides.createMailingKeyboard || createMailingKeyboard;
    const getAttachmentsFromRowImpl = overrides.getAttachmentsFromRow || getAttachmentsFromRow;
    const sendMessageWithTokenRetryImpl = overrides.sendMessageWithTokenRetry || sendMessageWithTokenRetry;
    const addAppLogImpl = overrides.addAppLog || addAppLog;

    const mailing = await getSheetDataImpl('РАССЫЛКА', fileCommunityId, profileId);
    const row = findRowByNumber(mailing, rowNumber);

    if (!row) {
        return { skipped: true, reason: 'row_missing', mailingRowNumber: rowNumber };
    }

    if (String(row['Статус'] || '').trim().startsWith('Отправлено')) {
        return { skipped: true, reason: 'already_sent', mailingRowNumber: rowNumber };
    }

    const userIds = await collectMailingRecipientsImpl(row, fileCommunityId, profileId);
    if (!userIds.length) {
        row['Статус'] = 'Ошибка';
        row['Ошибка'] = 'Нет получателей (проверьте ID/Группу в настройках рассылки)';
        await saveSheetDataImpl('РАССЫЛКА', mailing, fileCommunityId, profileId);
        invalidateCacheImpl('РАССЫЛКА', fileCommunityId, profileId);
        return { ok: false, mailingRowNumber: rowNumber, reason: 'no_recipients' };
    }

    const messageText = row['Сообщение Рассылки'] || '';
    const attachments = safeGetAttachments(getAttachmentsFromRowImpl, row, 'MAILING');
    const keyboard = createMailingKeyboardImpl(row);
    let successCount = 0;
    let errorCount = 0;

    for (const userId of userIds) {
        try {
            const success = await sendMessageWithTokenRetryImpl(
                userId,
                messageText,
                keyboard,
                actualGroupId,
                attachments,
                communityId,
                profileId
            );

            if (success) {
                successCount++;
            } else {
                errorCount++;
            }
        } catch (error) {
            errorCount++;
        }
    }

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

    const currentMskStr = formatMskDateTime(now);
    row['Факт. время отправки (по мск.)'] = currentMskStr;
    row['Фактическое время отправки'] = currentMskStr;

    await saveSheetDataImpl('РАССЫЛКА', mailing, fileCommunityId, profileId);
    invalidateCacheImpl('РАССЫЛКА', fileCommunityId, profileId);

    await addAppLogImpl({
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

    return {
        ok: true,
        mailingRowNumber: rowNumber,
        successCount,
        errorCount
    };
}

async function processDelayed(communityId = null, profileId = '1') {
    return processDelayedWithDependencies(communityId, profileId);
}

async function processMailing(communityId = null, profileId = '1') {
    return processMailingWithDependencies(communityId, profileId);
}

async function collectMailingRecipients(row, communityId, profileId = '1') {
    let userIds = [];

    log('debug', `📢 [MAILING] collectMailingRecipients: communityId=${communityId}`);
    log('debug', `📢 [MAILING] ID Получателей: "${row['ID Получателей']}"`);
    log('debug', `📢 [MAILING] ГРУППА Получателей: "${row['ГРУППА Получателей']}"`);

    const idsRaw = row['ID Получателей'] || '';
    if (idsRaw) {
        userIds.push(...idsRaw.split(/[\r\n,]+/).map(id => id.trim()).filter(id => id && /^\d+$/.test(id)));
    }

    const groupsRaw = row['ГРУППА Получателей'] || '';
    if (groupsRaw) {
        const requiredGroups = groupsRaw.split(/[\r\n,]+/).map(g => g.trim().toLowerCase()).filter(g => g);

        if (requiredGroups.length) {
            const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);

            for (const user of users) {
                const userGroups = (user['ГРУППА'] || '').split(/[\r\n,]+/).map(g => g.trim().toLowerCase()).filter(g => g);

                if (requiredGroups.some(req => userGroups.includes(req)) && !userIds.includes(user['ID'])) {
                    userIds.push(user['ID']);
                }
            }
        }
    }

    return [...new Set(userIds)];
}

module.exports = {
    processDelayed,
    processMailing,
    collectMailingRecipients,
    processDelayedDeliveryAction: processDelayedDeliveryActionWithDependencies,
    processMailingDeliveryAction: processMailingDeliveryActionWithDependencies,
    __testOnly: {
        buildDelayedDeliveryAction,
        buildMailingDeliveryAction,
        processDelayedWithDependencies,
        processMailingWithDependencies,
        processDelayedDeliveryActionWithDependencies,
        processMailingDeliveryActionWithDependencies,
        getCommunityFileContext,
        findRowByNumber,
        parseScheduledTime
    }
};
