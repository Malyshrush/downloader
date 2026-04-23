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
const { listUsers } = require('./users');
const { addAppLog } = require('./app-logs');
const { publishOutboundAction } = require('./event-queue');
const { sendMessageWithTokenRetry } = require('./messages');
const { performRowActions } = require('./row-actions');
const { createDelayedDeliveryStore } = require('./delayed-delivery-store');
const { createMailingDeliveryStore } = require('./mailing-delivery-store');

const isProcessingDelayed = {};
const lastProcessTime = {};
const delayedDeliveryStore = createDelayedDeliveryStore();
const mailingDeliveryStore = createMailingDeliveryStore();

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

const DELAYED_NUMBER_KEYS = ['№', 'в„–', 'РІвЂћвЂ“'];
const DELAYED_STEP_KEYS = ['Шаг', 'РЁР°Рі'];
const DELAYED_USER_ID_KEYS = ['ID Пользователя', 'ID РџРѕР»СЊР·РѕРІР°С‚РµР»СЏ'];
const DELAYED_TYPE_KEYS = ['Тип', 'РўРёРї'];
const DELAYED_SCHEDULED_AT_KEYS = ['Дата и время отправки', 'Р”Р°С‚Р° Рё РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё'];
const DELAYED_STATUS_KEYS = ['Статус', 'РЎС‚Р°С‚СѓСЃ', 'Р РЋРЎвЂљР В°РЎвЂљРЎС“РЎРѓ'];
const DELAYED_ERROR_KEYS = ['Ошибка', 'РћС€РёР±РєР°', 'Р С›РЎв‚¬Р С‘Р В±Р С”Р В°'];
const DELAYED_SENT_AT_KEYS = ['Фактическое время отправки', 'Р¤Р°РєС‚РёС‡РµСЃРєРѕРµ РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё'];
const DELAYED_SENT_AT_MSK_KEYS = ['Факт. время отправки (по мск.)', 'Р¤Р°РєС‚. РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)'];
const MESSAGE_STEP_KEYS = ['Шаг', 'РЁР°Рі'];
const MESSAGE_ANSWER_KEYS = ['Ответ', 'РћС‚РІРµС‚'];
const MAILING_NUMBER_KEYS = ['№', 'в„–', 'РІвЂћвЂ“'];
const MAILING_STATUS_KEYS = ['Статус', 'РЎС‚Р°С‚СѓСЃ'];
const MAILING_ERROR_KEYS = ['Ошибка', 'РћС€РёР±РєР°'];
const MAILING_SCHEDULED_AT_KEYS = [
    'Дата и время отправки (по мск.)',
    'Р”Р°С‚Р° Рё РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)',
    'Дата и время отправки',
    'Р”Р°С‚Р° Рё РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё',
    'Фактическое время отправки (по мск.)',
    'Р¤Р°РєС‚РёС‡РµСЃРєРѕРµ РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)'
];
const MAILING_MESSAGE_TEXT_KEYS = ['Сообщение Рассылки', 'РЎРѕРѕР±С‰РµРЅРёРµ Р Р°СЃСЃС‹Р»РєРё'];
const MAILING_SENT_AT_KEYS = ['Фактическое время отправки', 'Р¤Р°РєС‚РёС‡РµСЃРєРѕРµ РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё'];
const MAILING_SENT_AT_MSK_KEYS = ['Факт. время отправки (по мск.)', 'Р¤Р°РєС‚. РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)'];

function getFirstDefinedValue(row, keys) {
    for (const key of keys) {
        if (!row || !Object.prototype.hasOwnProperty.call(row, key)) continue;
        const value = row[key];
        if (value !== undefined && value !== null && String(value).trim() !== '') {
            return value;
        }
    }
    return '';
}

function setAllValues(row, keys, value) {
    for (const key of keys) {
        row[key] = value;
    }
}

function getDelayedRowNumber(row) {
    return String(getFirstDefinedValue(row, DELAYED_NUMBER_KEYS) || '').trim();
}

function getDelayedStatus(row) {
    return String(getFirstDefinedValue(row, DELAYED_STATUS_KEYS) || '').trim();
}

function getDelayedStepName(row) {
    return String(getFirstDefinedValue(row, DELAYED_STEP_KEYS) || '').trim();
}

function getDelayedUserId(row) {
    return String(getFirstDefinedValue(row, DELAYED_USER_ID_KEYS) || '').trim();
}

function getDelayedType(row) {
    return String(getFirstDefinedValue(row, DELAYED_TYPE_KEYS) || 'message').trim() || 'message';
}

function getDelayedScheduledAt(row) {
    return String(getFirstDefinedValue(row, DELAYED_SCHEDULED_AT_KEYS) || '').trim();
}

function setDelayedStatus(row, value) {
    setAllValues(row, DELAYED_STATUS_KEYS, value);
}

function setDelayedError(row, value) {
    setAllValues(row, DELAYED_ERROR_KEYS, value);
}

function setDelayedSentAt(row, value) {
    setAllValues(row, DELAYED_SENT_AT_KEYS, value);
    setAllValues(row, DELAYED_SENT_AT_MSK_KEYS, value);
}

function getMessageStepName(row) {
    return String(getFirstDefinedValue(row, MESSAGE_STEP_KEYS) || '').trim();
}

function getMessageAnswer(row) {
    return String(getFirstDefinedValue(row, MESSAGE_ANSWER_KEYS) || '').trim();
}

function getMailingRowNumber(row, fallback = '') {
    return String(getFirstDefinedValue(row, MAILING_NUMBER_KEYS) || fallback || '').trim();
}

function getMailingStatus(row) {
    return String(getFirstDefinedValue(row, MAILING_STATUS_KEYS) || '').trim();
}

function isPendingMailingStatus(status) {
    const value = String(status || '').trim();
    return value === 'Ожидает' || value === 'РћР¶РёРґР°РµС‚';
}

function isSentMailingStatus(status) {
    const value = String(status || '').trim();
    return value === 'Отправлено' || value.startsWith('Отправлено') || value === 'РћС‚РїСЂР°РІР»РµРЅРѕ' || value.startsWith('РћС‚РїСЂР°РІР»РµРЅРѕ');
}

function getMailingScheduledAt(row) {
    return String(getFirstDefinedValue(row, MAILING_SCHEDULED_AT_KEYS) || '').trim();
}

function getMailingMessageText(row) {
    return String(getFirstDefinedValue(row, MAILING_MESSAGE_TEXT_KEYS) || '').trim();
}

function setMailingStatus(row, value) {
    setAllValues(row, MAILING_STATUS_KEYS, value);
}

function setMailingError(row, value) {
    setAllValues(row, MAILING_ERROR_KEYS, value);
}

function setMailingSentAt(row, value) {
    setAllValues(row, MAILING_SENT_AT_KEYS, value);
    setAllValues(row, MAILING_SENT_AT_MSK_KEYS, value);
}

function findRowByNumber(rows, rowNumber) {
    const expected = String(rowNumber || '').trim();
    return rows.find(row => getDelayedRowNumber(row) === expected);
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
    const rowNumber = getMailingRowNumber(row, rowIndex + 1);
    const scheduledTimeStr = getMailingScheduledAt(row);
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

function getDelayedDeliveryStore(overrides = {}) {
    return overrides.delayedDeliveryStore || delayedDeliveryStore;
}

function isDelayedDeliveryStoreEnabled(overrides = {}) {
    const store = getDelayedDeliveryStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function getMailingDeliveryStore(overrides = {}) {
    return overrides.mailingDeliveryStore || mailingDeliveryStore;
}

function isMailingDeliveryStoreEnabled(overrides = {}) {
    const store = getMailingDeliveryStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
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

        if (isDelayedDeliveryStoreEnabled(overrides)) {
            const { fileCommunityId, actualGroupId } = await getCommunityFileContext(cid, profileId, overrides);
            const delayedRows = await getDelayedDeliveryStore(overrides).listDueRows(fileCommunityId, now, profileId);
            const messages = await getSheetDataImpl('РЎРћРћР‘Р©Р•РќРРЇ', fileCommunityId, profileId);
            const comments = await getSheetDataImpl('РљРћРњРњР•РќРўРђР РР Р’ РџРћРЎРўРђРҐ', fileCommunityId, profileId);
            let queuedCount = 0;

            for (const item of delayedRows) {
                if (item._delayedId) {
                    const userId = String(item['ID Пользователя'] || '').trim();
                    const stepName = String(item['Шаг'] || '').trim();
                    const type = String(item['Тип'] || 'message').trim() || 'message';
                    const rowSet = type === 'comment' ? comments : messages;
                    const row = rowSet.find(entry => String(entry['Шаг'] || '').trim() === stepName);
                    if (!row) continue;

                    const uniqueKey = item._delayedId || `${userId}_${stepName}_${String(item['Дата и время отправки'] || '')}`;
                    if (processedDelayedMessages.has(uniqueKey)) continue;
                    processedDelayedMessages.set(uniqueKey, nowTs);

                    await getDelayedDeliveryStore(overrides).updateDelayedRow(fileCommunityId, item._delayedId, function(rowDraft) {
                        rowDraft['Статус'] = 'В обработке';
                        rowDraft['Ошибка'] = '';
                        return { value: rowDraft };
                    }, profileId);

                    const actionId = buildSchedulerActionId('scheduler_delayed', [
                        profileId,
                        fileCommunityId,
                        item._delayedId,
                        item['Дата и время отправки'] || '',
                        userId,
                        stepName,
                        type
                    ]);
                    await publishOutboundActionImpl({
                        actionId,
                        actionType: 'send_delayed_delivery',
                        profileId: String(profileId || '1'),
                        communityId: String(cid || ''),
                        traceId: actionId,
                        payload: {
                            delayedRowNumber: item._delayedId,
                            delayedId: item._delayedId,
                            scheduledTimeStr: item['Дата и время отправки'] || '',
                            userId,
                            stepName,
                            type,
                            fileCommunityId,
                            actualGroupId,
                            communityId: String(cid || ''),
                            profileId: String(profileId || '1')
                        }
                    });
                    queuedCount += 1;
                    continue;
                }
                const userId = String(item['ID РџРѕР»СЊР·РѕРІР°С‚РµР»СЏ'] || '').trim();
                const stepName = String(item['РЁР°Рі'] || '').trim();
                const type = String(item['РўРёРї'] || 'message').trim() || 'message';
                const rowSet = type === 'comment' ? comments : messages;
                const row = rowSet.find(entry => String(entry['РЁР°Рі'] || '').trim() === stepName);
                const delayedId = String(item._delayedId || item['в„–'] || '').trim();

                const resolvedRow = row || (rowSet.length === 1 ? rowSet[0] : null);
                if (!resolvedRow) {
                    if (delayedId) {
                        await getDelayedDeliveryStore(overrides).updateDelayedRow(fileCommunityId, delayedId, function(rowDraft) {
                            rowDraft['РЎС‚Р°С‚СѓСЃ'] = 'РћС€РёР±РєР°';
                            rowDraft['РћС€РёР±РєР°'] = `РќРµ РЅР°Р№РґРµРЅ С€Р°Рі ${stepName || '(РїСѓСЃС‚Рѕ)'}`;
                            return { value: rowDraft };
                        }, profileId);
                    }
                    continue;
                }

                const uniqueKey = `${userId}_${stepName}_${String(item['Р”Р°С‚Р° Рё РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё'] || '')}`;
                if (processedDelayedMessages.has(uniqueKey)) continue;
                processedDelayedMessages.set(uniqueKey, nowTs);

                if (delayedId) {
                    await getDelayedDeliveryStore(overrides).updateDelayedRow(fileCommunityId, delayedId, function(rowDraft) {
                        rowDraft['РЎС‚Р°С‚СѓСЃ'] = 'Р’ РѕР±СЂР°Р±РѕС‚РєРµ';
                        rowDraft['РћС€РёР±РєР°'] = '';
                        return { value: rowDraft };
                    }, profileId);
                }

                await publishOutboundActionImpl(buildDelayedDeliveryAction({
                    item,
                    communityId: cid,
                    profileId,
                    fileCommunityId,
                    actualGroupId
                }));
                queuedCount += 1;
            }

            return {
                ok: true,
                queuedCount,
                fileCommunityId,
                actualGroupId
            };
        }

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

function applyMailingRuntimeState(row, state) {
    if (!state) return row;
    const merged = Object.assign({}, row);
    const status = getMailingStatus(state);
    const error = getFirstDefinedValue(state, MAILING_ERROR_KEYS);
    const sentAt = getFirstDefinedValue(state, MAILING_SENT_AT_KEYS);
    const sentAtMsk = getFirstDefinedValue(state, MAILING_SENT_AT_MSK_KEYS);

    if (status) {
        setMailingStatus(merged, status);
    }
    if (error || getFirstDefinedValue(state, MAILING_ERROR_KEYS) === '') {
        setMailingError(merged, error);
    }
    if (sentAt || sentAtMsk) {
        setMailingSentAt(merged, sentAtMsk || sentAt);
    }
    return merged;
}

async function loadMailingRuntimeState(row, fileCommunityId, profileId, overrides = {}) {
    if (!isMailingDeliveryStoreEnabled(overrides)) {
        return null;
    }
    const mailingId = getMailingRowNumber(row);
    if (!mailingId) {
        return null;
    }
    return getMailingDeliveryStore(overrides).getMailingState(fileCommunityId, mailingId, profileId);
}

async function updateMailingRuntimeState(fileCommunityId, mailingId, profileId, overrides = {}, mutator) {
    if (isMailingDeliveryStoreEnabled(overrides)) {
        return getMailingDeliveryStore(overrides).updateMailingState(fileCommunityId, mailingId, mutator, profileId);
    }
    return null;
}

async function processMailingWithDependencies(communityId = null, profileId = '1', overrides = {}) {
    const getActiveCommunityIdImpl = overrides.getActiveCommunityId || getActiveCommunityId;
    const getSheetDataImpl = overrides.getSheetData || getSheetData;
    const saveSheetDataImpl = overrides.saveSheetData || saveSheetData;
    const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
    const publishOutboundActionImpl = overrides.publishOutboundAction || publishOutboundAction;
    const useStructuredMailingStore = isMailingDeliveryStoreEnabled(overrides);
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
            const rowNumber = getMailingRowNumber(row, i + 1);
            const runtimeState = await loadMailingRuntimeState(row, fileCommunityId, profileId, overrides);
            const effectiveRow = applyMailingRuntimeState(row, runtimeState);

            if (!isPendingMailingStatus(getMailingStatus(effectiveRow))) {
                continue;
            }

            const scheduledValue = getMailingScheduledAt(effectiveRow);
            const schedule = parseScheduledTime(scheduledValue);
            if (!schedule.ok) {
                if (useStructuredMailingStore) {
                    await updateMailingRuntimeState(fileCommunityId, rowNumber, profileId, overrides, rowDraft => {
                        setMailingStatus(rowDraft, 'Ошибка');
                        setMailingError(rowDraft, schedule.error);
                        return { value: rowDraft };
                    });
                } else {
                    setMailingStatus(row, 'Ошибка');
                    setMailingError(row, schedule.error);
                    hasChanges = true;
                }
                continue;
            }

            if (schedule.date > now) {
                continue;
            }

            const mailingKey = `mail_${rowNumber || i}_${schedule.value}`;
            if (processedMailings.has(mailingKey)) {
                continue;
            }

            processedMailings.set(mailingKey, nowTs);
            if (useStructuredMailingStore) {
                await updateMailingRuntimeState(fileCommunityId, rowNumber, profileId, overrides, rowDraft => {
                    setMailingStatus(rowDraft, 'В обработке');
                    setMailingError(rowDraft, '');
                    return { value: rowDraft };
                });
            } else {
                setMailingStatus(row, 'В обработке');
                setMailingError(row, '');
            }

            try {
                const action = buildMailingDeliveryAction({
                    row: effectiveRow,
                    rowIndex: i,
                    communityId: cid,
                    profileId,
                    fileCommunityId,
                    actualGroupId
                });
                await publishOutboundActionImpl(action);
                queuedCount += 1;
                hasChanges = hasChanges || !useStructuredMailingStore;
            } catch (error) {
                if (useStructuredMailingStore) {
                    await updateMailingRuntimeState(fileCommunityId, rowNumber, profileId, overrides, rowDraft => {
                        setMailingStatus(rowDraft, 'Ошибка');
                        setMailingError(rowDraft, error.message);
                        return { value: rowDraft };
                    });
                } else {
                    setMailingStatus(row, 'Ошибка');
                    setMailingError(row, error.message);
                    hasChanges = true;
                }
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
    const delayedId = getDelayedRowNumber(item);
    if (isDelayedDeliveryStoreEnabled(overrides) && delayedId) {
        await getDelayedDeliveryStore(overrides).updateDelayedRow(fileCommunityId, delayedId, rowDraft => {
            setDelayedStatus(rowDraft, 'Ошибка');
            setDelayedError(rowDraft, String(message || 'Unknown scheduler delivery error'));
            return { value: rowDraft };
        }, profileId);
        return;
    }

    const saveSheetDataImpl = overrides.saveSheetData || saveSheetData;
    const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
    setDelayedStatus(item, 'Ошибка');
    setDelayedError(item, String(message || 'Unknown scheduler delivery error'));
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

    const messages = await getSheetDataImpl('СООБЩЕНИЯ', fileCommunityId, profileId);
    const comments = await getSheetDataImpl('КОММЕНТАРИИ В ПОСТАХ', fileCommunityId, profileId);
    const useStructuredDelayedStore = isDelayedDeliveryStoreEnabled(overrides);
    const delayed = useStructuredDelayedStore
        ? null
        : await getSheetDataImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);
    const item = useStructuredDelayedStore
        ? await getDelayedDeliveryStore(overrides).getDelayedRow(fileCommunityId, rowNumber, profileId)
        : findRowByNumber(delayed, rowNumber);

    if (!item) {
        return { skipped: true, reason: 'row_missing', delayedRowNumber: rowNumber };
    }

    if (getDelayedStatus(item) === 'Отправлено') {
        return { skipped: true, reason: 'already_sent', delayedRowNumber: rowNumber };
    }

    const stepName = getDelayedStepName(item);
    const type = getDelayedType(item);
    const userId = getDelayedUserId(item);
    const rowSet = type === 'comment' ? comments : messages;
    const row = rowSet.find(entry => getMessageStepName(entry) === stepName);

    if (!row) {
        await markDelayedDeliveryError(item, delayed, fileCommunityId, profileId, `Не найден шаг ${stepName || '(пусто)'}`, overrides);
        return { ok: false, delayedRowNumber: rowNumber, reason: 'missing_step' };
    }

    const answer = getMessageAnswer(row);
    const processedAnswer = await replaceVariablesImpl(answer, userId, actualGroupId, communityId, profileId);
    let attachments = safeGetAttachments(getAttachmentsFromRowImpl, row, 'MESSAGES');

    if (attachments.length === 0) {
        const messageRow = messages.find(entry => getMessageStepName(entry) === stepName);
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
        if (useStructuredDelayedStore) {
            await getDelayedDeliveryStore(overrides).updateDelayedRow(fileCommunityId, rowNumber, rowDraft => {
                setDelayedStatus(rowDraft, 'Отправлено');
                setDelayedError(rowDraft, '');
                setDelayedSentAt(rowDraft, currentMskStr);
                return { value: rowDraft };
            }, profileId);
        } else {
            setDelayedStatus(item, 'Отправлено');
            setDelayedError(item, '');
            setDelayedSentAt(item, currentMskStr);
            await saveSheetDataImpl('ОТЛОЖЕННЫЕ', delayed, fileCommunityId, profileId);
            invalidateCacheImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);
        }

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
    const useStructuredMailingStore = isMailingDeliveryStoreEnabled(overrides);

    const mailing = await getSheetDataImpl('РАССЫЛКА', fileCommunityId, profileId);
    const row = findRowByNumber(mailing, rowNumber);

    if (!row) {
        return { skipped: true, reason: 'row_missing', mailingRowNumber: rowNumber };
    }

    const runtimeState = await loadMailingRuntimeState(row, fileCommunityId, profileId, overrides);
    const effectiveRow = applyMailingRuntimeState(row, runtimeState);

    if (isSentMailingStatus(getMailingStatus(effectiveRow))) {
        return { skipped: true, reason: 'already_sent', mailingRowNumber: rowNumber };
    }

    const userIds = await collectMailingRecipientsImpl(effectiveRow, fileCommunityId, profileId);
    if (!userIds.length) {
        if (useStructuredMailingStore) {
            await updateMailingRuntimeState(fileCommunityId, rowNumber, profileId, overrides, rowDraft => {
                setMailingStatus(rowDraft, 'Ошибка');
                setMailingError(rowDraft, 'Нет получателей (проверьте ID/Группу в настройках рассылки)');
                return { value: rowDraft };
            });
        } else {
            setMailingStatus(row, 'Ошибка');
            setMailingError(row, 'Нет получателей (проверьте ID/Группу в настройках рассылки)');
            await saveSheetDataImpl('РАССЫЛКА', mailing, fileCommunityId, profileId);
            invalidateCacheImpl('РАССЫЛКА', fileCommunityId, profileId);
        }
        return { ok: false, mailingRowNumber: rowNumber, reason: 'no_recipients' };
    }

    const messageText = getMailingMessageText(effectiveRow);
    const attachments = safeGetAttachments(getAttachmentsFromRowImpl, effectiveRow, 'MAILING');
    const keyboard = createMailingKeyboardImpl(effectiveRow);
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
        setMailingStatus(effectiveRow, 'Отправлено');
        setMailingError(effectiveRow, '');
    } else if (successCount === 0) {
        setMailingStatus(effectiveRow, 'Ошибка');
        setMailingError(effectiveRow, `Не удалось отправить ни одному (${errorCount} ошибок)`);
    } else {
        setMailingStatus(effectiveRow, 'Отправлено (с ошибками)');
        setMailingError(effectiveRow, `Отправлено: ${successCount}, ошибок: ${errorCount}`);
    }

    const currentMskStr = formatMskDateTime(now);
    setMailingSentAt(effectiveRow, currentMskStr);

    if (useStructuredMailingStore) {
        await updateMailingRuntimeState(fileCommunityId, rowNumber, profileId, overrides, rowDraft => {
            setMailingStatus(rowDraft, getMailingStatus(effectiveRow));
            setMailingError(rowDraft, getFirstDefinedValue(effectiveRow, MAILING_ERROR_KEYS));
            setMailingSentAt(rowDraft, currentMskStr);
            return { value: rowDraft };
        });
    } else {
        setMailingStatus(row, getMailingStatus(effectiveRow));
        setMailingError(row, getFirstDefinedValue(effectiveRow, MAILING_ERROR_KEYS));
        setMailingSentAt(row, currentMskStr);
        await saveSheetDataImpl('РАССЫЛКА', mailing, fileCommunityId, profileId);
        invalidateCacheImpl('РАССЫЛКА', fileCommunityId, profileId);
    }

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
            const users = await listUsers(communityId, profileId);

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
