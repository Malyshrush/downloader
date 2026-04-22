/**
 * Модуль обработки комментариев
 */

const crypto = require('crypto');
const { log } = require('../utils/logger');
const { getSheetData } = require('./storage');
const { getVkToken } = require('./config');
const { sendComment: sendCommentVK } = require('./vk-api');
const { getAttachmentsFromRow, processAttachmentForComment } = require('./attachments');
const { replaceVariables } = require('./variables');
const { checkTriggerExists, checkTriggerMatch, checkAllConditions, normalizeTriggerMode } = require('./triggers');
const { updateUserData } = require('./users');
const { addAppLog } = require('./app-logs');
const { publishOutboundAction } = require('./event-queue');

// Кэш обработанных комментариев
const processedComments = new Map();
const COMMENT_TTL = 5 * 60 * 1000;

setInterval(() => {
    const now = Date.now();
    for (const [key, timestamp] of processedComments.entries()) {
        if (now - timestamp > COMMENT_TTL) {
            processedComments.delete(key);
        }
    }
}, 60000);

function buildCommentRowFingerprint(row = {}) {
    const normalized = {
        answer: row['РћС‚РІРµС‚'] || '',
        fallbackAnswer: row['Р—Р°РіРѕС‚РѕРІР»РµРЅРЅС‹Р№ РѕС‚РІРµС‚'] || '',
        trigger: row['РўСЂРёРіРіРµСЂ'] || ''
    };

    return crypto
        .createHash('sha1')
        .update(JSON.stringify(normalized))
        .digest('hex')
        .slice(0, 12);
}

function buildCommentOutboundAction(actionType, { comment, groupId, row, communityId, profileId }) {
    return {
        actionId: [
            'vk',
            'outbound',
            actionType,
            String(communityId || 'default'),
            String(profileId || '1'),
            String(comment?.id || 'no_comment_id'),
            String(comment?.from_id || '0'),
            buildCommentRowFingerprint(row)
        ].join(':'),
        actionType,
        communityId: String(communityId || 'default'),
        profileId: String(profileId || '1'),
        createdAt: new Date().toISOString(),
        source: 'comment-handler',
        payload: {
            comment,
            groupId,
            row,
            communityId: String(communityId || 'default'),
            profileId: String(profileId || '1')
        }
    };
}

/**
 * Обработать комментарий
 */
async function handleComment(data, profileId = '1') {
    try {
        const comment = data.object;
        
        // Очищаем текст от упоминаний сообщества
        let text = (comment.text || '').trim();
        text = text.replace(/\[club\d+\|[^\]]*\]/gi, '')
                   .replace(/\[public\d+\|[^\]]*\]/gi, '')
                   .replace(/@club\d+/gi, '')
                   .replace(/@public\d+/gi, '')
                   .trim();
        
        const userId = comment.from_id;
        const groupId = data.group_id;
        const communityId = groupId?.toString();
        let parsedPayload = null;
        if (comment.payload) {
            if (typeof comment.payload === 'string') {
                try {
                    parsedPayload = JSON.parse(comment.payload);
                } catch (e) {
                    parsedPayload = null;
                }
            } else if (typeof comment.payload === 'object') {
                parsedPayload = comment.payload;
            }
        }
        const eventContext = {
            payload: parsedPayload,
            attachments: Array.isArray(comment.attachments) ? comment.attachments : []
        };

        if (Array.isArray(comment.attachments) && comment.attachments.length) {
            log('debug', `💬 Comment event attachments: ${comment.attachments.map(item => item?.type || 'unknown').join(',')}`);
        } else {
            log('debug', '💬 Comment event attachments: none');
        }

        if (userId < 0) {
            log('debug', `💬 Comment from community, skipping`);
            return;
        }

        const postIdentifier = `-${groupId}_${comment.post_id}`;

        log('info', `💬 New comment from ${userId}: ${text}`);
        await addAppLog({
            tab: 'COMMENTS',
            title: 'Новый комментарий',
            summary: text ? 'Комментарий: "' + text + '"' : 'Комментарий без текста',
            details: ['Пользователь: ' + userId, 'Пост: ' + postIdentifier],
            communityId,
            profileId
        });

        // Проверка дублирования
        const commentKey = `comment_${comment.id}`;
        if (processedComments.has(commentKey)) {
            log('debug', `⚠️ Comment already processed: ${commentKey}`);
            return;
        }
        processedComments.set(commentKey, Date.now());

        await updateUserData(userId, communityId, profileId);

        const comments = await getSheetData('КОММЕНТАРИИ В ПОСТАХ', communityId, profileId);
        if (!comments) {
            log('error', `❌ Failed to load comments data`);
            return;
        }

        let matchedRowWithConditions = null;
        let matchedRowWithoutConditions = null;
        let fallbackRow = null;

        for (let i = 0; i < comments.length; i++) {
            const row = comments[i];
            const trigger = (row['Триггер'] || '').trim();
            const fallbackAnswer = (row['Заготовленный ответ'] || '').trim();
            const triggerMode = normalizeTriggerMode(row._triggerMode);

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
                postId: postIdentifier,
                commentText: text,
                eventType: 'comment',
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

        // Отправка ответа
        if (matchedRowWithConditions) {
            await publishOutboundAction(buildCommentOutboundAction('send_comment_response', {
                comment,
                groupId,
                row: matchedRowWithConditions,
                communityId,
                profileId
            }));
        } else if (matchedRowWithoutConditions) {
            await publishOutboundAction(buildCommentOutboundAction('send_comment_fallback', {
                comment,
                groupId,
                row: matchedRowWithoutConditions,
                communityId,
                profileId
            }));
        } else if (fallbackRow) {
            await publishOutboundAction(buildCommentOutboundAction('send_comment_fallback', {
                comment,
                groupId,
                row: fallbackRow,
                communityId,
                profileId
            }));
        }
    } catch (error) {
        log('error', 'Comment handling error:', error);
    }
}

/**
 * Отправить комментарий и выполнить действия
 */
async function sendCommentAndPerformActions(comment, groupId, row, communityId, profileId = '1') {
    try {
        const answer = (row['Ответ'] || '').trim();
        const processedAnswer = await replaceVariables(answer, comment.from_id, `-${groupId}`, communityId, profileId);
        const replyText = `@id${comment.from_id} ${processedAnswer}`;

        let attachments = getAttachmentsFromRow(row, 'COMMENTS');
        const processedAttachments = [];
        for (const attachment of attachments) {
            const prepared = await processAttachmentForComment(attachment, groupId);
            if (prepared) processedAttachments.push(prepared);
        }
        attachments = [...new Set(processedAttachments)];
        log('info', `📎 [COMMENT] Total attachments: ${attachments.length} → ${attachments.join(',')}`);

        const token = await getVkToken(0, communityId, profileId);
        const success = await sendCommentVK(-groupId, comment.post_id, replyText, comment.id, attachments, token);
        
        if (success && !success.error) {
            log('info', `✅ Comment sent successfully with ${attachments.length} attachments`);
            
            const { performRowActions } = require('./row-actions');
            await performRowActions(row, comment.from_id, `-${groupId}`, true, communityId, profileId);

            await addAppLog({
                tab: 'COMMENTS',
                title: 'Отправлен ответ на комментарий',
                summary: 'Пост: ' + comment.post_id,
                details: [
                    'Пользователь: ' + comment.from_id,
                    processedAnswer ? 'Ответ: "' + processedAnswer + '"' : 'Ответ без текста',
                    attachments.length ? 'Вложений: ' + attachments.length : ''
                ],
                communityId,
                profileId
            });
        } else {
            log('error', `❌ Comment send FAILED: ${success.error?.error_msg}`);
        }
    } catch (error) {
        log('error', '❌ Error sending comment:', error);
    }
}

/**
 * Отправить fallback комментарий
 */
async function sendFallbackCommentFromRow(comment, groupId, row, communityId, profileId = '1') {
    try {
        const fallbackAnswer = (row['Заготовленный ответ'] || '').trim();
        if (!fallbackAnswer) return;

        const processedAnswer = await replaceVariables(fallbackAnswer, comment.from_id, `-${groupId}`, communityId, profileId);
        const replyText = `@id${comment.from_id} ${processedAnswer}`;

        log('debug', `💬 Sending fallback comment to ${comment.from_id}`);

        const token = await getVkToken(0, communityId, profileId);
        await sendCommentVK(-groupId, comment.post_id, replyText, comment.id, null, token);
        log('debug', `✅ Fallback comment sent successfully`);
    } catch (error) {
        log('error', '❌ Error sending fallback comment:', error);
    }
}

module.exports = {
    handleComment,
    sendCommentAndPerformActions,
    sendFallbackCommentFromRow,
    __testOnly: {
        buildCommentOutboundAction
    }
};
