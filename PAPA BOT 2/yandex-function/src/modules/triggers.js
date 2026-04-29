/**
 * Модуль обработки триггеров
 */

const { log } = require('../utils/logger');
const { checkVariableConditions } = require('./variables');
const { checkUserGroups, getUserCurrentSteps } = require('./users');

function normalizeTriggerMode(mode) {
    const normalized = String(mode || 'TEXT').trim().toUpperCase();
    return ['TEXT', 'BUTTON', 'FILE'].includes(normalized) ? normalized : 'TEXT';
}

function getButtonLabelFromEvent(eventContext = {}) {
    if (!eventContext || !eventContext.payload) return '';
    const payload = eventContext.payload;
    return String(payload.buttonLabel || payload.label || '').trim();
}

function getAttachmentDebugInfo(eventContext = {}) {
    const attachments = Array.isArray(eventContext.attachments) ? eventContext.attachments : [];
    return attachments.map((attachment, index) => {
        const type = attachment?.type || 'unknown';
        const ext = attachment?.doc?.ext || attachment?.audio_message?.link_ogg || '';
        return `${index + 1}:${type}${ext && typeof ext === 'string' && !ext.startsWith('http') ? '.' + ext : ''}`;
    }).join(', ');
}

function checkFileTriggerByExtension(trigger, eventContext = {}) {
    const attachments = Array.isArray(eventContext.attachments) ? eventContext.attachments : [];
    if (!attachments.length) {
        return { matched: false, reason: 'no_attachments' };
    }

    const rawFilter = String(trigger || '').trim();
    if (!rawFilter) {
        return { matched: true, reason: 'any_attachment' };
    }

    const filters = rawFilter.split(/[\s,;\n]+/).map(item => item.trim().replace(/^\./, '').toLowerCase()).filter(Boolean);
    if (!filters.length) {
        return { matched: true, reason: 'empty_filter_after_parse' };
    }

    for (const attachment of attachments) {
        const type = String(attachment?.type || '').toLowerCase();
        const docExt = String(attachment?.doc?.ext || '').toLowerCase();
        const candidates = [type, docExt].filter(Boolean);
        if (filters.some(filter => candidates.includes(filter))) {
            return { matched: true, reason: `matched_${type || docExt}`, filter: filters.join(',') };
        }
    }

    return { matched: false, reason: 'extension_mismatch', filter: filters.join(',') };
}

/**
 * Проверить существование триггера в тексте
 */
async function checkTriggerExists(text, trigger, triggerMode = 'TEXT', eventContext = {}) {
    const mode = normalizeTriggerMode(triggerMode);
    log('debug', `🧭 checkTriggerExists: mode=${mode}, trigger="${trigger || ''}", text="${text || ''}", buttonLabel="${getButtonLabelFromEvent(eventContext)}", attachments=[${getAttachmentDebugInfo(eventContext)}]`);

    if (mode === 'FILE') {
        const fileCheck = checkFileTriggerByExtension(trigger, eventContext);
        log('debug', `🧭 FILE trigger exists check: matched=${fileCheck.matched}, reason=${fileCheck.reason}, filter=${fileCheck.filter || 'any'}`);
        return fileCheck.matched;
    }

    if (mode === 'BUTTON') {
        const buttonLabel = getButtonLabelFromEvent(eventContext);
        const matched = !!buttonLabel;
        log('debug', `🧭 BUTTON trigger exists check: buttonLabel="${buttonLabel}", matched=${matched}`);
        return matched;
    }

    if (!trigger) return false;
    
    const triggers = trigger.split(',').map(t => t.trim()).filter(t => t);
    const lowerText = text.toLowerCase();
    
    return triggers.some(t => lowerText.includes(t.toLowerCase()));
}

/**
 * Проверить точное совпадение триггера
 */
async function checkTriggerMatch(text, trigger, matchType, caseSensitive, userId, groupId, communityId = null, profileId = '1', triggerMode = 'TEXT', eventContext = {}) {
    try {
        const mode = normalizeTriggerMode(triggerMode);
        log('debug', `🧭 checkTriggerMatch: mode=${mode}, trigger="${trigger || ''}", text="${text || ''}", buttonLabel="${getButtonLabelFromEvent(eventContext)}", attachments=[${getAttachmentDebugInfo(eventContext)}]`);

        if (mode === 'FILE') {
            const fileCheck = checkFileTriggerByExtension(trigger, eventContext);
            log('debug', `🔍 FILE trigger mode: matched=${fileCheck.matched}, reason=${fileCheck.reason}, filter=${fileCheck.filter || 'any'}`);
            return fileCheck.matched;
        }

        const sourceText = mode === 'BUTTON' ? getButtonLabelFromEvent(eventContext) : text;
        if (mode === 'BUTTON' && !sourceText) {
            log('debug', '❌ BUTTON trigger mode: buttonLabel not found in payload');
            return false;
        }

        if (!trigger || !trigger.trim()) {
            log('debug', `❌ No trigger specified`);
            return false;
        }
        
        let cleanTrigger = trigger.trim();

        // Замена переменных в триггере
        if (cleanTrigger.includes('{$') || cleanTrigger.includes('[$') || cleanTrigger.includes('%')) {
            log('debug', `🔍 Trigger contains variables, replacing...`);
            const { replaceVariables } = require('./variables');
            const processedTrigger = await replaceVariables(cleanTrigger, userId, groupId, communityId, profileId);
            cleanTrigger = processedTrigger.trim();
        }

        log('debug', `🔍 Trigger: original= "${trigger}", processed= "${cleanTrigger}"`);

        if (!cleanTrigger) {
            log('debug', `❌ Empty trigger after processing`);
            return false;
        }

        const triggerPatterns = cleanTrigger.split(',').map(t => t.trim()).filter(t => t);
        log('debug', `🔍 Trigger patterns: [${triggerPatterns.map(p => `"${p}"`).join(', ')}]`);

        // Нормализация matchType
        const rawMatchType = (matchType || '').trim().replace(/\s+/g, '').toUpperCase();
        const actualMatchType = rawMatchType || 'НЕТОЧНО';
        const isCaseSensitive = String(caseSensitive || '').trim().toLowerCase() === 'важно';

        log('debug', `🔍 Match type RAW="${matchType}" → NORMALIZED="${actualMatchType}", Case sensitive: ${isCaseSensitive}`);

        for (const pattern of triggerPatterns) {
            log('debug', `🔍 Checking pattern: "${pattern}" vs source text: "${sourceText}" (mode=${mode})`);

            let baseMatch = false;
            const textLower = sourceText.toLowerCase();
            const patternLower = pattern.toLowerCase();

            if (actualMatchType === 'ТОЧНО') {
                baseMatch = textLower === patternLower;
                log('debug', `🔍 [Step 1/2] EXACT match (case-insensitive): ${baseMatch}`);
            } else {
                baseMatch = textLower.includes(patternLower);
                log('debug', `🔍 [Step 1/2] CONTAINS match (case-insensitive): ${baseMatch}`);
            }

            if (!baseMatch) {
                log('debug', `❌ [Step 1/2] FAILED: Base match not found`);
                continue;
            }

            let finalMatch = baseMatch;

            if (isCaseSensitive && baseMatch) {
                if (actualMatchType === 'ТОЧНО') {
                    finalMatch = text === pattern;
                    if (mode === 'BUTTON') finalMatch = sourceText === pattern;
                    log('debug', `🔍 [Step 2/2] EXACT match (case-sensitive): ${finalMatch}`);
                } else {
                    finalMatch = sourceText.includes(pattern);
                    log('debug', `🔍 [Step 2/2] CONTAINS match (case-sensitive): ${finalMatch}`);
                }
            } else {
                log('debug', `🔍 [Step 2/2] SKIP: Case sensitivity not required`);
            }

            if (finalMatch) {
                log('debug', `✅ Pattern "${pattern}" MATCHED!`);
                return true;
            } else {
                log('debug', `❌ Pattern "${pattern}" not matched (failed at step 2)`);
            }
        }

        log('debug', `❌ No trigger patterns matched`);
        return false;
    } catch (error) {
        log('error', `❌ Error in checkTriggerMatch:`, error);
        return false;
    }
}

/**
 * Проверить все условия строки
 */
async function checkAllConditions(row, options) {
    const { userId, groupId, text, eventType = 'message', postId, commentText, communityId = null, profileId = '1' } = options;
    const cid = communityId;

    // 1. Глобальная переменная
    const globalVarCondition = (row['Глобальная'] || '').trim();
    if (globalVarCondition) {
        const varConditionsMet = await checkVariableConditions('', globalVarCondition, '', userId, groupId, cid, profileId);
        if (!varConditionsMet) return false;
    }

    // 2. Пользовательская переменная
    const userVarCondition = (row['Пользовательская'] || '').trim();
    if (userVarCondition) {
        const varConditionsMet = await checkVariableConditions(userVarCondition, '', '', userId, groupId, cid, profileId);
        if (!varConditionsMet) return false;
    }

    const sharedVarCondition = (row['Переменная ПВС'] || '').trim();
    if (sharedVarCondition) {
        const varConditionsMet = await checkVariableConditions('', '', sharedVarCondition, userId, groupId, cid, profileId);
        if (!varConditionsMet) return false;
    }

    // 3. Группа
    const requiredGroups = (row['Ответить если в Группе'] || '').trim();
    if (requiredGroups) {
        if (!await checkUserGroups(userId, requiredGroups, cid, profileId)) return false;
    }

    // 4. Шаг (для сообщений)
    if (eventType === 'message') {
        const requiredStep = (row['Ответил на Шаг'] || '').trim();
        if (requiredStep) {
            const userSteps = await getUserCurrentSteps(userId, null, cid, profileId);
            if (!checkStepCondition(requiredStep, userSteps)) return false;
        }
    }

    // 5. Пост и отметка (для комментариев)
    if (eventType === 'comment') {
        const postCondition = (row['Пост'] || '').trim();
        if (postCondition && postCondition !== 'ВСЕ') {
            const lines = postCondition.split(/\r?\n/);
            let allowedNormalized = [];
            
            for (const line of lines) {
                allowedNormalized.push(...line.split(',').map(p => p.trim()).filter(p => p));
            }
            
            let postMatch = false;
            for (const candidate of allowedNormalized) {
                const normalizedCandidate = normalizePostId(candidate, Math.abs(groupId));
                if (normalizedCandidate && normalizedCandidate === postId) {
                    postMatch = true;
                    break;
                }
            }
            if (!postMatch) return false;
        }
        
        const mentionCondition = (row['Отметили'] || '').trim().toUpperCase();
        if (mentionCondition) {
            const mentioned = checkCommunityMention(commentText, groupId);
            if (mentionCondition === 'ДА' && !mentioned) return false;
            if (mentionCondition === 'НЕТ' && mentioned) return false;
        }
    }
    
    return true;
}

/**
 * Проверить условие шага
 */
function checkStepCondition(requiredSteps, userSteps) {
    if (!requiredSteps || !requiredSteps.trim()) return true;
    if (!userSteps || !userSteps.trim()) return false;

    const requiredStepsArray = requiredSteps.split(/[\r\n,]+/).map(s => s.trim()).filter(s => s);
    const userStepsArray = userSteps.split(/[\r\n,]+/).map(s => s.trim()).filter(s => s);

    log('debug', `🔍 Step check: required=[${requiredStepsArray}], user=[${userStepsArray}]`);

    return requiredStepsArray.some(step => userStepsArray.includes(step));
}

/**
 * Нормализовать ID поста
 */
function normalizePostId(input, groupId) {
    if (!input || typeof input !== 'string') return null;
    
    let trimmed = input.trim();
    if (trimmed === '' || trimmed.toUpperCase() === 'ВСЕ') return null;

    // Удаляем префиксы URL
    trimmed = trimmed.replace(/^https?:\/\/vk\.com\//, '');
    trimmed = trimmed.replace(/^vk\.com\//, '');

    // Pattern wall-xxx_yyy
    const match = trimmed.match(/wall-?(\d+)_(\d+)$/);
    if (match) {
        const ownerId = -Math.abs(parseInt(match[1]));
        const postId = parseInt(match[2]);
        return `${ownerId}_${postId}`;
    }

    // Формат -group_post
    const matchNeg = trimmed.match(/^(-?\d+)_(\d+)$/);
    if (matchNeg) {
        let ownerId = parseInt(matchNeg[1]);
        const postId = parseInt(matchNeg[2]);
        if (ownerId > 0 && groupId) ownerId = -groupId;
        return `${ownerId}_${postId}`;
    }

    // Просто номер поста
    const justNumber = parseInt(trimmed);
    if (!isNaN(justNumber) && groupId) {
        return `${-groupId}_${justNumber}`;
    }

    return null;
}

/**
 * Проверить упоминание сообщества
 */
function checkCommunityMention(text, groupId) {
    const patterns = [`[club${groupId}|`, `[public${groupId}|`, `[id-${groupId}|`];
    const lowerText = (text || '').toLowerCase();
    return patterns.some(p => lowerText.includes(p.toLowerCase()));
}

module.exports = {
    checkTriggerExists,
    checkTriggerMatch,
    normalizeTriggerMode,
    checkAllConditions,
    checkStepCondition,
    normalizePostId,
    checkCommunityMention
};
