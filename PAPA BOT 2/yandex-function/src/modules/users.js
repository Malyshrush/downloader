/**
 * Модуль управления пользователями
 */

const { getSheetData, saveSheetData, invalidateCache } = require('./storage');
const { getVkToken } = require('./config');
const { getUserName } = require('./vk-api');
const { log } = require('../utils/logger');
const { addAppLog } = require('./app-logs');

// Кэш имён пользователей
const userNamesCache = {};

/**
 * Обновить данные пользователя (добавить если новый)
 */
async function updateUserData(userId, communityId = null, profileId = '1') {
    try {
        const cid = communityId;
        log('debug', '👤 Starting updateUserData for ' + userId + ' (Community: ' + cid + ')');
        
        const userName = await getUserVKName(userId, communityId, profileId);
        if (!userName) {
            log('debug', '❌ Failed to get user name for ' + userId);
            return false;
        }
        
        log('debug', '✅ Got user name: ' + userName);
        
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', cid, profileId) || [];
        log('debug', '📊 Total users in database: ' + users.length);
        
        const existingUser = users.find(u => u['ID'] == userId);
        if (existingUser) {
            log('debug', '✅ User ' + userId + ' already exists in database');
            return true;
        }
        
        log('debug', '❌ User ' + userId + ' not found, adding to database...');
        const success = await addNewUserToSheet(userId, userName, cid, profileId);
        
        if (success) {
            log('debug', '✅ User ' + userId + ' successfully added to database');
            return true;
        }
        
        return false;
    } catch (error) {
        log('error', '❌ Error in updateUserData:', error);
        return false;
    }
}

/**
 * Получить имя пользователя из VK
 */
async function getUserVKName(userId, communityId = null, profileId = '1') {
    if (userNamesCache[userId]) {
        return userNamesCache[userId];
    }

    try {
        const token = await getVkToken(0, communityId, profileId);
        if (!token) {
            log('error', 'getVkToken() is not set!');
            return null;
        }

        const name = await getUserName(userId, token);
        if (name) {
            userNamesCache[userId] = name;
            return name;
        }
        
        return null;
    } catch (error) {
        log('error', 'Error getting user name from VK:', error);
        return null;
    }
}

/**
 * Добавить нового пользователя
 */
async function addNewUserToSheet(userId, userName, communityId = null, profileId = '1') {
    try {
        log('debug', `📝 Adding user ${userId} - ${userName}`);

        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId) || [];
        let sharedDisplayNames = '';
        let sharedDisplayValues = '';
        try {
            const { getProfileUserSharedVariables } = require('./variables');
            const sharedVars = await getProfileUserSharedVariables(userId, profileId);
            const names = [];
            const values = [];
            Object.entries(sharedVars || {}).forEach(function([name, value]) {
                names.push(String(name || '').trim());
                values.push(String(value || '').trim());
            });
            sharedDisplayNames = names.join('\n');
            sharedDisplayValues = values.join('\n');
        } catch (e) {}

        users.push({
            'ID': userId.toString(),
            'ИМЯ': userName,
            'ГРУППА': '',
            'Пользовательская': '',
            'Значения ПП': '',
            'Переменная ПВС': sharedDisplayNames,
            'Значение ПВС': sharedDisplayValues,
            'Текущий Бот': '',
            'Текущий Шаг': '',
            'Отправленные Шаги': ''
        });

        await saveSheetData('ПОЛЬЗОВАТЕЛИ', users, communityId, profileId);
        log('debug', `✅ User ${userId} added to sheet`);
        await addAppLog({
            tab: 'USERS',
            title: 'Добавлен новый пользователь',
            summary: userName,
            details: ['ID: ' + userId],
            communityId,
            profileId
        });
        return true;
    } catch (error) {
        log('error', '❌ Error adding user to sheet:', error.message);
        return false;
    }
}

/**
 * Получить переменные пользователя
 */
async function getUserVariables(userId, communityId = null, profileId = '1') {
    try {
        const cid = communityId;
        log('debug', '🔧 Getting user variables for ' + userId + ' (Community: ' + cid + ')');
        
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', cid, profileId);
        if (!users) return {};
        
        const user = users.find(u => u['ID'] == userId);
        if (!user) return {};

        const varNamesRaw = (user['Пользовательская'] || '');
        const varValuesRaw = (user['Значения ПП'] || '');
        
        const varNames = varNamesRaw.split(/[\r\n,]+/).map(v => v.trim().toLowerCase()).filter(v => v);
        const varValues = varValuesRaw.split(/[\r\n,]+/).map(v => v.trim()).filter(v => v);
        
        const variables = {};
        for (let i = 0; i < Math.min(varNames.length, varValues.length); i++) {
            variables[varNames[i]] = varValues[i];
        }
        
        return variables;
    } catch (error) {
        log('error', '❌ Error getting user variables:', error);
        return {};
    }
}

/**
 * Обновить переменные пользователя
 */
async function updateUserVariables(userId, variables, forceOverwrite = true, communityId = null, profileId = '1') {
    try {
        const cid = communityId;
        log('debug', '🔧 Updating user variables for ' + userId + ' (Community: ' + cid + ')');
        
        invalidateCache('ПОЛЬЗОВАТЕЛИ', cid, profileId);
        
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', cid, profileId);
        const idx = users.findIndex(u => u['ID'] == userId);
        if (idx === -1) return;

        const varNames = [];
        const varValues = [];
        for (const [name, value] of Object.entries(variables)) {
            varNames.push(name.trim().toLowerCase());
            varValues.push(value || '');
        }
        
        users[idx]['Пользовательская'] = varNames.join('\n');
        users[idx]['Значения ПП'] = varValues.join('\n');

        await saveSheetData('ПОЛЬЗОВАТЕЛИ', users, cid, profileId);
        invalidateCache('ПОЛЬЗОВАТЕЛИ', cid, profileId);
        try {
            const { syncUserVariableCatalog } = require('./variables');
            await syncUserVariableCatalog(varNames, cid, profileId);
        } catch (e) {}
        log('debug', '✅ User variables updated for ' + userId);
    } catch (error) {
        log('error', '❌ Error updating user variables:', error);
    }
}

/**
 * Обновить бота и шаг пользователя
 */
async function updateUserBotAndStep(userId, bot, step, communityId = null, profileId = '1') {
    try {
        log('debug', `🤖 Saving bot and step: bot="${bot}", step="${step}" for user ${userId}`);
        
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const userRow = users.find(r => r['ID'] == userId);
        if (!userRow) {
            log('error', `❌ User ${userId} not found in ПОЛЬЗОВАТЕЛИ sheet`);
            return;
        }

        const currentBotsRaw = userRow['Текущий Бот'] || '';
        const currentStepsRaw = userRow['Текущий Шаг'] || '';

        const currentBots = currentBotsRaw ? currentBotsRaw.split(/[\r\n]+/).map(b => b.trim()).filter(b => b) : [];
        const currentSteps = currentStepsRaw ? currentStepsRaw.split(/[\r\n]+/).map(s => s.trim()).filter(s => s) : [];

        let botIndex = currentBots.findIndex(b => b === bot);

        if (botIndex === -1) {
            // Новый бот - добавляем
            currentBots.push(bot);
            currentSteps.push(step);
            log('debug', `➕ Added new bot "${bot}" with step "${step}"`);
        } else {
            // Бот существует - добавляем шаг к истории
            const prevSteps = currentSteps[botIndex] || '';
            currentSteps[botIndex] = prevSteps ? `${prevSteps} ⏩ ${step}` : step;
            log('debug', `📝 Updated bot "${bot}" steps: "${currentSteps[botIndex]}"`);
        }

        userRow['Текущий Бот'] = currentBots.join('\n');
        userRow['Текущий Шаг'] = currentSteps.join('\n');

        await saveSheetData('ПОЛЬЗОВАТЕЛИ', users, communityId, profileId);
        invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);

        log('debug', `✅ Bot and step saved for user ${userId}`);
        await addAppLog({
            tab: 'USERS',
            title: 'Обновлён бот пользователя',
            summary: 'Пользователь переведён в бота ' + bot,
            details: ['Пользователь: ' + userId, 'Шаг: ' + step],
            communityId,
            profileId
        });
    } catch (error) {
        log('error', '❌ Error updating user bot and step:', error);
    }
}

/**
 * Удалить бота из списка пользователя
 */
async function removeUserBotAndStep(userId, bot, communityId = null, profileId = '1') {
    try {
        const targetBot = String(bot || '').trim();
        if (!targetBot) return;

        log('debug', `🤖 Removing bot: bot="${targetBot}" for user ${userId}`);

        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const userRow = users.find(r => r['ID'] == userId);
        if (!userRow) {
            log('error', `❌ User ${userId} not found in ПОЛЬЗОВАТЕЛИ sheet`);
            return;
        }

        const currentBots = String(userRow['Текущий Бот'] || '')
            .split(/[\r\n]+/)
            .map(item => item.trim())
            .filter(item => item);
        const currentSteps = String(userRow['Текущий Шаг'] || '')
            .split(/[\r\n]+/)
            .map(item => item.trim())
            .filter(item => item);

        const nextBots = [];
        const nextSteps = [];

        currentBots.forEach((currentBot, idx) => {
            if (currentBot !== targetBot) {
                nextBots.push(currentBot);
                nextSteps.push(currentSteps[idx] || '');
            }
        });

        userRow['Текущий Бот'] = nextBots.join('\n');
        userRow['Текущий Шаг'] = nextSteps.join('\n');

        await saveSheetData('ПОЛЬЗОВАТЕЛИ', users, communityId, profileId);
        invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);

        log('debug', `✅ Bot removed for user ${userId}: ${targetBot}`);
        await addAppLog({
            tab: 'USERS',
            title: 'Пользователь исключён из бота',
            summary: 'Удалён бот ' + targetBot,
            details: ['Пользователь: ' + userId],
            communityId,
            profileId
        });
    } catch (error) {
        log('error', '❌ Error removing user bot and step:', error);
    }
}

/**
 * Обновить группы пользователя
 */
async function updateUserGroups(userId, addGroupsStr = '', removeGroupsStr = '', communityId = null, profileId = '1') {
    try {
        log('debug', `👥 Updating groups for user ${userId}: add="${addGroupsStr}", remove="${removeGroupsStr}"`);
        
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const userRow = users.find(r => r['ID'] == userId);
        if (!userRow) {
            log('error', `❌ User ${userId} not found in ПОЛЬЗОВАТЕЛИ sheet`);
            return;
        }

        const currentGroupsRaw = (userRow['ГРУППА'] || '');
        const currentGroups = currentGroupsRaw.split(/[\r\n,]+/).map(g => g.trim().toLowerCase()).filter(g => g);
        let groupHistory = {};
        try {
            groupHistory = JSON.parse(userRow['_История групп'] || '{}') || {};
        } catch (e) {
            groupHistory = {};
        }
        const nowIso = new Date().toISOString();

        // Обработка удаления — поддержка и запятых, и новых строк
        const removeGroups = removeGroupsStr.split(/[\r\n,]+/).map(g => g.trim().toLowerCase()).filter(g => g);
        let newGroups = currentGroups.filter(group => !removeGroups.includes(group));

        // Обработка добавления с проверкой на дубликаты — поддержка и запятых, и новых строк
        const addGroups = addGroupsStr.split(/[\r\n,]+/).map(g => g.trim().toLowerCase()).filter(g => g);

        for (const group of addGroups) {
            if (!newGroups.includes(group)) {
                newGroups.push(group);
                groupHistory[group] = Object.assign({}, groupHistory[group] || {}, {
                    joinedAt: nowIso,
                    lastAction: 'joined'
                });
                log('debug', `➕ Added group: "${group}"`);
            } else {
                log('debug', `⚠️ Group "${group}" already exists for user ${userId}, skipping`);
            }
        }

        for (const group of removeGroups) {
            if (currentGroups.includes(group)) {
                groupHistory[group] = Object.assign({}, groupHistory[group] || {}, {
                    leftAt: nowIso,
                    lastAction: 'left'
                });
            }
        }

        userRow['ГРУППА'] = newGroups.join('\n');
        userRow['_История групп'] = JSON.stringify(groupHistory);
        await saveSheetData('ПОЛЬЗОВАТЕЛИ', users, communityId, profileId);
        invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);

        log('debug', `✅ Groups updated for user ${userId}: ${newGroups.join(', ')}`);
        const details = ['Пользователь: ' + userId];
        if (addGroupsStr.trim()) details.push('Добавлено: ' + addGroupsStr.trim());
        if (removeGroupsStr.trim()) details.push('Удалено: ' + removeGroupsStr.trim());
        await addAppLog({
            tab: 'USERS',
            title: 'Обновлены группы пользователя',
            summary: newGroups.length ? 'Текущие группы: ' + newGroups.join(', ') : 'Группы пользователя очищены',
            details,
            communityId,
            profileId
        });
    } catch (error) {
        log('error', '❌ Error updating user groups:', error);
    }
}

/**
 * Проверить группы пользователя
 */
async function checkUserGroups(userId, required, communityId = null, profileId = '1') {
    try {
        if (!required || !required.trim()) return true;
        
        const cid = communityId;
        log('debug', '🔍 Checking groups for user ' + userId + ' (Community: ' + cid + ')');
        
        invalidateCache('ПОЛЬЗОВАТЕЛИ', cid, profileId);
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', cid, profileId);
        const user = users.find(u => u['ID'] == userId);
        if (!user) return false;

        const userGroups = (user['ГРУППА'] || '').split(/[\r\n,]+/).map(g => g.trim().toLowerCase()).filter(g => g);
        const requiredGroupsArray = required.split(/[\r\n,]+/).map(g => g.trim().toLowerCase()).filter(g => g);
        
        return requiredGroupsArray.some(req => userGroups.includes(req));
    } catch (error) {
        log('error', '❌ Error checking user groups:', error);
        return false;
    }
}

/**
 * Проверить, был ли шаг уже отправлен
 */
async function checkStepAlreadySent(userId, bot, step, communityId = null, profileId = '1') {
    try {
        log('debug', `🔍 Checking if step "${step}" already sent to user ${userId}`);
        
        invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const user = users.find(u => u['ID'] == userId);
        if (!user) {
            log('debug', `❌ User ${userId} not found`);
            return false;
        }

        const sentStepsRaw = (user['Отправленные Шаги'] || '');
        const sentSteps = sentStepsRaw.split(/[\r\n,]+/).map(s => s.trim()).filter(s => s);
        const stepKey = `${bot}:${step}`;
        const alreadySent = sentSteps.includes(stepKey);
        
        log('debug', `🔍 Sent steps: [${sentSteps}], Checking: ${stepKey}, Already sent: ${alreadySent}`);
        return alreadySent;
    } catch (error) {
        log('error', '❌ Error checking step duplication:', error);
        return false;
    }
}

/**
 * Отметить шаг как отправленный
 */
async function markStepAsSent(userId, bot, step, communityId = null, profileId = '1') {
    try {
        log('debug', `📝 Marking step "${step}" as sent for user ${userId}`);
        
        invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const idx = users.findIndex(u => u['ID'] == userId);
        if (idx === -1) {
            log('error', `❌ User ${userId} not found`);
            return;
        }

        const sentStepsRaw = (users[idx]['Отправленные Шаги'] || '');
        const sentSteps = sentStepsRaw.split(/[\r\n,]+/).map(s => s.trim()).filter(s => s);
        const stepKey = `${bot}:${step}`;

        if (!sentSteps.includes(stepKey)) {
            sentSteps.push(stepKey);
            users[idx]['Отправленные Шаги'] = sentSteps.join('\n');
            log('debug', `✅ Marked step ${stepKey} as sent`);
            
            await saveSheetData('ПОЛЬЗОВАТЕЛИ', users, communityId, profileId);
            invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        } else {
            log('debug', `⚠️ Step ${stepKey} already marked as sent`);
        }
    } catch (error) {
        log('error', '❌ Error marking step as sent:', error);
    }
}

/**
 * Очистить историю отправленных шагов
 */
async function clearStepSentHistory(userId, bot = null, communityId = null, profileId = '1') {
    try {
        log('debug', `🗑️ Clearing step history for user ${userId}, bot: ${bot || 'all'}`);
        
        invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const idx = users.findIndex(u => u['ID'] == userId);
        if (idx === -1) {
            log('error', `❌ User ${userId} not found`);
            return;
        }

        if (bot) {
            const sentStepsRaw = (users[idx]['Отправленные Шаги'] || '');
            const sentSteps = sentStepsRaw.split(/[\r\n,]+/).map(s => s.trim()).filter(s => s)
                .filter(s => !s.startsWith(`${bot}:`));
            users[idx]['Отправленные Шаги'] = sentSteps.join('\n');
        } else {
            users[idx]['Отправленные Шаги'] = '';
        }
        
        await saveSheetData('ПОЛЬЗОВАТЕЛИ', users, communityId, profileId);
        invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        log('debug', `✅ Step history cleared`);
    } catch (error) {
        log('error', '❌ Error clearing step history:', error);
    }
}

async function deleteUserData(userId, communityId = null, profileId = '1') {
    try {
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const nextUsers = (users || []).filter(function(user) {
            return String(user['ID'] || '').trim() !== String(userId || '').trim();
        });
        if (nextUsers.length === (users || []).length) {
            return false;
        }

        await saveSheetData('ПОЛЬЗОВАТЕЛИ', nextUsers, communityId, profileId);
        invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        await addAppLog({
            tab: 'USERS',
            title: 'Удалены данные пользователя',
            summary: 'Пользователь удалён из базы профиля',
            details: ['Пользователь: ' + userId],
            communityId,
            profileId,
            level: 'warn'
        });
        return true;
    } catch (error) {
        log('error', '❌ Error deleting user data:', error);
        return false;
    }
}

/**
 * Получить текущие шаги пользователя
 */
async function getUserCurrentSteps(userId, bot = null, communityId = null, profileId = '1') {
    try {
        const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
        const user = users.find(u => u['ID'] == userId);
        if (!user) return '';

        const allBots = (user['Текущий Бот'] || '').split(/[\n,]+/).map(b => b.trim()).filter(b => b);
        const allSteps = (user['Текущий Шаг'] || '').split(/[\n,]+/).map(s => s.trim()).filter(s => s);

        if (bot) {
            const botIndex = allBots.findIndex(b => b === bot);
            if (botIndex !== -1 && allSteps[botIndex]) {
                return allSteps[botIndex];
            }
            return '';
        }

        return user ? (user['Текущий Шаг'] || '') : '';
    } catch (error) {
        log('error', 'Error getting user steps:', error);
        return '';
    }
}

module.exports = {
    updateUserData,
    getUserVKName,
    addNewUserToSheet,
    getUserVariables,
    updateUserVariables,
    updateUserBotAndStep,
    removeUserBotAndStep,
    updateUserGroups,
    checkUserGroups,
    checkStepAlreadySent,
    markStepAsSent,
    clearStepSentHistory,
    getUserCurrentSteps,
    deleteUserData
};
