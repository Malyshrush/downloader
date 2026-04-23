/**
 * Модуль управления пользователями
 */

const { getSheetData, updateSheetData } = require('./storage');
const { getVkToken } = require('./config');
const { getUserName } = require('./vk-api');
const { log } = require('../utils/logger');
const { addAppLog } = require('./app-logs');
const { createUserStateStore, buildUserScope } = require('./user-state-store');

const USERS_SHEET = 'ПОЛЬЗОВАТЕЛИ';
const COLUMN_ID = 'ID';
const COLUMN_NAME = 'ИМЯ';
const COLUMN_GROUPS = 'ГРУППА';
const COLUMN_USER_VARIABLE_NAMES = 'Пользовательская';
const COLUMN_USER_VARIABLE_VALUES = 'Значения ПП';
const COLUMN_SHARED_VARIABLE_NAMES = 'Переменная ПВС';
const COLUMN_SHARED_VARIABLE_VALUES = 'Значение ПВС';
const COLUMN_CURRENT_BOT = 'Текущий Бот';
const COLUMN_CURRENT_STEP = 'Текущий Шаг';
const COLUMN_SENT_STEPS = 'Отправленные Шаги';
const COLUMN_GROUP_HISTORY = '_История групп';

const userNamesCache = {};
const userStateStore = createUserStateStore();

function normalizeUserId(userId) {
    return String(userId || '').trim();
}

function normalizeLines(value, pattern = /[\r\n,]+/) {
    return String(value || '')
        .split(pattern)
        .map(item => item.trim())
        .filter(Boolean);
}

function normalizeLowercaseLines(value) {
    return normalizeLines(value).map(item => item.toLowerCase());
}

function findUserIndex(rows, userId) {
    const normalizedUserId = normalizeUserId(userId);
    return (Array.isArray(rows) ? rows : []).findIndex(function(row) {
        return normalizeUserId(row && row[COLUMN_ID]) === normalizedUserId;
    });
}

function getUserStateStore(overrides = {}) {
    return overrides.userStateStore || userStateStore;
}

function isStructuredUserStateEnabled(overrides = {}) {
    const store = getUserStateStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

async function getUserRowWithDependencies(userId, communityId = null, profileId = '1', overrides = {}) {
    if (isStructuredUserStateEnabled(overrides)) {
        return getUserStateStore(overrides).getUserRow(buildUserScope(communityId, profileId), userId);
    }

    const sheetGetter = overrides.getSheetData || getSheetData;
    const rows = await sheetGetter(USERS_SHEET, communityId, profileId);
    const index = findUserIndex(rows, userId);
    return index === -1 ? null : rows[index];
}

async function listUsersWithDependencies(communityId = null, profileId = '1', overrides = {}) {
    if (isStructuredUserStateEnabled(overrides)) {
        return getUserStateStore(overrides).listUserRows(buildUserScope(communityId, profileId));
    }

    const sheetGetter = overrides.getSheetData || getSheetData;
    const rows = await sheetGetter(USERS_SHEET, communityId, profileId);
    return Array.isArray(rows) ? rows : [];
}

async function mutateUserRowWithDependencies(userId, communityId = null, profileId = '1', mutator, overrides = {}) {
    if (typeof mutator !== 'function') {
        throw new Error('mutator must be a function');
    }

    if (isStructuredUserStateEnabled(overrides)) {
        return getUserStateStore(overrides).updateUserRow(
            buildUserScope(communityId, profileId),
            userId,
            mutator
        );
    }

    const sheetUpdater = overrides.updateSheetData || updateSheetData;
    let found = false;
    let value;

    const result = await sheetUpdater(USERS_SHEET, communityId, profileId, async rows => {
        const nextRows = Array.isArray(rows) ? rows : [];
        const index = findUserIndex(nextRows, userId);
        if (index === -1) {
            return nextRows;
        }

        found = true;
        const row = nextRows[index];
        const mutationResult = await mutator(row, nextRows, index);
        if (mutationResult && typeof mutationResult === 'object' && Object.prototype.hasOwnProperty.call(mutationResult, 'value')) {
            value = mutationResult.value;
        }
        return nextRows;
    });

    return {
        found,
        changed: Boolean(found && result && result.changed),
        value
    };
}

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

        const existingUser = await getUserRowWithDependencies(userId, cid, profileId);
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

async function addNewUserToSheetWithDependencies(userId, userName, communityId = null, profileId = '1', overrides = {}) {
    try {
        log('debug', `📝 Adding user ${userId} - ${userName}`);

        let sharedDisplayNames = '';
        let sharedDisplayValues = '';
        try {
            const sharedVarsGetter = overrides.getProfileUserSharedVariables
                || require('./variables').getProfileUserSharedVariables;
            const sharedVars = await sharedVarsGetter(userId, profileId);
            const names = [];
            const values = [];
            Object.entries(sharedVars || {}).forEach(function([name, value]) {
                names.push(String(name || '').trim());
                values.push(String(value || '').trim());
            });
            sharedDisplayNames = names.join('\n');
            sharedDisplayValues = values.join('\n');
        } catch (error) {}

        const nextRow = {
            [COLUMN_ID]: String(userId),
            [COLUMN_NAME]: userName,
            [COLUMN_GROUPS]: '',
            [COLUMN_USER_VARIABLE_NAMES]: '',
            [COLUMN_USER_VARIABLE_VALUES]: '',
            [COLUMN_SHARED_VARIABLE_NAMES]: sharedDisplayNames,
            [COLUMN_SHARED_VARIABLE_VALUES]: sharedDisplayValues,
            [COLUMN_CURRENT_BOT]: '',
            [COLUMN_CURRENT_STEP]: '',
            [COLUMN_SENT_STEPS]: ''
        };

        if (isStructuredUserStateEnabled(overrides)) {
            await getUserStateStore(overrides).putUserRow(buildUserScope(communityId, profileId), nextRow);
        } else {
            const sheetUpdater = overrides.updateSheetData || updateSheetData;
            await sheetUpdater(USERS_SHEET, communityId, profileId, rows => {
                const users = Array.isArray(rows) ? rows : [];
                users.push(nextRow);
                return users;
            });
        }

        log('debug', `✅ User ${userId} added to sheet`);
        const appLogger = overrides.addAppLog || addAppLog;
        await appLogger({
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

async function addNewUserToSheet(userId, userName, communityId = null, profileId = '1') {
    return addNewUserToSheetWithDependencies(userId, userName, communityId, profileId);
}

async function getUserVariablesWithDependencies(userId, communityId = null, profileId = '1', overrides = {}) {
    try {
        const cid = communityId;
        log('debug', '🔧 Getting user variables for ' + userId + ' (Community: ' + cid + ')');

        const user = await getUserRowWithDependencies(userId, cid, profileId, overrides);
        if (!user) return {};

        const varNames = normalizeLowercaseLines(user[COLUMN_USER_VARIABLE_NAMES], /[\r\n,]+/);
        const varValues = normalizeLines(user[COLUMN_USER_VARIABLE_VALUES], /[\r\n,]+/);
        const variables = {};

        for (let index = 0; index < Math.min(varNames.length, varValues.length); index += 1) {
            variables[varNames[index]] = varValues[index];
        }

        return variables;
    } catch (error) {
        log('error', '❌ Error getting user variables:', error);
        return {};
    }
}

async function getUserVariables(userId, communityId = null, profileId = '1') {
    return getUserVariablesWithDependencies(userId, communityId, profileId);
}

async function updateUserVariablesWithDependencies(userId, variables, forceOverwrite = true, communityId = null, profileId = '1', overrides = {}) {
    try {
        const cid = communityId;
        log('debug', '🔧 Updating user variables for ' + userId + ' (Community: ' + cid + ')');

        const varNames = [];
        const varValues = [];
        Object.entries(variables || {}).forEach(function([name, value]) {
            varNames.push(String(name || '').trim().toLowerCase());
            varValues.push(value || '');
        });

        const result = await mutateUserRowWithDependencies(userId, cid, profileId, userRow => {
            userRow[COLUMN_USER_VARIABLE_NAMES] = varNames.join('\n');
            userRow[COLUMN_USER_VARIABLE_VALUES] = varValues.join('\n');
            return { value: userRow };
        }, overrides);
        if (!result.found) return;

        try {
            const syncCatalog = overrides.syncUserVariableCatalog || require('./variables').syncUserVariableCatalog;
            await syncCatalog(varNames, cid, profileId);
        } catch (error) {}
        log('debug', '✅ User variables updated for ' + userId);
    } catch (error) {
        log('error', '❌ Error updating user variables:', error);
    }
}

async function updateUserVariables(userId, variables, forceOverwrite = true, communityId = null, profileId = '1') {
    return updateUserVariablesWithDependencies(userId, variables, forceOverwrite, communityId, profileId);
}

async function updateUserBotAndStepWithDependencies(userId, bot, step, communityId = null, profileId = '1', overrides = {}) {
    try {
        log('debug', `🤖 Saving bot and step: bot="${bot}", step="${step}" for user ${userId}`);

        const result = await mutateUserRowWithDependencies(userId, communityId, profileId, userRow => {
            const currentBots = normalizeLines(userRow[COLUMN_CURRENT_BOT], /[\r\n]+/);
            const currentSteps = normalizeLines(userRow[COLUMN_CURRENT_STEP], /[\r\n]+/);
            const botIndex = currentBots.findIndex(function(item) {
                return item === bot;
            });

            if (botIndex === -1) {
                currentBots.push(bot);
                currentSteps.push(step);
                log('debug', `➡ Added new bot "${bot}" with step "${step}"`);
            } else {
                const prevSteps = currentSteps[botIndex] || '';
                currentSteps[botIndex] = prevSteps ? `${prevSteps} ↩ ${step}` : step;
                log('debug', `📝 Updated bot "${bot}" steps: "${currentSteps[botIndex]}"`);
            }

            userRow[COLUMN_CURRENT_BOT] = currentBots.join('\n');
            userRow[COLUMN_CURRENT_STEP] = currentSteps.join('\n');
            return { value: userRow };
        }, overrides);

        if (!result.found) {
            log('error', `❌ User ${userId} not found in ${USERS_SHEET} sheet`);
            return;
        }

        log('debug', `✅ Bot and step saved for user ${userId}`);
        const appLogger = overrides.addAppLog || addAppLog;
        await appLogger({
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

async function updateUserBotAndStep(userId, bot, step, communityId = null, profileId = '1') {
    return updateUserBotAndStepWithDependencies(userId, bot, step, communityId, profileId);
}

async function removeUserBotAndStepWithDependencies(userId, bot, communityId = null, profileId = '1', overrides = {}) {
    try {
        const targetBot = String(bot || '').trim();
        if (!targetBot) return;

        log('debug', `🤖 Removing bot: bot="${targetBot}" for user ${userId}`);

        const result = await mutateUserRowWithDependencies(userId, communityId, profileId, userRow => {
            const currentBots = normalizeLines(userRow[COLUMN_CURRENT_BOT], /[\r\n]+/);
            const currentSteps = normalizeLines(userRow[COLUMN_CURRENT_STEP], /[\r\n]+/);
            const nextBots = [];
            const nextSteps = [];

            currentBots.forEach(function(currentBot, index) {
                if (currentBot !== targetBot) {
                    nextBots.push(currentBot);
                    nextSteps.push(currentSteps[index] || '');
                }
            });

            userRow[COLUMN_CURRENT_BOT] = nextBots.join('\n');
            userRow[COLUMN_CURRENT_STEP] = nextSteps.join('\n');
            return { value: userRow };
        }, overrides);

        if (!result.found) {
            log('error', `❌ User ${userId} not found in ${USERS_SHEET} sheet`);
            return;
        }

        log('debug', `✅ Bot removed for user ${userId}: ${targetBot}`);
        const appLogger = overrides.addAppLog || addAppLog;
        await appLogger({
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

async function removeUserBotAndStep(userId, bot, communityId = null, profileId = '1') {
    return removeUserBotAndStepWithDependencies(userId, bot, communityId, profileId);
}

async function updateUserGroupsWithDependencies(userId, addGroupsStr = '', removeGroupsStr = '', communityId = null, profileId = '1', overrides = {}) {
    try {
        log('debug', `👥 Updating groups for user ${userId}: add="${addGroupsStr}", remove="${removeGroupsStr}"`);

        let updatedGroups = [];
        const result = await mutateUserRowWithDependencies(userId, communityId, profileId, userRow => {
            const currentGroups = normalizeLowercaseLines(userRow[COLUMN_GROUPS]);
            let groupHistory = {};
            try {
                groupHistory = JSON.parse(userRow[COLUMN_GROUP_HISTORY] || '{}') || {};
            } catch (error) {
                groupHistory = {};
            }
            const nowIso = new Date().toISOString();
            const removeGroups = normalizeLowercaseLines(removeGroupsStr);
            const addGroups = normalizeLowercaseLines(addGroupsStr);
            const nextGroups = currentGroups.filter(function(group) {
                return !removeGroups.includes(group);
            });

            for (const group of addGroups) {
                if (!nextGroups.includes(group)) {
                    nextGroups.push(group);
                    groupHistory[group] = Object.assign({}, groupHistory[group] || {}, {
                        joinedAt: nowIso,
                        lastAction: 'joined'
                    });
                    log('debug', `➡ Added group: "${group}"`);
                } else {
                    log('debug', `⚠ Group "${group}" already exists for user ${userId}, skipping`);
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

            updatedGroups = nextGroups;
            userRow[COLUMN_GROUPS] = nextGroups.join('\n');
            userRow[COLUMN_GROUP_HISTORY] = JSON.stringify(groupHistory);
            return { value: userRow };
        }, overrides);

        if (!result.found) {
            log('error', `❌ User ${userId} not found in ${USERS_SHEET} sheet`);
            return;
        }

        log('debug', `✅ Groups updated for user ${userId}: ${updatedGroups.join(', ')}`);
        const details = ['Пользователь: ' + userId];
        if (String(addGroupsStr || '').trim()) details.push('Добавлено: ' + String(addGroupsStr).trim());
        if (String(removeGroupsStr || '').trim()) details.push('Удалено: ' + String(removeGroupsStr).trim());
        const appLogger = overrides.addAppLog || addAppLog;
        await appLogger({
            tab: 'USERS',
            title: 'Обновлены группы пользователя',
            summary: updatedGroups.length ? 'Текущие группы: ' + updatedGroups.join(', ') : 'Группы пользователя очищены',
            details,
            communityId,
            profileId
        });
    } catch (error) {
        log('error', '❌ Error updating user groups:', error);
    }
}

async function updateUserGroups(userId, addGroupsStr = '', removeGroupsStr = '', communityId = null, profileId = '1') {
    return updateUserGroupsWithDependencies(userId, addGroupsStr, removeGroupsStr, communityId, profileId);
}

async function checkUserGroups(userId, required, communityId = null, profileId = '1') {
    try {
        if (!required || !required.trim()) return true;

        const cid = communityId;
        log('debug', '🔍 Checking groups for user ' + userId + ' (Community: ' + cid + ')');

        const user = await getUserRowWithDependencies(userId, cid, profileId);
        if (!user) return false;

        const userGroups = normalizeLowercaseLines(user[COLUMN_GROUPS]);
        const requiredGroups = normalizeLowercaseLines(required);
        return requiredGroups.some(function(group) {
            return userGroups.includes(group);
        });
    } catch (error) {
        log('error', '❌ Error checking user groups:', error);
        return false;
    }
}

async function checkStepAlreadySent(userId, bot, step, communityId = null, profileId = '1') {
    try {
        log('debug', `🔍 Checking if step "${step}" already sent to user ${userId}`);

        const user = await getUserRowWithDependencies(userId, communityId, profileId);
        if (!user) {
            log('debug', `❌ User ${userId} not found`);
            return false;
        }

        const sentSteps = normalizeLines(user[COLUMN_SENT_STEPS]);
        const stepKey = `${bot}:${step}`;
        const alreadySent = sentSteps.includes(stepKey);
        log('debug', `🔍 Sent steps: [${sentSteps}], Checking: ${stepKey}, Already sent: ${alreadySent}`);
        return alreadySent;
    } catch (error) {
        log('error', '❌ Error checking step duplication:', error);
        return false;
    }
}

async function markStepAsSentWithDependencies(userId, bot, step, communityId = null, profileId = '1', overrides = {}) {
    try {
        log('debug', `📝 Marking step "${step}" as sent for user ${userId}`);

        const result = await mutateUserRowWithDependencies(userId, communityId, profileId, userRow => {
            const sentSteps = normalizeLines(userRow[COLUMN_SENT_STEPS]);
            const stepKey = `${bot}:${step}`;

            if (!sentSteps.includes(stepKey)) {
                sentSteps.push(stepKey);
                userRow[COLUMN_SENT_STEPS] = sentSteps.join('\n');
                log('debug', `✅ Marked step ${stepKey} as sent`);
                return { value: userRow };
            }

            log('debug', `⚠ Step ${stepKey} already marked as sent`);
            return { value: userRow };
        }, overrides);

        if (!result.found) {
            log('error', `❌ User ${userId} not found`);
        }
    } catch (error) {
        log('error', '❌ Error marking step as sent:', error);
    }
}

async function markStepAsSent(userId, bot, step, communityId = null, profileId = '1') {
    return markStepAsSentWithDependencies(userId, bot, step, communityId, profileId);
}

async function clearStepSentHistoryWithDependencies(userId, bot = null, communityId = null, profileId = '1', overrides = {}) {
    try {
        log('debug', `🗑️ Clearing step history for user ${userId}, bot: ${bot || 'all'}`);

        const result = await mutateUserRowWithDependencies(userId, communityId, profileId, userRow => {
            if (bot) {
                const sentSteps = normalizeLines(userRow[COLUMN_SENT_STEPS]).filter(function(item) {
                    return !item.startsWith(`${bot}:`);
                });
                userRow[COLUMN_SENT_STEPS] = sentSteps.join('\n');
            } else {
                userRow[COLUMN_SENT_STEPS] = '';
            }
            return { value: userRow };
        }, overrides);

        if (!result.found) {
            log('error', `❌ User ${userId} not found`);
            return;
        }

        log('debug', '✅ Step history cleared');
    } catch (error) {
        log('error', '❌ Error clearing step history:', error);
    }
}

async function clearStepSentHistory(userId, bot = null, communityId = null, profileId = '1') {
    return clearStepSentHistoryWithDependencies(userId, bot, communityId, profileId);
}

async function deleteUserDataWithDependencies(userId, communityId = null, profileId = '1', overrides = {}) {
    try {
        const normalizedUserId = normalizeUserId(userId);
        let changed = false;

        if (isStructuredUserStateEnabled(overrides)) {
            const deleteResult = await getUserStateStore(overrides).deleteUserRow(
                buildUserScope(communityId, profileId),
                normalizedUserId
            );
            changed = Boolean(deleteResult && deleteResult.deleted);
        } else {
            const sheetUpdater = overrides.updateSheetData || updateSheetData;
            const result = await sheetUpdater(USERS_SHEET, communityId, profileId, rows => {
                const users = Array.isArray(rows) ? rows : [];
                return users.filter(function(user) {
                    return normalizeUserId(user && user[COLUMN_ID]) !== normalizedUserId;
                });
            });
            changed = Boolean(result && result.changed);
        }

        if (!changed) {
            return false;
        }

        const appLogger = overrides.addAppLog || addAppLog;
        await appLogger({
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

async function deleteUserData(userId, communityId = null, profileId = '1') {
    return deleteUserDataWithDependencies(userId, communityId, profileId);
}

async function getUserCurrentSteps(userId, bot = null, communityId = null, profileId = '1') {
    try {
        const user = await getUserRowWithDependencies(userId, communityId, profileId);
        if (!user) return '';

        const allBots = normalizeLines(user[COLUMN_CURRENT_BOT], /[\n,]+/);
        const allSteps = normalizeLines(user[COLUMN_CURRENT_STEP], /[\n,]+/);

        if (bot) {
            const botIndex = allBots.findIndex(function(currentBot) {
                return currentBot === bot;
            });
            if (botIndex !== -1 && allSteps[botIndex]) {
                return allSteps[botIndex];
            }
            return '';
        }

        return user ? (user[COLUMN_CURRENT_STEP] || '') : '';
    } catch (error) {
        log('error', 'Error getting user steps:', error);
        return '';
    }
}

async function listUsers(communityId = null, profileId = '1') {
    return listUsersWithDependencies(communityId, profileId);
}

module.exports = {
    updateUserData,
    getUserVKName,
    addNewUserToSheet,
    getUserVariables,
    listUsers,
    updateUserVariables,
    updateUserBotAndStep,
    removeUserBotAndStep,
    updateUserGroups,
    checkUserGroups,
    checkStepAlreadySent,
    markStepAsSent,
    clearStepSentHistory,
    getUserCurrentSteps,
    deleteUserData,
    __testOnly: {
        getUserRowWithDependencies,
        getUserVariablesWithDependencies,
        listUsersWithDependencies,
        mutateUserRowWithDependencies,
        addNewUserToSheetWithDependencies,
        updateUserVariablesWithDependencies,
        updateUserBotAndStepWithDependencies,
        removeUserBotAndStepWithDependencies,
        updateUserGroupsWithDependencies,
        markStepAsSentWithDependencies,
        clearStepSentHistoryWithDependencies,
        deleteUserDataWithDependencies
    }
};
