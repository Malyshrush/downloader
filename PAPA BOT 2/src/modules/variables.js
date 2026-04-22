/**
 * Модуль работы с переменными (пользовательские, глобальные, VK)
 */

const { getSheetData, saveSheetData, updateSheetData, invalidateCache } = require('./storage');
const { getUserVariables: getUserVarsFromSheet } = require('./users');
const { log } = require('../utils/logger');
const { addAppLog } = require('./app-logs');
const { createUserStateStore, buildUserScope } = require('./user-state-store');
const { createCommunityVariablesStore } = require('./community-variables-store');
const { createProfileUserSharedStore, buildProfileUserSharedScope } = require('./profile-user-shared-store');
const { createSharedVariablesStore, buildSharedVariablesScope } = require('./shared-variables-store');

const userStateStore = createUserStateStore();
const communityVariablesStore = createCommunityVariablesStore();
const profileUserSharedStore = createProfileUserSharedStore();
const sharedVariablesStore = createSharedVariablesStore();

function buildSharedVariableDisplay(variables) {
    const names = [];
    const values = [];
    for (const [name, value] of Object.entries(variables || {})) {
        names.push(String(name || '').trim());
        values.push(String(value || '').trim());
    }
    return {
        names: names.join('\n'),
        values: values.join('\n')
    };
}

function getUserStateStore(overrides = {}) {
    return overrides.userStateStore || userStateStore;
}

function isStructuredUserStateEnabled(overrides = {}) {
    const store = getUserStateStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function getProfileUserSharedStore(overrides = {}) {
    return overrides.profileUserSharedStore || profileUserSharedStore;
}

function getCommunityVariablesStore(overrides = {}) {
    return overrides.communityVariablesStore || communityVariablesStore;
}

function isCommunityVariablesStoreEnabled(overrides = {}) {
    const store = getCommunityVariablesStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function isProfileUserSharedStoreEnabled(overrides = {}) {
    const store = getProfileUserSharedStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function getSharedVariablesStore(overrides = {}) {
    return overrides.sharedVariablesStore || sharedVariablesStore;
}

function isSharedVariablesStoreEnabled(overrides = {}) {
    const store = getSharedVariablesStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function buildProfileUserSharedRowsFromEntries(entries) {
    const rows = [];
    for (const entry of Array.isArray(entries) ? entries : []) {
        const userId = String(entry && entry.userId || '').trim();
        if (!userId) continue;
        for (const [name, value] of Object.entries(entry && entry.variables || {})) {
            const normalizedName = String(name || '').trim();
            if (!normalizedName) continue;
            rows.push({
                ID: userId,
                'РџРµСЂРµРјРµРЅРЅР°СЏ РџР’РЎ': normalizedName,
                'Р—РЅР°С‡РµРЅРёРµ РџР’РЎ': String(value || '').trim()
            });
        }
    }
    return rows;
}

function buildSharedVariableCatalogMap(rows) {
    const byName = new Map();
    for (const row of Array.isArray(rows) ? rows : []) {
        const varName = String(row && row['РџРµСЂРµРјРµРЅРЅР°СЏ РџР’РЎ'] || '').trim();
        const value = String(row && row['Р—РЅР°С‡РµРЅРёРµ РџР’РЎ'] || '').trim();
        if (!varName) continue;
        const key = varName.toLowerCase();
        if (!byName.has(key)) {
            byName.set(key, { name: varName, values: new Set() });
        }
        if (value) {
            byName.get(key).values.add(value);
        }
    }

    const variables = {};
    for (const item of byName.values()) {
        variables[item.name] = Array.from(item.values).join('\n');
    }
    return variables;
}

function buildGlobalVariableRows(variables) {
    return Object.entries(variables || {}).map(function([name, value]) {
        return {
            'Пользовательская': '',
            'Глобальная': name,
            'Значение ГП': value || '',
            'ПЕРЕМЕННЫЕ ВК': '',
            'Значение/Описание ПВК': ''
        };
    });
}

function buildSharedVariableRows(variables) {
    return Object.entries(variables || {}).map(function([name, value]) {
        return {
            'Переменная ПВС': name,
            'Значение ПВС': value || ''
        };
    });
}

function buildCommunityVariableStateFromRows(rows) {
    const globalVars = {};
    const vkVars = {};
    const userVariableNames = [];
    const seenUserVariableNames = new Set();

    for (const row of Array.isArray(rows) ? rows : []) {
        const globalName = String(row && row['Глобальная'] || '').trim().toLowerCase();
        if (globalName) {
            globalVars[globalName] = String(row && row['Значение ГП'] || '').trim();
        }

        const vkName = String(row && (row['ПЕРЕМЕННЫЕ ВК'] || row['Переменные ВК']) || '').trim().toLowerCase();
        if (vkName) {
            vkVars[vkName] = String(row && row['Значение/Описание ПВК'] || '').trim();
        }

        const userName = String(row && row['Пользовательская'] || '').trim().toLowerCase();
        if (userName && !seenUserVariableNames.has(userName)) {
            seenUserVariableNames.add(userName);
            userVariableNames.push(userName);
        }
    }

    return {
        globalVars,
        vkVars,
        userVariableNames
    };
}

async function getProfileUserSharedVariableRowsWithDependencies(profileId = '1', overrides = {}) {
    try {
        if (isProfileUserSharedStoreEnabled(overrides)) {
            const entries = await getProfileUserSharedStore(overrides).listUserEntries(buildProfileUserSharedScope(profileId));
            return buildProfileUserSharedRowsFromEntries(entries);
        }
        const rows = await getSheetData('ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ', null, profileId);
        return Array.isArray(rows) ? rows : [];
    } catch (error) {
        log('error', '❌ Error getting profile user shared variable rows:', error);
        return [];
    }
}

async function getProfileUserSharedVariableRows(profileId = '1') {
    return getProfileUserSharedVariableRowsWithDependencies(profileId);
}

async function getProfileUserSharedVariablesWithDependencies(userId, profileId = '1', overrides = {}) {
    try {
        if (isProfileUserSharedStoreEnabled(overrides)) {
            const variables = await getProfileUserSharedStore(overrides).getUserVariables(
                buildProfileUserSharedScope(profileId),
                userId
            );
            return variables || {};
        }

        const rows = await getProfileUserSharedVariableRowsWithDependencies(profileId, overrides);
        const normalizedUserId = String(userId || '').trim();
        const sharedVars = {};
        rows.forEach(function(row) {
            if (String(row['ID'] || '').trim() !== normalizedUserId) return;
            const varName = String(row['Переменная ПВС'] || '').trim().toLowerCase();
            if (!varName) return;
            sharedVars[varName] = String(row['Значение ПВС'] || '').trim();
        });
        return sharedVars;
    } catch (error) {
        log('error', '❌ Error getting profile user shared variables:', error);
        return {};
    }
}

async function getProfileUserSharedVariables(userId, profileId = '1') {
    return getProfileUserSharedVariablesWithDependencies(userId, profileId);
}

async function syncProfileSharedVariableCatalog(profileId = '1', rows = null) {
    try {
        log('debug', '🔄 syncProfileSharedVariableCatalog: Starting for profile ' + profileId);
        const sourceRows = Array.isArray(rows) ? rows : await getProfileUserSharedVariableRows(profileId);
        log('debug', '📊 syncProfileSharedVariableCatalog: Got ' + sourceRows.length + ' source rows');
        const byName = new Map();

        sourceRows.forEach(function(row) {
            const varName = String(row['Переменная ПВС'] || '').trim();
            const value = String(row['Значение ПВС'] || '').trim();
            if (!varName) return;
            const key = varName.toLowerCase();
            if (!byName.has(key)) {
                byName.set(key, { name: varName, values: new Set() });
            }
            if (value) {
                byName.get(key).values.add(value);
            }
        });

        log('debug', '📊 syncProfileSharedVariableCatalog: Aggregated ' + byName.size + ' unique variable names');

        const catalogRows = Array.from(byName.values()).map(function(item) {
            return {
                'Переменная ПВС': item.name,
                'Значение ПВС': Array.from(item.values).join('\n')
            };
        });

        log('debug', '📊 syncProfileSharedVariableCatalog: Saving ' + catalogRows.length + ' catalog rows');

        invalidateCache('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', null, profileId);
        await saveSheetData('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', catalogRows, null, profileId);
        invalidateCache('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', null, profileId);
        log('debug', '✅ syncProfileSharedVariableCatalog: Completed successfully');
    } catch (error) {
        log('error', '❌ Error syncing profile shared variable catalog:', error);
    }
}

async function syncProfileUserSharedVariablesToUsers(userId, variables, profileId = '1') {
    try {
        log('debug', '🔄 syncProfileUserSharedVariablesToUsers: Starting for user ' + userId + ', profile ' + profileId + ', variables: ' + JSON.stringify(variables));
        const normalizedUserId = String(userId || '').trim();
        const display = buildSharedVariableDisplay(variables);
        log('debug', '📊 syncProfileUserSharedVariablesToUsers: Display names="' + display.names + '", values="' + display.values + '"');
        const { loadBotConfig, getAllCommunityIds } = require('./config');
        await loadBotConfig(profileId);
        const communityIds = getAllCommunityIds(profileId);
        log('debug', '📊 syncProfileUserSharedVariablesToUsers: Found ' + communityIds.length + ' communities: ' + communityIds.join(', '));

        for (const communityId of communityIds) {
            log('debug', '🔄 syncProfileUserSharedVariablesToUsers: Processing community ' + communityId);
            invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
            const users = await getSheetData('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
            log('debug', '📊 syncProfileUserSharedVariablesToUsers: Loaded ' + (users ? users.length : 0) + ' users');
            const idx = (users || []).findIndex(function(row) {
                return String(row['ID'] || '').trim() === normalizedUserId;
            });
            log('debug', '📊 syncProfileUserSharedVariablesToUsers: User index=' + idx);
            if (idx === -1) {
                log('debug', '⚠️ syncProfileUserSharedVariablesToUsers: User ' + userId + ' NOT FOUND in users sheet for community ' + communityId + ', skipping');
                continue;
            }

            users[idx]['Переменная ПВС'] = display.names;
            users[idx]['Значение ПВС'] = display.values;
            log('debug', '📊 syncProfileUserSharedVariablesToUsers: Updated user row with ПВС');

            await saveSheetData('ПОЛЬЗОВАТЕЛИ', users, communityId, profileId);
            invalidateCache('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
            log('debug', '✅ syncProfileUserSharedVariablesToUsers: Saved for community ' + communityId);
        }
        log('debug', '✅ syncProfileUserSharedVariablesToUsers: Completed');
    } catch (error) {
        log('error', '❌ Error syncing profile user shared variables to users sheet:', error);
    }
}

async function updateProfileUserSharedVariables(userId, variables, profileId = '1') {
    try {
        log('debug', '🔄 updateProfileUserSharedVariables: Starting for user ' + userId + ', profile ' + profileId + ', variables: ' + JSON.stringify(variables));
        const normalizedUserId = String(userId || '').trim();
        const rows = await getProfileUserSharedVariableRows(profileId);
        log('debug', '📊 updateProfileUserSharedVariables: Loaded ' + rows.length + ' existing rows');
        const nextRows = rows.filter(function(row) {
            return String(row['ID'] || '').trim() !== normalizedUserId;
        });
        log('debug', '📊 updateProfileUserSharedVariables: After filtering, ' + nextRows.length + ' rows remain');

        Object.entries(variables || {}).forEach(function([name, value]) {
            const normalizedName = String(name || '').trim();
            if (!normalizedName) return;
            nextRows.push({
                'ID': normalizedUserId,
                'Переменная ПВС': normalizedName,
                'Значение ПВС': String(value || '').trim()
            });
            log('debug', '📊 updateProfileUserSharedVariables: Added new row: ID=' + normalizedUserId + ', name=' + normalizedName + ', value=' + value);
        });

        log('debug', '📊 updateProfileUserSharedVariables: Total rows to save: ' + nextRows.length);

        invalidateCache('ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ', null, profileId);
        await saveSheetData('ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ', nextRows, null, profileId);
        invalidateCache('ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ', null, profileId);
        log('debug', '✅ updateProfileUserSharedVariables: Saved to S3');
        await syncProfileSharedVariableCatalog(profileId, nextRows);
        log('debug', '✅ updateProfileUserSharedVariables: Catalog synced');
        await syncProfileUserSharedVariablesToUsers(normalizedUserId, variables, profileId);
        log('debug', '✅ updateProfileUserSharedVariables: Users sheet synced');
    } catch (error) {
        log('error', '❌ Error updating profile user shared variables:', error);
    }
}

/**
 * Получить глобальные переменные
 */
async function getGlobalVariables(communityId = null, profileId = '1') {
    try {
        const cid = communityId;
        log('debug', '🔧 Getting global variables (Community: ' + cid + ')');
        
        const varsSheet = await getSheetData('ПЕРЕМЕННЫЕ', cid, profileId);
        if (!varsSheet) return { globalVars: {}, vkVars: {} };
        
        const globalVars = {};
        const vkVars = {};
        
        for (const row of varsSheet) {
            if (row['Глобальная'] && row['Значение ГП']) {
                globalVars[row['Глобальная'].trim().toLowerCase()] = row['Значение ГП'].trim();
            }
            if (row['ПЕРЕМЕННЫЕ ВК'] && row['Значение/Описание ПВК']) {
                vkVars[row['ПЕРЕМЕННЫЕ ВК'].trim().toLowerCase()] = row['Значение/Описание ПВК'].trim();
            }
        }
        
        return { globalVars, vkVars };
    } catch (error) {
        log('error', '❌ Error getting global variables:', error);
        return { globalVars: {}, vkVars: {} };
    }
}

async function getSharedVariablesWithDependencies(profileId = '1', overrides = {}) {
    try {
        if (isSharedVariablesStoreEnabled(overrides)) {
            return getSharedVariablesStore(overrides).listVariables(buildSharedVariablesScope(profileId));
        }
        const varsSheet = await getSheetData('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', null, profileId);
        if (!varsSheet) return {};

        const sharedVars = {};
        for (const row of varsSheet) {
            if (row['Переменная ПВС']) {
                sharedVars[String(row['Переменная ПВС']).trim().toLowerCase()] = String(row['Значение ПВС'] || '').trim();
            }
        }

        return sharedVars;
    } catch (error) {
        log('error', '❌ Error getting shared variables:', error);
        return {};
    }
}

async function getSharedVariables(profileId = '1') {
    return getSharedVariablesWithDependencies(profileId);
}

/**
 * Обновить глобальные переменные
 */
async function updateGlobalVariables(variables, communityId = null, profileId = '1') {
    try {
        const cid = communityId;
        log('debug', '🔧 Updating global variables (Community: ' + cid + ')');
        
        invalidateCache('ПЕРЕМЕННЫЕ', cid, profileId);
        const sheet = await getSheetData('ПЕРЕМЕННЫЕ', cid, profileId);
        const nonGlobalRows = sheet.filter(row => !(row['Глобальная'] || '').trim());
        const globalRows = Object.entries(variables || {}).map(([name, value]) => ({
            'Пользовательская': '',
            'Глобальная': name,
            'Значение ГП': value || '',
            'Переменные ВК': '',
            'Значение/Описание ПВК': ''
        }));

        await saveSheetData('ПЕРЕМЕННЫЕ', nonGlobalRows.concat(globalRows), cid, profileId);
        invalidateCache('ПЕРЕМЕННЫЕ', cid, profileId);
        log('debug', '✅ Global variables saved successfully');
    } catch (error) {
        log('error', '❌ Error updating global variables:', error);
    }
}

async function updateSharedVariables(variables, profileId = '1') {
    try {
        invalidateCache('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', null, profileId);
        const sheet = Object.entries(variables || {}).map(([name, value]) => ({
            'Переменная ПВС': name,
            'Значение ПВС': value || ''
        }));

        await saveSheetData('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', sheet, null, profileId);
        invalidateCache('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', null, profileId);
    } catch (error) {
        log('error', '❌ Error updating shared variables:', error);
    }
}

async function syncUserVariableCatalog(variableNames, communityId = null, profileId = '1') {
    try {
        const names = Array.isArray(variableNames)
            ? variableNames.map(function(name) { return String(name || '').trim(); }).filter(Boolean)
            : [];
        if (!names.length) return;

        invalidateCache('ПЕРЕМЕННЫЕ', communityId, profileId);
        const sheet = await getSheetData('ПЕРЕМЕННЫЕ', communityId, profileId);
        names.forEach(function(name) {
            const exists = sheet.some(function(row) {
                return String(row['Пользовательская'] || '').trim().toLowerCase() === name.toLowerCase();
            });
            if (!exists) {
                sheet.push({
                    'Пользовательская': name,
                    'Глобальная': '',
                    'Значение ГП': '',
                    'Переменные ВК': '',
                    'Значение/Описание ПВК': ''
                });
            }
        });
        await saveSheetData('ПЕРЕМЕННЫЕ', sheet, communityId, profileId);
        invalidateCache('ПЕРЕМЕННЫЕ', communityId, profileId);
    } catch (error) {
        log('error', '❌ Error syncing user variable catalog:', error);
    }
}

/**
 * Заменить переменные в тексте
 */
async function replaceVariables(text, userId, groupId, communityId = null, profileId = '1') {
    try {
        log('debug', `🔧 REPLACE_VARS: Starting for text: "${text}"`);

        if (!text || typeof text !== 'string') {
            return text;
        }

        // Для чтения файлов переменных используем vk_group_id (числовой) чтобы найти правильный файл
        let fileCommunityId = communityId;
        try {
            const { getCommunityConfig } = require('./config');
            const config = await getCommunityConfig(communityId, profileId);
            if (config && config.vk_group_id) {
                fileCommunityId = config.vk_group_id.toString();
            }
        } catch(e) {}

        const [userVars, { globalVars, vkVars }, sharedVars] = await Promise.all([
            getUserVarsFromSheet(userId, fileCommunityId, profileId),
            getGlobalVariables(fileCommunityId, profileId),
            getProfileUserSharedVariables(userId, profileId)
        ]);

        // Получаем настоящее имя пользователя из VK
        let vkUserName = userId.toString();
        try {
            const { getUserName } = require('./vk-api');
            // Берём токен из ENV или из конфига
            const { getVkToken, getCommunityConfig } = require('./config');
            const communityConfig = await getCommunityConfig(communityId, profileId);
            const token = await getVkToken(0, communityId, profileId);
            if (token) {
                const name = await getUserName(userId, token);
                if (name) vkUserName = name;
            }
        } catch (e) {
            log('debug', `⚠️ Could not get VK user name: ${e.message}`);
        }

        const vkStandardVars = {
            'vk_user': vkUserName,
            'vk_user_id': userId,
            'vk_group_id': groupId,
            'vk_date': new Date().toLocaleDateString('ru-RU'),
            'vk_time': new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }),
            // Совместимость с популярными форматами
            'username': vkUserName,
            'fullname': vkUserName,
            'userid': userId,
            'group': '@club' + groupId,
            'unsubscribe': '',
            'city': '',
            'country': '',
            'gender': '',
            'ref': '',
            'utm_source': '',
            'utm_medium': '',
            'utm_campaign': ''
        };

        let result = text;

        // Пользовательские {$var$}
        result = result.replace(/\{\$([^}]+)\$\}/g, (_, v) => {
            const key = v.trim().toLowerCase();
            const value = userVars[key] !== undefined ? userVars[key] : _;
            log('debug', `🔧 Replaced user var ${_} → ${value}`);
            return value;
        });

        // Глобальные [$var$]
        result = result.replace(/\[\$([^[\]]+)\$\]/g, (_, v) => {
            const key = v.trim().toLowerCase();
            const value = globalVars[key] !== undefined ? globalVars[key] : _;
            log('debug', `🔧 Global var lookup: [${v}] → key="${key}" → value="${value}"`);
            return value;
        });

        result = result.replace(/#([^#]+)#/g, (_, v) => {
            const key = v.trim().toLowerCase();
            const value = sharedVars[key] !== undefined ? sharedVars[key] : _;
            log('debug', `🔧 Replaced shared var #${v}# → ${value}`);
            return value;
        });

        // VK переменные %%
        result = result.replace(/%([^%]+)%/g, (_, v) => {
            const key = v.trim().toLowerCase();
            const value = vkVars[key] || vkStandardVars[key] || _;
            log('debug', `🔧 Replaced VK var ${_} → ${value}`);
            return value;
        });

        log('debug', `🔧 REPLACE_VARS: Completed, result: "${result}"`);
        return result;
    } catch (error) {
        log('error', '❌ Error in replaceVariables:', error);
        return text;
    }
}

/**
 * Выполнить действия с переменными
 */
async function performVariableActions(actions, userId, groupId, forceOverwrite = true, communityId = null, varType = 'auto', profileId = '1') {
    try {
        if (!actions || typeof actions !== 'string') return;

        // Для чтения/записи файлов используем vk_group_id чтобы найти правильный файл
        let fileCommunityId = communityId;
        try {
            const { getCommunityConfig } = require('./config');
            const config = await getCommunityConfig(communityId, profileId);
            if (config && config.vk_group_id) {
                fileCommunityId = config.vk_group_id.toString();
            }
        } catch(e) {}

        log('debug', '🔧 Starting variable actions for community ' + communityId + ' (file: ' + fileCommunityId + ', type: ' + varType + '): ' + '"' + actions + '"');

        invalidateCache('ПОЛЬЗОВАТЕЛИ', fileCommunityId, profileId);
        invalidateCache('ПЕРЕМЕННЫЕ', fileCommunityId, profileId);

        const [userVars, { globalVars }, sharedVars] = await Promise.all([
            getUserVarsFromSheet(userId, fileCommunityId, profileId),
            getGlobalVariables(fileCommunityId, profileId),
            getProfileUserSharedVariables(userId, profileId)
        ]);

        // Нормализация действий (поддержка ; и новых строк)
        const normalizedActions = actions
            .replace(/\r\n/g, ';')
            .replace(/\n/g, ';')
            .replace(/\r/g, ';');

        const actionList = normalizedActions.split(';').map(a => a.trim()).filter(a => a.length > 0);

        const updatedUserVars = { ...userVars };
        const updatedGlobalVars = { ...globalVars };
        const updatedSharedVars = { ...sharedVars };
        let hasUpdates = false;

        log('debug', `🔥 Parsed ${actionList.length} actions`);

        for (const action of actionList) {
            try {
                log('debug', `🔧 Processing action: "${action}"`);

                if (!action.includes('=')) {
                    log('debug', `⚠️ Skipping - no = sign`);
                    continue;
                }

                const parts = action.split('=').map(p => p.trim());
                if (parts.length < 2) {
                    log('debug', `⚠️ Skipping - invalid format`);
                    continue;
                }

                const leftSide = parts[0].trim();
                let rightSide = parts.slice(1).join('=').trim();

                // Проверка: текст в кавычках
                let isQuotedText = false;
                if (rightSide.startsWith('"') && rightSide.endsWith('"')) {
                    isQuotedText = true;
                    rightSide = rightSide.slice(1, -1);
                    log('debug', `🔧 Quoted text detected: "${rightSide}"`);
                }

                // Заменяем переменные в правой части
                if (!isQuotedText) {
                    // Сначала глобальные [$...$]
                    rightSide = rightSide.replace(/\[\$([^[\]]+)\$\]/g, (match, varName) => {
                        const key = varName.trim().toLowerCase();
                        const value = globalVars[key] !== undefined ? globalVars[key] :
                                      userVars[key] !== undefined ? userVars[key] : '0';
                        log('debug', `🔧 Replaced global var ${match} → "${value}"`);
                        return value;
                    });

                    // Потом пользовательские {$...$}
                    rightSide = rightSide.replace(/\{\$([^}]+)\$\}/g, (match, varName) => {
                        const key = varName.trim().toLowerCase();
                        const value = userVars[key] !== undefined ? userVars[key] :
                                     globalVars[key] !== undefined ? globalVars[key] : '0';
                        log('debug', `🔧 Replaced user var ${match} → "${value}"`);
                        return value;
                    });

                    rightSide = rightSide.replace(/#([^#]+)#/g, (match, varName) => {
                        const key = varName.trim().toLowerCase();
                        const value = sharedVars[key] !== undefined ? sharedVars[key] : '0';
                        log('debug', `🔧 Replaced shared var ${match} → "${value}"`);
                        return value;
                    });

                    log('debug', `🔧 After replacement: "${rightSide}"`);
                }

                // ✅ Замена имён переменных нового формата — ОДНИМ ПРОХОДОМ!
                // Собираем все известные имена (ИЗ ИСХОДНЫХ данных, НЕ из обновлённых)
                const allKnownVars = {};
                for (const [k, v] of Object.entries(userVars)) { allKnownVars[k] = v; }
                for (const [k, v] of Object.entries(globalVars)) { allKnownVars[k] = v; }

                // Заменяем только если rightSide НЕ в кавычках
                if (!isQuotedText) {
                    // Сортируем по длине убывания чтобы pp_balance заменился раньше pp
                    const sortedVarNames = Object.keys(allKnownVars).sort((a, b) => b.length - a.length);

                    if (sortedVarNames.length > 0) {
                        // Создаём ОДИН regex со всеми переменными (заменяем за один проход!)
                        const escapedNames = sortedVarNames.map(n => n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
                        const combinedRegex = new RegExp('\\b(' + escapedNames.join('|') + ')\\b', 'g');

                        rightSide = rightSide.replace(combinedRegex, (match) => {
                            const varValue = allKnownVars[match];
                            if (!isNaN(varValue) && varValue !== '') {
                                log('debug', `🔧 Replaced new-format var "${match}" → "${varValue}"`);
                                return varValue;
                            }
                            return match; // Не заменяем если не число
                        });
                    }
                    log('debug', `🔧 After new-format replacement: "${rightSide}"`);
                }

                let value = rightSide;

                // Математические выражения
                const mathPattern = /^[\d\s.\+\-\*\/\(\)]+$/;
                if (mathPattern.test(rightSide.replace(/\s+/g, ''))) {
                    try {
                        const normalizedExpression = rightSide.replace(/,/g, '.');
                        value = eval(normalizedExpression).toString();
                        log('debug', `🔧 Evaluated math: "${rightSide}" → "${value}"`);
                    } catch (e) {
                        log('debug', `⚠️ Math evaluation error: ${e.message}`);
                        value = rightSide;
                    }
                }

                // Сохраняем в переменную — поддержка старого и нового формата
                if (leftSide.startsWith('{$') && leftSide.endsWith('$}')) {
                    // Старый формат: {$pp1$} = ...
                    const varName = leftSide.slice(2, -2).trim().toLowerCase();
                    updatedUserVars[varName] = value;
                    hasUpdates = true;
                    log('debug', `🔧 Set user var: {$${varName}$} = ${value}`);
                } else if (leftSide.startsWith('[$') && leftSide.endsWith('$]')) {
                    // Старый формат: [$gp1$] = ...
                    const varName = leftSide.slice(2, -2).trim().toLowerCase();
                    updatedGlobalVars[varName] = value;
                    hasUpdates = true;
                    log('debug', `🔧 Set global var: [$${varName}$] = ${value}`);
                } else if (varType === 'user') {
                    // Явно указано — пользовательская
                    updatedUserVars[leftSide.toLowerCase()] = value;
                    hasUpdates = true;
                    log('debug', `🔧 Set user var (forced): ${leftSide} = ${value}`);
                } else if (varType === 'global') {
                    // Явно указано — глобальная
                    updatedGlobalVars[leftSide.toLowerCase()] = value;
                    hasUpdates = true;
                    log('debug', `🔧 Set global var (forced): ${leftSide} = ${value}`);
                } else if (varType === 'shared') {
                    updatedSharedVars[leftSide.toLowerCase()] = value;
                    hasUpdates = true;
                    log('debug', `🔧 Set shared var (forced): ${leftSide} = ${value}`);
                } else if (leftSide.startsWith('pp')) {
                    // Авто: начинается с pp → пользовательская
                    updatedUserVars[leftSide.toLowerCase()] = value;
                    hasUpdates = true;
                    log('debug', `🔧 Set user var (auto pp): ${leftSide} = ${value}`);
                } else if (leftSide.startsWith('gp')) {
                    // Авто: начинается с gp → глобальная
                    updatedGlobalVars[leftSide.toLowerCase()] = value;
                    hasUpdates = true;
                    log('debug', `🔧 Set global var (auto gp): ${leftSide} = ${value}`);
                } else if (leftSide.startsWith('pvs')) {
                    updatedSharedVars[leftSide.toLowerCase()] = value;
                    hasUpdates = true;
                    log('debug', `🔧 Set shared var (auto pvs): ${leftSide} = ${value}`);
                } else {
                    log('debug', `⚠️ Unknown variable format: "${leftSide}"`);
                }
            } catch (e) {
                log('error', `❌ Error processing action: "${action}"`, e);
            }
        }

        // Сохраняем изменения
        if (hasUpdates) {
            const { updateUserVariables } = require('./users');
            await Promise.all([
                updateUserVariables(userId, updatedUserVars, forceOverwrite, fileCommunityId, profileId),
                updateGlobalVariables(updatedGlobalVars, fileCommunityId, profileId),
                updateProfileUserSharedVariables(userId, updatedSharedVars, profileId)
            ]);
            await syncUserVariableCatalog(Object.keys(updatedUserVars), fileCommunityId, profileId);
            log('debug', `✅ Variable actions completed successfully`);
            await addAppLog({
                tab: 'VARIABLES',
                title: 'Выполнены действия с переменными',
                summary: 'Обновлены пользовательские, глобальные или профильные переменные.',
                details: [
                    'Пользователь: ' + userId,
                    'Действия: ' + actions.replace(/\s+/g, ' ').trim()
                ],
                communityId: fileCommunityId,
                profileId
            });
        }
    } catch (error) {
        log('error', '❌ Error performing variable actions:', error);
    }
}

/**
 * Проверить условия переменных
 */
async function checkVariableConditions(userVarCondition, globalVarCondition, sharedVarCondition, userId, groupId, communityId = null, profileId = '1') {
    try {
        log('debug', `🔍 Checking variable conditions for user ${userId}`);

        // Проверка пользовательских переменных
        if (userVarCondition && userVarCondition.trim()) {
            const userVars = await getUserVarsFromSheet(userId, communityId, profileId);
            const conditions = userVarCondition.split(/[;,]/).map(c => c.trim()).filter(c => c);

            for (const condition of conditions) {
                log('debug', `🔍 Checking user var condition: "${condition}"`);

                const { varName, operator, expectedValue } = parseCondition(condition);
                const varValue = userVars[varName.toLowerCase()];
                
                if (!evaluateCondition(varValue, operator, expectedValue)) {
                    log('debug', `❌ User variable condition failed: ${condition}`);
                    return false;
                }
            }
        }

        // Проверка глобальных переменных
        if (globalVarCondition && globalVarCondition.trim()) {
            const { globalVars } = await getGlobalVariables(communityId, profileId);
            const conditions = globalVarCondition.split(/[;,]/).map(c => c.trim()).filter(c => c);

            for (const condition of conditions) {
                log('debug', `🔍 Checking global var condition: "${condition}"`);

                const { varName, operator, expectedValue } = parseCondition(condition);
                const varValue = globalVars[varName.toLowerCase()];
                
                if (!evaluateCondition(varValue, operator, expectedValue)) {
                    log('debug', `❌ Global variable condition failed: ${condition}`);
                    return false;
                }
            }
        }

        if (sharedVarCondition && sharedVarCondition.trim()) {
            const sharedVars = await getProfileUserSharedVariables(userId, profileId);
            const conditions = sharedVarCondition.split(/[;,]/).map(c => c.trim()).filter(c => c);

            for (const condition of conditions) {
                log('debug', `🔍 Checking shared var condition: "${condition}"`);

                const { varName, operator, expectedValue } = parseCondition(condition);
                const varValue = sharedVars[varName.toLowerCase()];

                if (!evaluateCondition(varValue, operator, expectedValue)) {
                    log('debug', `❌ Shared variable condition failed: ${condition}`);
                    return false;
                }
            }
        }

        log('debug', `✅ All variable conditions passed`);
        return true;
    } catch (error) {
        log('error', '❌ Error in checkVariableConditions:', error);
        return false;
    }
}

/**
 * Разобрать условие
 */
function parseCondition(condition) {
    let varName, operator, expectedValue;

    if (condition.includes('!=')) {
        [varName, expectedValue] = condition.split('!=').map(s => s.trim());
        operator = '!=';
    } else if (condition.includes('>=')) {
        [varName, expectedValue] = condition.split('>=').map(s => s.trim());
        operator = '>=';
    } else if (condition.includes('<=')) {
        [varName, expectedValue] = condition.split('<=').map(s => s.trim());
        operator = '<=';
    } else if (condition.includes('=')) {
        [varName, expectedValue] = condition.split('=').map(s => s.trim());
        operator = '=';
    } else if (condition.includes('>')) {
        [varName, expectedValue] = condition.split('>').map(s => s.trim());
        operator = '>';
    } else if (condition.includes('<')) {
        [varName, expectedValue] = condition.split('<').map(s => s.trim());
        operator = '<';
    } else {
        varName = condition.trim();
        operator = 'exists';
    }

    return { varName, operator, expectedValue };
}

/**
 * Вычислить условие
 */
function evaluateCondition(varValue, operator, expectedValue) {
    switch (operator) {
        case 'exists':
            return varValue !== undefined;
        case '=':
            return varValue === expectedValue;
        case '!=':
            return varValue !== expectedValue;
        case '>':
            return parseFloat(varValue) > parseFloat(expectedValue);
        case '>=':
            return parseFloat(varValue) >= parseFloat(expectedValue);
        case '<':
            return parseFloat(varValue) < parseFloat(expectedValue);
        case '<=':
            return parseFloat(varValue) <= parseFloat(expectedValue);
        default:
            return false;
    }
}

async function syncProfileSharedVariableCatalogWithDependencies(profileId = '1', rows = null, overrides = {}) {
    try {
        log('debug', 'syncProfileSharedVariableCatalog: Starting for profile ' + profileId);
        const sourceRows = Array.isArray(rows)
            ? rows
            : await (overrides.getProfileUserSharedVariableRows || getProfileUserSharedVariableRowsWithDependencies)(profileId, overrides);
        const catalogVariables = buildSharedVariableCatalogMap(sourceRows);
        if (isSharedVariablesStoreEnabled(overrides)) {
            await getSharedVariablesStore(overrides).replaceVariables(
                buildSharedVariablesScope(profileId),
                catalogVariables
            );
            return;
        }
        const byName = new Map();

        sourceRows.forEach(function(row) {
            const varName = String(row['Переменная ПВС'] || '').trim();
            const value = String(row['Значение ПВС'] || '').trim();
            if (!varName) return;
            const key = varName.toLowerCase();
            if (!byName.has(key)) {
                byName.set(key, { name: varName, values: new Set() });
            }
            if (value) {
                byName.get(key).values.add(value);
            }
        });

        const catalogRows = Array.from(byName.values()).map(function(item) {
            return {
                'Переменная ПВС': item.name,
                'Значение ПВС': Array.from(item.values).join('\n')
            };
        });

        const sheetUpdater = overrides.updateSheetData || updateSheetData;
        await sheetUpdater('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', null, profileId, function() {
            return catalogRows;
        });
    } catch (error) {
        log('error', '❌ Error syncing profile shared variable catalog:', error);
    }
}

async function syncProfileSharedVariableCatalog(profileId = '1', rows = null) {
    return syncProfileSharedVariableCatalogWithDependencies(profileId, rows);
}

async function syncProfileUserSharedVariablesToUsersWithDependencies(userId, variables, profileId = '1', overrides = {}) {
    try {
        const normalizedUserId = String(userId || '').trim();
        const display = buildSharedVariableDisplay(variables);
        const loadBotConfig = overrides.loadBotConfig || require('./config').loadBotConfig;
        const getAllCommunityIds = overrides.getAllCommunityIds || require('./config').getAllCommunityIds;
        const sheetUpdater = overrides.updateSheetData || updateSheetData;

        await loadBotConfig(profileId);
        const communityIds = getAllCommunityIds(profileId);

        for (const communityId of communityIds) {
            if (isStructuredUserStateEnabled(overrides)) {
                await getUserStateStore(overrides).updateUserRow(
                    buildUserScope(communityId, profileId),
                    normalizedUserId,
                    function(userRow) {
                        userRow['Переменная ПВС'] = display.names;
                        userRow['Значение ПВС'] = display.values;
                        return { value: userRow };
                    }
                );
                continue;
            }

            await sheetUpdater('ПОЛЬЗОВАТЕЛИ', communityId, profileId, function(rows) {
                const users = Array.isArray(rows) ? rows : [];
                const idx = users.findIndex(function(row) {
                    return String(row['ID'] || '').trim() === normalizedUserId;
                });
                if (idx === -1) {
                    return users;
                }

                users[idx]['Переменная ПВС'] = display.names;
                users[idx]['Значение ПВС'] = display.values;
                return users;
            });
        }
    } catch (error) {
        log('error', '❌ Error syncing profile user shared variables to users sheet:', error);
    }
}

async function syncProfileUserSharedVariablesToUsers(userId, variables, profileId = '1') {
    return syncProfileUserSharedVariablesToUsersWithDependencies(userId, variables, profileId);
}

async function updateProfileUserSharedVariablesWithDependencies(userId, variables, profileId = '1', overrides = {}) {
    try {
        const normalizedUserId = String(userId || '').trim();
        const normalizedVariables = {};
        Object.entries(variables || {}).forEach(function([name, value]) {
            const normalizedName = String(name || '').trim();
            if (!normalizedName) return;
            normalizedVariables[normalizedName.toLowerCase()] = String(value || '').trim();
        });

        if (isProfileUserSharedStoreEnabled(overrides)) {
            await getProfileUserSharedStore(overrides).putUserVariables(
                buildProfileUserSharedScope(profileId),
                normalizedUserId,
                normalizedVariables
            );
            const structuredSyncCatalog = overrides.syncProfileSharedVariableCatalog || syncProfileSharedVariableCatalogWithDependencies;
            const structuredSyncUsers = overrides.syncProfileUserSharedVariablesToUsers || syncProfileUserSharedVariablesToUsersWithDependencies;
            await structuredSyncCatalog(profileId, null, overrides);
            await structuredSyncUsers(normalizedUserId, normalizedVariables, profileId, overrides);
            return;
        }

        const sheetUpdater = overrides.updateSheetData || updateSheetData;
        const result = await sheetUpdater('ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ', null, profileId, function(rows) {
            const nextRows = (Array.isArray(rows) ? rows : []).filter(function(row) {
                return String(row['ID'] || '').trim() !== normalizedUserId;
            });

            Object.entries(variables || {}).forEach(function([name, value]) {
                const normalizedName = String(name || '').trim();
                if (!normalizedName) return;
                nextRows.push({
                    ID: normalizedUserId,
                    'Переменная ПВС': normalizedName,
                    'Значение ПВС': String(value || '').trim()
                });
            });

            return nextRows;
        });

        const nextRows = Array.isArray(result && result.value) ? result.value : [];
        const syncCatalog = overrides.syncProfileSharedVariableCatalog || syncProfileSharedVariableCatalogWithDependencies;
        const syncUsers = overrides.syncProfileUserSharedVariablesToUsers || syncProfileUserSharedVariablesToUsersWithDependencies;
        await syncCatalog(profileId, nextRows, overrides);
        await syncUsers(normalizedUserId, variables, profileId, overrides);
    } catch (error) {
        log('error', '❌ Error updating profile user shared variables:', error);
    }
}

async function updateProfileUserSharedVariables(userId, variables, profileId = '1') {
    return updateProfileUserSharedVariablesWithDependencies(userId, variables, profileId);
}

async function getGlobalVariablesWithDependencies(communityId = null, profileId = '1', overrides = {}) {
    try {
        if (isCommunityVariablesStoreEnabled(overrides)) {
            const structuredState = await getCommunityVariablesStore(overrides).listVariableState(communityId, profileId);
            const hasStructuredGlobals = Object.keys(structuredState.globalVars || {}).length > 0;
            const hasStructuredVk = Object.keys(structuredState.vkVars || {}).length > 0;

            if (hasStructuredGlobals && hasStructuredVk) {
                return {
                    globalVars: structuredState.globalVars || {},
                    vkVars: structuredState.vkVars || {}
                };
            }

            const varsSheet = await getSheetData('РџР•Р Р•РњР•РќРќР«Р•', communityId, profileId);
            const fallbackState = buildCommunityVariableStateFromRows(varsSheet);
            return {
                globalVars: hasStructuredGlobals ? (structuredState.globalVars || {}) : fallbackState.globalVars,
                vkVars: hasStructuredVk ? (structuredState.vkVars || {}) : fallbackState.vkVars
            };
        }

        const varsSheet = await getSheetData('РџР•Р Р•РњР•РќРќР«Р•', communityId, profileId);
        const fallbackState = buildCommunityVariableStateFromRows(varsSheet);
        return {
            globalVars: fallbackState.globalVars,
            vkVars: fallbackState.vkVars
        };
    } catch (error) {
        log('error', 'вќЊ Error getting global variables:', error);
        return { globalVars: {}, vkVars: {} };
    }
}

async function getGlobalVariables(communityId = null, profileId = '1') {
    return getGlobalVariablesWithDependencies(communityId, profileId);
}

async function updateGlobalVariablesWithDependencies(variables, communityId = null, profileId = '1', overrides = {}) {
    try {
        if (isCommunityVariablesStoreEnabled(overrides)) {
            await getCommunityVariablesStore(overrides).replaceGlobalVariables(communityId, variables || {}, profileId);
            return;
        }
        const cid = communityId;
        const sheetUpdater = overrides.updateSheetData || updateSheetData;
        await sheetUpdater('ПЕРЕМЕННЫЕ', cid, profileId, function(rows) {
            const sheet = Array.isArray(rows) ? rows : [];
            const nonGlobalRows = sheet.filter(function(row) {
                return !(row['Глобальная'] || '').trim();
            });
            return nonGlobalRows.concat(buildGlobalVariableRows(variables));
        });
    } catch (error) {
        log('error', '❌ Error updating global variables:', error);
    }
}

async function updateGlobalVariables(variables, communityId = null, profileId = '1') {
    return updateGlobalVariablesWithDependencies(variables, communityId, profileId);
}

async function updateSharedVariablesWithDependencies(variables, profileId = '1', overrides = {}) {
    try {
        if (isSharedVariablesStoreEnabled(overrides)) {
            await getSharedVariablesStore(overrides).replaceVariables(
                buildSharedVariablesScope(profileId),
                variables || {}
            );
            return;
        }
        const sheetUpdater = overrides.updateSheetData || updateSheetData;
        await sheetUpdater('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ', null, profileId, function() {
            return buildSharedVariableRows(variables);
        });
    } catch (error) {
        log('error', '❌ Error updating shared variables:', error);
    }
}

async function updateSharedVariables(variables, profileId = '1') {
    return updateSharedVariablesWithDependencies(variables, profileId);
}

async function syncUserVariableCatalogWithDependencies(variableNames, communityId = null, profileId = '1', overrides = {}) {
    try {
        const names = Array.isArray(variableNames)
            ? variableNames.map(function(name) { return String(name || '').trim(); }).filter(Boolean)
            : [];
        if (!names.length) return;

        if (isCommunityVariablesStoreEnabled(overrides)) {
            await getCommunityVariablesStore(overrides).ensureUserVariableCatalog(communityId, names, profileId);
            return;
        }

        const sheetUpdater = overrides.updateSheetData || updateSheetData;
        await sheetUpdater('ПЕРЕМЕННЫЕ', communityId, profileId, function(rows) {
            const sheet = Array.isArray(rows) ? rows : [];
            names.forEach(function(name) {
                const exists = sheet.some(function(row) {
                    return String(row['Пользовательская'] || '').trim().toLowerCase() === name.toLowerCase();
                });
                if (!exists) {
                    sheet.push({
                        'Пользовательская': name,
                        'Глобальная': '',
                        'Значение ГП': '',
                        'ПЕРЕМЕННЫЕ ВК': '',
                        'Значение/Описание ПВК': ''
                    });
                }
            });
            return sheet;
        });
    } catch (error) {
        log('error', '❌ Error syncing user variable catalog:', error);
    }
}

async function syncUserVariableCatalog(variableNames, communityId = null, profileId = '1') {
    return syncUserVariableCatalogWithDependencies(variableNames, communityId, profileId);
}

module.exports = {
    getGlobalVariables,
    updateGlobalVariables,
    getSharedVariables,
    updateSharedVariables,
    getProfileUserSharedVariables,
    updateProfileUserSharedVariables,
    syncProfileSharedVariableCatalog,
    syncUserVariableCatalog,
    replaceVariables,
    performVariableActions,
    checkVariableConditions,
    __testOnly: {
        getGlobalVariablesWithDependencies,
        getProfileUserSharedVariableRowsWithDependencies,
        getProfileUserSharedVariablesWithDependencies,
        getSharedVariablesWithDependencies,
        updateGlobalVariablesWithDependencies,
        updateSharedVariablesWithDependencies,
        syncUserVariableCatalogWithDependencies,
        syncProfileSharedVariableCatalogWithDependencies,
        syncProfileUserSharedVariablesToUsersWithDependencies,
        updateProfileUserSharedVariablesWithDependencies
    }
};
