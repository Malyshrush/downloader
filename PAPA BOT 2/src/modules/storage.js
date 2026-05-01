/**
 * Модуль работы с Yandex Object Storage (S3)
 * Файлы данных сегментированы по сообществам: messages_community_777.json
 */

const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { log } = require('../utils/logger');
const { createHotStateStore } = require('./hot-state-store');
const { createDelayedDeliveryStore } = require('./delayed-delivery-store');
const { createMailingDeliveryStore } = require('./mailing-delivery-store');
const { createStructuredTriggerStore } = require('./structured-trigger-store');
const { createMessageRuleStore } = require('./message-rule-store');
const { createCommentRuleStore } = require('./comment-rule-store');
const { createCommunityVariablesStore } = require('./community-variables-store');
const { createProfileUserSharedStore } = require('./profile-user-shared-store');
const { createSharedVariablesStore } = require('./shared-variables-store');
const { createUserStateStore, buildUserScope } = require('./user-state-store');

const BUCKET_NAME = process.env.BUCKET_NAME || 'bot-data-storage';
const S3_TIMEOUT_MS = 10000; // 10 секунд таймаут

const s3Client = new S3Client({
    region: 'ru-central1',
    endpoint: 'https://storage.yandexcloud.net',
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    }
});

const hotStateStore = createHotStateStore();
const delayedDeliveryStore = createDelayedDeliveryStore();
const mailingDeliveryStore = createMailingDeliveryStore();
const structuredTriggerStore = createStructuredTriggerStore();
const messageRuleStore = createMessageRuleStore();
const commentRuleStore = createCommentRuleStore();
const communityVariablesStore = createCommunityVariablesStore();
const profileUserSharedStore = createProfileUserSharedStore();
const sharedVariablesStore = createSharedVariablesStore();
const userStateStore = createUserStateStore();
const rawS3Client = s3Client;

const FILE_BASE = {
    'СООБЩЕНИЯ': 'messages_community',
    'КОММЕНТАРИИ В ПОСТАХ': 'comments',
    'ТРИГГЕРЫ': 'triggers',
    'ЛОГИ ПРИЛОЖЕНИЯ': 'app_logs',
    'ПОЛЬЗОВАТЕЛИ': 'users',
    'ГРУППЫ': 'groups',
    'ПЕРЕМЕННЫЕ': 'variables',
    'ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ': 'profile_shared_variables',
    'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ': 'profile_user_shared_variables',
    'РАССЫЛКА': 'mailing',
    'ОТЛОЖЕННЫЕ': 'delayed'
};

const COMMON_FILES = ['admin_auth.json'];

function buildDefaultCommonFile(fileName) {
    if (fileName !== 'admin_auth.json') {
        return {};
    }

    return {
        defaultProfileId: '1',
        profiles: {
            '1': {
                id: '1',
                name: 'РџСЂРѕС„РёР»СЊ 1',
                username: process.env.ADMIN_USERNAME || 'admin',
                password: process.env.ADMIN_PASSWORD || 'admin123',
                recoveryEmail: process.env.ADMIN_EMAIL || 'admin@example.com'
            }
        }
    };
}

function isJsonHotStateKey(key) {
    return typeof key === 'string' && key.endsWith('.json');
}

function buildHotStateGetResponse(jsonText) {
    return {
        Body: {
            transformToString: async () => jsonText
        }
    };
}

const proxyS3Client = {
    send: async command => {
        const commandName = command?.constructor?.name || '';
        const key = String(command?.input?.Key || '').trim();
        const bucket = String(command?.input?.Bucket || '').trim();

        if (!isJsonHotStateKey(key) || (bucket && bucket !== BUCKET_NAME)) {
            return rawS3Client.send(command);
        }

        if (commandName === 'GetObjectCommand') {
            const result = await hotStateStore.loadJsonObject(key, { defaultValue: undefined });
            if (result.source === 'default') {
                const error = new Error(`NoSuchKey: ${key}`);
                error.name = 'NoSuchKey';
                throw error;
            }
            return buildHotStateGetResponse(result.jsonText || JSON.stringify(result.value, null, 2));
        }

        if (commandName === 'PutObjectCommand') {
            const body = command?.input?.Body;
            const jsonText = Buffer.isBuffer(body) ? body.toString('utf8') : String(body || '');
            await hotStateStore.saveJsonObject(key, JSON.parse(jsonText || '{}'));
            return { ETag: '', $metadata: { httpStatusCode: 200 } };
        }

        return rawS3Client.send(command);
    }
};

function normalizeProfileId(profileId) {
    const normalized = String(profileId || '1').trim();
    return normalized || '1';
}

function getLegacyFileName(sheetName, communityId) {
    const base = FILE_BASE[sheetName];
    if (!base) return null;
    return `${base}${communityId ? `_${communityId}` : ''}.json`;
}

function getFileName(sheetName, communityId, profileId = '1') {
    const base = FILE_BASE[sheetName];
    if (!base) return null;
    const pid = normalizeProfileId(profileId);
    return `${base}_profile_${pid}${communityId ? `_${communityId}` : ''}.json`;
}

const DEFAULT_DATA = {
    'СООБЩЕНИЯ': [{
        "№": "", "Триггер": "", "Бот": "", "Шаг": "", "Ответ": "", "Вложения": "",
        "Точно/Не точно": "", "Регистр": "", "Ответить если в Группе": "",
        "Пользовательская": "", "Глобальная": "", "Переменная ПВС": "", "Задержка отправки на Шаг": "",
        "ДОБАВИТЬ ГРУППУ": "", "УДАЛИТЬ ГРУППУ": "", "Отправить на Шаг": "",
        "Действия с ПП": "", "Действия с ГП": "", "Действия с ПВС": "", "Действия с ПП/ГП/ПВК": "", "Заготовленный ответ": ""
    }],
    'КОММЕНТАРИИ В ПОСТАХ': [{
        "№": "", "Триггер": "", "Пост": "", "Отметили": "", "Ответ": "", "Вложения": "",
        "Точно/Не точно": "", "Регистр": "", "Ответить если в Группе": "", "Ответил на Шаг": "", "Пользовательская": "", "Глобальная": "", "Переменная ПВС": "", "Действия с ПП": "", "Действия с ГП": "", "Действия с ПВС": "", "Заготовленный ответ": ""
    }],
    'ТРИГГЕРЫ': [],
    'ЛОГИ ПРИЛОЖЕНИЯ': [],
    'ПОЛЬЗОВАТЕЛИ': [{
        'ID': '', 'ИМЯ': '', 'ГРУППА': '', 'Пользовательская': '',
        'Значения ПП': '', 'Переменная ПВС': '', 'Значение ПВС': '', 'Текущий Бот': '', 'Текущий Шаг': '', 'Отправленные Шаги': ''
    }],
    'ГРУППЫ': [{
        'Группа': '', 'Описание': ''
    }],
    'ПЕРЕМЕННЫЕ': [{
        "Пользовательская": "", "Значение ПП": "", "Глобальная": "",
        "Значение ГП": "", "ПЕРЕМЕННЫЕ ВК": "%vk_user%", "Значение/Описание ПВК": "имя пользователя"
    }],
    'ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ': [{
        "Переменная ПВС": "", "Значение ПВС": ""
    }],
    'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ': [{
        'ID': '', 'Переменная ПВС': '', 'Значение ПВС': ''
    }],
    'РАССЫЛКА': [],
    'ОТЛОЖЕННЫЕ': []
};

const memoryCache = { data: {}, lastUpdated: {}, ttl: {
    'ПОЛЬЗОВАТЕЛИ': 5000, 'ГРУППЫ': 5000, 'ПЕРЕМЕННЫЕ': 5000, 'СООБЩЕНИЯ': 300000,
    'КОММЕНТАРИИ В ПОСТАХ': 300000, 'ТРИГГЕРЫ': 5000, 'ЛОГИ ПРИЛОЖЕНИЯ': 0, 'ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ': 5000, 'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ': 5000, 'РАССЫЛКА': 0, 'ОТЛОЖЕННЫЕ': 3000
}};

// S3 запрос с таймаутом
async function s3Send(command) {
    return Promise.race([
        proxyS3Client.send(command),
        new Promise((_, reject) => setTimeout(() => reject(new Error('S3 timeout')), S3_TIMEOUT_MS))
    ]);
}

function getS3Client() { return proxyS3Client; }
function getBucketName() { return BUCKET_NAME; }

function cloneValue(value) {
    if (value === undefined) return undefined;
    return JSON.parse(JSON.stringify(value));
}

function getMailingDeliveryStore(overrides = {}) {
    return overrides.mailingDeliveryStore || mailingDeliveryStore;
}

function getDelayedDeliveryStore(overrides = {}) {
    return overrides.delayedDeliveryStore || delayedDeliveryStore;
}

function getStructuredTriggerStore(overrides = {}) {
    return overrides.structuredTriggerStore || structuredTriggerStore;
}

function getMessageRuleStore(overrides = {}) {
    return overrides.messageRuleStore || messageRuleStore;
}

function getCommentRuleStore(overrides = {}) {
    return overrides.commentRuleStore || commentRuleStore;
}

function getCommunityVariablesStore(overrides = {}) {
    return overrides.communityVariablesStore || communityVariablesStore;
}

function getProfileUserSharedStore(overrides = {}) {
    return overrides.profileUserSharedStore || profileUserSharedStore;
}

function getSharedVariablesStore(overrides = {}) {
    return overrides.sharedVariablesStore || sharedVariablesStore;
}

function isDelayedDeliveryStoreEnabled(overrides = {}) {
    const store = getDelayedDeliveryStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function isStructuredTriggerStoreEnabled(overrides = {}) {
    const store = getStructuredTriggerStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function getUserStateStore(overrides = {}) {
    return overrides.userStateStore || userStateStore;
}

function isUserStateStoreEnabled(overrides = {}) {
    const store = getUserStateStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function isMessageRuleStoreEnabled(overrides = {}) {
    const store = getMessageRuleStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function isCommentRuleStoreEnabled(overrides = {}) {
    const store = getCommentRuleStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function isMailingDeliveryStoreEnabled(overrides = {}) {
    const store = getMailingDeliveryStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function isCommunityVariablesStoreEnabled(overrides = {}) {
    const store = getCommunityVariablesStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function isProfileUserSharedStoreEnabled(overrides = {}) {
    const store = getProfileUserSharedStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

function isSharedVariablesStoreEnabled(overrides = {}) {
    const store = getSharedVariablesStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

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

function getMailingRowNumber(row, fallback = '') {
    return String(getFirstDefinedValue(row, ['№', 'в„–', 'РІвЂћвЂ“']) || fallback || '').trim();
}

function getDelayedRowNumber(row, fallback = '') {
    return String(getFirstDefinedValue(row, ['№', 'в„–', 'РІвЂћвЂ“']) || fallback || '').trim();
}

function normalizeVariableName(value) {
    return String(value || '').trim().toLowerCase();
}

function buildCommunityVariableStateFromRows(rows) {
    const globalVars = {};
    const vkVars = {};
    const userVariableNames = [];
    const seenUserVariableNames = new Set();

    for (const row of Array.isArray(rows) ? rows : []) {
        const globalName = normalizeVariableName(row && row['Глобальная']);
        if (globalName) {
            globalVars[globalName] = String(row && row['Значение ГП'] || '').trim();
        }

        const vkName = normalizeVariableName(row && (row['ПЕРЕМЕННЫЕ ВК'] || row['Переменные ВК']));
        if (vkName) {
            vkVars[vkName] = String(row && row['Значение/Описание ПВК'] || '').trim();
        }

        const userName = normalizeVariableName(row && row['Пользовательская']);
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

function buildProfileUserSharedEntriesFromRows(rows) {
    const byUserId = new Map();

    for (const row of Array.isArray(rows) ? rows : []) {
        const userId = String(row && row.ID || '').trim();
        const variableName = normalizeVariableName(row && row['Переменная ПВС']);
        if (!userId || !variableName) continue;
        if (!byUserId.has(userId)) {
            byUserId.set(userId, {});
        }
        byUserId.get(userId)[variableName] = String(row && row['Значение ПВС'] || '').trim();
    }

    return Array.from(byUserId.entries()).map(function([userId, variables]) {
        return {
            userId,
            variables
        };
    });
}

function buildProfileUserSharedRowsFromEntries(entries) {
    const rows = [];

    for (const entry of Array.isArray(entries) ? entries : []) {
        const userId = String(entry && entry.userId || '').trim();
        if (!userId) continue;

        for (const [name, value] of Object.entries(entry && entry.variables || {})) {
            const variableName = normalizeVariableName(name);
            if (!variableName) continue;
            rows.push({
                ID: userId,
                'Переменная ПВС': variableName,
                'Значение ПВС': String(value || '').trim()
            });
        }
    }

    return rows;
}

function buildProfileUserSharedVariablesFromUserRow(row) {
    const names = String(row && row['\u041f\u0435\u0440\u0435\u043c\u0435\u043d\u043d\u0430\u044f \u041f\u0412\u0421'] || '')
        .split(/\r?\n/)
        .map(item => normalizeVariableName(item))
        .filter(Boolean);
    const values = String(row && row['\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u041f\u0412\u0421'] || '')
        .split(/\r?\n/);
    const variables = {};

    names.forEach(function(name, index) {
        variables[name] = String(values[index] || '').trim();
    });

    return variables;
}

function buildSharedVariableCatalogMapFromEntries(entries) {
    const byName = new Map();

    for (const entry of Array.isArray(entries) ? entries : []) {
        for (const [name, value] of Object.entries(entry && entry.variables || {})) {
            const variableName = normalizeVariableName(name);
            if (!variableName) continue;
            if (!byName.has(variableName)) {
                byName.set(variableName, { name: variableName, values: new Set() });
            }
            if (String(value || '').trim()) {
                byName.get(variableName).values.add(String(value || '').trim());
            }
        }
    }

    const variables = {};
    for (const item of byName.values()) {
        variables[item.name] = Array.from(item.values).join('\n');
    }
    return variables;
}

function buildSharedVariableCatalogMap(rows) {
    const byName = new Map();

    for (const row of Array.isArray(rows) ? rows : []) {
        const variableName = String(row && row['Переменная ПВС'] || '').trim();
        const value = String(row && row['Значение ПВС'] || '').trim();
        if (!variableName) continue;
        const key = variableName.toLowerCase();
        if (!byName.has(key)) {
            byName.set(key, { name: variableName, values: new Set() });
        }
        if (value) {
            byName.get(key).values.add(value);
        }
    }

    const variables = {};
    for (const item of byName.values()) {
        variables[item.name.toLowerCase()] = Array.from(item.values).join('\n');
    }
    return variables;
}

function applyMailingRuntimeState(row, state) {
    if (!state) return cloneValue(row);
    const merged = cloneValue(row);
    const status = getFirstDefinedValue(state, ['Статус', 'РЎС‚Р°С‚СѓСЃ']);
    const error = getFirstDefinedValue(state, ['Ошибка', 'РћС€РёР±РєР°']);
    const sentAt = getFirstDefinedValue(state, ['Фактическое время отправки', 'Р¤Р°РєС‚РёС‡РµСЃРєРѕРµ РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё']);
    const sentAtMsk = getFirstDefinedValue(state, ['Факт. время отправки (по мск.)', 'Р¤Р°РєС‚. РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)']);

    if (status) {
        setAllValues(merged, ['Статус', 'РЎС‚Р°С‚СѓСЃ'], status);
    }
    if (error || getFirstDefinedValue(state, ['Ошибка', 'РћС€РёР±РєР°']) === '') {
        setAllValues(merged, ['Ошибка', 'РћС€РёР±РєР°'], error);
    }
    if (sentAt || sentAtMsk) {
        setAllValues(merged, ['Фактическое время отправки', 'Р¤Р°РєС‚РёС‡РµСЃРєРѕРµ РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё'], sentAt || sentAtMsk);
        setAllValues(merged, ['Факт. время отправки (по мск.)', 'Р¤Р°РєС‚. РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)'], sentAtMsk || sentAt);
    }

    return merged;
}

function applyDelayedRuntimeState(row, state) {
    if (!state) return cloneValue(row);
    const merged = cloneValue(row);
    const status = getFirstDefinedValue(state, ['Статус', 'РЎС‚Р°С‚СѓСЃ']);
    const error = getFirstDefinedValue(state, ['Ошибка', 'РћС€РёР±РєР°']);
    const sentAt = getFirstDefinedValue(state, ['Фактическое время отправки', 'Р¤Р°РєС‚РёС‡РµСЃРєРѕРµ РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё']);
    const sentAtMsk = getFirstDefinedValue(state, ['Факт. время отправки (по мск.)', 'Р¤Р°РєС‚. РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)']);

    if (status) {
        setAllValues(merged, ['Статус', 'РЎС‚Р°С‚СѓСЃ'], status);
    }
    if (error || getFirstDefinedValue(state, ['Ошибка', 'РћС€РёР±РєР°']) === '') {
        setAllValues(merged, ['Ошибка', 'РћС€РёР±РєР°'], error);
    }
    if (sentAt || sentAtMsk) {
        setAllValues(merged, ['Фактическое время отправки', 'Р¤Р°РєС‚РёС‡РµСЃРєРѕРµ РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё'], sentAt || sentAtMsk);
        setAllValues(merged, ['Факт. время отправки (по мск.)', 'Р¤Р°РєС‚. РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)'], sentAtMsk || sentAt);
    }

    return merged;
}

async function applySheetRuntimeOverlay(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isMailingSheet = sheetName === 'РАССЫЛКА' || sheetName === 'Р РђРЎРЎР«Р›РљРђ';
    const isDelayedSheet = sheetName === 'ОТЛОЖЕННЫЕ' || sheetName === 'РћРўР›РћР–Р•РќРќР«Р•';
    const isUsersSheet = sheetName === 'ПОЛЬЗОВАТЕЛИ' || sheetName === 'РџРћР›Р¬Р—РћР’РђРўР•Р›Р';
    const isProfileSharedSheet = sheetName === 'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ' || sheetName === 'РџР’РЎ РџРћР›Р¬Р—РћР’РђРўР•Р›Р•Р™ РџР РћР¤РР›РЇ' || sheetName === 'Р СџР вЂ™Р РЋ Р СџР С›Р вЂєР В¬Р вЂ”Р С›Р вЂ™Р С’Р СћР вЂўР вЂєР вЂўР в„ў Р СџР В Р С›Р В¤Р ВР вЂєР Р‡';

    if (!isMailingSheet && !isDelayedSheet && !isUsersSheet && !isProfileSharedSheet) {
        return rows;
    }
    if (!isUsersSheet && !isProfileSharedSheet && (!Array.isArray(rows) || rows.length === 0)) {
        return rows;
    }

    if (isUsersSheet) {
        if (!isUserStateStoreEnabled(overrides)) {
            return rows;
        }
        const runtimeRows = await getUserStateStore(overrides).listUserRows(buildUserScope(communityId, profileId));
        return Array.isArray(runtimeRows) && runtimeRows.length ? runtimeRows : rows;
    }

    if (isProfileSharedSheet) {
        if (!isProfileUserSharedStoreEnabled(overrides)) {
            return rows;
        }
        const entries = await getProfileUserSharedStore(overrides).listUserEntries(profileId);
        return buildProfileUserSharedRowsFromEntries(entries);
    }

    if (isMailingSheet && !isMailingDeliveryStoreEnabled(overrides)) {
        return rows;
    }
    if (isDelayedSheet && !isDelayedDeliveryStoreEnabled(overrides)) {
        return rows;
    }

    const result = [];
    for (const row of rows) {
        if (isMailingSheet) {
            const mailingId = getMailingRowNumber(row);
            if (!mailingId) {
                result.push(cloneValue(row));
                continue;
            }
            const runtimeState = await getMailingDeliveryStore(overrides).getMailingState(communityId, mailingId, profileId);
            result.push(applyMailingRuntimeState(row, runtimeState));
            continue;
        }

        const delayedId = getDelayedRowNumber(row);
        if (!delayedId) {
            result.push(cloneValue(row));
            continue;
        }
        const runtimeState = await getDelayedDeliveryStore(overrides).getDelayedRow(communityId, delayedId, profileId);
        result.push(applyDelayedRuntimeState(row, runtimeState));
    }
    return result;
}

async function syncUsersSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isUsersSheet = sheetName === 'ПОЛЬЗОВАТЕЛИ' || sheetName === 'РџРћР›Р¬Р—РћР’РђРўР•Р›Р';
    if (!isUsersSheet) {
        return { synced: false, backend: 'skipped' };
    }
    if (!isUserStateStoreEnabled(overrides)) {
        return { synced: false, backend: 'disabled' };
    }

    const normalizedRows = Array.isArray(rows)
        ? rows
            .map(row => cloneValue(row && typeof row === 'object' ? row : {}))
            .filter(row => String(row && row.ID || '').trim())
        : [];
    const result = await getUserStateStore(overrides).replaceUserRows(
        buildUserScope(communityId, profileId),
        normalizedRows
    );
    if (isProfileUserSharedStoreEnabled(overrides)) {
        const profileSharedStore = getProfileUserSharedStore(overrides);
        for (const row of normalizedRows) {
            const userId = String(row && row.ID || '').trim();
            if (!userId) continue;
            await profileSharedStore.putUserVariables(
                profileId,
                userId,
                buildProfileUserSharedVariablesFromUserRow(row)
            );
        }

        if (isSharedVariablesStoreEnabled(overrides) && typeof profileSharedStore.listUserEntries === 'function') {
            const entries = await profileSharedStore.listUserEntries(profileId);
            await getSharedVariablesStore(overrides).replaceVariables(
                profileId,
                buildSharedVariableCatalogMapFromEntries(entries)
            );
        }
    }

    return {
        synced: true,
        backend: result.backend || 'ydb-user-state',
        stored: result.stored || 0,
        deleted: result.deleted || 0
    };
}

async function syncStructuredTriggerSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isStructuredTriggerSheet = sheetName === 'ТРИГГЕРЫ' || sheetName === 'РўР РР“Р“Р•Р Р«';
    if (!isStructuredTriggerSheet) {
        return { synced: false, backend: 'skipped' };
    }
    if (!isStructuredTriggerStoreEnabled(overrides)) {
        return { synced: false, backend: 'disabled' };
    }

    const normalizedRows = Array.isArray(rows) ? cloneValue(rows) : [];
    const result = await getStructuredTriggerStore(overrides).replaceTriggerRows(communityId, normalizedRows, profileId);
    return {
        synced: true,
        backend: result.backend || 'ydb-structured-triggers',
        stored: result.stored || 0
    };
}

async function syncMessageRuleSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isMessageSheet = sheetName === 'СООБЩЕНИЯ' || sheetName === 'РЎРћРћР‘Р©Р•РќРРЇ';
    if (!isMessageSheet) {
        return { synced: false, backend: 'skipped' };
    }
    if (!isMessageRuleStoreEnabled(overrides)) {
        return { synced: false, backend: 'disabled' };
    }

    const normalizedRows = Array.isArray(rows) ? cloneValue(rows) : [];
    const result = await getMessageRuleStore(overrides).replaceRuleRows(communityId, normalizedRows, profileId);
    return {
        synced: true,
        backend: result.backend || 'ydb-message-rules',
        stored: result.stored || 0
    };
}

async function syncCommentRuleSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isCommentSheet = sheetName === 'КОММЕНТАРИИ В ПОСТАХ' || sheetName === 'РљРћРњРњР•РќРўРђР РР Р’ РџРћРЎРўРђРҐ';
    if (!isCommentSheet) {
        return { synced: false, backend: 'skipped' };
    }
    if (!isCommentRuleStoreEnabled(overrides)) {
        return { synced: false, backend: 'disabled' };
    }

    const normalizedRows = Array.isArray(rows) ? cloneValue(rows) : [];
    const result = await getCommentRuleStore(overrides).replaceRuleRows(communityId, normalizedRows, profileId);
    return {
        synced: true,
        backend: result.backend || 'ydb-comment-rules',
        stored: result.stored || 0
    };
}

async function syncCommunityVariablesSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isVariablesSheet = sheetName === 'ПЕРЕМЕННЫЕ' || sheetName === 'РџР•Р Р•РњР•РќРќР«Р•';
    if (!isVariablesSheet) {
        return { synced: false, backend: 'skipped' };
    }
    if (!isCommunityVariablesStoreEnabled(overrides)) {
        return { synced: false, backend: 'disabled' };
    }

    const normalizedRows = Array.isArray(rows) ? cloneValue(rows) : [];
    const state = buildCommunityVariableStateFromRows(normalizedRows);
    await getCommunityVariablesStore(overrides).replaceGlobalVariables(communityId, state.globalVars, profileId);
    await getCommunityVariablesStore(overrides).replaceVkVariables(communityId, state.vkVars, profileId);
    await getCommunityVariablesStore(overrides).ensureUserVariableCatalog(communityId, state.userVariableNames, profileId);
    return {
        synced: true,
        backend: 'ydb-community-variables',
        stored: Object.keys(state.globalVars).length + Object.keys(state.vkVars).length + state.userVariableNames.length
    };
}

async function syncProfileUserSharedSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isProfileSharedSheet = sheetName === 'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ' || sheetName === 'РџР’РЎ РџРћР›Р¬Р—РћР’РђРўР•Р›Р•Р™ РџР РћР¤РР›РЇ';
    if (!isProfileSharedSheet) {
        return { synced: false, backend: 'skipped' };
    }
    if (!isProfileUserSharedStoreEnabled(overrides) && !isSharedVariablesStoreEnabled(overrides)) {
        return { synced: false, backend: 'disabled' };
    }

    const normalizedRows = Array.isArray(rows) ? cloneValue(rows) : [];
    const entries = buildProfileUserSharedEntriesFromRows(normalizedRows);
    const sharedCatalog = buildSharedVariableCatalogMap(normalizedRows);

    if (isProfileUserSharedStoreEnabled(overrides)) {
        await getProfileUserSharedStore(overrides).replaceUserEntries(profileId, entries);
    }
    if (isSharedVariablesStoreEnabled(overrides)) {
        await getSharedVariablesStore(overrides).replaceVariables(profileId, sharedCatalog);
    }

    return {
        synced: true,
        backend: 'ydb-profile-user-shared',
        stored: entries.length
    };
}

async function syncSharedVariablesSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isSharedSheet = sheetName === 'ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ' || sheetName === 'РџР•Р Р•РњР•РќРќР«Р• Р’РЎР•РҐ РЎРћРћР‘Р©Р•РЎРўР’';
    if (!isSharedSheet) {
        return { synced: false, backend: 'skipped' };
    }
    if (!isSharedVariablesStoreEnabled(overrides)) {
        return { synced: false, backend: 'disabled' };
    }

    const normalizedRows = Array.isArray(rows) ? cloneValue(rows) : [];
    const sharedCatalog = buildSharedVariableCatalogMap(normalizedRows);
    const result = await getSharedVariablesStore(overrides).replaceVariables(profileId, sharedCatalog);
    return {
        synced: true,
        backend: result.backend || 'ydb-shared-variables',
        stored: result.stored || Object.keys(sharedCatalog).length
    };
}

async function syncMailingSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isMailingSheet = sheetName === 'РАССЫЛКА' || sheetName === 'Р РђРЎРЎР«Р›РљРђ';
    if (!isMailingSheet) {
        return { synced: false, backend: 'skipped' };
    }
    if (!isMailingDeliveryStoreEnabled(overrides)) {
        return { synced: false, backend: 'disabled' };
    }

    const normalizedRows = Array.isArray(rows) ? cloneValue(rows) : [];
    const result = await getMailingDeliveryStore(overrides).replaceMailingRows(communityId, normalizedRows, profileId);
    return {
        synced: true,
        backend: result.backend || 'ydb-mailing-delivery',
        stored: result.rows || normalizedRows.length
    };
}

async function syncDelayedSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const isDelayedSheet = sheetName === 'ОТЛОЖЕННЫЕ' || sheetName === 'РћРўР›РћР–Р•РќРќР«Р•';
    if (!isDelayedSheet) {
        return { synced: false, backend: 'skipped' };
    }
    if (!isDelayedDeliveryStoreEnabled(overrides)) {
        return { synced: false, backend: 'disabled' };
    }

    const normalizedRows = Array.isArray(rows) ? cloneValue(rows) : [];
    const result = await getDelayedDeliveryStore(overrides).replaceDelayedRows(communityId, normalizedRows, profileId);
    return {
        synced: true,
        backend: result.backend || 'ydb-delayed-delivery',
        stored: result.rows || normalizedRows.length
    };
}

async function syncStructuredReadModelSheet(sheetName, rows, communityId, profileId = '1', overrides = {}) {
    const handlers = [
        syncUsersSheet,
        syncStructuredTriggerSheet,
        syncMessageRuleSheet,
        syncCommentRuleSheet,
        syncCommunityVariablesSheet,
        syncProfileUserSharedSheet,
        syncSharedVariablesSheet,
        syncMailingSheet,
        syncDelayedSheet
    ];

    for (const handler of handlers) {
        const result = await handler(sheetName, rows, communityId, profileId, overrides);
        if (result && result.synced) {
            return result;
        }
    }

    return {
        synced: false,
        backend: 'skipped'
    };
}

async function initializeStorage() {
    log('info', '🔧 Checking Object Storage initialization...');

    for (const fileName of COMMON_FILES) {
        await hotStateStore.ensureJsonObject(fileName, buildDefaultCommonFile(fileName));
        try {
            await s3Send(new GetObjectCommand({ Bucket: BUCKET_NAME, Key: fileName }));
        } catch (error) {
            if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
                const body = JSON.stringify({
                        defaultProfileId: '1',
                        profiles: {
                            '1': {
                                id: '1',
                                name: 'Профиль 1',
                        username: process.env.ADMIN_USERNAME || 'admin',
                        password: process.env.ADMIN_PASSWORD || 'admin123',
                        recoveryEmail: process.env.ADMIN_EMAIL || 'admin@example.com'
                            }
                        }
                    }, null, 2);
                await s3Send(new PutObjectCommand({
                    Bucket: BUCKET_NAME, Key: fileName,
                    Body: body, ContentType: 'application/json'
                }));
                log('info', `✅ Created ${fileName}`);
            }
        }
    }

    log('info', '✅ Storage initialization completed');
}

function invalidateCache(sheetName, communityId, profileId = '1') {
    const pid = normalizeProfileId(profileId);
    const cacheKey = communityId ? `${pid}_${sheetName}_${communityId}` : `${pid}_${sheetName}`;
    delete memoryCache.data[cacheKey];
    delete memoryCache.lastUpdated[cacheKey];
    if (sheetName === 'СООБЩЕНИЯ' || sheetName === 'КОММЕНТАРИИ В ПОСТАХ' || sheetName === 'ПЕРЕМЕННЫЕ' || sheetName === 'ПОЛЬЗОВАТЕЛИ') {
        const uk = communityId ? `${pid}_ПОЛЬЗОВАТЕЛИ_${communityId}` : `${pid}_ПОЛЬЗОВАТЕЛИ`;
        delete memoryCache.data[uk]; delete memoryCache.lastUpdated[uk];
    }
}

async function getSheetData(sheetName, communityId, profileId = '1') {
    const pid = normalizeProfileId(profileId);
    const fileName = getFileName(sheetName, communityId, pid);
    if (!fileName) { log('error', `Unknown sheet: ${sheetName}`); return []; }

    const cacheKey = communityId ? `${pid}_${sheetName}_${communityId}` : `${pid}_${sheetName}`;
    const now = Date.now();
    const ttl = isUserStateStoreEnabled() && sheetName === 'ПОЛЬЗОВАТЕЛИ' ? 0 : (memoryCache.ttl[sheetName] || 300000);

    // Debug logging
    log('debug', `📂 getSheetData: sheet=${sheetName}, communityId=${communityId}, fileName=${fileName}, cacheKey=${cacheKey}, cacheHit=${!!memoryCache.data[cacheKey] && (now - memoryCache.lastUpdated[cacheKey]) < ttl}`);

    if (memoryCache.data[cacheKey] && (now - memoryCache.lastUpdated[cacheKey]) < ttl) {
        log('debug', `💾 getSheetData: Returning from cache, ${memoryCache.data[cacheKey].length} rows`);
        return memoryCache.data[cacheKey];
    }

    try {
        const result = await hotStateStore.loadJsonObject(fileName, {
            defaultValue: DEFAULT_DATA[sheetName] || [],
            legacyKeys: pid === '1' ? [getLegacyFileName(sheetName, communityId)] : []
        });
        const json = await applySheetRuntimeOverlay(sheetName, result.value, communityId, pid);
        memoryCache.data[cacheKey] = json;
        memoryCache.lastUpdated[cacheKey] = now;
        log('debug', `📥 Loaded ${fileName}: ${json?.length || 0} rows`);
        return json;
    } catch (error) {
        if (pid === '1') {
            const legacyFileName = getLegacyFileName(sheetName, communityId);
            try {
                const legacyResponse = await s3Send(new GetObjectCommand({ Bucket: BUCKET_NAME, Key: legacyFileName }));
                const legacyData = await legacyResponse.Body.transformToString();
                const legacyJson = await applySheetRuntimeOverlay(sheetName, JSON.parse(legacyData), communityId, pid);
                memoryCache.data[cacheKey] = legacyJson;
                memoryCache.lastUpdated[cacheKey] = now;
                log('debug', `📥 Loaded legacy file ${legacyFileName}: ${legacyJson?.length || 0} rows`);
                return legacyJson;
            } catch (legacyError) {
                log('warn', `Legacy file ${legacyFileName} not found: ${legacyError.message}`);
            }
        }
        log('warn', `File ${fileName} not found or error: ${error.message}, returning default`);
        const defaultData = DEFAULT_DATA[sheetName] || [];
        log('debug', `📥 getSheetData: Returning default, ${defaultData.length} rows`);
        return defaultData;
    }
}

async function saveSheetData(sheetName, data, communityId, profileId = '1') {
    const pid = normalizeProfileId(profileId);
    const fileName = getFileName(sheetName, communityId, pid);
    if (!fileName) throw new Error(`Unknown sheet: ${sheetName}`);

    try {
        log('info', `💾 Saving ${fileName}...`);
        await hotStateStore.saveJsonObject(fileName, data);
        await syncStructuredReadModelSheet(sheetName, data, communityId, pid);
        const cacheKey = communityId ? `${pid}_${sheetName}_${communityId}` : `${pid}_${sheetName}`;
        delete memoryCache.data[cacheKey]; delete memoryCache.lastUpdated[cacheKey];
        if (sheetName === 'СООБЩЕНИЯ' || sheetName === 'КОММЕНТАРИИ В ПОСТАХ' || sheetName === 'ПЕРЕМЕННЫЕ' || sheetName === 'ПОЛЬЗОВАТЕЛИ') {
            const uk = communityId ? `${pid}_ПОЛЬЗОВАТЕЛИ_${communityId}` : `${pid}_ПОЛЬЗОВАТЕЛИ`;
            delete memoryCache.data[uk]; delete memoryCache.lastUpdated[uk];
        }
        log('info', `✅ Saved ${fileName}`);
        return true;
    } catch (error) {
        log('error', `❌ Error saving ${fileName}:`, error.message);
        throw error;
    }
}

async function updateSheetData(sheetName, communityId, profileId = '1', updater) {
    if (typeof updater !== 'function') {
        throw new Error('updater must be a function');
    }

    const pid = normalizeProfileId(profileId);
    const fileName = getFileName(sheetName, communityId, pid);
    if (!fileName) throw new Error(`Unknown sheet: ${sheetName}`);

    const result = await hotStateStore.updateJsonObject(
        fileName,
        currentValue => {
            const normalizedCurrent = Array.isArray(currentValue)
                ? currentValue
                : JSON.parse(JSON.stringify(DEFAULT_DATA[sheetName] || []));
            return updater(normalizedCurrent);
        },
        {
            defaultValue: DEFAULT_DATA[sheetName] || [],
            legacyKeys: pid === '1' ? [getLegacyFileName(sheetName, communityId)] : []
        }
    );

    const cacheKey = communityId ? `${pid}_${sheetName}_${communityId}` : `${pid}_${sheetName}`;
    memoryCache.data[cacheKey] = result.value;
    memoryCache.lastUpdated[cacheKey] = Date.now();
    await syncStructuredReadModelSheet(sheetName, result.value, communityId, pid);
    if (sheetName === 'РЎРћРћР‘Р©Р•РќРРЇ' || sheetName === 'РљРћРњРњР•РќРўРђР РР Р’ РџРћРЎРўРђРҐ' || sheetName === 'РџР•Р Р•РњР•РќРќР«Р•') {
        const uk = communityId ? `${pid}_РџРћР›Р¬Р—РћР’РђРўР•Р›Р_${communityId}` : `${pid}_РџРћР›Р¬Р—РћР’РђРўР•Р›Р`;
        delete memoryCache.data[uk];
        delete memoryCache.lastUpdated[uk];
    }

    return result;
}

function getFileMap() { return FILE_BASE; }

module.exports = {
    getS3Client, getBucketName, initializeStorage, invalidateCache,
    getSheetData, saveSheetData, updateSheetData, getFileMap, DEFAULT_DATA, getFileName,
    getLegacyFileName, normalizeProfileId,
    __testOnly: {
        applySheetRuntimeOverlay,
        syncStructuredTriggerSheet,
        syncMessageRuleSheet,
        syncCommentRuleSheet,
        syncCommunityVariablesSheet,
        syncProfileUserSharedSheet,
        syncSharedVariablesSheet,
        syncUsersSheet,
        syncStructuredReadModelSheet
    }
};
