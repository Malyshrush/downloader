/**
 * Модуль работы с Yandex Object Storage (S3)
 * Файлы данных сегментированы по сообществам: messages_community_777.json
 */

const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { log } = require('../utils/logger');
const { createHotStateStore } = require('./hot-state-store');

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
    if (sheetName === 'СООБЩЕНИЯ' || sheetName === 'КОММЕНТАРИИ В ПОСТАХ' || sheetName === 'ПЕРЕМЕННЫЕ') {
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
    const ttl = memoryCache.ttl[sheetName] || 300000;

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
        const json = result.value;
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
                const legacyJson = JSON.parse(legacyData);
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
        const cacheKey = communityId ? `${pid}_${sheetName}_${communityId}` : `${pid}_${sheetName}`;
        delete memoryCache.data[cacheKey]; delete memoryCache.lastUpdated[cacheKey];
        if (sheetName === 'СООБЩЕНИЯ' || sheetName === 'КОММЕНТАРИИ В ПОСТАХ' || sheetName === 'ПЕРЕМЕННЫЕ') {
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

function getFileMap() { return FILE_BASE; }

module.exports = {
    getS3Client, getBucketName, initializeStorage, invalidateCache,
    getSheetData, saveSheetData, getFileMap, DEFAULT_DATA, getFileName,
    getLegacyFileName, normalizeProfileId
};
