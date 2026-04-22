const fs = require('fs');
const path = require('path');
const { GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { log } = require('../utils/logger');
const { getS3Client, getBucketName } = require('./storage');
const { createHotStateStore } = require('./hot-state-store');

const BOT_VERSION_KEY = 'bot_version.json';

function getLocalVersionFileCandidates() {
    return [
        path.join(__dirname, '..', '..', 'bot-version.json'),
        path.join(process.cwd(), 'bot-version.json')
    ];
}

function readLocalVersionFile() {
    for (const filePath of getLocalVersionFileCandidates()) {
        try {
            if (fs.existsSync(filePath)) {
                return JSON.parse(fs.readFileSync(filePath, 'utf8'));
            }
        } catch (error) {
            log('warn', '⚠️ Failed to read local bot-version.json: ' + error.message);
        }
    }
    return null;
}

async function readBucketVersionFile() {
    try {
        const response = await getS3Client().send(new GetObjectCommand({
            Bucket: getBucketName(),
            Key: BOT_VERSION_KEY
        }));
        const content = await response.Body.transformToString();
        return JSON.parse(content);
    } catch (error) {
        return null;
    }
}

function normalizeParts(parts) {
    return Array.isArray(parts) ? parts.map(function(part) {
        return {
            key: String(part.key || '').trim(),
            value: String(part.value || '').trim(),
            label: String(part.label || '').trim(),
            description: String(part.description || '').trim(),
            currentSummary: String(part.currentSummary || '').trim(),
            history: Array.isArray(part.history) ? part.history.map(function(item) {
                return {
                    version: String(item.version || '').trim(),
                    summary: String(item.summary || '').trim()
                };
            }).filter(function(item) {
                return item.version || item.summary;
            }) : []
        };
    }) : [];
}

function normalizeCapabilities(capabilities) {
    return Array.isArray(capabilities) ? capabilities.map(function(group) {
        return {
            title: String(group.title || '').trim(),
            items: Array.isArray(group.items)
                ? group.items.map(function(item) { return String(item || '').trim(); }).filter(Boolean)
                : []
        };
    }).filter(function(group) {
        return group.title || group.items.length;
    }) : [];
}

function buildDisplayVersion(parts) {
    const values = normalizeParts(parts).map(function(part) { return part.value || '0000'; });
    return 'version ' + values.join('.');
}

function normalizeVersionData(data) {
    const normalizedParts = normalizeParts(data?.parts || []);
    return {
        displayVersion: buildDisplayVersion(normalizedParts),
        updatedAt: new Date().toISOString().slice(0, 10),
        baseline: data?.baseline !== false,
        note: String(data?.note || '').trim(),
        capabilities: normalizeCapabilities(data?.capabilities || []),
        parts: normalizedParts
    };
}

async function getBotVersionData() {
    const bucketVersion = await readBucketVersionFileWithDependencies();
    if (bucketVersion) return normalizeVersionData(bucketVersion);

    const localVersion = readLocalVersionFile();
    if (localVersion) return normalizeVersionData(localVersion);

    return normalizeVersionData({
        baseline: false,
        note: 'Файл версии не найден.',
        parts: []
    });
}

async function saveBotVersionData(data) {
    return saveBotVersionDataWithDependencies(data);
}

async function readBucketVersionFileWithDependencies(overrides = {}) {
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    try {
        const response = await hotStateStore.loadJsonObject(BOT_VERSION_KEY, {
            defaultValue: undefined
        });
        if (!response || response.source === 'default') {
            return null;
        }
        return response.value;
    } catch (error) {
        return null;
    }
}

async function saveBotVersionDataWithDependencies(data, overrides = {}) {
    const normalized = normalizeVersionData(data);
    const hotStateStore = overrides.hotStateStore || createHotStateStore();
    await hotStateStore.saveJsonObject(BOT_VERSION_KEY, normalized);
    return normalized;
}

module.exports = {
    BOT_VERSION_KEY,
    buildDisplayVersion,
    getBotVersionData,
    saveBotVersionData,
    __testOnly: {
        readBucketVersionFileWithDependencies,
        saveBotVersionDataWithDependencies
    }
};
