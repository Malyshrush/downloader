const crypto = require('node:crypto');
const { GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getS3Client } = require('./storage');

const DEFAULT_BUCKET_NAME = 'bot-data-storage';
const MAX_IMAGE_BYTES = 2 * 1024 * 1024;
const MIME_TO_EXTENSION = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/webp': 'webp'
};

function clean(value) {
    return String(value || '').trim();
}

function cleanKeySegment(value, fallback = '') {
    const segment = clean(value) || fallback;
    if (!/^[a-zA-Z0-9_-]+$/.test(segment)) {
        throw new Error('Invalid Mini App asset key segment');
    }
    return segment;
}

function getBucketName() {
    return process.env.BUCKET_NAME || DEFAULT_BUCKET_NAME;
}

function validateMiniAppImageUpload({ contentType, buffer } = {}) {
    const type = clean(contentType).toLowerCase();
    if (!Buffer.isBuffer(buffer) || buffer.length === 0) throw new Error('Mini App image is empty');
    if (!MIME_TO_EXTENSION[type]) throw new Error('Unsupported Mini App image type');
    if (buffer.length > MAX_IMAGE_BYTES) throw new Error('Mini App image is too large');
    return { contentType: type, extension: MIME_TO_EXTENSION[type] };
}

function createMiniAppAssetId() {
    return 'asset_' + crypto.randomBytes(12).toString('hex');
}

function buildMiniAppAssetKey({ profileId, communityId, assetId, extension }) {
    const safeProfileId = cleanKeySegment(profileId, '1');
    const safeCommunityId = cleanKeySegment(communityId);
    const safeAssetId = cleanKeySegment(assetId);
    const safeExtension = cleanKeySegment(extension);
    return `miniapp-assets/profile_${safeProfileId}/community_${safeCommunityId}/${safeAssetId}.${safeExtension}`;
}

function createMiniAppAssetUrl({ baseUrl, assetId }) {
    const url = new URL(clean(baseUrl));
    url.search = '';
    url.searchParams.set('miniappAsset', clean(assetId));
    return url.toString();
}

async function saveMiniAppAssetWithDependencies(payload = {}, overrides = {}) {
    const validation = validateMiniAppImageUpload(payload);
    const assetId = clean(payload.assetId) || createMiniAppAssetId();
    const key = buildMiniAppAssetKey({
        profileId: payload.profileId,
        communityId: payload.communityId,
        assetId,
        extension: validation.extension
    });
    const s3Client = overrides.s3Client || getS3Client();

    await s3Client.send(new PutObjectCommand({
        Bucket: getBucketName(),
        Key: key,
        Body: payload.buffer,
        ContentType: validation.contentType
    }));

    return {
        assetId,
        key,
        contentType: validation.contentType,
        url: createMiniAppAssetUrl({ baseUrl: payload.baseUrl, assetId })
    };
}

async function streamToBuffer(stream) {
    if (Buffer.isBuffer(stream)) return stream;
    if (!stream) return Buffer.alloc(0);
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks);
}

async function readMiniAppAssetWithDependencies(assetId, overrides = {}) {
    const cleanAssetId = clean(assetId);
    const lookup = overrides.lookupAsset ? await overrides.lookupAsset(cleanAssetId) : null;
    if (!lookup || !lookup.key) {
        throw new Error('Mini App asset lookup is required');
    }
    const key = lookup.key;
    const contentType = lookup && lookup.contentType ? lookup.contentType : undefined;
    const s3Client = overrides.s3Client || getS3Client();
    const result = await s3Client.send(new GetObjectCommand({
        Bucket: getBucketName(),
        Key: key
    }));

    return {
        buffer: await streamToBuffer(result.Body),
        contentType: result.ContentType || contentType || ''
    };
}

module.exports = {
    validateMiniAppImageUpload,
    createMiniAppAssetId,
    buildMiniAppAssetKey,
    createMiniAppAssetUrl,
    saveMiniAppAssetWithDependencies,
    readMiniAppAssetWithDependencies
};
