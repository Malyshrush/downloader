const test = require('node:test');
const assert = require('node:assert/strict');

const {
    validateMiniAppImageUpload,
    buildMiniAppAssetKey,
    createMiniAppAssetUrl,
    saveMiniAppAssetWithDependencies
} = require('../src/modules/miniapp-assets');

test('validateMiniAppImageUpload accepts png and jpeg under size limit', () => {
    assert.doesNotThrow(() => validateMiniAppImageUpload({
        contentType: 'image/png',
        buffer: Buffer.from('png')
    }));
    assert.doesNotThrow(() => validateMiniAppImageUpload({
        contentType: 'image/jpeg',
        buffer: Buffer.from('jpg')
    }));
});

test('validateMiniAppImageUpload rejects non-image content', () => {
    assert.throws(() => validateMiniAppImageUpload({
        contentType: 'application/pdf',
        buffer: Buffer.from('pdf')
    }), /Unsupported Mini App image type/);
});

test('validateMiniAppImageUpload rejects empty buffer before checking content type', () => {
    assert.throws(() => validateMiniAppImageUpload({
        contentType: undefined,
        buffer: Buffer.alloc(0)
    }), /Mini App image is empty/);
});

test('validateMiniAppImageUpload rejects non-buffer image as empty', () => {
    assert.throws(() => validateMiniAppImageUpload({
        contentType: 'image/png',
        buffer: null
    }), /Mini App image is empty/);
});

test('buildMiniAppAssetKey scopes assets by profile and community', () => {
    const key = buildMiniAppAssetKey({
        profileId: '1',
        communityId: '229445618',
        assetId: 'asset_abc',
        extension: 'png'
    });

    assert.equal(key, 'miniapp-assets/profile_1/community_229445618/asset_abc.png');
});

test('createMiniAppAssetUrl points to PAPA BOT public asset endpoint', () => {
    const url = createMiniAppAssetUrl({
        baseUrl: 'https://bot.example/handler',
        assetId: 'asset_abc'
    });

    assert.equal(url, 'https://bot.example/handler?miniappAsset=asset_abc');
});

test('saveMiniAppAssetWithDependencies writes image to S3 and returns public url', async () => {
    const calls = [];
    const fakeS3Client = {
        async send(command) {
            calls.push(command);
            return {};
        }
    };
    const buffer = Buffer.from('png');

    const result = await saveMiniAppAssetWithDependencies({
        profileId: '1',
        communityId: '229445618',
        assetId: 'asset_abc',
        contentType: 'image/png',
        buffer,
        baseUrl: 'https://bot.example/handler?old=query'
    }, { s3Client: fakeS3Client });

    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0].input, {
        Bucket: 'bot-data-storage',
        Key: 'miniapp-assets/profile_1/community_229445618/asset_abc.png',
        Body: buffer,
        ContentType: 'image/png'
    });
    assert.deepEqual(result, {
        assetId: 'asset_abc',
        key: 'miniapp-assets/profile_1/community_229445618/asset_abc.png',
        contentType: 'image/png',
        url: 'https://bot.example/handler?miniappAsset=asset_abc'
    });
});
