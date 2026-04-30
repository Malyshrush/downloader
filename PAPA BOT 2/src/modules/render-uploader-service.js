const fs = require('fs');
const axios = require('axios');
const FormData = require('form-data');

const VK_UPLOAD_TIMEOUT_MS = 300000;
const VK_API_TIMEOUT_MS = 60000;

function appendFileToFormWithDependencies(form, fieldName, file, overrides = {}) {
    const createReadStream = overrides.createReadStream || fs.createReadStream;
    form.append(fieldName, createReadStream(file.path), {
        filename: file.originalname,
        contentType: file.mimetype || 'application/octet-stream'
    });
}

async function cleanupFileWithDependencies(filePath, overrides = {}) {
    if (!filePath) return;
    const unlink = overrides.unlink || fs.promises.unlink.bind(fs.promises);
    try {
        await unlink(filePath);
    } catch (error) {
        if (error && error.code !== 'ENOENT') {
            throw error;
        }
    }
}

async function vkGet(url, params, overrides = {}) {
    const httpGet = overrides.httpGet || axios.get;
    return httpGet(url, {
        params,
        timeout: VK_API_TIMEOUT_MS
    });
}

async function vkPost(url, data, options = {}, overrides = {}) {
    const httpPost = overrides.httpPost || axios.post;
    return httpPost(url, data, {
        timeout: VK_UPLOAD_TIMEOUT_MS,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        ...options
    });
}

function createUploadForm(file, fieldName, overrides = {}) {
    const FormCtor = overrides.FormDataCtor || FormData;
    const form = new FormCtor();
    appendFileToFormWithDependencies(form, fieldName, file, overrides);
    return form;
}

function normalizeGroupId(groupId) {
    return Math.abs(Number(groupId));
}

function buildVkError(prefix, error) {
    if (!error) return new Error(prefix);
    if (error.data?.error?.error_msg) return new Error(`${prefix}: ${error.data.error.error_msg}`);
    if (error.response?.data?.error?.error_msg) return new Error(`${prefix}: ${error.response.data.error.error_msg}`);
    return new Error(`${prefix}: ${error.message || String(error)}`);
}

function isPeerRequiredError(error) {
    const message = String(
        error?.message ||
        error?.data?.error?.error_msg ||
        error?.response?.data?.error?.error_msg ||
        ''
    ).toLowerCase();
    return message.includes('peer_id') || message.includes('peer id');
}

async function tryUploadStrategies(strategies) {
    const errors = [];
    for (const strategy of strategies) {
        if (!strategy.enabled) continue;
        try {
            return await strategy.run();
        } catch (error) {
            errors.push(`${strategy.name}: ${error.message}`);
            if (strategy.onlyOnPeerRequired && !isPeerRequiredError(error)) {
                break;
            }
        }
    }
    throw new Error(errors.join('; ') || 'VK upload failed');
}

async function uploadPhotoToMessages(userToken, groupId, file, overrides = {}) {
    const communityToken = overrides.communityToken || '';
    const absGroupId = normalizeGroupId(groupId);
    return tryUploadStrategies([
        {
            name: 'photo messages user token',
            enabled: !!userToken,
            run: () => uploadPhotoToMessagesWithToken(userToken, absGroupId, file, overrides)
        },
        {
            name: 'photo messages community token',
            enabled: !!communityToken,
            run: () => uploadPhotoToMessagesWithToken(communityToken, absGroupId, file, overrides)
        }
    ]);
}

async function uploadPhotoToMessagesWithToken(token, absGroupId, file, overrides = {}) {
    const uploadRes = await vkGet('https://api.vk.com/method/photos.getMessagesUploadServer', {
        group_id: absGroupId,
        access_token: token,
        v: '5.199'
    }, overrides);
    if (uploadRes.data.error) throw new Error('VK API Error: ' + uploadRes.data.error.error_msg);

    const form = createUploadForm(file, 'photo', overrides);
    const uploadResult = await vkPost(uploadRes.data.response.upload_url, form, {
        headers: form.getHeaders()
    }, overrides);

    const { server, photo, hash } = uploadResult.data;
    if (!server || !photo || !hash) throw new Error('Ошибка загрузки фото на сервер VK');

    const saveRes = await vkPost('https://api.vk.com/method/photos.saveMessagesPhoto', null, {
        params: { server, photo, hash, group_id: absGroupId, access_token: token, v: '5.199' }
    }, overrides);
    if (saveRes.data.error) throw new Error('VK API Error: ' + saveRes.data.error.error_msg);

    const savedPhoto = saveRes.data.response[0];
    return `photo${savedPhoto.owner_id}_${savedPhoto.id}`;
}

async function uploadPhotoToWall(userToken, groupId, file, overrides = {}) {
    const communityToken = overrides.communityToken || '';
    const absGroupId = normalizeGroupId(groupId);
    return tryUploadStrategies([
        {
            name: 'photo wall user token',
            enabled: !!userToken,
            run: () => uploadPhotoToWallWithToken(userToken, absGroupId, file, overrides)
        },
        {
            name: 'photo wall community token',
            enabled: !!communityToken,
            run: () => uploadPhotoToWallWithToken(communityToken, absGroupId, file, overrides)
        }
    ]);
}

async function uploadPhotoToWallWithToken(token, absGroupId, file, overrides = {}) {
    const uploadRes = await vkGet('https://api.vk.com/method/photos.getWallUploadServer', {
        group_id: absGroupId,
        access_token: token,
        v: '5.199'
    }, overrides);
    if (uploadRes.data.error) throw new Error(uploadRes.data.error.error_msg);

    const form = createUploadForm(file, 'photo', overrides);
    const uploadResult = await vkPost(uploadRes.data.response.upload_url, form, {
        headers: form.getHeaders()
    }, overrides);

    const { server, photo, hash } = uploadResult.data;
    if (!server || !photo || !hash) throw new Error('Ошибка загрузки фото на сервер VK');

    const saveRes = await vkPost('https://api.vk.com/method/photos.saveWallPhoto', null, {
        params: { group_id: absGroupId, server, photo, hash, access_token: token, v: '5.199' }
    }, overrides);
    if (saveRes.data.error) throw new Error(saveRes.data.error.error_msg);

    const savedPhoto = saveRes.data.response[0];
    return `photo${savedPhoto.owner_id}_${savedPhoto.id}`;
}

async function uploadDocToMessages(userToken, groupId, file, overrides = {}) {
    const communityToken = overrides.communityToken || '';
    const absGroupId = normalizeGroupId(groupId);
    return tryUploadStrategies([
        {
            name: 'doc messages user token',
            enabled: !!userToken,
            run: () => uploadDocToMessagesWithUserToken(userToken, file, overrides)
        },
        {
            name: 'doc wall user token',
            enabled: !!userToken,
            onlyOnPeerRequired: false,
            run: () => uploadDocToWallWithToken(userToken, absGroupId, file, overrides)
        },
        {
            name: 'doc messages community token',
            enabled: !!communityToken,
            run: () => uploadDocToMessagesWithCommunityToken(communityToken, absGroupId, file, overrides)
        }
    ]);
}

async function uploadDocToMessagesWithUserToken(userToken, file, overrides = {}) {
    const uploadServerRes = await vkGet('https://api.vk.com/method/docs.getMessagesUploadServer', {
        type: 'doc',
        access_token: userToken,
        v: '5.199'
    }, overrides);
    if (uploadServerRes.data.error) throw new Error('VK API Error: ' + uploadServerRes.data.error.error_msg);

    const form = createUploadForm(file, 'file', overrides);
    const uploadResult = await vkPost(uploadServerRes.data.response.upload_url, form, {
        headers: form.getHeaders()
    }, overrides);

    const uploadedFile = uploadResult.data.file;
    if (!uploadedFile) throw new Error('Ошибка загрузки документа на сервер VK');

    const saveRes = await vkPost('https://api.vk.com/method/docs.save', null, {
        params: { file: uploadedFile, access_token: userToken, v: '5.199' }
    }, overrides);
    if (saveRes.data.error) throw new Error('VK API Error: ' + saveRes.data.error.error_msg);

    const savedDoc = saveRes.data.response.doc || saveRes.data.response;
    if (!savedDoc) throw new Error('Документ не сохранён');
    return `doc${savedDoc.owner_id}_${savedDoc.id}`;
}

async function uploadDocToMessagesWithCommunityToken(communityToken, absGroupId, file, overrides = {}) {
    const peerId = -absGroupId;
    const uploadServerRes = await vkGet('https://api.vk.com/method/docs.getMessagesUploadServer', {
        peer_id: peerId,
        access_token: communityToken,
        v: '5.199'
    }, overrides);
    if (uploadServerRes.data.error) throw new Error('VK API Error: ' + uploadServerRes.data.error.error_msg);

    const form = createUploadForm(file, 'file', overrides);
    const uploadResult = await vkPost(uploadServerRes.data.response.upload_url, form, {
        headers: form.getHeaders()
    }, overrides);

    const uploadedFile = uploadResult.data.file;
    if (!uploadedFile) throw new Error('Ошибка загрузки документа на сервер VK');

    const saveRes = await vkPost('https://api.vk.com/method/docs.save', null, {
        params: { file: uploadedFile, peer_id: peerId, access_token: communityToken, v: '5.199' }
    }, overrides);
    if (saveRes.data.error) throw new Error('VK API Error: ' + saveRes.data.error.error_msg);

    const savedDoc = saveRes.data.response.doc || saveRes.data.response;
    if (!savedDoc) throw new Error('Документ не сохранён');
    return `doc${savedDoc.owner_id}_${savedDoc.id}`;
}

async function uploadDocToWall(userToken, groupId, file, overrides = {}) {
    const communityToken = overrides.communityToken || '';
    const absGroupId = normalizeGroupId(groupId);
    return tryUploadStrategies([
        {
            name: 'doc wall user token',
            enabled: !!userToken,
            run: () => uploadDocToWallWithToken(userToken, absGroupId, file, overrides)
        },
        {
            name: 'doc wall community token',
            enabled: !!communityToken,
            run: () => uploadDocToWallWithToken(communityToken, absGroupId, file, overrides)
        }
    ]);
}

async function uploadDocToWallWithToken(token, absGroupId, file, overrides = {}) {
    const uploadServerRes = await vkGet('https://api.vk.com/method/docs.getWallUploadServer', {
        group_id: absGroupId,
        access_token: token,
        v: '5.199'
    }, overrides);
    if (uploadServerRes.data.error) throw new Error(uploadServerRes.data.error.error_msg);

    const form = createUploadForm(file, 'file', overrides);
    const uploadResult = await vkPost(uploadServerRes.data.response.upload_url, form, {
        headers: form.getHeaders()
    }, overrides);

    const uploadedFile = uploadResult.data.file;
    if (!uploadedFile) throw new Error('Ошибка загрузки документа на сервер VK');

    const saveRes = await vkPost('https://api.vk.com/method/docs.save', null, {
        params: { file: uploadedFile, group_id: absGroupId, access_token: token, v: '5.199' }
    }, overrides);
    if (saveRes.data.error) throw new Error(saveRes.data.error.error_msg);

    const savedDoc = saveRes.data.response.doc;
    if (!savedDoc) throw new Error('Документ не сохранён');
    return `doc${savedDoc.owner_id}_${savedDoc.id}`;
}

async function uploadVideoToMessages(userToken, groupId, file, overrides = {}) {
    const saveRes = await vkGet('https://api.vk.com/method/video.save', {
        access_token: userToken,
        name: file.originalname || 'video.mp4',
        privacy_view: 'only_me',
        v: '5.199'
    }, overrides);
    if (saveRes.data.error) throw new Error('VK API Error: ' + saveRes.data.error.error_msg);

    const { upload_url, video_id, owner_id } = saveRes.data.response;
    const form = createUploadForm(file, 'video_file', overrides);
    await vkPost(upload_url, form, {
        headers: form.getHeaders()
    }, overrides);

    return `video${owner_id}_${video_id}`;
}

async function uploadVideoToWall(userToken, groupId, file, overrides = {}) {
    return uploadVideoToMessages(userToken, groupId, file, overrides);
}

async function handleUploadRequestWithDependencies(req, overrides = {}) {
    const uploadPhotoToMessagesImpl = overrides.uploadPhotoToMessages || uploadPhotoToMessages;
    const uploadPhotoToWallImpl = overrides.uploadPhotoToWall || uploadPhotoToWall;
    const uploadDocToMessagesImpl = overrides.uploadDocToMessages || uploadDocToMessages;
    const uploadDocToWallImpl = overrides.uploadDocToWall || uploadDocToWall;
    const uploadVideoToMessagesImpl = overrides.uploadVideoToMessages || uploadVideoToMessages;
    const cleanupFile = overrides.cleanupFile || (filePath => cleanupFileWithDependencies(filePath, overrides));

    const body = req?.body || {};
    const file = req?.file || null;
    const userToken = body.user_token;
    const communityToken = body.community_token;
    const groupId = body.group_id;
    const target = body.target;

    if (!groupId || !file || !target) {
        throw new Error('Missing required fields (group_id, file, target)');
    }
    if (!userToken) {
        throw new Error('Missing user_token (required for file upload)');
    }

    try {
        const mime = String(file.mimetype || '').toLowerCase();
        let attachment = null;

        if (mime.startsWith('image/')) {
            attachment = target === 'comments'
                ? await uploadPhotoToWallImpl(userToken, groupId, file, { ...overrides, communityToken })
                : await uploadPhotoToMessagesImpl(userToken, groupId, file, { ...overrides, communityToken });
        } else if (mime.startsWith('video/')) {
            attachment = await uploadVideoToMessagesImpl(userToken, groupId, file, { ...overrides, communityToken });
        } else {
            attachment = target === 'comments'
                ? await uploadDocToWallImpl(userToken, groupId, file, { ...overrides, communityToken })
                : await uploadDocToMessagesImpl(userToken, groupId, file, { ...overrides, communityToken });
        }

        return {
            success: true,
            attachment,
            fileName: file.originalname || '',
            fileType: file.mimetype || 'application/octet-stream',
            fileSize: Number(file.size || 0)
        };
    } finally {
        if (file && file.path) {
            await cleanupFile(file.path);
        }
    }
}

module.exports = {
    handleUploadRequestWithDependencies,
    uploadPhotoToMessages,
    uploadPhotoToWall,
    uploadDocToMessages,
    uploadDocToWall,
    uploadVideoToMessages,
    uploadVideoToWall,
    __testOnly: {
        appendFileToFormWithDependencies,
        cleanupFileWithDependencies,
        handleUploadRequestWithDependencies
    }
};
