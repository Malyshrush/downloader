/**
 * Модуль работы с вложениями (загрузка, обработка)
 */

const axios = require('axios');
const FormData = require('form-data');
const { log } = require('../utils/logger');
const { getUserToken, getVkToken, getVkGroupId } = require('./config');
const { vkGet } = require('./vk-api');
const RENDER_INITIAL_UPLOAD_TIMEOUT_MS = 20000;
const RENDER_RETRY_UPLOAD_TIMEOUT_MS = 120000;
const RENDER_WAKE_TIMEOUT_MS = 60000;
const RENDER_WAKE_POLL_INTERVAL_MS = 5000;

// Поля вложений для разных типов
const ATTACHMENT_FIELDS = {
    MESSAGES: ['Вложения', 'Вложения к ответу'],
    COMMENTS: ['Вложения', 'Вложения к ответу'],
    MAILING: ['Вложение к рассылке']
};

/**
 * Получить вложения из строки конфигурации
 */
function getAttachmentsFromRow(row, type) {
    const fields = ATTACHMENT_FIELDS[type] || [];
    let attachments = [];
    
    for (const field of fields) {
        const raw = (row[field] || '').trim();
        if (raw) {
            const items = raw.split(/[\n,]+/).map(a => a.trim()).filter(a => a);
            attachments.push(...items);
            log('debug', `📎 Field "${field}": found ${items.length} attachments`);
        }
    }
    
    const unique = [...new Set(attachments)];
    log('debug', `📎 Total unique attachments: ${unique.length}`);
    return unique;
}

/**
 * Обработать вложение через User Token
 */
async function processAttachmentWithUserToken(attachment, groupId) {
    try {
        log('debug', `🔗 Processing attachment: ${attachment}`);
        
        const match = attachment.match(/^(doc|photo|video)(-?\d+)_(\d+)$/);
        if (!match) {
            log('debug', `⚠️ Invalid attachment format: ${attachment}`);
            return attachment;
        }
        
        const [, type, ownerIdStr, id] = match;
        const ownerId = parseInt(ownerIdStr);
        const absGroupId = Math.abs(parseInt(groupId));

        // Если вложение уже принадлежит группе
        if (ownerId === -absGroupId && type !== 'doc') {
            log('debug', `✅ Attachment already owned by group: ${attachment}`);
            return attachment;
        }

        if (type === 'doc' && ownerId === -absGroupId) {
            log('debug', `✅ Doc already owned by group: ${attachment}`);
            return attachment;
        }

        // ✅ Для документов пользователя — VK НЕ может прикрепить чужой документ к сообщению
        // Нужно скачать и загрузить в сообщество
        if (type === 'doc') {
            log('debug', `📄 Doc belongs to user, downloading and re-uploading to community...`);
            const MAX_RETRIES = 3;
            let lastError = null;

            for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                if (attempt > 1) {
                    const delayMs = attempt * 1000;
                    log('debug', `🔄 Retry ${attempt}/${MAX_RETRIES} for doc upload, waiting ${delayMs}ms...`);
                    await new Promise(resolve => setTimeout(resolve, delayMs));
                }

                try {
                    const groupToken = await getVkToken(0, groupId);
                    const docUserToken = await getUserToken(groupId?.toString());

                    // 1. Скачиваем документ через User Token
                    const docRes = await vkGet('docs.getById', {
                        docs: `${ownerId}_${id}`,
                        access_token: docUserToken
                    });

                    const doc = (docRes.response?.items?.[0]) || (Array.isArray(docRes.response) ? docRes.response[0] : null);
                    if (!doc || !doc.url) {
                        log('warn', `⚠️ Cannot get doc ${ownerId}_${id}, sending as-is`);
                        return `doc${ownerId}_${id}`;
                    }

                    // 2. Скачиваем файл
                    const downloadRes = await axios.get(doc.url, {
                        responseType: 'arraybuffer',
                        timeout: 60000,
                        maxRedirects: 20
                    });
                    const fileBuffer = Buffer.from(downloadRes.data);
                    const fileName = doc.title || `doc_${id}.${doc.ext || 'txt'}`;
                    const mimeType = doc.mime_type || 'application/octet-stream';
                    log('debug', `📄 Doc downloaded: ${fileName}, size=${fileBuffer.length} bytes`);

                    // 3. Получаем URL загрузки для сообщества
                    log('debug', `📤 Step 3/5: Getting upload URL with peer_id=${Math.abs(parseInt(ownerId))}...`);
                    const uploadServerRes = await vkGet('docs.getMessagesUploadServer', {
                        group_id: Math.abs(parseInt(groupId)),
                        peer_id: Math.abs(parseInt(ownerId)),
                        type: 'doc',
                        access_token: groupToken
                    });
                    log('debug', `📤 Upload server response: ${JSON.stringify(uploadServerRes).substring(0, 300)}`);

                    if (uploadServerRes.error) {
                        log('warn', `⚠️ Cannot get upload server: ${uploadServerRes.error.error_msg}, sending as-is`);
                        return `doc${ownerId}_${id}`;
                    }

                    // 4. Загружаем файл
                    log('debug', `📤 Step 4/5: Uploading file to VK (${fileBuffer.length} bytes)...`);
                    const formData = new FormData();
                    formData.append('file', fileBuffer, { filename: fileName, contentType: mimeType });
                    const uploadRes = await axios.post(uploadServerRes.response.upload_url, formData, {
                        headers: formData.getHeaders(),
                        maxContentLength: Infinity,
                        maxBodyLength: Infinity,
                        timeout: 30000
                    });
                    log('debug', `📤 Upload response: ${JSON.stringify(uploadRes.data).substring(0, 300)}`);

                    // 5. Сохраняем документ
                    log('debug', `📤 Step 5/5: Saving document...`);
                    const saveRes = await vkGet('docs.save', {
                        file: uploadRes.data.file,
                        access_token: groupToken
                    });
                    log('debug', `📤 Save response: ${JSON.stringify(saveRes).substring(0, 300)}`);

                    if (saveRes.error) {
                        log('warn', `⚠️ Cannot save doc: ${saveRes.error.error_msg}, sending as-is`);
                        return `doc${ownerId}_${id}`;
                    }

                    const savedDoc = saveRes.response.doc || saveRes.response;
                    const newAttachment = `doc${savedDoc.owner_id}_${savedDoc.id}`;
                    log('info', `✅ Doc re-uploaded: doc${ownerId}_${id} → ${newAttachment}${attempt > 1 ? ` (retry ${attempt})` : ''}`);
                    return newAttachment;
                } catch (err) {
                    lastError = err;
                    const isRetryable = err.message.includes('502') || err.message.includes('504') || err.message.includes('ETIMEDOUT') || err.message.includes('ECONNRESET') || err.message.includes('ECONNREFUSED') || err.code === 'ETIMEDOUT' || err.code === 'ECONNRESET' || err.code === 'ECONNABORTED';
                    log('warn', `⚠️ Doc upload attempt ${attempt}/${MAX_RETRIES} failed: ${err.message} (retryable: ${isRetryable})`);

                    if (!isRetryable || attempt === MAX_RETRIES) {
                        log('error', `❌ Doc re-upload failed after ${attempt} attempts: ${err.message}, sending as-is`);
                        return `doc${ownerId}_${id}`;
                    }
                }
            }

            // На случай если цикл завершился без return (не должно произойти)
            log('error', `❌ Doc re-upload failed: ${lastError?.message || 'unknown error'}, sending as-is`);
            return `doc${ownerId}_${id}`;
        }

        const userToken = await getUserToken(groupId?.toString());
        if (!userToken) {
            log('warn', `⚠️ User Token not set, using original attachment`);
            return attachment;
        }

        log('debug', `📎 Attachment type: ${type}, ownerId: ${ownerId}, id: ${id}, groupId: ${absGroupId}`);

        // Скачиваем и перезагружаем
        const newAttachment = await reuploadAttachment(type, ownerId, id, userToken, groupId);
        
        if (newAttachment) {
            log('info', `✅ Re-uploaded: ${attachment} → ${newAttachment}`);
            return newAttachment;
        }
        
        log('warn', `⚠️ Re-upload returned null, using original: ${attachment}`);
        return attachment;
    } catch (error) {
        log('error', `❌ Error processing attachment ${attachment}:`, error.message);
        log('error', error.stack);
        return attachment;
    }
}

/**
 * Перезагрузить вложение
 */
async function reuploadAttachment(type, ownerId, id, userToken, groupId) {
    let fileBuffer = null;
    let fileName = `attachment_${id}`;
    let mimeType = 'application/octet-stream';

    try {
        // ✅ Для фото и документов пользователя используем User Token
        // Community Token не может скачивать фото пользователя через photos.getById
        const downloadToken = userToken;

        if (!downloadToken) {
            log('warn', `⚠️ No User Token for downloading ${type}, using original`);
            return `${type}${ownerId}_${id}`;
        }

        log('debug', `📥 Downloading ${type} ${ownerId}_${id} with User Token ${downloadToken.substring(0, 15)}...`);

        // Скачиваем файл
        if (type === 'photo') {
            const photoRes = await vkGet('photos.getById', {
                photos: `${ownerId}_${id}`,
                access_token: downloadToken
            });
            
            log('debug', `📷 photos.getById response: ${JSON.stringify(photoRes).substring(0, 300)}`);
            
            if (photoRes.response?.items?.[0]) {
                const photo = photoRes.response.items[0];
                const imageUrl = photo.sizes?.sort((a, b) => b.width - a.width)[0]?.url || photo.url;
                
                if (imageUrl) {
                    const downloadRes = await axios.get(imageUrl, { responseType: 'arraybuffer', timeout: 60000 });
                    fileBuffer = Buffer.from(downloadRes.data);
                    fileName = `photo_${id}.jpg`;
                    mimeType = 'image/jpeg';
                    log('debug', `📷 Photo downloaded: ${fileName}, size=${fileBuffer.length} bytes`);
                } else {
                    log('warn', `⚠️ Photo has no image URL`);
                }
            } else {
                log('warn', `⚠️ photos.getById returned no items`);
            }
        } else if (type === 'doc') {
            log('debug', `📄 Calling docs.getById for ${ownerId}_${id}...`);
            const docRes = await vkGet('docs.getById', {
                docs: `${ownerId}_${id}`,
                access_token: userToken
            });
            
            log('debug', `📄 docs.getById response: ${JSON.stringify(docRes).substring(0, 500)}`);
            
            // VK docs API returns array in response, not items
            const doc = (docRes.response?.items?.[0]) || (Array.isArray(docRes.response) ? docRes.response[0] : null);
            
            if (!doc) {
                log('warn', `⚠️ docs.getById returned no items for ${ownerId}_${id}`);
            } else if (doc.url) {
                log('debug', `📄 Doc found: title="${doc.title}", ext="${doc.ext}"`);
                log('debug', `📥 Downloading doc from URL...`);
                try {
                    const downloadRes = await axios.get(doc.url, { 
                        responseType: 'arraybuffer', 
                        timeout: 60000,
                        maxRedirects: 5,
                        validateStatus: () => true
                    });
                    
                    if (downloadRes.status >= 400) {
                        log('error', `❌ Doc download failed: status ${downloadRes.status}`);
                    } else {
                        fileBuffer = Buffer.from(downloadRes.data);
                        fileName = doc.title || `doc_${id}.${doc.ext || 'bin'}`;
                        mimeType = doc.mime_type || 'application/octet-stream';
                        log('debug', `📄 Doc downloaded: ${fileName}, size=${fileBuffer.length} bytes`);
                    }
                } catch (downloadError) {
                    log('error', `❌ Doc download error: ${downloadError.message}`);
                }
            } else {
                log('warn', `⚠️ Doc has no URL, cannot download`);
            }
        } else if (type === 'video') {
            log('warn', `⚠️ Video re-upload not supported, using original: ${ownerId}_${id}`);
            return `${type}${ownerId}_${id}`;
        }

        if (!fileBuffer) {
            log('warn', `⚠️ Could not download attachment (fileBuffer is null)`);
            return null;
        }

        // Загружаем через User Token
        if (type === 'photo') {
            return await uploadPhotoToMessages(fileBuffer, fileName, mimeType, groupId);
        } else if (type === 'doc') {
            return await uploadDocToMessages(fileBuffer, fileName, mimeType, groupId);
        }
        
        return null;
    } catch (e) {
        log('error', `❌ Failed to reupload attachment: ${e.message}`);
        log('error', e.stack);
        return null;
    }
}

/**
 * Загрузить фото в сообщения
 */
async function uploadPhotoToMessages(buffer, filename, mimeType, groupId) {
    const absGroupId = groupId ? Math.abs(parseInt(groupId)) : getVkGroupId();
    if (!absGroupId) throw new Error('VK Group ID не задан');

    log('info', `📷 [PHOTO UPLOAD] Starting: ${filename}, group_id: ${absGroupId}`);

    let userTokenError = null;
    let groupTokenError = null;

    // Попытка 1: User Token (приоритет)
    try {
        const userToken = await getUserToken(groupId?.toString());
        if (!userToken) throw new Error('User Token не настроен');
        
        const uploadServerRes = await vkGet('photos.getMessagesUploadServer', {
            group_id: absGroupId,
            access_token: userToken
        });
        
        if (uploadServerRes.error) throw new Error(uploadServerRes.error.error_msg);
        const uploadUrl = uploadServerRes.response.upload_url;

        const formData = new FormData();
        formData.append('photo', buffer, { filename, contentType: mimeType });

        const uploadRes = await axios.post(uploadUrl, formData, {
            headers: formData.getHeaders(),
            maxContentLength: Infinity,
            maxBodyLength: Infinity
        });

        const { server, photo, hash } = uploadRes.data;
        if (!server || !photo || !hash) throw new Error('Ошибка загрузки фото на сервер ВК');

        const saveRes = await vkGet('photos.saveMessagesPhoto', {
            server,
            photo,
            hash,
            group_id: absGroupId,
            access_token: userToken
        });
        
        if (saveRes.error) throw new Error(saveRes.error.error_msg);
        const savedPhoto = saveRes.response[0];
        log('info', `✅ [PHOTO UPLOAD] Success via User Token: photo${savedPhoto.owner_id}_${savedPhoto.id}`);
        return `photo${savedPhoto.owner_id}_${savedPhoto.id}`;
    } catch (error) {
        userTokenError = error.message;
        log('warn', `⚠️ [PHOTO UPLOAD] User Token failed: ${userTokenError}`);
    }

    // Попытка 2: Group Token (fallback)
    try {
        const groupToken = await getVkToken(0, groupId);
        
        const uploadServerRes = await vkGet('photos.getMessagesUploadServer', {
            group_id: absGroupId,
            access_token: groupToken
        });
        
        if (uploadServerRes.error) throw new Error(uploadServerRes.error.error_msg);
        const uploadUrl = uploadServerRes.response.upload_url;

        const formData = new FormData();
        formData.append('photo', buffer, { filename, contentType: mimeType });

        const uploadRes = await axios.post(uploadUrl, formData, {
            headers: formData.getHeaders(),
            maxContentLength: Infinity,
            maxBodyLength: Infinity
        });

        const { server, photo, hash } = uploadRes.data;
        if (!server || !photo || !hash) throw new Error('Ошибка загрузки фото на сервер ВК');

        const saveRes = await vkGet('photos.saveMessagesPhoto', {
            server,
            photo,
            hash,
            group_id: absGroupId,
            access_token: groupToken
        });
        
        if (saveRes.error) throw new Error(saveRes.error.error_msg);
        const savedPhoto = saveRes.response[0];
        log('info', `✅ [PHOTO UPLOAD] Success via Group Token (fallback): photo${savedPhoto.owner_id}_${savedPhoto.id}`);
        return `photo${savedPhoto.owner_id}_${savedPhoto.id}`;
    } catch (error) {
        groupTokenError = error.message;
        log('error', `❌ [PHOTO UPLOAD] Group Token failed: ${groupTokenError}`);
    }

    // Обе попытки провалились - выдаём детальную ошибку
    throw new Error(`Не удалось загрузить фото. User Token: ${userTokenError}. Group Token: ${groupTokenError}`);
}

/**
 * Загрузить документ в сообщения
 */
async function uploadDocToMessages(buffer, filename, mimeType, groupId) {
    const absGroupId = groupId ? Math.abs(parseInt(groupId)) : getVkGroupId();
    if (!absGroupId) throw new Error('VK Group ID не настроен!');

    log('info', `📎 [DOC UPLOAD] Starting: ${filename}, group_id: ${absGroupId}`);
    
    const fileSizeMB = (buffer.length / (1024 * 1024)).toFixed(2);

    // Попытка 1: User Token (приоритет)
    try {
        const userToken = await getUserToken(groupId?.toString());
        if (!userToken) throw new Error('User Token не настроен');
        
        log('debug', `📎 Trying User Token`);
        
        const uploadServerRes = await vkGet('docs.getMessagesUploadServer', {
            type: 'doc',
            access_token: userToken
        });
        
        if (uploadServerRes.error) throw new Error(uploadServerRes.error.error_msg);
        const uploadUrl = uploadServerRes.response.upload_url;

        const formData = new FormData();
        formData.append('file', buffer, { filename, contentType: mimeType });

        const uploadRes = await axios.post(uploadUrl, formData, {
            headers: formData.getHeaders(),
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            timeout: 300000
        });

        const { file } = uploadRes.data;
        if (!file) throw new Error('Ошибка загрузки документа: no file in response');

        const saveRes = await vkGet('docs.save', {
            file,
            access_token: userToken
        });
        
        if (saveRes.error) throw new Error(saveRes.error.error_msg);

        const savedDoc = saveRes.response.doc || saveRes.response;
        if (!savedDoc) throw new Error('Документ не сохранён');

        log('info', `✅ [DOC UPLOAD] Success via User Token: doc${savedDoc.owner_id}_${savedDoc.id}`);
        return `doc${savedDoc.owner_id}_${savedDoc.id}`;
    } catch (userTokenError) {
        log('warn', `⚠️ [DOC UPLOAD] User Token failed: ${userTokenError.message}`);
        
        // Для файлов >3.5MB используем Render сервис
        if (fileSizeMB > 3.5) {
            log('info', `📎 [DOC UPLOAD] File >3.5MB, using Render service...`);
            try {
                const result = await uploadViaRenderServiceWithDependencies(buffer, filename, mimeType, 'messages', absGroupId);
                log('info', `✅ [DOC UPLOAD] Success via Render: ${result}`);
                return result;
            } catch (renderError) {
                log('error', `❌ [DOC UPLOAD] Render service failed: ${renderError.message}`);
                throw new Error(`Не удалось загрузить документ >3.5MB: ${renderError.message}`);
            }
        }
        
        // Попытка 2: Group Token с peer_id (для файлов ≤3.5MB)
        try {
            const groupToken = await getVkToken(0, groupId);
            const peerId = -absGroupId;
            
            log('debug', `📎 Trying Group Token with peer_id: ${peerId}`);
            
            const uploadServerRes = await vkGet('docs.getMessagesUploadServer', {
                peer_id: peerId,
                access_token: groupToken
            });
            
            if (uploadServerRes.error) throw new Error(uploadServerRes.error.error_msg);
            const uploadUrl = uploadServerRes.response.upload_url;

            const formData = new FormData();
            formData.append('file', buffer, { filename, contentType: mimeType });

            const uploadRes = await axios.post(uploadUrl, formData, {
                headers: formData.getHeaders(),
                maxContentLength: Infinity,
                maxBodyLength: Infinity,
                timeout: 300000
            });

            const { file } = uploadRes.data;
            if (!file) throw new Error('Ошибка загрузки документа: no file in response');

            const saveRes = await vkGet('docs.save', {
                file,
                peer_id: peerId,
                access_token: groupToken
            });
            
            if (saveRes.error) throw new Error(saveRes.error.error_msg);

            const savedDoc = saveRes.response.doc || saveRes.response;
            if (!savedDoc) throw new Error('Документ не сохранён');

            log('info', `✅ [DOC UPLOAD] Success via Group Token: doc${savedDoc.owner_id}_${savedDoc.id}`);
            return `doc${savedDoc.owner_id}_${savedDoc.id}`;
        } catch (groupTokenError) {
            log('error', `❌ [DOC UPLOAD] All methods failed. User Token: ${userTokenError.message}, Group Token: ${groupTokenError.message}`);
            throw new Error(`Не удалось загрузить документ. Убедитесь, что USER TOKEN настроен правильно.`);
        }
    }
}

/**
 * Загрузить видео в сообщения
 */
async function uploadVideoToMessages(buffer, filename, mimeType, groupId) {
    const userToken = await getUserToken(groupId?.toString());
    if (!userToken) throw new Error('User Token не настроен!');
    
    const absGroupId = groupId ? Math.abs(parseInt(groupId)) : getVkGroupId();
    if (!absGroupId) throw new Error('VK Group ID не задан');

    const fileSizeMB = (buffer.length / (1024 * 1024)).toFixed(2);
    log('info', `🎬 [VIDEO UPLOAD] Size: ${fileSizeMB} MB`);

    if (fileSizeMB > 200) {
        throw new Error(`Размер видео превышает лимит (200MB). Ваш файл: ${fileSizeMB}MB`);
    }

    // Для видео >3.5MB используем Render сервис (обход лимита Yandex Cloud Functions)
    if (fileSizeMB > 3.5) {
        log('info', `🎬 [VIDEO UPLOAD] File >3.5MB, using Render service...`);
        try {
            const result = await uploadViaRenderServiceWithDependencies(buffer, filename, mimeType, 'messages', absGroupId);
            log('info', `✅ [VIDEO UPLOAD] Success via Render: ${result}`);
            return result;
        } catch (renderError) {
            log('error', `❌ [VIDEO UPLOAD] Render service failed: ${renderError.message}`);
            throw new Error(`Не удалось загрузить видео >3.5MB: ${renderError.message}`);
        }
    }

    // Для видео ≤3.5MB - стандартный метод через User Token
    return await uploadVideoStandard(buffer, filename, mimeType, userToken, absGroupId);
}

/**
 * Стандартная загрузка видео (до 100MB)
 */
async function uploadVideoStandard(buffer, filename, mimeType, token, groupId) {
    // ✅ ИСПРАВЛЕНИЕ: Убираем group_id, загружаем на личную страницу пользователя
    const saveRes = await vkGet('video.save', {
        name: filename || 'video.mp4',
        description: 'Загружено ботом',
        privacy_view: 'only_me',
        access_token: token
    });
    
    if (saveRes.error) throw new Error(saveRes.error.error_msg);
    const { upload_url, video_id, owner_id } = saveRes.response;

    const formData = new FormData();
    formData.append('video_file', buffer, { filename, contentType: mimeType });

    await axios.post(upload_url, formData, {
        headers: formData.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 600000
    });

    return `video${owner_id}_${video_id}`;
}

/**
 * Загрузка видео через сервер (100-200MB)
 */
async function uploadVideoViaServer(buffer, filename, mimeType, token, groupId) {
    const serverRes = await vkGet('video.getUploadServer', {
        group_id: Math.abs(groupId),
        access_token: token
    });
    
    if (serverRes.error) throw new Error(serverRes.error.error_msg);
    const uploadUrl = serverRes.response.upload_url;

    const formData = new FormData();
    formData.append('video_file', buffer, { filename, contentType: mimeType });

    const uploadRes = await axios.post(uploadUrl, formData, {
        headers: formData.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 600000
    });

    const { video_id, owner_id } = uploadRes.data;

    const saveRes = await vkGet('video.save', {
        video_id,
        owner_id,
        name: filename || 'video.mp4',
        description: 'Загружено ботом',
        group_id: Math.abs(groupId),
        access_token: token
    });
    
    if (saveRes.error) throw new Error(saveRes.error.error_msg);
    const saved = saveRes.response;
    
    return `video${saved.owner_id}_${saved.video_id}`;
}

/**
 * Подготовить вложение для комментария на стене
 */
async function processAttachmentForComment(attachment, groupId) {
    return processAttachmentWithUserToken(attachment, groupId);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function createRenderUploadFormData(buffer, filename, mimeType, userToken, groupId, target) {
    const formData = new FormData();
    formData.append('file', buffer, { filename, contentType: mimeType });
    formData.append('user_token', userToken);
    formData.append('group_id', Math.abs(parseInt(groupId)));
    formData.append('target', target);
    return formData;
}

function isRenderWakeCandidate(error) {
    const status = error?.response?.status;
    if ([502, 503, 504].includes(status)) return true;

    const code = String(error?.code || '').toUpperCase();
    if (['ECONNABORTED', 'ETIMEDOUT', 'ECONNRESET', 'ENOTFOUND', 'EHOSTUNREACH'].includes(code)) {
        return true;
    }

    const message = String(error?.message || '').toLowerCase();
    return (
        message.includes('timeout') ||
        message.includes('timed out') ||
        message.includes('socket hang up') ||
        message.includes('network error') ||
        message.includes('service unavailable') ||
        message.includes('bad gateway') ||
        message.includes('gateway timeout')
    );
}

async function waitForRenderServiceWake(renderUrl, overrides = {}) {
    const pingRender = overrides.pingRender || (async () => {
        try {
            await axios.get(`${renderUrl}/upload`, { timeout: 5000 });
            return true;
        } catch (error) {
            return false;
        }
    });
    const sleepImpl = overrides.sleep || sleep;
    const startedAt = Date.now();
    let attempt = 0;

    while ((Date.now() - startedAt) < RENDER_WAKE_TIMEOUT_MS) {
        attempt += 1;
        const awake = await pingRender(attempt);
        if (awake) {
            return true;
        }

        const elapsed = Date.now() - startedAt;
        const remaining = RENDER_WAKE_TIMEOUT_MS - elapsed;
        if (remaining <= 0) {
            break;
        }

        await sleepImpl(Math.min(RENDER_WAKE_POLL_INTERVAL_MS, remaining));
    }

    return false;
}

async function uploadViaRenderServiceWithDependencies(buffer, filename, mimeType, target, groupId, overrides = {}) {
    const renderUrl = overrides.renderUrl || process.env.RENDER_UPLOAD_URL || 'https://vk-uploader.onrender.com';
    const getUserTokenImpl = overrides.getUserToken || getUserToken;
    const createFormDataImpl = overrides.createFormData || createRenderUploadFormData;
    const uploadRequest = overrides.uploadRequest || (async (url, formData, timeoutMs) => {
        const response = await axios.post(url, formData, {
            headers: formData.getHeaders(),
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            timeout: timeoutMs
        });
        return response.data;
    });

    const userToken = await getUserTokenImpl(groupId?.toString());
    if (!userToken) {
        throw new Error('User Token не настроен для загрузки через Render');
    }

    const uploadUrl = `${renderUrl}/upload`;
    log('info', `[RENDER UPLOAD] Uploading to ${uploadUrl}`);

    const executeUpload = async timeoutMs => {
        const formData = createFormDataImpl(buffer, filename, mimeType, userToken, groupId, target);
        const payload = await uploadRequest(uploadUrl, formData, timeoutMs);
        if (!payload?.success) {
            throw new Error(payload?.error || 'Render upload failed');
        }
        return payload.attachment;
    };

    try {
        return await executeUpload(RENDER_INITIAL_UPLOAD_TIMEOUT_MS);
    } catch (error) {
        if (!isRenderWakeCandidate(error)) {
            throw error;
        }

        log('warn', `[RENDER UPLOAD] Initial upload failed, waking Render service: ${error.message}`);
        const awake = await waitForRenderServiceWake(renderUrl, overrides);
        if (!awake) {
            throw new Error('Render service did not wake within 60 seconds');
        }

        log('info', '[RENDER UPLOAD] Render service woke up, retrying upload');
        return executeUpload(RENDER_RETRY_UPLOAD_TIMEOUT_MS);
    }
}

/**
 * Загрузить файл через Render сервис (deprecated - не используется)
 */
async function uploadViaRenderService(buffer, filename, mimeType, target, groupId) {
    const renderUrl = process.env.RENDER_UPLOAD_URL || 'https://vk-uploader.onrender.com';
    const userToken = await getUserToken(groupId?.toString());
    
    if (!userToken) {
        throw new Error('User Token не настроен для загрузки через Render');
    }
    
    log('info', `🌐 [RENDER UPLOAD] Uploading to ${renderUrl}/upload`);
    
    const formData = new FormData();
    formData.append('file', buffer, { filename, contentType: mimeType });
    formData.append('user_token', userToken);
    formData.append('group_id', Math.abs(parseInt(groupId)));
    formData.append('target', target);
    
    const response = await axios.post(`${renderUrl}/upload`, formData, {
        headers: formData.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 120000
    });
    
    if (!response.data.success) {
        throw new Error(response.data.error || 'Render upload failed');
    }
    
    return response.data.attachment;
}

/**
 * Универсальная функция загрузки в VK
 */
async function uploadToVK(buffer, filename, mimeType, target, groupId) {
    log('info', `📎 [UPLOAD] type=${mimeType}, target=${target}, size=${(buffer.length/1024/1024).toFixed(2)}MB`);

    if (mimeType.startsWith('image/')) {
        return await uploadPhotoToMessages(buffer, filename, mimeType, groupId);
    } else if (mimeType.startsWith('video/')) {
        return await uploadVideoToMessages(buffer, filename, mimeType, groupId);
    } else {
        return await uploadDocToMessages(buffer, filename, mimeType, groupId);
    }
}

module.exports = {
    getAttachmentsFromRow,
    processAttachmentWithUserToken,
    processAttachmentForComment,
    uploadPhotoToMessages,
    uploadDocToMessages,
    uploadVideoToMessages,
    uploadToVK,
    __testOnly: {
        isRenderWakeCandidate,
        waitForRenderServiceWake,
        uploadViaRenderServiceWithDependencies
    }
};
