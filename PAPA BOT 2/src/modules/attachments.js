/**
 * Модуль работы с вложениями (загрузка, обработка)
 */

const axios = require('axios');
const FormData = require('form-data');
const { log } = require('../utils/logger');
const { getUserToken, getVkToken, getVkGroupId } = require('./config');
const { vkGet } = require('./vk-api');
const { resolveAttachmentUploadSource } = require('./attachment-upload-settings');
const RENDER_INITIAL_UPLOAD_TIMEOUT_MS = 20000;
const RENDER_RETRY_UPLOAD_TIMEOUT_MS = 120000;
const RENDER_FINAL_RETRY_DELAY_MS = 10000;

function getVkDocumentCdnFallbackUrl(rawUrl) {
    try {
        const parsed = new URL(rawUrl);
        const match = parsed.hostname.match(/^(psv\d+)\.userapi\.com$/i);
        if (!match) return '';
        parsed.hostname = `${match[1]}.vkuseraudio.net`;
        return parsed.toString();
    } catch (_) {
        return '';
    }
}

function getAxiosTlsFailureUrl(error, originalUrl) {
    const candidates = [
        error?.request?._currentUrl,
        error?.request?._redirectable?._currentUrl,
        error?.request?._options?.href,
        error?.request?._redirectable?._options?.href,
        originalUrl
    ];
    return candidates.find(candidate => getVkDocumentCdnFallbackUrl(candidate)) || originalUrl;
}

function buildVkDocumentDownloadOptions(axiosOptions = {}) {
    const callerBeforeRedirect = axiosOptions.beforeRedirect;
    return Object.assign({}, axiosOptions, {
        beforeRedirect(options, responseDetails, requestDetails) {
            const currentHostname = String(options.hostname || options.host || '').split(':')[0];
            const match = currentHostname.match(/^(psv\d+)\.userapi\.com$/i);
            if (match) {
                const safeHostname = `${match[1]}.vkuseraudio.net`;
                options.hostname = safeHostname;
                options.host = safeHostname;
                options.servername = safeHostname;
                if (options.headers) {
                    options.headers.host = safeHostname;
                }
            }
            if (typeof callerBeforeRedirect === 'function') {
                callerBeforeRedirect(options, responseDetails, requestDetails);
            }
        }
    });
}

async function downloadVkDocument(rawUrl, axiosOptions = {}) {
    const safeAxiosOptions = buildVkDocumentDownloadOptions(axiosOptions);
    try {
        return await axios.get(rawUrl, safeAxiosOptions);
    } catch (error) {
        const isTlsHostnameMismatch = error?.code === 'ERR_TLS_CERT_ALTNAME_INVALID'
            || /Hostname\/IP does not match certificate/i.test(String(error?.message || ''));
        const failedUrl = isTlsHostnameMismatch ? getAxiosTlsFailureUrl(error, rawUrl) : rawUrl;
        const fallbackUrl = isTlsHostnameMismatch ? getVkDocumentCdnFallbackUrl(failedUrl) : '';
        if (!fallbackUrl) throw error;

        log('warn', `VK document CDN certificate mismatch for ${new URL(rawUrl).hostname}; retrying through ${new URL(fallbackUrl).hostname}`);
        return axios.get(fallbackUrl, safeAxiosOptions);
    }
}

function isRetryableDocumentTransferError(error) {
    const status = Number(error?.response?.status);
    if ([405, 408, 425, 429, 500, 502, 503, 504].includes(status)) return true;

    const code = String(error?.code || '').toUpperCase();
    if ([
        'ECONNABORTED',
        'ETIMEDOUT',
        'ECONNRESET',
        'ECONNREFUSED',
        'ENOTFOUND',
        'EHOSTUNREACH',
        'ERR_TLS_CERT_ALTNAME_INVALID',
        'SELF_SIGNED_CERT_IN_CHAIN',
        'DEPTH_ZERO_SELF_SIGNED_CERT'
    ].includes(code)) return true;

    const message = String(error?.message || '').toLowerCase();
    return (
        message.includes('hostname/ip does not match certificate') ||
        message.includes('self-signed certificate') ||
        message.includes('timeout') ||
        message.includes('timed out') ||
        message.includes('socket hang up') ||
        message.includes('network error') ||
        message.includes('status code 405') ||
        message.includes('status code 429') ||
        message.includes('status code 502') ||
        message.includes('status code 503') ||
        message.includes('status code 504')
    );
}

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
async function processAttachmentWithUserToken(attachment, groupId, options = {}) {
    try {
        log('debug', `🔗 Processing attachment: ${attachment}`);
        
        const match = attachment.match(/^(doc|photo|video)(-?\d+)_(\d+)(?:_([A-Za-z0-9_-]+))?$/);
        if (!match) {
            log('debug', `⚠️ Invalid attachment format: ${attachment}`);
            return attachment;
        }
        
        const [, type, ownerIdStr, id, accessKey = ''] = match;
        const ownerId = parseInt(ownerIdStr);
        const absGroupId = Math.abs(parseInt(groupId));
        const target = String(options.target || '').toLowerCase();
        const needsWallPhoto = target === 'comment' && type === 'photo';
        const targetPeerId = Number.parseInt(options.peerId, 10);

        // VK can save an unpublished wall photo under the User Token owner.
        // Such photos are attachable only while their access_key is preserved.
        if (needsWallPhoto && accessKey) {
            log('debug', `✅ Comment wall photo already has access_key: ${attachment}`);
            return attachment;
        }

        // Если вложение уже принадлежит группе
        if (ownerId === -absGroupId && type !== 'doc' && !needsWallPhoto) {
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
            const MAX_RETRIES = 5;
            let lastError = null;

            for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                if (attempt > 1) {
                    const delayMs = attempt * 1000;
                    log('debug', `🔄 Retry ${attempt}/${MAX_RETRIES} for doc upload, waiting ${delayMs}ms...`);
                    const sleepImpl = options.sleep || (ms => new Promise(resolve => setTimeout(resolve, ms)));
                    await sleepImpl(delayMs);
                }

                try {
                    const groupToken = await getVkToken(0, groupId);
                    const docUserToken = await getUserToken(groupId?.toString());

                    // 1. Скачиваем документ через User Token
                    const docRes = await vkGet('docs.getById', {
                        docs: `${ownerId}_${id}`,
                        access_token: docUserToken
                    });

                    if (docRes.error) {
                        const sourceError = new Error(`VK cannot read source document ${ownerId}_${id}: ${docRes.error.error_msg || 'unknown API error'}`);
                        sourceError.code = 'VK_DOCUMENT_SOURCE_UNAVAILABLE';
                        sourceError.vkErrorCode = docRes.error.error_code;
                        throw sourceError;
                    }

                    const doc = (docRes.response?.items?.[0]) || (Array.isArray(docRes.response) ? docRes.response[0] : null);
                    if (!doc || !doc.url) {
                        const sourceError = new Error(`VK source document ${ownerId}_${id} is unavailable`);
                        sourceError.code = 'VK_DOCUMENT_SOURCE_UNAVAILABLE';
                        throw sourceError;
                    }

                    // 2. Скачиваем файл
                    const downloadRes = await downloadVkDocument(doc.url, {
                        responseType: 'arraybuffer',
                        timeout: 60000,
                        maxRedirects: 20
                    });
                    const fileBuffer = Buffer.from(downloadRes.data);
                    const fileName = doc.title || `doc_${id}.${doc.ext || 'txt'}`;
                    const mimeType = doc.mime_type || 'application/octet-stream';
                    log('debug', `📄 Doc downloaded: ${fileName}, size=${fileBuffer.length} bytes`);

                    // 3. Получаем URL загрузки для сообщества
                    const uploadPeerId = Number.isFinite(targetPeerId) ? targetPeerId : Math.abs(parseInt(ownerId));
                    log('debug', `Document upload: requesting message upload server for peer_id=${uploadPeerId}`);
                    const uploadServerRes = await vkGet('docs.getMessagesUploadServer', {
                        group_id: Math.abs(parseInt(groupId)),
                        peer_id: uploadPeerId,
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
                    if (err?.code === 'VK_DOCUMENT_SOURCE_UNAVAILABLE') {
                        throw err;
                    }
                    const isRetryable = isRetryableDocumentTransferError(err);
                    log('warn', `⚠️ Doc upload attempt ${attempt}/${MAX_RETRIES} failed: ${err.message} (retryable: ${isRetryable})`);

                    if (!isRetryable) {
                        log('error', `❌ Doc re-upload failed after ${attempt} attempts: ${err.message}, sending as-is`);
                        return `doc${ownerId}_${id}`;
                    }

                    if (attempt === MAX_RETRIES) {
                        const exhaustedError = new Error(`VK document re-upload failed after ${MAX_RETRIES} transient attempts: ${err.message}`);
                        exhaustedError.code = 'VK_DOCUMENT_REUPLOAD_EXHAUSTED';
                        exhaustedError.cause = err;
                        throw exhaustedError;
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

        if (type === 'video') {
            let resolvedAccessKey = accessKey;
            if (!resolvedAccessKey) {
                const videoRes = await vkGet('video.get', {
                    videos: `${ownerId}_${id}`,
                    access_token: userToken
                });
                resolvedAccessKey = videoRes.response?.items?.[0]?.access_key || '';
                if (resolvedAccessKey) {
                    log('info', `Recovered private video access_key for video${ownerId}_${id}`);
                }
            }

            log('info', `Keeping video private for message delivery: video${ownerId}_${id}`);
            return `video${ownerId}_${id}${resolvedAccessKey ? `_${resolvedAccessKey}` : ''}`;
        }

        log('debug', `📎 Attachment type: ${type}, ownerId: ${ownerId}, id: ${id}, groupId: ${absGroupId}`);

        // Скачиваем и перезагружаем
        const newAttachment = await reuploadAttachment(type, ownerId, id, userToken, groupId, options, accessKey);
        
        if (newAttachment) {
            log('info', `✅ Re-uploaded: ${attachment} → ${newAttachment}`);
            return newAttachment;
        }
        
        if (needsWallPhoto) {
            log('warn', `⚠️ Comment photo is inaccessible and has no access_key: ${attachment}`);
            return null;
        }

        log('warn', `⚠️ Re-upload returned null, using original: ${attachment}`);
        return attachment;
    } catch (error) {
        log('error', `❌ Error processing attachment ${attachment}:`, error.message);
        log('error', error.stack);
        if (error?.code === 'VK_DOCUMENT_REUPLOAD_EXHAUSTED') {
            throw error;
        }
        if (error?.code === 'VK_DOCUMENT_SOURCE_UNAVAILABLE') {
            throw error;
        }
        if (String(options.target || '').toLowerCase() === 'comment' && String(attachment || '').startsWith('photo')) {
            return null;
        }
        return attachment;
    }
}

/**
 * Перезагрузить вложение
 */
async function reuploadAttachment(type, ownerId, id, userToken, groupId, options = {}, accessKey = '') {
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
                photos: `${ownerId}_${id}${accessKey ? `_${accessKey}` : ''}`,
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
                    const downloadRes = await downloadVkDocument(doc.url, { 
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
            if (String(options.target || '').toLowerCase() === 'comment') {
                return await uploadPhotoToWall(fileBuffer, fileName, mimeType, groupId);
            }
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

async function uploadPhotoToCommunityMessages(buffer, filename, mimeType, groupId, profileId = null) {
    const absGroupId = groupId ? Math.abs(parseInt(groupId, 10)) : getVkGroupId();
    if (!absGroupId) throw new Error('VK Group ID is not set');
    const groupToken = await getVkToken(0, groupId, profileId);
    if (!groupToken) throw new Error('Community Token is not configured');
    const server = await vkGet('photos.getMessagesUploadServer', { group_id: absGroupId, access_token: groupToken });
    if (server.error) throw new Error(server.error.error_msg);
    const formData = new FormData();
    formData.append('photo', buffer, { filename, contentType: mimeType });
    const uploaded = await axios.post(server.response.upload_url, formData, { headers: formData.getHeaders(), maxContentLength: Infinity, maxBodyLength: Infinity });
    const saved = await vkGet('photos.saveMessagesPhoto', { server: uploaded.data.server, photo: uploaded.data.photo, hash: uploaded.data.hash, group_id: absGroupId, access_token: groupToken });
    if (saved.error) throw new Error(saved.error.error_msg);
    const item = saved.response[0];
    return `photo${item.owner_id}_${item.id}`;
}

/**
 * Загрузить документ в сообщения
 */
async function uploadPhotoToWall(buffer, filename, mimeType, groupId) {
    const absGroupId = groupId ? Math.abs(parseInt(groupId)) : getVkGroupId();
    if (!absGroupId) throw new Error('VK Group ID is not set');

    log('info', `[WALL PHOTO UPLOAD] Starting: ${filename}, group_id: ${absGroupId}`);

    const userToken = await getUserToken(groupId?.toString());
    if (!userToken) {
        throw new Error('VK User Token is required to upload photo attachments for wall comments');
    }

    const uploadServerRes = await vkGet('photos.getWallUploadServer', {
        group_id: absGroupId,
        access_token: userToken
    });

    if (uploadServerRes.error) {
        throw new Error(`VK User Token failed for wall photo upload: ${uploadServerRes.error.error_msg}`);
    }
    const uploadUrl = uploadServerRes.response.upload_url;

    const formData = new FormData();
    formData.append('photo', buffer, { filename, contentType: mimeType });

    const uploadRes = await axios.post(uploadUrl, formData, {
        headers: formData.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity
    });

    const { server, photo, hash } = uploadRes.data;
    if (!server || !photo || !hash) throw new Error('VK wall photo upload failed');

    const saveRes = await vkGet('photos.saveWallPhoto', {
        server,
        photo,
        hash,
        group_id: absGroupId,
        access_token: userToken
    });

    if (saveRes.error) {
        throw new Error(`VK User Token failed for wall photo save: ${saveRes.error.error_msg}`);
    }
    const savedPhoto = Array.isArray(saveRes.response) ? saveRes.response[0] : saveRes.response;
    const accessKey = String(savedPhoto.access_key || '').trim();
    const attachment = `photo${savedPhoto.owner_id}_${savedPhoto.id}${accessKey ? `_${accessKey}` : ''}`;
    log('info', `[WALL PHOTO UPLOAD] Success: ${attachment}`);
    return attachment;
}

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

async function uploadDocToProfileUserMessages(buffer, filename, mimeType, groupId, profileId = null) {
    const userToken = await getUserToken(groupId?.toString(), profileId);
    if (!userToken) throw new Error('VK User Token is required to upload a profile document');

    const uploadServerRes = await vkGet('docs.getMessagesUploadServer', {
        type: 'doc',
        access_token: userToken
    });
    if (uploadServerRes.error) throw new Error(`VK User Token failed for profile document upload: ${uploadServerRes.error.error_msg}`);

    const formData = new FormData();
    formData.append('file', buffer, { filename, contentType: mimeType });
    const uploadRes = await axios.post(uploadServerRes.response.upload_url, formData, {
        headers: formData.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 300000
    });
    if (!uploadRes.data?.file) throw new Error('VK profile document upload returned no file token');

    const saveRes = await vkGet('docs.save', { file: uploadRes.data.file, access_token: userToken });
    if (saveRes.error) throw new Error(`VK User Token failed to save profile document: ${saveRes.error.error_msg}`);
    const savedDoc = saveRes.response?.doc || saveRes.response;
    if (!savedDoc?.owner_id || !savedDoc?.id) throw new Error('VK profile document save returned no document');
    return `doc${savedDoc.owner_id}_${savedDoc.id}`;
}

async function uploadDocToCommunityMessages(buffer, filename, mimeType, groupId, profileId = null) {
    const absGroupId = groupId ? Math.abs(parseInt(groupId, 10)) : getVkGroupId();
    if (!absGroupId) throw new Error('VK Group ID is not set');
    const groupToken = await getVkToken(0, groupId, profileId);
    if (!groupToken) throw new Error('Community Token is not configured');
    const peerId = -absGroupId;
    const server = await vkGet('docs.getMessagesUploadServer', { peer_id: peerId, access_token: groupToken });
    if (server.error) throw new Error(server.error.error_msg);
    const formData = new FormData();
    formData.append('file', buffer, { filename, contentType: mimeType });
    const uploaded = await axios.post(server.response.upload_url, formData, { headers: formData.getHeaders(), maxContentLength: Infinity, maxBodyLength: Infinity, timeout: 300000 });
    if (!uploaded.data?.file) throw new Error('VK document upload returned no file token');
    const saved = await vkGet('docs.save', { file: uploaded.data.file, peer_id: peerId, access_token: groupToken });
    if (saved.error) throw new Error(saved.error.error_msg);
    const item = saved.response.doc || saved.response;
    return `doc${item.owner_id}_${item.id}`;
}

async function uploadDocToWall(buffer, filename, mimeType, groupId, profileId = null) {
    const absGroupId = groupId ? Math.abs(parseInt(groupId, 10)) : getVkGroupId();
    if (!absGroupId) throw new Error('VK Group ID is not set');

    const userToken = await getUserToken(groupId?.toString(), profileId);
    if (!userToken) {
        throw new Error('VK User Token is required to upload a reusable community document');
    }

    try {
        const uploadServerRes = await vkGet('docs.getWallUploadServer', {
            group_id: absGroupId,
            access_token: userToken
        });
        if (uploadServerRes.error) {
            throw new Error(`VK User Token failed for community document upload: ${uploadServerRes.error.error_msg}`);
        }

        const formData = new FormData();
        formData.append('file', buffer, { filename, contentType: mimeType });
        const uploadRes = await axios.post(uploadServerRes.response.upload_url, formData, {
            headers: formData.getHeaders(),
            maxContentLength: Infinity,
            maxBodyLength: Infinity,
            timeout: 300000
        });
        if (!uploadRes.data?.file) {
            throw new Error('VK community document upload returned no file token');
        }

        const saveRes = await vkGet('docs.save', {
            file: uploadRes.data.file,
            group_id: absGroupId,
            access_token: userToken
        });
        if (saveRes.error) {
            throw new Error(`VK User Token failed to save community document: ${saveRes.error.error_msg}`);
        }

        const savedDoc = saveRes.response?.doc || saveRes.response;
        if (!savedDoc?.owner_id || !savedDoc?.id) {
            throw new Error('VK community document save returned no document');
        }
        const attachment = `doc${savedDoc.owner_id}_${savedDoc.id}`;
        log('info', `[DOC WALL UPLOAD] Success: ${attachment}`);
        return attachment;
    } catch (wallError) {
        const message = String(wallError?.message || '');
        if (!/access denied|can't upload docs to this group/i.test(message)) throw wallError;

        // Some VK communities reject docs.getWallUploadServer for an administrator's
        // User Token. The messages upload flow still creates a community-owned doc.
        const attachment = await uploadDocToCommunityMessages(buffer, filename, mimeType, groupId, profileId);
        log('warn', `[DOC WALL UPLOAD] VK denied wall upload; used community message upload: ${attachment}`);
        return attachment;
    }
}

/**
 * Загрузить видео в сообщения
 */
async function uploadVideoToMessages(buffer, filename, mimeType, groupId, profileId = null) {
    const userToken = await getUserToken(groupId?.toString(), profileId);
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
            const uploadSource = await resolveAttachmentUploadSource(profileId, 'video');
            const result = await uploadViaRenderServiceWithDependencies(buffer, filename, mimeType, 'messages', absGroupId, { profileId, userVideoPrivacy: uploadSource.settings.userVideoPrivacy.effective });
            log('info', `✅ [VIDEO UPLOAD] Success via Render: ${result}`);
            return result;
        } catch (renderError) {
            log('error', `❌ [VIDEO UPLOAD] Render service failed: ${renderError.message}`);
            throw new Error(`Не удалось загрузить видео >3.5MB: ${renderError.message}`);
        }
    }

    // Для видео ≤3.5MB - стандартный метод через User Token
    const uploadSource = await resolveAttachmentUploadSource(profileId, 'video');
    return await uploadVideoStandard(buffer, filename, mimeType, userToken, absGroupId, uploadSource.source, uploadSource.settings.userVideoPrivacy.effective);
}

/**
 * Стандартная загрузка видео (до 100MB)
 */
async function uploadVideoStandard(buffer, filename, mimeType, token, groupId, source = 'user', userVideoPrivacy = 'all') {
    // Сохраняем видео в видеокаталоге сообщества, чтобы User Token не становился
    // владельцем медиа в личном профиле.
    const saveRes = await vkGet('video.save', {
        name: filename || 'video.mp4',
        description: 'Загружено ботом',
        ...(source === 'community'
            ? { group_id: Math.abs(groupId), privacy_view: 'all' }
            : { privacy_view: userVideoPrivacy }),
        access_token: token
    });
    
    if (saveRes.error) throw new Error(saveRes.error.error_msg);
    const { upload_url, video_id, owner_id, access_key: accessKey = '' } = saveRes.response;

    const formData = new FormData();
    formData.append('video_file', buffer, { filename, contentType: mimeType });

    await axios.post(upload_url, formData, {
        headers: formData.getHeaders(),
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 600000
    });

    return `video${owner_id}_${video_id}${accessKey ? `_${accessKey}` : ''}`;
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
        privacy_view: 'nobody',
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
    return processAttachmentWithUserToken(attachment, groupId, { target: 'comment' });
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function createRenderUploadFormData(buffer, filename, mimeType, userToken, groupId, target, userVideoPrivacy = '') {
    const formData = new FormData();
    formData.append('file', buffer, { filename, contentType: mimeType });
    formData.append('user_token', userToken);
    formData.append('group_id', Math.abs(parseInt(groupId)));
    formData.append('target', target);
    if (userVideoPrivacy) formData.append('user_video_privacy', userVideoPrivacy);
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

    const userToken = await getUserTokenImpl(groupId?.toString(), overrides.profileId);
    if (!userToken) {
        throw new Error('User Token не настроен для загрузки через Render');
    }

    const uploadUrl = `${renderUrl}/upload`;
    log('info', `[RENDER UPLOAD] Uploading to ${uploadUrl}`);

    const executeUpload = async timeoutMs => {
        const formData = createFormDataImpl(buffer, filename, mimeType, userToken, groupId, target, overrides.userVideoPrivacy);
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

        log('warn', `[RENDER UPLOAD] Initial upload failed, retrying with extended timeout: ${error.message}`);
        try {
            return await executeUpload(RENDER_RETRY_UPLOAD_TIMEOUT_MS);
        } catch (retryError) {
            if (!isRenderWakeCandidate(retryError)) {
                throw retryError;
            }
        }

        log('warn', `[RENDER UPLOAD] Extended retry failed, waiting before final retry: ${error.message}`);
        const sleepImpl = overrides.sleep || sleep;
        await sleepImpl(RENDER_FINAL_RETRY_DELAY_MS);

        log('info', '[RENDER UPLOAD] Performing final retry after backoff');
        try {
            return await executeUpload(RENDER_RETRY_UPLOAD_TIMEOUT_MS);
        } catch (retryError) {
            throw new Error(`Render responded, but upload failed: ${retryError.message}`);
        }
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
async function uploadToVK(buffer, filename, mimeType, target, groupId, profileId = null) {
    log('info', `📎 [UPLOAD] type=${mimeType}, target=${target}, size=${(buffer.length/1024/1024).toFixed(2)}MB`);

    const type = mimeType.startsWith('image/') ? 'image' : (mimeType.startsWith('video/') ? 'video' : 'document');
    if (type === 'document' && target === 'profile_document') {
        return uploadDocToProfileUserMessages(buffer, filename, mimeType, groupId, profileId);
    }
    const uploadSource = await resolveAttachmentUploadSource(profileId, type);
    if (mimeType.startsWith('image/')) {
        if (target === 'comment' || target === 'comments') {
            return await uploadPhotoToWall(buffer, filename, mimeType, groupId);
        }
        return uploadSource.source === 'community'
            ? uploadPhotoToCommunityMessages(buffer, filename, mimeType, groupId, profileId)
            : uploadPhotoToMessages(buffer, filename, mimeType, groupId);
    } else if (mimeType.startsWith('video/')) {
        return await uploadVideoToMessages(buffer, filename, mimeType, groupId, profileId);
    } else if (target === 'wall' || target === 'comment' || target === 'comments') {
        return await uploadDocToWall(buffer, filename, mimeType, groupId, profileId);
    } else {
        return uploadSource.source === 'community'
            ? uploadDocToCommunityMessages(buffer, filename, mimeType, groupId, profileId)
            : uploadDocToMessages(buffer, filename, mimeType, groupId);
    }
}

// Render-relay уже обошёл лимит Yandex Cloud, поэтому видео на финальном
// серверном шаге нельзя повторно отправлять в Render — это создаёт рекурсивный
// цикл и приводит к тайм-ауту. Здесь видео сразу загружается в VK User Token.
async function uploadToVKFromRenderRelay(buffer, filename, mimeType, target, groupId, profileId = null) {
    if (String(mimeType || '').toLowerCase().startsWith('video/')) {
        const userToken = await getUserToken(groupId?.toString(), profileId);
        if (!userToken) throw new Error('User Token не настроен для загрузки видео.');
        const uploadSource = await resolveAttachmentUploadSource(profileId, 'video');
        return uploadVideoStandard(buffer, filename, mimeType, userToken, groupId, uploadSource.source, uploadSource.settings.userVideoPrivacy.effective);
    }
    return uploadToVK(buffer, filename, mimeType, target, groupId, profileId);
}

module.exports = {
    getAttachmentsFromRow,
    getVkDocumentCdnFallbackUrl,
    getAxiosTlsFailureUrl,
    buildVkDocumentDownloadOptions,
    downloadVkDocument,
    isRetryableDocumentTransferError,
    processAttachmentWithUserToken,
    processAttachmentForComment,
    uploadPhotoToMessages,
    uploadPhotoToCommunityMessages,
    uploadPhotoToWall,
    uploadDocToMessages,
    uploadDocToProfileUserMessages,
    uploadDocToCommunityMessages,
    uploadDocToWall,
    uploadVideoToMessages,
    uploadToVK,
    uploadToVKFromRenderRelay,
    __testOnly: {
        isRenderWakeCandidate,
        uploadViaRenderServiceWithDependencies
    }
};
