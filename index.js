const express = require('express');
const multer = require('multer');
const axios = require('axios');
const FormData = require('form-data');

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

// Разрешаем CORS для админ-панели
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
async function uploadPhotoToMessages(userToken, groupId, buffer, filename) {
  console.log('[UPLOAD PHOTO] Starting upload for messages:', filename);
  
  const uploadRes = await axios.get('https://api.vk.com/method/photos.getMessagesUploadServer', {
    params: { access_token: userToken, v: '5.199' }
  });
  if (uploadRes.data.error) throw new Error('VK API Error: ' + uploadRes.data.error.error_msg);

  const uploadUrl = uploadRes.data.response.upload_url;
  const form = new FormData();
  form.append('photo', buffer, { filename, contentType: 'image/jpeg' });
  const uploadResult = await axios.post(uploadUrl, form, { headers: form.getHeaders(), maxContentLength: Infinity, maxBodyLength: Infinity });

  const { server, photo, hash } = uploadResult.data;
  if (!server || !photo || !hash) throw new Error('Ошибка загрузки фото на сервер ВК');

  const saveRes = await axios.post('https://api.vk.com/method/photos.saveMessagesPhoto', null, {
    params: { server, photo, hash, access_token: userToken, v: '5.199' }
  });
  if (saveRes.data.error) throw new Error('VK API Error: ' + saveRes.data.error.error_msg);

  const savedPhoto = saveRes.data.response[0];
  console.log('[UPLOAD PHOTO] Success:', `photo${savedPhoto.owner_id}_${savedPhoto.id}`);
  return `photo${savedPhoto.owner_id}_${savedPhoto.id}`;
}

async function uploadPhotoToWall(userToken, groupId, buffer, filename) {
  const uploadRes = await axios.get('https://api.vk.com/method/photos.getWallUploadServer', {
    params: { group_id: Math.abs(groupId), access_token: userToken, v: '5.199' }
  });
  if (uploadRes.data.error) throw new Error(uploadRes.data.error.error_msg);

  const uploadUrl = uploadRes.data.response.upload_url;
  const form = new FormData();
  form.append('photo', buffer, { filename, contentType: 'image/jpeg' });
  const uploadResult = await axios.post(uploadUrl, form, { headers: form.getHeaders(), maxContentLength: Infinity, maxBodyLength: Infinity });

  const { server, photo, hash } = uploadResult.data;
  if (!server || !photo || !hash) throw new Error('Ошибка загрузки фото на сервер ВК');

  const saveRes = await axios.post('https://api.vk.com/method/photos.saveWallPhoto', null, {
    params: { group_id: Math.abs(groupId), server, photo, hash, access_token: userToken, v: '5.199' }
  });
  if (saveRes.data.error) throw new Error(saveRes.data.error.error_msg);

  const savedPhoto = saveRes.data.response[0];
  return `photo${savedPhoto.owner_id}_${savedPhoto.id}`;
}

// ========== КЭШ user_id ==========
let cachedUserId = null;

async function getUserId(userToken) {
  if (cachedUserId) return cachedUserId;
  
  console.log('[AUTH] Getting user_id from token...');
  
  // Пробуем account.getProfileInfo - работает для User Token
  let res = await axios.get('https://api.vk.com/method/account.getProfileInfo', {
    params: { access_token: userToken, v: '5.199' }
  });
  
  console.log('[AUTH] account.getProfileInfo response:', JSON.stringify(res.data).substring(0, 200));
  
  if (res.data.response?.id) {
    cachedUserId = res.data.response.id;
    console.log('[AUTH] User ID from account.getProfileInfo:', cachedUserId);
    return cachedUserId;
  }
  
  // Если не получилось - пробуем users.get
  res = await axios.get('https://api.vk.com/method/users.get', {
    params: { access_token: userToken, v: '5.199' }
  });
  
  console.log('[AUTH] users.get response:', JSON.stringify(res.data).substring(0, 200));
  
  if (res.data.error) throw new Error('VK API Error: ' + res.data.error.error_msg);
  if (!res.data.response || !res.data.response[0]) {
    throw new Error('Не удалось получить user_id. User Token должен быть получен через https://vkhost.github.io/ → VK Admin');
  }
  
  cachedUserId = res.data.response[0].id;
  console.log('[AUTH] User ID:', cachedUserId);
  return cachedUserId;
}

async function uploadDocToMessages(userToken, groupId, buffer, filename) {
  console.log('[UPLOAD DOC] Starting upload for:', filename, 'group_id:', groupId);
  
  console.log('[UPLOAD DOC] Requesting messages upload URL with type: doc');
  
  const uploadServerRes = await axios.get('https://api.vk.com/method/docs.getMessagesUploadServer', {
    params: { type: 'doc', access_token: userToken, v: '5.199' }
  });
  if (uploadServerRes.data.error) throw new Error('VK API Error: ' + uploadServerRes.data.error.error_msg);

  const uploadUrl = uploadServerRes.data.response.upload_url;
  const form = new FormData();
  form.append('file', buffer, { filename, contentType: 'application/octet-stream' });
  const uploadResult = await axios.post(uploadUrl, form, { headers: form.getHeaders(), maxContentLength: Infinity, maxBodyLength: Infinity });

  const { file } = uploadResult.data;
  if (!file) throw new Error('Ошибка загрузки документа на сервер ВК');

  console.log('[UPLOAD DOC] File uploaded, saving...');
  
  const saveRes = await axios.post('https://api.vk.com/method/docs.save', null, {
    params: { file, access_token: userToken, v: '5.199' }
  });
  
  if (saveRes.data.error) throw new Error('VK API Error: ' + saveRes.data.error.error_msg);

  const savedDoc = saveRes.data.response.doc;
  if (!savedDoc) throw new Error('Документ не сохранён');

  console.log('[UPLOAD DOC] Success:', `doc${savedDoc.owner_id}_${savedDoc.id}`);
  return `doc${savedDoc.owner_id}_${savedDoc.id}`;
}

async function uploadDocToWall(userToken, groupId, buffer, filename) {
  const uploadServerRes = await axios.get('https://api.vk.com/method/docs.getWallUploadServer', {
    params: { group_id: Math.abs(groupId), access_token: userToken, v: '5.199' }
  });
  if (uploadServerRes.data.error) throw new Error(uploadServerRes.data.error.error_msg);

  const uploadUrl = uploadServerRes.data.response.upload_url;
  const form = new FormData();
  form.append('file', buffer, { filename, contentType: 'application/octet-stream' });
  const uploadResult = await axios.post(uploadUrl, form, { headers: form.getHeaders(), maxContentLength: Infinity, maxBodyLength: Infinity });

  const { file } = uploadResult.data;
  if (!file) throw new Error('Ошибка загрузки документа на сервер ВК');

  const saveRes = await axios.post('https://api.vk.com/method/docs.save', null, {
    params: { file, group_id: Math.abs(groupId), access_token: userToken, v: '5.199' }
  });
  if (saveRes.data.error) throw new Error(saveRes.data.error.error_msg);

  const savedDoc = saveRes.data.response.doc;
  if (!savedDoc) throw new Error('Документ не сохранён');

  return `doc${savedDoc.owner_id}_${savedDoc.id}`;
}

async function uploadVideoToMessages(userToken, groupId, buffer, filename) {
  console.log('[UPLOAD VIDEO] Starting upload for:', filename, 'group_id:', groupId);
  
  const saveRes = await axios.get('https://api.vk.com/method/video.save', {
    params: { access_token: userToken, name: filename || 'video.mp4', privacy_view: 'only_me', v: '5.199' }
  });
  if (saveRes.data.error) throw new Error('VK API Error: ' + saveRes.data.error.error_msg);

  const { upload_url, video_id, owner_id } = saveRes.data.response;
  const form = new FormData();
  form.append('video_file', buffer, { filename, contentType: 'video/mp4' });
  
  console.log('[UPLOAD VIDEO] Uploading to VK server...');
  await axios.post(upload_url, form, { 
    headers: form.getHeaders(), 
    maxContentLength: Infinity, 
    maxBodyLength: Infinity,
    timeout: 300000
  });

  console.log('[UPLOAD VIDEO] Success:', `video${owner_id}_${video_id}`);
  return `video${owner_id}_${video_id}`;
}

async function uploadVideoToWall(userToken, groupId, buffer, filename) {
  return uploadVideoToMessages(userToken, groupId, buffer, filename);
}

// ========== ОСНОВНОЙ ОБРАБОТЧИК ==========
app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    const { user_token, community_token, vk_token, group_id, target, secret } = req.body;
    const file = req.file;
    
    console.log('[UPLOAD] Received request:', {
      has_community_token: !!community_token,
      has_vk_token: !!vk_token,
      has_user_token: !!user_token,
      group_id,
      target,
      has_file: !!file,
      filename: file?.originalname
    });
    
    if (!group_id || !file || !target) {
      return res.status(400).json({ success: false, error: 'Missing required fields (group_id, file, target)' });
    }

    // Определяем приоритет токенов
    const primaryToken = user_token || community_token || vk_token;
    const fallbackToken = (user_token && (community_token || vk_token)) ? (community_token || vk_token) : null;
    
    if (!primaryToken) {
      return res.status(400).json({ success: false, error: 'Missing token (user_token or community_token required)' });
    }

    const mime = file.mimetype;
    let attachment = null;
    let lastError = null;

    // Функция загрузки с выбранным токеном
    const tryUpload = async (token, tokenType) => {
      console.log(`[UPLOAD] Trying with ${tokenType}:`, token.substring(0, 20) + '...');
      
      if (mime.startsWith('image/')) {
        if (target === 'comment') {
          return await uploadPhotoToWall(token, group_id, file.buffer, file.originalname);
        } else {
          return await uploadPhotoToMessages(token, group_id, file.buffer, file.originalname);
        }
      } else if (mime.startsWith('video/')) {
        return await uploadVideoToMessages(token, group_id, file.buffer, file.originalname);
      } else {
        if (target === 'comment') {
          return await uploadDocToWall(token, group_id, file.buffer, file.originalname);
        } else {
          return await uploadDocToMessages(token, group_id, file.buffer, file.originalname);
        }
      }
    };

    // Попытка 1: основной токен
    try {
      attachment = await tryUpload(primaryToken, user_token ? 'user_token' : 'community_token');
      console.log('[UPLOAD] Success with primary token');
    } catch (err) {
      lastError = err;
      console.log('[UPLOAD] Primary token failed:', err.message);
      
      // Попытка 2: fallback токен
      if (fallbackToken) {
        try {
          console.log('[UPLOAD] Trying fallback token...');
          attachment = await tryUpload(fallbackToken, 'fallback_token');
          console.log('[UPLOAD] Success with fallback token');
        } catch (fallbackErr) {
          console.log('[UPLOAD] Fallback token also failed:', fallbackErr.message);
          throw new Error(`Both tokens failed. Primary: ${err.message}. Fallback: ${fallbackErr.message}`);
        }
      } else {
        throw err;
      }
    }

    res.json({ success: true, attachment });
  } catch (err) {
    console.error('[UPLOAD ERROR]', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
