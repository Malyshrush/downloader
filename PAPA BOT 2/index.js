const fs = require('fs');
const os = require('os');
const path = require('path');
const express = require('express');
const multer = require('multer');
const axios = require('axios');

const { handleUploadRequestWithDependencies } = require('./src/modules/render-uploader-service');

const app = express();
const UPLOAD_TMP_DIR = path.join(os.tmpdir(), 'papa-vk-uploader');
const UPLOAD_RESULT_TTL_MS = 15 * 60 * 1000;
const uploadResults = new Map();
const uploadInFlight = new Map();

fs.mkdirSync(UPLOAD_TMP_DIR, { recursive: true });

function normalizeUploadId(value) {
  const uploadId = String(value || '').trim();
  return /^[a-zA-Z0-9_.:-]{8,128}$/.test(uploadId) ? uploadId : '';
}

function pruneUploadResults(now = Date.now()) {
  for (const [uploadId, entry] of uploadResults.entries()) {
    if (!entry || now - entry.createdAt > UPLOAD_RESULT_TTL_MS) {
      uploadResults.delete(uploadId);
    }
  }
}

function rememberUploadResult(uploadId, statusCode, payload) {
  if (!uploadId) return;
  pruneUploadResults();
  uploadResults.set(uploadId, {
    createdAt: Date.now(),
    statusCode,
    payload
  });
}

async function cleanupDuplicateTempFile(file) {
  if (!file?.path) return;
  try {
    await fs.promises.unlink(file.path);
  } catch (_error) {
    // Multer temp cleanup is best-effort for duplicate idempotency hits.
  }
}

const upload = multer({
  storage: multer.diskStorage({
    destination: function (_req, _file, cb) {
      cb(null, UPLOAD_TMP_DIR);
    },
    filename: function (_req, file, cb) {
      const safeName = String(file.originalname || 'upload.bin').replace(/[^\w.\-]+/g, '_');
      cb(null, Date.now() + '_' + Math.random().toString(36).slice(2, 8) + '_' + safeName);
    }
  })
});

const RELAY_CHUNK_SIZE = 1.5 * 1024 * 1024;
const PAPA_BOT_UPLOAD_URL = String(process.env.PAPA_BOT_UPLOAD_URL || 'https://functions.yandexcloud.net/d4eg37ikm3vl5tm1mjld').replace(/\/+$/, '');

async function uploadLargeFileThroughPapa(req) {
  const file = req.file;
  const body = req.body || {};
  const renderGrant = String(body.render_grant || '').trim();
  if (!renderGrant) throw new Error('Render relay grant is missing');
  if (!file?.path) throw new Error('Файл не получен Render');
  const totalChunks = Math.max(1, Math.ceil(Number(file.size || 0) / RELAY_CHUNK_SIZE));
  const handle = await fs.promises.open(file.path, 'r');
  try {
    for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex += 1) {
      const start = chunkIndex * RELAY_CHUNK_SIZE;
      const length = Math.min(RELAY_CHUNK_SIZE, Number(file.size || 0) - start);
      const chunk = Buffer.alloc(length);
      await handle.read(chunk, 0, length, start);
      const response = await axios.post(PAPA_BOT_UPLOAD_URL, {
        action: 'upload_attachment_chunk',
        upload_id: body.upload_id,
        chunk_index: chunkIndex,
        total_chunks: totalChunks,
        render_grant: renderGrant,
        chunk_base64: chunk.toString('base64'),
        fileName: file.originalname,
        fileType: file.mimetype || 'application/octet-stream',
        fileSize: Number(file.size || 0),
        target: body.target,
        groupId: body.group_id,
        communityId: body.community_id,
        profileId: body.profile_id
      }, {
        headers: {
          'Content-Type': 'application/json',
        },
        timeout: 300000,
        maxContentLength: Infinity,
        maxBodyLength: Infinity
      });
      if (!response.data?.success) throw new Error(response.data?.error || 'PAPA BOT relay upload failed');
      if (chunkIndex === totalChunks - 1) return response.data;
    }
  } finally {
    await handle.close();
    await fs.promises.unlink(file.path).catch(() => {});
  }
  throw new Error('PAPA BOT relay upload did not return a result');
}

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Cache-Control, Pragma, X-Requested-With, Accept, Origin');
  res.header('Access-Control-Max-Age', '86400');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

app.get('/', (_req, res) => {
  res.status(200).json({ ok: true, service: 'vk-uploader' });
});

app.get('/healthz', (_req, res) => {
  res.status(200).json({ ok: true, service: 'vk-uploader' });
});

app.get('/upload-result', async (req, res) => {
  const uploadId = normalizeUploadId(req.query?.upload_id);
  console.log('[UPLOAD RESULT] Lookup:', {
    upload_id: uploadId || null,
    has_cached_result: uploadId ? uploadResults.has(uploadId) : false,
    in_flight: uploadId ? uploadInFlight.has(uploadId) : false
  });

  if (!uploadId) {
    return res.status(400).json({ success: false, error: 'upload_id is required' });
  }

  pruneUploadResults();

  const cached = uploadResults.get(uploadId);
  if (cached) {
    return res.status(cached.statusCode).json(cached.payload);
  }

  if (uploadInFlight.has(uploadId)) {
    return res.status(202).json({ success: false, pending: true });
  }

  return res.status(404).json({ success: false, error: 'upload result not found' });
});

app.post('/upload', upload.single('file'), async (req, res) => {
  const uploadId = normalizeUploadId(req.body?.upload_id);

  try {
    pruneUploadResults();

    if (uploadId) {
      const cached = uploadResults.get(uploadId);
      if (cached) {
        await cleanupDuplicateTempFile(req.file);
        return res.status(cached.statusCode).json(cached.payload);
      }

      const pending = uploadInFlight.get(uploadId);
      if (pending) {
        await cleanupDuplicateTempFile(req.file);
        const result = await pending;
        return res.json(result);
      }
    }

    console.log('[UPLOAD] Received request:', {
      upload_id: uploadId || null,
      has_user_token: !!req.body?.user_token,
      has_community_token: !!req.body?.community_token,
      group_id: req.body?.group_id,
      target: req.body?.target,
      has_file: !!req.file,
      filename: req.file?.originalname,
      size: req.file?.size,
      mimetype: req.file?.mimetype
    });

    const uploadPromise = req.body?.user_token
      ? handleUploadRequestWithDependencies(req)
      : uploadLargeFileThroughPapa(req);
    if (uploadId) {
      uploadInFlight.set(uploadId, uploadPromise);
    }

    const result = await uploadPromise;
    rememberUploadResult(uploadId, 200, result);
    console.log('[UPLOAD] Success:', {
      upload_id: uploadId || null,
      attachment: result.attachment,
      fileName: result.fileName,
      fileSize: result.fileSize
    });
    res.json(result);
  } catch (error) {
    console.error('[UPLOAD ERROR]', error);
    const payload = { success: false, error: error.message };
    rememberUploadResult(uploadId, 500, payload);
    res.status(500).json(payload);
  } finally {
    if (uploadId) {
      uploadInFlight.delete(uploadId);
    }
  }
});

const PORT = process.env.PORT || 3000;

if (require.main === module) {
  app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
}

module.exports = {
  app,
  __testOnly: {
    normalizeUploadId,
    uploadResults,
    uploadInFlight,
    rememberUploadResult,
    pruneUploadResults
  }
};
