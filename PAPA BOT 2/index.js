const fs = require('fs');
const os = require('os');
const path = require('path');
const express = require('express');
const multer = require('multer');

const { handleUploadRequestWithDependencies } = require('./src/modules/render-uploader-service');

const app = express();
const UPLOAD_TMP_DIR = path.join(os.tmpdir(), 'papa-vk-uploader');

fs.mkdirSync(UPLOAD_TMP_DIR, { recursive: true });

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

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
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

app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    console.log('[UPLOAD] Received request:', {
      has_user_token: !!req.body?.user_token,
      has_community_token: !!req.body?.community_token,
      group_id: req.body?.group_id,
      target: req.body?.target,
      has_file: !!req.file,
      filename: req.file?.originalname,
      size: req.file?.size,
      mimetype: req.file?.mimetype
    });
    const result = await handleUploadRequestWithDependencies(req);
    console.log('[UPLOAD] Success:', {
      attachment: result.attachment,
      fileName: result.fileName,
      fileSize: result.fileSize
    });
    res.json(result);
  } catch (error) {
    console.error('[UPLOAD ERROR]', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

const PORT = process.env.PORT || 3000;

if (require.main === module) {
  app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
}

module.exports = {
  app
};
