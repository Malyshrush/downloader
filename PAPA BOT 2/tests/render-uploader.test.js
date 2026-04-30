const assert = require('node:assert/strict');

const uploader = require('../src/modules/render-uploader-service');

async function run(name, fn) {
  try {
    await fn();
    process.stdout.write('PASS ' + name + '\n');
  } catch (error) {
    process.stderr.write('FAIL ' + name + '\n');
    throw error;
  }
}

(async function main() {
  await run('handleUploadRequestWithDependencies returns attachment metadata and cleans temp file on success', async () => {
    const cleanedPaths = [];
    const seenFiles = [];

    const result = await uploader.__testOnly.handleUploadRequestWithDependencies(
      {
        body: {
          user_token: 'user-token',
          group_id: '229445618',
          target: 'messages'
        },
        file: {
          path: 'C:\\tmp\\video.mp4',
          originalname: 'video.mp4',
          mimetype: 'video/mp4',
          size: 7340032
        }
      },
      {
        uploadVideoToMessages: async (userToken, groupId, file) => {
          seenFiles.push({ userToken, groupId, file });
          return 'video1_2';
        },
        cleanupFile: async filePath => {
          cleanedPaths.push(filePath);
        }
      }
    );

    assert.deepEqual(result, {
      success: true,
      attachment: 'video1_2',
      fileName: 'video.mp4',
      fileType: 'video/mp4',
      fileSize: 7340032
    });
    assert.equal(seenFiles.length, 1);
    assert.equal(seenFiles[0].file.path, 'C:\\tmp\\video.mp4');
    assert.deepEqual(cleanedPaths, ['C:\\tmp\\video.mp4']);
  });

  await run('handleUploadRequestWithDependencies cleans temp file on failure', async () => {
    const cleanedPaths = [];

    await assert.rejects(
      uploader.__testOnly.handleUploadRequestWithDependencies(
        {
          body: {
            user_token: 'user-token',
            group_id: '229445618',
            target: 'messages'
          },
          file: {
            path: 'C:\\tmp\\broken.pdf',
            originalname: 'broken.pdf',
            mimetype: 'application/pdf',
            size: 4096
          }
        },
        {
          uploadDocToMessages: async () => {
            throw new Error('vk save failed');
          },
          cleanupFile: async filePath => {
            cleanedPaths.push(filePath);
          }
        }
      ),
      /vk save failed/
    );

    assert.deepEqual(cleanedPaths, ['C:\\tmp\\broken.pdf']);
  });

  await run('appendFileToFormWithDependencies appends a stream from local temp path', async () => {
    const calls = [];

    uploader.__testOnly.appendFileToFormWithDependencies(
      {
        append: (fieldName, value, options) => {
          calls.push({ fieldName, value, options });
        }
      },
      'file',
      {
        path: 'C:\\tmp\\payload.bin',
        originalname: 'payload.bin',
        mimetype: 'application/octet-stream'
      },
      {
        createReadStream: filePath => ({ kind: 'stream', filePath })
      }
    );

    assert.deepEqual(calls, [
      {
        fieldName: 'file',
        value: { kind: 'stream', filePath: 'C:\\tmp\\payload.bin' },
        options: {
          filename: 'payload.bin',
          contentType: 'application/octet-stream'
        }
      }
    ]);
  });

  await run('uploadDocToMessages uses user token first and falls back when VK requires peer_id', async () => {
    const httpCalls = [];
    const formAppends = [];

    const attachment = await uploader.uploadDocToMessages(
      'user-token',
      '229445618',
      {
        path: 'C:\\tmp\\payload.pdf',
        originalname: 'payload.pdf',
        mimetype: 'application/pdf',
        size: 4096
      },
      {
        FormDataCtor: function FakeFormData() {
          return {
            append(fieldName, value, options) {
              formAppends.push({ fieldName, value, options });
            },
            getHeaders() {
              return { 'content-type': 'multipart/form-data' };
            }
          };
        },
        createReadStream: filePath => ({ kind: 'stream', filePath }),
        httpGet: async (url, options) => {
          httpCalls.push(['GET', url, options.params]);
          if (url === 'https://api.vk.com/method/docs.getMessagesUploadServer') {
            assert.equal(options.params.access_token, 'user-token');
            assert.equal(Object.prototype.hasOwnProperty.call(options.params, 'peer_id'), false);
            return { data: { error: { error_msg: 'peer_id is required for community messages' } } };
          }
          assert.equal(url, 'https://api.vk.com/method/docs.getWallUploadServer');
          assert.equal(options.params.access_token, 'user-token');
          assert.equal(options.params.group_id, 229445618);
          return { data: { response: { upload_url: 'https://upload.vk.test/doc' } } };
        },
        httpPost: async (url, data, options) => {
          httpCalls.push(['POST', url, options.params || null]);
          if (url === 'https://upload.vk.test/doc') {
            return { data: { file: 'uploaded-doc-token' } };
          }
          assert.equal(url, 'https://api.vk.com/method/docs.save');
          assert.equal(options.params.group_id, 229445618);
          return { data: { response: { doc: { owner_id: -229445618, id: 700 } } } };
        }
      }
    );

    assert.equal(attachment, 'doc-229445618_700');
    assert.equal(formAppends[0].fieldName, 'file');
    assert.deepEqual(httpCalls.map(call => call[0]), ['GET', 'GET', 'POST', 'POST']);
  });

  await run('uploadPhotoToMessages passes group_id with user token like Yandex uploader', async () => {
    const calls = [];

    const attachment = await uploader.uploadPhotoToMessages(
      'user-token',
      '229445618',
      {
        path: 'C:\\tmp\\photo.jpg',
        originalname: 'photo.jpg',
        mimetype: 'image/jpeg',
        size: 4096
      },
      {
        FormDataCtor: function FakeFormData() {
          return {
            append() {},
            getHeaders() {
              return { 'content-type': 'multipart/form-data' };
            }
          };
        },
        createReadStream: filePath => ({ kind: 'stream', filePath }),
        httpGet: async (url, options) => {
          calls.push(['GET', url, options.params]);
          assert.equal(url, 'https://api.vk.com/method/photos.getMessagesUploadServer');
          assert.equal(options.params.access_token, 'user-token');
          assert.equal(options.params.group_id, 229445618);
          return { data: { response: { upload_url: 'https://upload.vk.test/photo' } } };
        },
        httpPost: async (url, data, options) => {
          calls.push(['POST', url, options.params || null]);
          if (url === 'https://upload.vk.test/photo') {
            return { data: { server: 1, photo: '[]', hash: 'hash' } };
          }
          assert.equal(url, 'https://api.vk.com/method/photos.saveMessagesPhoto');
          assert.equal(options.params.access_token, 'user-token');
          assert.equal(options.params.group_id, 229445618);
          return { data: { response: [{ owner_id: -229445618, id: 42 }] } };
        }
      }
    );

    assert.equal(attachment, 'photo-229445618_42');
    assert.deepEqual(calls.map(call => call[0]), ['GET', 'POST', 'POST']);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
