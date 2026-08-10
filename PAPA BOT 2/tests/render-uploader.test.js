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

  await run('uploadVideoToMessages defaults to recipient-viewable privacy and preserves access_key', async () => {
    const calls = [];
    const attachment = await uploader.uploadVideoToMessages(
      'user-token',
      '229445618',
      {
        path: 'C:\\tmp\\clip.mp4',
        originalname: 'clip.mp4',
        mimetype: 'video/mp4',
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
          assert.equal(url, 'https://api.vk.com/method/video.save');
          assert.equal(options.params.privacy_view, 'all');
          assert.equal(options.params.group_id, undefined);
          return {
            data: {
              response: {
                upload_url: 'https://upload.vk.test/video',
                owner_id: 27894453,
                video_id: 456,
                access_key: 'render-video-key'
              }
            }
          };
        },
        httpPost: async url => {
          calls.push(['POST', url]);
          assert.equal(url, 'https://upload.vk.test/video');
          return { data: { ok: 1 } };
        }
      }
    );

    assert.equal(attachment, 'video27894453_456_render-video-key');
    assert.deepEqual(calls.map(call => call[0]), ['GET', 'POST']);
  });

  await run('uploadPhotoToWall preserves access_key returned by VK', async () => {
    const attachment = await uploader.uploadPhotoToWall(
      'user-token',
      '229445618',
      {
        path: 'C:\\tmp\\wall-photo.jpg',
        originalname: 'wall-photo.jpg',
        mimetype: 'image/jpeg',
        size: 4096
      },
      {
        communityToken: 'community-token-must-not-be-used',
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
          assert.equal(url, 'https://api.vk.com/method/photos.getWallUploadServer');
          assert.equal(options.params.access_token, 'user-token');
          assert.equal(options.params.group_id, 229445618);
          return { data: { response: { upload_url: 'https://upload.vk.test/wall-photo' } } };
        },
        httpPost: async (url, data, options) => {
          if (url === 'https://upload.vk.test/wall-photo') {
            return { data: { server: 1, photo: '[]', hash: 'hash' } };
          }
          assert.equal(url, 'https://api.vk.com/method/photos.saveWallPhoto');
          assert.equal(options.params.access_token, 'user-token');
          return {
            data: {
              response: [{
                owner_id: 27894453,
                id: 457239999,
                access_key: 'private-wall-key'
              }]
            }
          };
        }
      }
    );

    assert.equal(attachment, 'photo27894453_457239999_private-wall-key');
  });

  await run('Render upload treats singular comment target as a wall attachment', async () => {
    const routes = [];
    const result = await uploader.handleUploadRequestWithDependencies(
      {
        body: {
          user_token: 'user-token',
          community_token: 'community-token',
          group_id: '229445618',
          target: 'comment'
        },
        file: {
          path: 'C:\\tmp\\wall-photo.jpg',
          originalname: 'wall-photo.jpg',
          mimetype: 'image/jpeg',
          size: 4096
        }
      },
      {
        uploadPhotoToWall: async () => {
          routes.push('wall');
          return 'photo27894453_457239999_private-wall-key';
        },
        uploadPhotoToMessages: async () => {
          routes.push('messages');
          return 'unexpected';
        },
        cleanupFile: async () => {}
      }
    );

    assert.deepEqual(routes, ['wall']);
    assert.equal(result.attachment, 'photo27894453_457239999_private-wall-key');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
