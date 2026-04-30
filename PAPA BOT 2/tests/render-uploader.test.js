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
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
