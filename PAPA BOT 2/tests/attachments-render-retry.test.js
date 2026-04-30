const assert = require('node:assert/strict');

const attachments = require('../src/modules/attachments');

async function run(name, fn) {
  try {
    await fn();
    process.stdout.write('PASS ' + name + '\n');
  } catch (error) {
    process.stderr.write('FAIL ' + name + '\n');
    throw error;
  }
}

function buildTimeoutError(message = 'timeout exceeded') {
  const error = new Error(message);
  error.code = 'ECONNABORTED';
  return error;
}

(async function main() {
  await run('uploadViaRenderService retries with extended timeout before wake checks', async () => {
    const calls = [];
    let uploadAttempt = 0;

    const attachment = await attachments.__testOnly.uploadViaRenderServiceWithDependencies(
      Buffer.from('big-file'),
      'video.mp4',
      'video/mp4',
      'messages',
      '229445618',
      {
        getUserToken: async groupId => {
          assert.equal(groupId, '229445618');
          return 'user-token';
        },
        createFormData: () => ({
          headers: { 'content-type': 'multipart/form-data' },
          append() {},
          getHeaders() {
            return this.headers;
          }
        }),
        uploadRequest: async () => {
          uploadAttempt += 1;
          calls.push(['upload', uploadAttempt]);
          if (uploadAttempt === 1) {
            throw buildTimeoutError('first upload timed out');
          }
          return { success: true, attachment: 'video1_2' };
        },
        pingRender: async attempt => {
          calls.push(['ping', attempt]);
          return attempt >= 1;
        },
        sleep: async ms => {
          calls.push(['sleep', ms]);
        }
      }
    );

    assert.equal(attachment, 'video1_2');
    assert.deepEqual(calls, [
      ['upload', 1],
      ['upload', 2]
    ]);
  });

  await run('uploadViaRenderService treats signal timed out as wake candidate and retries before wake', async () => {
    const calls = [];
    let uploadAttempt = 0;

    const attachment = await attachments.__testOnly.uploadViaRenderServiceWithDependencies(
      Buffer.from('big-file'),
      'video.mp4',
      'video/mp4',
      'messages',
      '229445618',
      {
        getUserToken: async () => 'user-token',
        createFormData: () => ({
          headers: { 'content-type': 'multipart/form-data' },
          append() {},
          getHeaders() {
            return this.headers;
          }
        }),
        uploadRequest: async () => {
          uploadAttempt += 1;
          calls.push(['upload', uploadAttempt]);
          if (uploadAttempt === 1) {
            const error = new Error('signal timed out');
            error.name = 'TimeoutError';
            throw error;
          }
          return { success: true, attachment: 'video1_3' };
        },
        pingRender: async attempt => {
          calls.push(['ping', attempt]);
          return attempt >= 1;
        },
        sleep: async ms => {
          calls.push(['sleep', ms]);
        }
      }
    );

    assert.equal(attachment, 'video1_3');
    assert.deepEqual(calls, [
      ['upload', 1],
      ['upload', 2]
    ]);
  });

  await run('uploadViaRenderService wakes Render only after extended retry also fails', async () => {
    const calls = [];
    let uploadAttempt = 0;

    const attachment = await attachments.__testOnly.uploadViaRenderServiceWithDependencies(
      Buffer.from('big-file'),
      'video.mp4',
      'video/mp4',
      'messages',
      '229445618',
      {
        getUserToken: async () => 'user-token',
        createFormData: () => ({
          headers: { 'content-type': 'multipart/form-data' },
          append() {},
          getHeaders() {
            return this.headers;
          }
        }),
        uploadRequest: async () => {
          uploadAttempt += 1;
          calls.push(['upload', uploadAttempt]);
          if (uploadAttempt < 3) {
            throw buildTimeoutError('upload timed out');
          }
          return { success: true, attachment: 'video1_4' };
        },
        pingRender: async attempt => {
          calls.push(['ping', attempt]);
          return attempt >= 2;
        },
        sleep: async ms => {
          calls.push(['sleep', ms]);
        }
      }
    );

    assert.equal(attachment, 'video1_4');
    assert.deepEqual(calls, [
      ['upload', 1],
      ['upload', 2],
      ['ping', 1],
      ['sleep', 5000],
      ['ping', 2],
      ['upload', 3]
    ]);
  });

  await run('uploadViaRenderService exposes distinct error after successful wake but failed final retry', async () => {
    let uploadAttempt = 0;

    await assert.rejects(
      attachments.__testOnly.uploadViaRenderServiceWithDependencies(
        Buffer.from('big-file'),
        'video.mp4',
        'video/mp4',
        'messages',
        '229445618',
        {
          getUserToken: async () => 'user-token',
          createFormData: () => ({
            headers: { 'content-type': 'multipart/form-data' },
            append() {},
            getHeaders() {
              return this.headers;
            }
          }),
          uploadRequest: async () => {
            uploadAttempt += 1;
            if (uploadAttempt < 3) {
              throw buildTimeoutError('first upload timed out');
            }
            throw new Error('vk upload timed out');
          },
          pingRender: async () => true,
          sleep: async () => {}
        }
      ),
      /Render woke up, but upload failed: vk upload timed out/
    );
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
