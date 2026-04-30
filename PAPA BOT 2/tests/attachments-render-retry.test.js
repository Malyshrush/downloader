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
  await run('uploadViaRenderService retries after waking sleeping Render service', async () => {
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
          return attempt >= 3;
        },
        sleep: async ms => {
          calls.push(['sleep', ms]);
        }
      }
    );

    assert.equal(attachment, 'video1_2');
    assert.deepEqual(calls, [
      ['upload', 1],
      ['ping', 1],
      ['sleep', 5000],
      ['ping', 2],
      ['sleep', 5000],
      ['ping', 3],
      ['upload', 2]
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
