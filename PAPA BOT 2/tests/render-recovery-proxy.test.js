const assert = require('node:assert/strict');

const { __testOnly } = require('../src/handler');

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
  await run('handleRecoverRenderUpload proxies upload result lookup to Render', async () => {
    const calls = [];
    const response = await __testOnly.handleRecoverRenderUpload(
      {
        body: JSON.stringify({
          action: 'recover_render_upload',
          upload_id: 'upload_test_123'
        })
      },
      {
        httpGet: async (url, options) => {
          calls.push({ url, options });
          return {
            status: 200,
            data: {
              success: true,
              attachment: 'doc1_2'
            }
          };
        }
      }
    );

    assert.equal(response.statusCode, 200);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, 'https://vk-uploader.onrender.com/upload-result');
    assert.equal(calls[0].options.params.upload_id, 'upload_test_123');
    assert.deepEqual(JSON.parse(response.body), { success: true, attachment: 'doc1_2' });
  });

  await run('handleRecoverRenderUpload rejects invalid upload ids before calling Render', async () => {
    let called = false;
    const response = await __testOnly.handleRecoverRenderUpload(
      {
        body: JSON.stringify({ upload_id: '../bad' })
      },
      {
        httpGet: async () => {
          called = true;
        }
      }
    );

    assert.equal(called, false);
    assert.equal(response.statusCode, 400);
    assert.equal(JSON.parse(response.body).success, false);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
