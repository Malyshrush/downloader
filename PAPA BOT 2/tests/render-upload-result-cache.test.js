const test = require('node:test');
const assert = require('node:assert/strict');

const { app, __testOnly } = require('../index');

function listen(appInstance) {
  return new Promise(resolve => {
    const server = appInstance.listen(0, () => resolve(server));
  });
}

test('render upload result endpoint returns cached successful upload', async () => {
  __testOnly.uploadResults.clear();
  __testOnly.uploadInFlight.clear();
  __testOnly.rememberUploadResult('upload_test_123', 200, {
    success: true,
    attachment: 'doc1_2'
  });

  const server = await listen(app);
  try {
    const port = server.address().port;
    const response = await fetch(`http://127.0.0.1:${port}/upload-result?upload_id=upload_test_123`);
    const payload = await response.json();

    assert.equal(response.status, 200);
    assert.deepEqual(payload, { success: true, attachment: 'doc1_2' });
  } finally {
    server.close();
  }
});

test('render upload result endpoint reports pending upload without duplicating work', async () => {
  __testOnly.uploadResults.clear();
  __testOnly.uploadInFlight.clear();
  __testOnly.uploadInFlight.set('upload_pending_123', new Promise(() => {}));

  const server = await listen(app);
  try {
    const port = server.address().port;
    const response = await fetch(`http://127.0.0.1:${port}/upload-result?upload_id=upload_pending_123`);
    const payload = await response.json();

    assert.equal(response.status, 202);
    assert.deepEqual(payload, { success: false, pending: true });
  } finally {
    __testOnly.uploadInFlight.clear();
    server.close();
  }
});

test('render service allows browser recovery CORS preflight headers', async () => {
  const server = await listen(app);
  try {
    const port = server.address().port;
    const response = await fetch(`http://127.0.0.1:${port}/upload-result?upload_id=upload_test_123`, {
      method: 'OPTIONS',
      headers: {
        Origin: 'https://functions.yandexcloud.net',
        'Access-Control-Request-Method': 'GET',
        'Access-Control-Request-Headers': 'cache-control, pragma'
      }
    });

    assert.equal(response.status, 200);
    const allowedHeaders = response.headers.get('access-control-allow-headers') || '';
    assert.match(allowedHeaders.toLowerCase(), /cache-control/);
    assert.match(allowedHeaders.toLowerCase(), /pragma/);
  } finally {
    server.close();
  }
});
