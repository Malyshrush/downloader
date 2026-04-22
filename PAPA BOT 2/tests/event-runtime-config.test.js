const assert = require('node:assert/strict');

const {
  buildEventRuntimeConfig,
  isCloudEventRuntimeEnabled
} = require('../src/modules/event-runtime-config');

function run(name, fn) {
  try {
    fn();
    process.stdout.write('PASS ' + name + '\n');
  } catch (error) {
    process.stderr.write('FAIL ' + name + '\n');
    throw error;
  }
}

run('buildEventRuntimeConfig defaults to stub mode without cloud env', () => {
  const config = buildEventRuntimeConfig({});
  assert.equal(config.mode, 'stub');
  assert.equal(isCloudEventRuntimeEnabled(config), false);
});

run('buildEventRuntimeConfig switches to cloud mode when YMQ and YDB env is present', () => {
  const config = buildEventRuntimeConfig({
    AWS_ACCESS_KEY_ID: 'key',
    AWS_SECRET_ACCESS_KEY: 'secret',
    YMQ_INCOMING_QUEUE_URL: 'https://ymq.example/incoming',
    YMQ_OUTBOUND_QUEUE_URL: 'https://ymq.example/outbound',
    YDB_DOCAPI_ENDPOINT: 'https://docapi.example/db'
  });
  assert.equal(config.mode, 'cloud');
  assert.equal(isCloudEventRuntimeEnabled(config), true);
});
