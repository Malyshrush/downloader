const assert = require('node:assert/strict');

const {
  buildLegacySheetAccessPolicy,
  isLegacySheetAccessAllowed,
  assertLegacySheetAccessAllowed
} = require('../src/modules/legacy-sheet-access-policy');

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
  await run('legacy sheet access is allowed outside cloud runtime', async () => {
    const policy = buildLegacySheetAccessPolicy({
      EVENT_QUEUE_MODE: 'stub'
    });

    assert.equal(isLegacySheetAccessAllowed('variables', policy), true);
    assert.equal(isLegacySheetAccessAllowed('app_logs', policy), true);
    assert.equal(isLegacySheetAccessAllowed('scheduler', policy), true);
  });

  await run('legacy variables, app logs, and scheduler sheet access are blocked by default in cloud runtime', async () => {
    const policy = buildLegacySheetAccessPolicy({
      EVENT_QUEUE_MODE: 'cloud',
      YMQ_INCOMING_QUEUE_URL: 'incoming',
      YMQ_OUTBOUND_QUEUE_URL: 'outgoing',
      YDB_DOCAPI_ENDPOINT: 'https://docapi.example.test',
      AWS_ACCESS_KEY_ID: 'key',
      AWS_SECRET_ACCESS_KEY: 'secret'
    });

    assert.equal(isLegacySheetAccessAllowed('variables', policy), false);
    assert.equal(isLegacySheetAccessAllowed('app_logs', policy), false);
    assert.equal(isLegacySheetAccessAllowed('scheduler', policy), false);
  });

  await run('legacy sheet access can be re-enabled explicitly per channel in cloud runtime', async () => {
    const policy = buildLegacySheetAccessPolicy({
      EVENT_QUEUE_MODE: 'cloud',
      YMQ_INCOMING_QUEUE_URL: 'incoming',
      YMQ_OUTBOUND_QUEUE_URL: 'outgoing',
      YDB_DOCAPI_ENDPOINT: 'https://docapi.example.test',
      AWS_ACCESS_KEY_ID: 'key',
      AWS_SECRET_ACCESS_KEY: 'secret',
      ALLOW_LEGACY_SCHEDULER_SHEET_ACCESS: 'true',
      ALLOW_LEGACY_VARIABLES_SHEET_ACCESS: 'true'
    });

    assert.equal(isLegacySheetAccessAllowed('scheduler', policy), true);
    assert.equal(isLegacySheetAccessAllowed('variables', policy), true);
    assert.equal(isLegacySheetAccessAllowed('app_logs', policy), false);
  });

  await run('assertLegacySheetAccessAllowed throws for blocked cloud-runtime access', async () => {
    const policy = buildLegacySheetAccessPolicy({
      EVENT_QUEUE_MODE: 'cloud',
      YMQ_INCOMING_QUEUE_URL: 'incoming',
      YMQ_OUTBOUND_QUEUE_URL: 'outgoing',
      YDB_DOCAPI_ENDPOINT: 'https://docapi.example.test',
      AWS_ACCESS_KEY_ID: 'key',
      AWS_SECRET_ACCESS_KEY: 'secret'
    });

    assert.throws(() => assertLegacySheetAccessAllowed('variables', policy), /disabled/);
    assert.throws(() => assertLegacySheetAccessAllowed('scheduler', policy), /disabled/);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
