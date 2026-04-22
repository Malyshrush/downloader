const assert = require('node:assert/strict');

const botVersionStore = require('../src/modules/bot-version-store');

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
  await run('saveBotVersionDataWithDependencies writes normalized version data through hot-state store', async () => {
    const calls = [];
    const result = await botVersionStore.__testOnly.saveBotVersionDataWithDependencies({
      note: 'release',
      parts: [{ key: 'core', value: '0001' }]
    }, {
      hotStateStore: {
        saveJsonObject: async (objectKey, value) => {
          calls.push({ objectKey, value });
          return { primary: 'ydb' };
        }
      }
    });

    assert.equal(result.displayVersion, 'version 0001');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].objectKey, 'bot_version.json');
    assert.equal(calls[0].value.note, 'release');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
