const assert = require('node:assert/strict');

const config = require('../src/modules/config');

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
  await run('saveFullConfigWithDependencies writes profile config through hot-state store', async () => {
    const calls = [];
    const fullConfig = {
      communities: {
        abc: { vk_group_id: 123 }
      },
      active_community: 'abc'
    };

    const result = await config.__testOnly.saveFullConfigWithDependencies(fullConfig, '7', {
      hotStateStore: {
        saveJsonObject: async (objectKey, value) => {
          calls.push({ objectKey, value });
          return { primary: 'ydb' };
        }
      }
    });

    assert.equal(result.active_community, 'abc');
    assert.deepEqual(calls, [
      {
        objectKey: 'bot_config_profile_7.json',
        value: fullConfig
      }
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
