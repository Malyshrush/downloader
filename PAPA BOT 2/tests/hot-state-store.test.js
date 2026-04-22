const assert = require('node:assert/strict');

const { createHotStateStore } = require('../src/modules/hot-state-store');

async function run(name, fn) {
  try {
    await fn();
    process.stdout.write('PASS ' + name + '\n');
  } catch (error) {
    process.stderr.write('FAIL ' + name + '\n');
    throw error;
  }
}

function createConfig(overrides = {}) {
  return {
    mode: 'cloud',
    ymqRegion: 'ru-central1',
    ydbDocApiEndpoint: 'https://docapi.example.test',
    ydbHotStateTable: 'hot_state_objects',
    awsAccessKeyId: 'key',
    awsSecretAccessKey: 'secret',
    bucketName: 'bot-data-storage2',
    ...overrides
  };
}

(async function main() {
  await run('loadJsonObject prefers YDB hot state over S3 backup', async () => {
    const calls = [];
    const store = createHotStateStore(
      createConfig(),
      {
        readHotStateItem: async objectKey => {
          calls.push(`ydb:${objectKey}`);
          return { objectKey, jsonText: '{"from":"ydb"}' };
        },
        readS3Object: async objectKey => {
          calls.push(`s3:${objectKey}`);
          return { objectKey, jsonText: '{"from":"s3"}' };
        },
        writeHotStateItem: async () => {
          throw new Error('unexpected ydb write');
        },
        writeS3Object: async () => {
          throw new Error('unexpected s3 write');
        },
        log: () => {}
      }
    );

    const result = await store.loadJsonObject('admin_auth.json', {
      defaultValue: {}
    });

    assert.deepEqual(result.value, { from: 'ydb' });
    assert.equal(result.source, 'ydb');
    assert.deepEqual(calls, ['ydb:admin_auth.json']);
  });

  await run('loadJsonObject falls back to S3 legacy key and backfills YDB', async () => {
    const calls = [];
    const store = createHotStateStore(
      createConfig(),
      {
        readHotStateItem: async objectKey => {
          calls.push(`ydb:${objectKey}`);
          return null;
        },
        readS3Object: async objectKey => {
          calls.push(`s3:${objectKey}`);
          if (objectKey === 'bot_config_profile_1.json') {
            const error = new Error('NoSuchKey');
            error.name = 'NoSuchKey';
            throw error;
          }
          return { objectKey, jsonText: '{"from":"legacy"}' };
        },
        writeHotStateItem: async (objectKey, jsonText) => {
          calls.push(`backfill:${objectKey}:${jsonText}`);
        },
        writeS3Object: async () => {
          throw new Error('unexpected s3 write');
        },
        log: () => {}
      }
    );

    const result = await store.loadJsonObject('bot_config_profile_1.json', {
      defaultValue: {},
      legacyKeys: ['bot_config.json']
    });

    assert.deepEqual(result.value, { from: 'legacy' });
    assert.equal(result.source, 's3');
    assert.deepEqual(calls, [
      'ydb:bot_config_profile_1.json',
      's3:bot_config_profile_1.json',
      's3:bot_config.json',
      'backfill:bot_config_profile_1.json:{"from":"legacy"}'
    ]);
  });

  await run('saveJsonObject writes YDB first and keeps S3 as best-effort backup', async () => {
    const calls = [];
    const store = createHotStateStore(
      createConfig(),
      {
        readHotStateItem: async () => null,
        readS3Object: async () => null,
        writeHotStateItem: async (objectKey, jsonText) => {
          calls.push(`ydb:${objectKey}:${jsonText}`);
        },
        writeS3Object: async objectKey => {
          calls.push(`s3:${objectKey}`);
          throw new Error('backup unavailable');
        },
        log: () => {}
      }
    );

    const result = await store.saveJsonObject('admin_sessions.json', { sessions: {} });

    assert.equal(result.primary, 'ydb');
    assert.equal(result.backupAttempted, true);
    assert.equal(result.backupError, 'backup unavailable');
    assert.equal(calls[0], `ydb:admin_sessions.json:${JSON.stringify({ sessions: {} }, null, 2)}`);
    assert.equal(calls[1], 's3:admin_sessions.json');
  });

  await run('updateJsonObject loads current value and skips write when unchanged', async () => {
    const calls = [];
    const store = createHotStateStore(
      createConfig(),
      {
        readHotStateItem: async objectKey => {
          calls.push(`read:${objectKey}`);
          return { objectKey, jsonText: '{"enabled":true}' };
        },
        writeHotStateItem: async () => {
          calls.push('unexpected-ydb-write');
        },
        writeS3Object: async () => {
          calls.push('unexpected-s3-write');
        },
        log: () => {}
      }
    );

    const result = await store.updateJsonObject('app_logs_settings.json', value => {
      value.enabled = true;
    }, {
      defaultValue: { enabled: false }
    });

    assert.equal(result.changed, false);
    assert.deepEqual(result.value, { enabled: true });
    assert.deepEqual(calls, ['read:app_logs_settings.json']);
  });

  await run('deleteJsonObject removes hot state from YDB and S3 backup', async () => {
    const calls = [];
    const store = createHotStateStore(
      createConfig(),
      {
        deleteHotStateItem: async objectKey => {
          calls.push(`ydb:${objectKey}`);
        },
        deleteS3Object: async objectKey => {
          calls.push(`s3:${objectKey}`);
        },
        log: () => {}
      }
    );

    const result = await store.deleteJsonObject('profile_dashboard.json');

    assert.deepEqual(result, {
      objectKey: 'profile_dashboard.json',
      deletedFromYdb: true,
      deletedFromS3: true
    });
    assert.deepEqual(calls, ['ydb:profile_dashboard.json', 's3:profile_dashboard.json']);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
