const assert = require('node:assert/strict');

const appLogs = require('../src/modules/app-logs');

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
  await run('getAppLogSettings loads normalized flag from hot-state store', async () => {
    const calls = [];
    const result = await appLogs.__testOnly.getAppLogSettingsWithDependencies('community-a', '11', {
      hotStateStore: {
        loadJsonObject: async (objectKey, options) => {
          calls.push({ objectKey, options });
          return { value: { enabled: false } };
        }
      }
    });

    assert.deepEqual(result, { enabled: false });
    assert.deepEqual(calls, [
      {
        objectKey: 'app_logs_settings_profile_11_community-a.json',
        options: { defaultValue: { enabled: true } }
      }
    ]);
  });

  await run('addAppLog writes through sheet mutation and keeps max row count', async () => {
    const existingRows = Array.from({ length: 300 }, (_, index) => ({ id: 'old-' + index }));
    const calls = [];

    await appLogs.__testOnly.addAppLogWithDependencies(
      {
        profileId: '7',
        communityId: 'community-b',
        tab: 'USERS',
        title: 'Created',
        summary: 'New user',
        details: ['ID: 15'],
        level: 'warn',
        meta: { actor: 'admin' }
      },
      {
        getAppLogSettings: async () => ({ enabled: true }),
        updateSheetData: async (sheetName, communityId, profileId, updater) => {
          const nextRows = await updater(existingRows.slice());
          calls.push({ sheetName, communityId, profileId, nextRows });
          return { changed: true, value: nextRows };
        }
      }
    );

    assert.equal(calls.length, 1);
    assert.equal(calls[0].sheetName, 'ЛОГИ ПРИЛОЖЕНИЯ');
    assert.equal(calls[0].communityId, 'community-b');
    assert.equal(calls[0].profileId, '7');
    assert.equal(calls[0].nextRows.length, 300);
    assert.equal(calls[0].nextRows[0].tab, 'USERS');
    assert.equal(calls[0].nextRows[0].title, 'Created');
    assert.equal(calls[0].nextRows[0].summary, 'New user');
    assert.deepEqual(calls[0].nextRows[0].details, ['ID: 15']);
    assert.equal(calls[0].nextRows[0].level, 'warn');
    assert.deepEqual(calls[0].nextRows[0].meta, { actor: 'admin' });
  });

  await run('addAppLog writes one structured YDB row when app-log store is enabled', async () => {
    const storeCalls = [];

    await appLogs.__testOnly.addAppLogWithDependencies(
      {
        profileId: '7',
        communityId: 'community-b',
        tab: 'USERS',
        title: 'Created',
        summary: 'New user',
        details: ['ID: 15']
      },
      {
        getAppLogSettings: async () => ({ enabled: true }),
        appLogsStore: {
          isEnabled: () => true,
          addLog: async (scope, row) => {
            storeCalls.push({ scope, row });
          }
        },
        updateSheetData: async () => {
          throw new Error('sheet mutation should not be used when structured store is enabled');
        }
      }
    );

    assert.equal(storeCalls.length, 1);
    assert.equal(storeCalls[0].scope, '7:community-b');
    assert.equal(storeCalls[0].row.tab, 'USERS');
    assert.equal(storeCalls[0].row.title, 'Created');
  });

  await run('getAppLogs reads structured rows when app-log store is enabled', async () => {
    const rows = await appLogs.__testOnly.getAppLogsWithDependencies('community-z', '9', 2, {
      appLogsStore: {
        isEnabled: () => true,
        listLogs: async (scope, limit) => {
          assert.equal(scope, '9:community-z');
          assert.equal(limit, 2);
          return [{ id: 'log_2' }, { id: 'log_1' }];
        }
      },
      getSheetData: async () => {
        throw new Error('sheet fallback should not be used when structured store is enabled');
      }
    });

    assert.deepEqual(rows, [{ id: 'log_2' }, { id: 'log_1' }]);
  });

  await run('clearAppLogs clears structured scope when app-log store is enabled', async () => {
    const clearCalls = [];

    await appLogs.__testOnly.clearAppLogsWithDependencies('community-c', '5', {
      appLogsStore: {
        isEnabled: () => true,
        clearLogs: async scope => {
          clearCalls.push(scope);
          return { deletedCount: 3 };
        }
      },
      updateSheetData: async () => {
        throw new Error('sheet fallback should not be used when structured store is enabled');
      }
    });

    assert.deepEqual(clearCalls, ['5:community-c']);
  });

  await run('deleteAppLogsFile removes log object through hot-state store', async () => {
    const calls = [];
    const result = await appLogs.__testOnly.deleteAppLogsFileWithDependencies('community-c', '5', {
      hotStateStore: {
        deleteJsonObject: async objectKey => {
          calls.push(objectKey);
          return { deletedFromYdb: true, deletedFromS3: true };
        }
      }
    });

    assert.deepEqual(result, {
      fileName: 'app_logs_profile_5_community-c.json'
    });
    assert.deepEqual(calls, ['app_logs_profile_5_community-c.json']);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
