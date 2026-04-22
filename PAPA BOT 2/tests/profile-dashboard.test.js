const assert = require('node:assert/strict');

const profileDashboard = require('../src/modules/profile-dashboard');

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
  await run('loadDashboardData uses hot-state store and normalizes missing sections', async () => {
    const calls = [];

    const result = await profileDashboard.__testOnly.loadDashboardDataWithDependencies({
      hotStateStore: {
        loadJsonObject: async (objectKey, options) => {
          calls.push({ objectKey, options });
          return {
            value: {
              profiles: {
                '7': { dailyUsed: 10 }
              }
            },
            source: 'ydb'
          };
        }
      }
    });

    assert.deepEqual(result, {
      profiles: {
        '7': { dailyUsed: 10 }
      },
      limitRequests: []
    });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].objectKey, 'profile_dashboard.json');
    assert.deepEqual(calls[0].options.defaultValue, {
      profiles: {},
      limitRequests: []
    });
  });

  await run('saveDashboardData writes normalized dashboard object through hot-state store', async () => {
    const calls = [];

    const result = await profileDashboard.__testOnly.saveDashboardDataWithDependencies(
      {
        profiles: {
          '7': { dailyUsed: 11 }
        },
        limitRequests: null
      },
      {
        hotStateStore: {
          saveJsonObject: async (objectKey, value) => {
            calls.push({ objectKey, value });
            return { primary: 'ydb' };
          }
        }
      }
    );

    assert.deepEqual(result, {
      profiles: {
        '7': { dailyUsed: 11 }
      },
      limitRequests: []
    });
    assert.deepEqual(calls, [
      {
        objectKey: 'profile_dashboard.json',
        value: {
          profiles: {
            '7': { dailyUsed: 11 }
          },
          limitRequests: []
        }
      }
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
