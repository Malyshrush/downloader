const assert = require('node:assert/strict');

const adminProfiles = require('../src/modules/admin-profiles');
const adminSecurity = require('../src/modules/admin-security');
const adminSessions = require('../src/modules/admin-sessions');

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
  await run('loadAdminAuthWithDependencies reads admin_auth.json through hot-state store', async () => {
    const calls = [];
    const result = await adminProfiles.__testOnly.loadAdminAuthWithDependencies({
      hotStateStore: {
        loadJsonObject: async (objectKey, options) => {
          calls.push({ objectKey, options });
          return {
            value: {
              defaultProfileId: '7',
              profiles: {
                '7': {
                  id: '7',
                  username: 'demo',
                  password: 'secret'
                }
              }
            }
          };
        }
      }
    });

    assert.equal(result.defaultProfileId, '7');
    assert.equal(result.profiles['7'].username, 'demo');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].objectKey, 'admin_auth.json');
  });

  await run('saveSecurityDataWithDependencies writes normalized security state through hot-state store', async () => {
    const calls = [];
    const result = await adminSecurity.__testOnly.saveSecurityDataWithDependencies({
      loginAttempts: { demo: { attempts: 1 } },
      promoCodes: null
    }, {
      hotStateStore: {
        saveJsonObject: async (objectKey, value) => {
          calls.push({ objectKey, value });
          return { primary: 'ydb' };
        }
      }
    });

    assert.deepEqual(result.promoCodes, []);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].objectKey, 'admin_security.json');
    assert.deepEqual(calls[0].value.promoCodes, []);
  });

  await run('loadAdminSessionsWithDependencies reads admin_sessions.json through hot-state store', async () => {
    const calls = [];
    const result = await adminSessions.__testOnly.loadAdminSessionsWithDependencies({
      hotStateStore: {
        loadJsonObject: async (objectKey, options) => {
          calls.push({ objectKey, options });
          return {
            value: {
              sessions: {
                sess_1: {
                  sessionId: 'sess_1',
                  profileId: '2',
                  lastVerifiedIp: '203.0.113.10'
                }
              }
            }
          };
        }
      }
    });

    assert.equal(result.sessions.sess_1.profileId, '2');
    assert.equal(result.sessions.sess_1.lastVerifiedIp, '203.0.113.10');
    assert.equal(calls.length, 1);
    assert.equal(calls[0].objectKey, 'admin_sessions.json');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
