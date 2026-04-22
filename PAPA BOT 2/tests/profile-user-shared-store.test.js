const assert = require('node:assert/strict');

const {
  createProfileUserSharedStore,
  buildProfileUserSharedScope
} = require('../src/modules/profile-user-shared-store');

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
    ydbProfileUserSharedTable: 'profile_user_shared_state',
    awsAccessKeyId: 'key',
    awsSecretAccessKey: 'secret',
    ...overrides
  };
}

(async function main() {
  await run('buildProfileUserSharedScope normalizes profile identifier', async () => {
    assert.equal(buildProfileUserSharedScope('7'), '7');
    assert.equal(buildProfileUserSharedScope(''), '1');
  });

  await run('profile user shared store writes one user variable object per profile', async () => {
    const calls = [];
    const store = createProfileUserSharedStore(
      createConfig(),
      {
        putItem: async item => {
          calls.push(item);
          return { ok: true };
        }
      }
    );

    await store.putUserVariables('7', '42', {
      pvs_score: '100',
      pvs_level: '7'
    });

    assert.equal(calls.length, 1);
    assert.equal(calls[0].profileScope, '7');
    assert.equal(calls[0].userId, '42');
    assert.deepEqual(calls[0].variables, {
      pvs_score: '100',
      pvs_level: '7'
    });
  });

  await run('profile user shared store reads one user variable object', async () => {
    const store = createProfileUserSharedStore(
      createConfig(),
      {
        getItem: async key => {
          assert.deepEqual(key, {
            profileScope: '7',
            userId: '42'
          });
          return {
            profileScope: '7',
            userId: '42',
            variables: {
              pvs_score: '100'
            }
          };
        }
      }
    );

    const variables = await store.getUserVariables('7', '42');

    assert.deepEqual(variables, {
      pvs_score: '100'
    });
  });

  await run('profile user shared store lists all user variable objects for one profile', async () => {
    const store = createProfileUserSharedStore(
      createConfig(),
      {
        queryItems: async request => {
          assert.equal(request.profileScope, '7');
          return {
            Items: [
              {
                profileScope: '7',
                userId: '42',
                variables: { pvs_score: '100' }
              },
              {
                profileScope: '7',
                userId: '77',
                variables: { pvs_score: '200' }
              }
            ]
          };
        }
      }
    );

    const entries = await store.listUserEntries('7');

    assert.deepEqual(entries, [
      {
        userId: '42',
        variables: { pvs_score: '100' }
      },
      {
        userId: '77',
        variables: { pvs_score: '200' }
      }
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
