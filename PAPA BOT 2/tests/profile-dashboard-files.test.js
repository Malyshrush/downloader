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
  await run('recordUploadedCommunityFile stores and de-duplicates files by attachment', async () => {
    let savedValue = null;
    let state = {
      profiles: {},
      limitRequests: []
    };

    await profileDashboard.__testOnly.recordUploadedCommunityFileWithDependencies(
      {
        profileId: '7',
        communityId: 'community-a',
        vkGroupId: '229445618',
        groupName: 'Test Group',
        fileName: 'brief.pdf',
        fileType: 'application/pdf',
        fileSize: 4096,
        attachment: 'doc1_2'
      },
      {
        hotStateStore: {
          loadJsonObject: async () => ({ value: state }),
          saveJsonObject: async (objectKey, value) => {
            assert.equal(objectKey, 'profile_dashboard.json');
            savedValue = value;
            state = value;
          }
        },
        getProfileById: async () => ({ id: '7', name: 'Profile 7', requestsLimit: 1000 })
      }
    );

    await profileDashboard.__testOnly.recordUploadedCommunityFileWithDependencies(
      {
        profileId: '7',
        communityId: 'community-a',
        vkGroupId: '229445618',
        groupName: 'Test Group',
        fileName: 'brief-updated.pdf',
        fileType: 'application/pdf',
        fileSize: 8192,
        attachment: 'doc1_2'
      },
      {
        hotStateStore: {
          loadJsonObject: async () => ({ value: state }),
          saveJsonObject: async (objectKey, value) => {
            assert.equal(objectKey, 'profile_dashboard.json');
            savedValue = value;
            state = value;
          }
        },
        getProfileById: async () => ({ id: '7', name: 'Profile 7', requestsLimit: 1000 })
      }
    );

    const files = savedValue.profiles['7'].communityFiles['229445618'];
    assert.equal(files.length, 1);
    assert.equal(files[0].fileName, 'brief-updated.pdf');
    assert.equal(files[0].fileSize, 8192);
    assert.equal(files[0].attachment, 'doc1_2');
  });

  await run('getProfileDashboardOverview exposes file catalog per community', async () => {
    const dashboard = await profileDashboard.__testOnly.getProfileDashboardOverviewWithDependencies(
      '7',
      {
        hotStateStore: {
          loadJsonObject: async () => ({
            value: {
              profiles: {
                '7': {
                  profileId: '7',
                  profileName: 'Profile 7',
                  dailyLimit: 1000,
                  dailyUsed: 10,
                  dailyUsageDay: '2026-04-30',
                  totalPapaRequests: 10,
                  totalMessages: 5,
                  totalComments: 2,
                  totalTriggers: 1,
                  communities: {
                    '229445618': {
                      communityId: '229445618',
                      papaRequests: 10,
                      messages: 5,
                      comments: 2,
                      triggers: 1,
                      lastEventAt: '2026-04-30T10:00:00.000Z'
                    }
                  },
                  limitHistory: [],
                  communityFiles: {
                    '229445618': [
                      {
                        attachment: 'doc1_2',
                        fileName: 'brief.pdf',
                        fileType: 'application/pdf',
                        fileSize: 4096,
                        uploadedAt: '2026-04-30T10:10:00.000Z',
                        communityId: 'community-a',
                        vkGroupId: '229445618',
                        groupName: 'Test Group'
                      }
                    ]
                  }
                }
              },
              limitRequests: []
            }
          }),
          saveJsonObject: async () => {}
        },
        getProfileById: async () => ({ id: '7', name: 'Profile 7', requestsLimit: 1000 }),
        getProfilePromoActivationStatus: async () => ({ attempts: 0, remainingAttempts: 3, blocked: false, nextResetAt: 0 }),
        loadBotConfig: async () => {},
        getFullConfig: () => ({
          communities: {
            'community-a': {
              vk_group_id: '229445618',
              group_name: 'Test Group'
            }
          }
        }),
        listUsers: async () => [{ ID: '1' }]
      }
    );

    assert.equal(dashboard.communities.length, 1);
    assert.ok(dashboard.communityFiles);
    assert.equal(dashboard.communityFiles['229445618'].length, 1);
    assert.equal(dashboard.communityFiles['229445618'][0].attachment, 'doc1_2');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
