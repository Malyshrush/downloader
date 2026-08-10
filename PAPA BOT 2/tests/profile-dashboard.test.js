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
      limitRequests: [],
      balanceTopUps: [],
      errorReports: [],
      suggestionReports: []
    });
    assert.equal(calls.length, 1);
    assert.equal(calls[0].objectKey, 'profile_dashboard.json');
    assert.deepEqual(calls[0].options.defaultValue, {
      profiles: {},
      limitRequests: [],
      balanceTopUps: [],
      errorReports: [],
      suggestionReports: []
    });
    assert.equal(calls[0].options.preferS3Backup, true);
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
      limitRequests: [],
      balanceTopUps: [],
      errorReports: [],
      suggestionReports: []
    });
    assert.deepEqual(calls, [
      {
        objectKey: 'profile_dashboard.json',
        value: {
          profiles: {
            '7': { dailyUsed: 11 }
          },
          limitRequests: [],
          balanceTopUps: [],
          errorReports: [],
          suggestionReports: []
        }
      }
    ]);
  });

  await run('recordProfileErrorReport stores admin bug report with screenshots', async () => {
    let state = { profiles: {}, limitRequests: [], balanceTopUps: [], errorReports: [] };
    const store = {
      getProfileById: async id => ({ id: String(id), name: 'Тестовый профиль', role: 'admin', requestsLimit: 1000 }),
      hotStateStore: {
        loadJsonObject: async () => ({ value: state }),
        saveJsonObject: async (_objectKey, value) => {
          state = value;
          return { primary: 'ydb' };
        }
      }
    };

    const report = await profileDashboard.__testOnly.recordProfileErrorReportWithDependencies('7', {
      description: 'Не открывается вкладка',
      principalProfileId: '1',
      communityId: '229445618',
      pageUrl: 'https://example.test/admin',
      screenshots: [{ attachment: 'doc27894453_700577664', fileName: 'screen.png', fileSize: 123 }]
    }, store);

    assert.equal(report.profileId, '7');
    assert.equal(report.profileName, 'Тестовый профиль');
    assert.equal(report.description, 'Не открывается вкладка');
    assert.equal(report.screenshots[0].attachment, 'doc27894453_700577664');
    assert.equal(state.errorReports.length, 1);
    assert.equal(state.errorReports[0].id, report.id);
  });

  await run('recordProfileErrorReport enforces five reports per Moscow day', async () => {
    let state = { profiles: {}, limitRequests: [], balanceTopUps: [], errorReports: [] };
    const store = {
      getProfileById: async id => ({ id: String(id), name: 'Тестовый профиль', role: 'admin', requestsLimit: 1000 }),
      hotStateStore: {
        loadJsonObject: async () => ({ value: state }),
        saveJsonObject: async (_objectKey, value) => {
          state = value;
          return { primary: 'ydb' };
        }
      }
    };

    for (let i = 0; i < 5; i++) {
      await profileDashboard.__testOnly.recordProfileErrorReportWithDependencies('7', {
        description: 'Ошибка ' + i,
        createdAt: '2026-06-05T10:00:0' + i + '.000Z'
      }, store);
    }

    await assert.rejects(
      () => profileDashboard.__testOnly.recordProfileErrorReportWithDependencies('7', {
        description: 'Шестая ошибка',
        createdAt: '2026-06-05T12:00:00.000Z'
      }, store),
      /Лимит отправки ошибок/
    );
  });

  await run('updateBugReportStatus fixed grants extra limit only once', async () => {
    let state = {
      profiles: {
        7: { profileId: '7', profileName: 'Тестовый профиль', extraRequestLimit: 5, balanceOperations: [] }
      },
      limitRequests: [],
      balanceTopUps: [],
      errorReports: [{
        id: 'error_1',
        profileId: '7',
        profileName: 'Тестовый профиль',
        description: 'Реальная ошибка',
        status: 'submitted',
        createdAt: '2026-06-05T10:00:00.000Z'
      }]
    };
    const store = {
      getProfileById: async id => ({ id: String(id), name: 'Тестовый профиль', role: 'admin', requestsLimit: 1000 }),
      hotStateStore: {
        loadJsonObject: async () => ({ value: state }),
        saveJsonObject: async (_objectKey, value) => {
          state = value;
          return { primary: 'ydb' };
        }
      }
    };

    const first = await profileDashboard.__testOnly.updateBugReportStatusWithDependencies('error_1', 'fixed', '1', store);
    const second = await profileDashboard.__testOnly.updateBugReportStatusWithDependencies('error_1', 'fixed', '1', store);

    assert.equal(first.reward.amount, 1000);
    assert.equal(second.reward, null);
    assert.equal(state.profiles['7'].extraRequestLimit, 1005);
    assert.equal(state.profiles['7'].balanceOperations.length, 1);
    assert.equal(state.errorReports[0].fixedRewardAmount, 1000);
  });

  await run('suggestion reports enforce daily limit and implemented grants extra limit once', async () => {
    let state = {
      profiles: {
        7: { profileId: '7', profileName: 'Тестовый профиль', extraRequestLimit: 10, balanceOperations: [] }
      },
      limitRequests: [],
      balanceTopUps: [],
      errorReports: [],
      suggestionReports: []
    };
    const store = {
      getProfileById: async id => ({ id: String(id), name: 'Тестовый профиль', role: 'admin', requestsLimit: 1000 }),
      hotStateStore: {
        loadJsonObject: async () => ({ value: state }),
        saveJsonObject: async (_objectKey, value) => {
          state = value;
          return { primary: 'ydb' };
        }
      }
    };

    for (let i = 0; i < 5; i++) {
      await profileDashboard.__testOnly.recordProfileSuggestionReportWithDependencies('7', {
        description: 'Предложение ' + i,
        createdAt: '2026-06-05T10:10:0' + i + '.000Z'
      }, store);
    }

    await assert.rejects(
      () => profileDashboard.__testOnly.recordProfileSuggestionReportWithDependencies('7', {
        description: 'Шестое предложение',
        createdAt: '2026-06-05T12:00:00.000Z'
      }, store),
      /Лимит отправки предложений/
    );

    const reportId = state.suggestionReports[0].id;
    const first = await profileDashboard.__testOnly.updateSuggestionReportStatusWithDependencies(reportId, 'implemented', '1', store);
    const second = await profileDashboard.__testOnly.updateSuggestionReportStatusWithDependencies(reportId, 'implemented', '1', store);

    assert.equal(first.reward.amount, 1000);
    assert.equal(second.reward, null);
    assert.equal(state.profiles['7'].extraRequestLimit, 1010);
    assert.equal(state.profiles['7'].balanceOperations.length, 1);
    assert.equal(state.suggestionReports[0].implementedRewardAmount, 1000);
  });

  await run('getProfileDashboardOverview returns newest enriched payment operations with safe fallbacks', async () => {
    const state = {
      profiles: {
        '7': {
          profileId: '7',
          profileName: 'Profile 7',
          communities: {},
          communityFiles: {},
          communityDocuments: {},
          paymentButtonPayments: [{
            paymentId: 'old',
            communityId: '100',
            userId: '42',
            sourceBot: '',
            createdAt: '2026-06-01T10:00:00.000Z'
          }, {
            paymentId: 'new',
            communityId: '100',
            userId: '42',
            sourceBot: 'Sales',
            createdAt: '2026-06-02T10:00:00.000Z'
          }, {
            paymentId: 'fallback',
            communityId: '999',
            userId: '77',
            createdAt: '2026-05-31T10:00:00.000Z'
          }]
        }
      },
      limitRequests: []
    };

    const result = await profileDashboard.__testOnly.getProfileDashboardOverviewWithDependencies('7', {
      hotStateStore: {
        loadJsonObject: async () => ({ value: state }),
        saveJsonObject: async () => {}
      },
      getProfileById: async () => ({ id: '7', name: 'Profile 7', requestsLimit: 1000 }),
      getAttachmentUploadSettings: async () => ({
        global: { image: 'community', document: 'user', video: 'community' },
        overrides: {},
        effective: { image: 'community', document: 'user', video: 'community' }
      }),
      getProfilePromoActivationStatus: async () => ({ attempts: 0, remainingAttempts: 3, blocked: false, nextResetAt: 0 }),
      loadBotConfig: async () => {},
      getFullConfig: () => ({
        communities: {
          main: { vk_group_id: '100', group_name: 'Main Community' }
        }
      }),
      listUsers: async communityId => communityId === '100'
        ? [{ ID: '42', 'ИМЯ': 'Иван Иванов' }]
        : []
    });

    assert.deepEqual(result.paymentOperations.map(item => item.paymentId), ['new', 'old', 'fallback']);
    assert.equal(result.paymentOperations[0].userName, 'Иван Иванов');
    assert.equal(result.paymentOperations[0].communityName, 'Main Community');
    assert.equal(result.paymentOperations[0].sourceBot, 'Sales');
    assert.equal(result.paymentOperations[1].sourceBot, '');
    assert.equal(result.paymentOperations[2].userName, '');
    assert.equal(result.paymentOperations[2].communityName, '');
    assert.deepEqual(result.attachmentUploadSettings.effective, { image: 'community', document: 'user', video: 'community' });
  });

  await run('getProfileDashboardOverview limits payment operations to 500 newest records', async () => {
    const paymentButtonPayments = Array.from({ length: 505 }, (_value, index) => ({
      paymentId: 'payment-' + index,
      communityId: '100',
      userId: '42',
      createdAt: new Date(Date.UTC(2026, 0, 1, 0, 0, index)).toISOString()
    }));
    const state = {
      profiles: {
        '7': {
          profileId: '7',
          profileName: 'Profile 7',
          communities: {},
          communityFiles: {},
          communityDocuments: {},
          paymentButtonPayments
        }
      },
      limitRequests: []
    };

    const result = await profileDashboard.__testOnly.getProfileDashboardOverviewWithDependencies('7', {
      hotStateStore: {
        loadJsonObject: async () => ({ value: state }),
        saveJsonObject: async () => {}
      },
      getProfileById: async () => ({ id: '7', name: 'Profile 7', requestsLimit: 1000 }),
      getProfilePromoActivationStatus: async () => ({ attempts: 0, remainingAttempts: 3, blocked: false, nextResetAt: 0 }),
      loadBotConfig: async () => {},
      getFullConfig: () => ({
        communities: {
          main: { vk_group_id: '100', group_name: 'Main Community' }
        }
      }),
      listUsers: async () => [{ ID: '42', 'ИМЯ': 'Иван Иванов' }]
    });

    assert.equal(result.paymentOperations.length, 500);
    assert.equal(result.paymentOperations[0].paymentId, 'payment-504');
  });

  await run('deleteProfilePaymentOperations removes only requested payment ids', async () => {
    let state = {
      profiles: {
        '7': {
          profileId: '7',
          paymentButtonPayments: [
            { paymentId: 'pay-1' },
            { paymentId: 'pay-2' },
            { paymentId: 'pay-3' },
            { providerPaymentId: 'provider-only' }
          ]
        }
      },
      limitRequests: []
    };
    const store = {
      hotStateStore: {
        loadJsonObject: async () => ({ value: state }),
        saveJsonObject: async (_key, value) => {
          state = value;
        }
      }
    };

    const result = await profileDashboard.__testOnly.deleteProfilePaymentOperationsWithDependencies(
      '7',
      ['pay-1', 'pay-3', 'pay-3', 'provider-only', 'missing'],
      store
    );

    assert.equal(result.removedCount, 3);
    assert.equal(result.requestedCount, 4);
    assert.deepEqual(
      state.profiles['7'].paymentButtonPayments.map(item => item.paymentId),
      ['pay-2']
    );
  });

  await run('deleteProfilePaymentOperations rejects an empty payment id list', async () => {
    await assert.rejects(
      () => profileDashboard.__testOnly.deleteProfilePaymentOperationsWithDependencies('7', [], {}),
      /paymentIds are required/
    );
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
