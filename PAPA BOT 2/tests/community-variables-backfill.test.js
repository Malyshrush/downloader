const assert = require('node:assert/strict');

const {
  buildCommunityVariableStateFromRows,
  backfillCommunityVariables
} = require('../scripts/backfill-community-variables');

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
  await run('buildCommunityVariableStateFromRows extracts global, vk and user catalog entries', async () => {
    const state = buildCommunityVariableStateFromRows([
      {
        'Пользовательская': 'pp_score',
        'Глобальная': '',
        'Значение ГП': '',
        'ПЕРЕМЕННЫЕ ВК': '',
        'Значение/Описание ПВК': ''
      },
      {
        'Пользовательская': '',
        'Глобальная': 'GP_LIMIT',
        'Значение ГП': '500',
        'ПЕРЕМЕННЫЕ ВК': '',
        'Значение/Описание ПВК': ''
      },
      {
        'Пользовательская': '',
        'Глобальная': '',
        'Значение ГП': '',
        'ПЕРЕМЕННЫЕ ВК': '%vk_user%',
        'Значение/Описание ПВК': 'Имя пользователя'
      }
    ]);

    assert.deepEqual(state, {
      globalVars: {
        gp_limit: '500'
      },
      vkVars: {
        '%vk_user%': 'Имя пользователя'
      },
      userVariableNames: ['pp_score']
    });
  });

  await run('backfillCommunityVariables writes every configured profile community and global scope', async () => {
    const calls = [];
    const summary = await backfillCommunityVariables({
      profileIds: ['1', '8'],
      includeGlobalScope: true,
      loadBotConfig: async profileId => ({
        communities: profileId === '1'
          ? { 'community-1': {}, 'community-2': {} }
          : { 'community-8': {} }
      }),
      getSheetData: async (sheetName, communityId, profileId) => {
        assert.equal(sheetName, 'ПЕРЕМЕННЫЕ');
        return [
          {
            'Пользовательская': 'pp_score',
            'Глобальная': '',
            'Значение ГП': '',
            'ПЕРЕМЕННЫЕ ВК': '',
            'Значение/Описание ПВК': ''
          },
          {
            'Пользовательская': '',
            'Глобальная': 'gp_limit',
            'Значение ГП': profileId + ':' + String(communityId || 'global'),
            'ПЕРЕМЕННЫЕ ВК': '',
            'Значение/Описание ПВК': ''
          },
          {
            'Пользовательская': '',
            'Глобальная': '',
            'Значение ГП': '',
            'ПЕРЕМЕННЫЕ ВК': '%vk_user%',
            'Значение/Описание ПВК': 'Имя пользователя'
          }
        ];
      },
      store: {
        isEnabled: () => true,
        replaceGlobalVariables: async (communityId, variables, profileId) => {
          calls.push({ method: 'replaceGlobalVariables', communityId, variables, profileId });
          return { stored: Object.keys(variables).length };
        },
        replaceVkVariables: async (communityId, variables, profileId) => {
          calls.push({ method: 'replaceVkVariables', communityId, variables, profileId });
          return { stored: Object.keys(variables).length };
        },
        ensureUserVariableCatalog: async (communityId, variableNames, profileId) => {
          calls.push({ method: 'ensureUserVariableCatalog', communityId, variableNames, profileId });
          return { stored: variableNames.length };
        }
      },
      log: () => {}
    });

    assert.equal(summary.scopes, 5);
    assert.equal(summary.globalVariables, 5);
    assert.equal(summary.vkVariables, 5);
    assert.equal(summary.userVariableNames, 5);
    assert.deepEqual(
      calls
        .filter(call => call.method === 'replaceGlobalVariables')
        .map(call => [call.profileId, call.communityId]),
      [
        ['1', null],
        ['1', 'community-1'],
        ['1', 'community-2'],
        ['8', null],
        ['8', 'community-8']
      ]
    );
    assert.equal(calls.length, 15);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
