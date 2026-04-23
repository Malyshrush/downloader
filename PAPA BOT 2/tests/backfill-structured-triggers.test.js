const assert = require('node:assert/strict');

const { backfillStructuredTriggers } = require('../scripts/backfill-structured-triggers');

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
  await run('backfillStructuredTriggers copies trigger rows into structured store', async () => {
    const calls = [];
    const summary = await backfillStructuredTriggers({
      profileIds: ['7'],
      loadBotConfig: async profileId => {
        assert.equal(profileId, '7');
        return {
          communities: {
            alpha: {},
            beta: {}
          }
        };
      },
      getSheetData: async (sheetName, communityId, profileId) => {
        assert.equal(sheetName, 'ТРИГГЕРЫ');
        assert.equal(profileId, '7');
        if (communityId === 'alpha') {
          return [{ 'Название': 'Alpha trigger' }];
        }
        if (communityId === 'beta') {
          return [{ 'Название': 'Beta 1' }, { 'Название': 'Beta 2' }];
        }
        throw new Error('unexpected community ' + communityId);
      },
      store: {
        isEnabled: () => true,
        replaceTriggerRows: async (communityId, rows, profileId) => {
          calls.push({ communityId, rows, profileId });
          return { stored: rows.length, backend: 'ydb-structured-triggers' };
        }
      },
      log: () => {}
    });

    assert.deepEqual(calls, [
      {
        communityId: 'alpha',
        rows: [{ 'Название': 'Alpha trigger' }],
        profileId: '7'
      },
      {
        communityId: 'beta',
        rows: [{ 'Название': 'Beta 1' }, { 'Название': 'Beta 2' }],
        profileId: '7'
      }
    ]);
    assert.deepEqual(summary, {
      profiles: 1,
      communities: 2,
      rows: 3
    });
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
