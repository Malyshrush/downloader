const assert = require('node:assert/strict');

const storage = require('../src/modules/storage');

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
  await run('syncStructuredTriggerSheet skips non-trigger sheets', async () => {
    const result = await storage.__testOnly.syncStructuredTriggerSheet(
      'СООБЩЕНИЯ',
      [{ '№': '1' }],
      'community-1',
      '7',
      {
        structuredTriggerStore: {
          isEnabled: () => true,
          replaceTriggerRows: async () => {
            throw new Error('trigger store should not be used');
          }
        }
      }
    );

    assert.deepEqual(result, {
      synced: false,
      backend: 'skipped'
    });
  });

  await run('syncStructuredTriggerSheet writes trigger rows into structured store', async () => {
    const calls = [];
    const rows = [{ 'Название': 'Trigger 1' }, { 'Название': 'Trigger 2' }];
    const result = await storage.__testOnly.syncStructuredTriggerSheet(
      'ТРИГГЕРЫ',
      rows,
      'community-1',
      '7',
      {
        structuredTriggerStore: {
          isEnabled: () => true,
          replaceTriggerRows: async (communityId, nextRows, profileId) => {
            calls.push({ communityId, nextRows, profileId });
            return { stored: nextRows.length, backend: 'ydb-structured-triggers' };
          }
        }
      }
    );

    assert.deepEqual(calls, [
      {
        communityId: 'community-1',
        nextRows: rows,
        profileId: '7'
      }
    ]);
    assert.deepEqual(result, {
      synced: true,
      backend: 'ydb-structured-triggers',
      stored: 2
    });
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
