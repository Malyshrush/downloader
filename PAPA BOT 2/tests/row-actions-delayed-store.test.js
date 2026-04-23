const assert = require('node:assert/strict');

const rowActions = require('../src/modules/row-actions');

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
  await run('scheduleStepMessage appends delayed row through structured store when enabled', async () => {
    const calls = [];

    await rowActions.__testOnly.scheduleStepMessageWithDependencies(
      '42',
      '229445618',
      'welcome,60',
      false,
      'community-1',
      '7',
      {
        now: new Date('2026-04-22T10:00:00.000Z'),
        getCommunityConfig: async () => ({ vk_group_id: 229445618 }),
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        saveSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        delayedDeliveryStore: {
          isEnabled: () => true,
          appendDelayedRow: async (communityId, row, profileId) => {
            calls.push({ communityId, row, profileId });
            return { row: Object.assign({ _delayedId: 'delayed-1' }, row) };
          }
        },
        addAppLog: async () => {}
      }
    );

    assert.equal(calls.length, 1);
    assert.equal(calls[0].communityId, '229445618');
    assert.equal(calls[0].profileId, '7');
    assert.equal(calls[0].row['Шаг'], 'welcome');
    assert.equal(calls[0].row['ID Пользователя'], '42');
    assert.equal(calls[0].row['Статус'], 'Ожидает');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
