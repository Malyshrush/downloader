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

  await run('scheduleStepMessage legacy fallback uses updateSheetData instead of saveSheetData', async () => {
    const updates = [];
    const invalidations = [];

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
        getSheetData: async () => [],
        updateSheetData: async (sheetName, communityId, profileId, updater) => {
          const nextRows = await updater([]);
          updates.push({ sheetName, communityId, profileId, nextRows });
          return { changed: true, value: nextRows };
        },
        saveSheetData: async () => {
          throw new Error('legacy saveSheetData should not be used');
        },
        invalidateCache: (sheetName, communityId, profileId) => {
          invalidations.push({ sheetName, communityId, profileId });
        },
        delayedDeliveryStore: {
          isEnabled: () => false
        },
        addAppLog: async () => {}
      }
    );

    assert.equal(updates.length, 1);
    assert.equal(updates[0].sheetName, 'ОТЛОЖЕННЫЕ');
    assert.equal(updates[0].communityId, '229445618');
    assert.equal(updates[0].profileId, '7');
    assert.equal(updates[0].nextRows.length, 1);
    assert.equal(updates[0].nextRows[0]['Шаг'], 'welcome');
    assert.equal(updates[0].nextRows[0]['ID Пользователя'], '42');
    assert.equal(updates[0].nextRows[0]['Статус'], 'Ожидает');
    assert.deepEqual(invalidations, [
      {
        sheetName: 'ОТЛОЖЕННЫЕ',
        communityId: '229445618',
        profileId: '7'
      }
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
