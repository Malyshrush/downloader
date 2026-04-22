const assert = require('node:assert/strict');

const users = require('../src/modules/users');

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
  await run('updateUserVariablesWithDependencies mutates only the target user through updateSheetData', async () => {
    const updatedSheets = [];
    const syncedCatalogs = [];

    await users.__testOnly.updateUserVariablesWithDependencies(
      '42',
      { Score: '100', Level: '7' },
      true,
      'community-1',
      '9',
      {
        updateSheetData: async (sheetName, communityId, profileId, updater) => {
          const nextRows = await updater([
            {
              ID: '42',
              'Пользовательская': 'old',
              'Значения ПП': '1'
            },
            {
              ID: '77',
              'Пользовательская': 'other',
              'Значения ПП': '2'
            }
          ]);
          updatedSheets.push({ sheetName, communityId, profileId, nextRows });
          return { changed: true, value: nextRows };
        },
        syncUserVariableCatalog: async (names, communityId, profileId) => {
          syncedCatalogs.push({ names, communityId, profileId });
        }
      }
    );

    assert.equal(updatedSheets.length, 1);
    assert.equal(updatedSheets[0].sheetName, 'ПОЛЬЗОВАТЕЛИ');
    assert.equal(updatedSheets[0].communityId, 'community-1');
    assert.equal(updatedSheets[0].profileId, '9');
    assert.equal(updatedSheets[0].nextRows[0]['Пользовательская'], 'score\nlevel');
    assert.equal(updatedSheets[0].nextRows[0]['Значения ПП'], '100\n7');
    assert.equal(updatedSheets[0].nextRows[1]['Пользовательская'], 'other');
    assert.deepEqual(syncedCatalogs, [
      {
        names: ['score', 'level'],
        communityId: 'community-1',
        profileId: '9'
      }
    ]);
  });

  await run('markStepAsSentWithDependencies avoids duplicate writes for an already-sent step', async () => {
    const updatedSheets = [];

    await users.__testOnly.markStepAsSentWithDependencies('42', 'bot-a', 'step-2', 'community-1', '9', {
      updateSheetData: async (sheetName, communityId, profileId, updater) => {
        const nextRows = await updater([
          {
            ID: '42',
            'Отправленные Шаги': 'bot-a:step-2'
          }
        ]);
        updatedSheets.push({ sheetName, communityId, profileId, nextRows });
        return { changed: false, value: nextRows };
      }
    });

    assert.equal(updatedSheets.length, 1);
    assert.equal(updatedSheets[0].nextRows[0]['Отправленные Шаги'], 'bot-a:step-2');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
