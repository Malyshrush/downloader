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
  await run('getUserVariablesWithDependencies reads variables from structured user state when enabled', async () => {
    const variables = await users.__testOnly.getUserVariablesWithDependencies('42', 'community-1', '9', {
      userStateStore: {
        isEnabled: () => true,
        getUserRow: async (userScope, userId) => {
          assert.equal(userScope, '9:community-1');
          assert.equal(userId, '42');
          return {
            ID: '42',
            'Пользовательская': 'score\nlevel',
            'Значения ПП': '100\n7'
          };
        }
      }
    });

    assert.deepEqual(variables, {
      score: '100',
      level: '7'
    });
  });

  await run('getUserRowWithDependencies uses injected sheet getter for legacy fallback', async () => {
    const row = await users.__testOnly.getUserRowWithDependencies('42', 'community-1', '9', {
      getSheetData: async (sheetName, communityId, profileId) => {
        assert.equal(sheetName, 'ПОЛЬЗОВАТЕЛИ');
        assert.equal(communityId, 'community-1');
        assert.equal(profileId, '9');
        return [
          { ID: '42', Name: 'Alice' },
          { ID: '77', Name: 'Bob' }
        ];
      }
    });

    assert.deepEqual(row, { ID: '42', Name: 'Alice' });
  });

  await run('listUsersWithDependencies uses injected sheet getter for legacy fallback', async () => {
    const rows = await users.__testOnly.listUsersWithDependencies('community-1', '9', {
      getSheetData: async (sheetName, communityId, profileId) => {
        assert.equal(sheetName, 'ПОЛЬЗОВАТЕЛИ');
        assert.equal(communityId, 'community-1');
        assert.equal(profileId, '9');
        return [
          { ID: '42', Name: 'Alice' },
          { ID: '77', Name: 'Bob' }
        ];
      }
    });

    assert.deepEqual(rows, [
      { ID: '42', Name: 'Alice' },
      { ID: '77', Name: 'Bob' }
    ]);
  });

  await run('updateUserVariablesWithDependencies mutates only the target user through userStateStore when enabled', async () => {
    const updates = [];
    const syncedCatalogs = [];

    await users.__testOnly.updateUserVariablesWithDependencies(
      '42',
      { Score: '100', Level: '7' },
      true,
      'community-1',
      '9',
      {
        updateSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        userStateStore: {
          isEnabled: () => true,
          updateUserRow: async (userScope, userId, mutator) => {
            const row = {
              ID: '42',
              'Пользовательская': 'old',
              'Значения ПП': '1'
            };
            const result = await mutator(row);
            updates.push({ userScope, userId, row, result });
            return { found: true, changed: true, value: row };
          }
        },
        syncUserVariableCatalog: async (names, communityId, profileId) => {
          syncedCatalogs.push({ names, communityId, profileId });
        }
      }
    );

    assert.equal(updates.length, 1);
    assert.equal(updates[0].userScope, '9:community-1');
    assert.equal(updates[0].userId, '42');
    assert.equal(updates[0].row['Пользовательская'], 'score\nlevel');
    assert.equal(updates[0].row['Значения ПП'], '100\n7');
    assert.deepEqual(syncedCatalogs, [
      {
        names: ['score', 'level'],
        communityId: 'community-1',
        profileId: '9'
      }
    ]);
  });

  await run('deleteUserDataWithDependencies deletes one structured user row when enabled', async () => {
    const deletions = [];

    const deleted = await users.__testOnly.deleteUserDataWithDependencies('42', 'community-1', '9', {
      updateSheetData: async () => {
        throw new Error('sheet fallback should not be used');
      },
      addAppLog: async () => undefined,
      userStateStore: {
        isEnabled: () => true,
        deleteUserRow: async (userScope, userId) => {
          deletions.push({ userScope, userId });
          return { deleted: true, backend: 'ydb-user-state' };
        }
      }
    });

    assert.equal(deleted, true);
    assert.deepEqual(deletions, [
      {
        userScope: '9:community-1',
        userId: '42'
      }
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
