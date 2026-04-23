const assert = require('node:assert/strict');

const structuredTriggers = require('../src/modules/structured-triggers');

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
  await run('loadStructuredTriggerRows uses structured store when initialized', async () => {
    const rows = await structuredTriggers.__testOnly.loadStructuredTriggerRows(
      'community-1',
      '7',
      {
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        structuredTriggerStore: {
          isEnabled: () => true,
          listTriggerRows: async (communityId, profileId) => {
            assert.equal(communityId, 'community-1');
            assert.equal(profileId, '7');
            return {
              initialized: true,
              rows: [{ 'Название': 'Stored trigger' }]
            };
          }
        }
      }
    );

    assert.deepEqual(rows, [{ 'Название': 'Stored trigger' }]);
  });

  await run('loadStructuredTriggerRows falls back to sheet when store is uninitialized', async () => {
    const rows = await structuredTriggers.__testOnly.loadStructuredTriggerRows(
      'community-1',
      '7',
      {
        getSheetData: async (sheetName, communityId, profileId) => {
          assert.equal(sheetName, 'ТРИГГЕРЫ');
          assert.equal(communityId, 'community-1');
          assert.equal(profileId, '7');
          return [{ 'Название': 'Sheet trigger' }];
        },
        structuredTriggerStore: {
          isEnabled: () => true,
          listTriggerRows: async () => ({
            initialized: false,
            rows: []
          })
        }
      }
    );

    assert.deepEqual(rows, [{ 'Название': 'Sheet trigger' }]);
  });

  await run('processStructuredTriggers uses structured trigger rows in runtime path', async () => {
    const calls = [];
    const result = await structuredTriggers.__testOnly.processStructuredTriggersWithDependencies(
      {
        type: 'message_new',
        group_id: 229445618,
        object: {
          message: {
            from_id: 42,
            text: 'hello'
          }
        }
      },
      '7',
      {
        getSheetData: async () => {
          throw new Error('sheet fallback should not be used');
        },
        structuredTriggerStore: {
          isEnabled: () => true,
          listTriggerRows: async () => ({
            initialized: true,
            rows: [
              {
                'Название': 'Stored trigger',
                'Код события': 'incoming_message'
              }
            ]
          })
        },
        addAppLog: async payload => {
          calls.push(['log', payload.summary]);
        },
        recordStructuredTriggerExecution: async (profileId, communityId) => {
          calls.push(['record', profileId, communityId]);
        }
      }
    );

    assert.deepEqual(result, { matched: true, handled: true });
    assert.deepEqual(calls, [
      ['log', 'Stored trigger'],
      ['record', '7', '229445618']
    ]);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
