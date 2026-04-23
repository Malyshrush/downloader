const assert = require('node:assert/strict');

const {
  META_TRIGGER_ID,
  buildStructuredTriggerScope,
  createStructuredTriggerStore
} = require('../src/modules/structured-trigger-store');

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
  await run('buildStructuredTriggerScope normalizes profile and community identifiers', async () => {
    assert.equal(buildStructuredTriggerScope(' 77 ', ' 8 '), '8:77');
    assert.equal(buildStructuredTriggerScope('', ''), '1:global');
    assert.equal(buildStructuredTriggerScope(null, '2'), '2:global');
  });

  await run('structured trigger store distinguishes uninitialized from empty', async () => {
    const store = createStructuredTriggerStore(
      {
        mode: 'cloud',
        ydbDocApiEndpoint: 'https://docapi',
        ydbStructuredTriggersTable: 'structured_trigger_entries',
        awsAccessKeyId: 'key',
        awsSecretAccessKey: 'secret',
        ymqRegion: 'ru-central1'
      },
      {
        queryItems: async () => ({ Items: [] }),
        batchWriteItems: async () => ({ ok: true })
      }
    );

    const result = await store.listTriggerRows('community-1', '7');
    assert.deepEqual(result, {
      initialized: false,
      rows: []
    });
  });

  await run('structured trigger store replaces rows and preserves order', async () => {
    const memory = [];
    const store = createStructuredTriggerStore(
      {
        mode: 'cloud',
        ydbDocApiEndpoint: 'https://docapi',
        ydbStructuredTriggersTable: 'structured_trigger_entries',
        awsAccessKeyId: 'key',
        awsSecretAccessKey: 'secret',
        ymqRegion: 'ru-central1'
      },
      {
        queryItems: async ({ triggerScope }) => ({
          Items: memory.filter(item => item.triggerScope === triggerScope)
        }),
        batchWriteItems: async operations => {
          for (const key of operations.deleteKeys || []) {
            const index = memory.findIndex(item => item.triggerScope === key.triggerScope && item.triggerId === key.triggerId);
            if (index !== -1) {
              memory.splice(index, 1);
            }
          }
          for (const item of operations.putItems || []) {
            memory.push(JSON.parse(JSON.stringify(item)));
          }
          return { ok: true };
        }
      }
    );

    const replaceResult = await store.replaceTriggerRows(
      'community-1',
      [
        { 'Название': 'First', 'Код события': 'incoming_message' },
        { 'Название': 'Second', 'Код события': 'wall_like' }
      ],
      '7'
    );

    assert.equal(replaceResult.stored, 2);
    assert.equal(memory.some(item => item.triggerId === META_TRIGGER_ID), true);

    const listed = await store.listTriggerRows('community-1', '7');
    assert.equal(listed.initialized, true);
    assert.deepEqual(listed.rows, [
      { 'Название': 'First', 'Код события': 'incoming_message' },
      { 'Название': 'Second', 'Код события': 'wall_like' }
    ]);

    listed.rows[0]['Название'] = 'Mutated';
    const listedAgain = await store.listTriggerRows('community-1', '7');
    assert.equal(listedAgain.rows[0]['Название'], 'First');
  });

  await run('structured trigger store keeps initialized empty scope after clearing rows', async () => {
    const memory = [
      {
        triggerScope: '7:community-1',
        triggerId: META_TRIGGER_ID,
        rowIndex: -1,
        meta: { initialized: true, rowCount: 1 }
      },
      {
        triggerScope: '7:community-1',
        triggerId: '000001',
        rowIndex: 0,
        row: { 'Название': 'Old row' }
      }
    ];
    const store = createStructuredTriggerStore(
      {
        mode: 'cloud',
        ydbDocApiEndpoint: 'https://docapi',
        ydbStructuredTriggersTable: 'structured_trigger_entries',
        awsAccessKeyId: 'key',
        awsSecretAccessKey: 'secret',
        ymqRegion: 'ru-central1'
      },
      {
        queryItems: async ({ triggerScope }) => ({
          Items: memory.filter(item => item.triggerScope === triggerScope)
        }),
        batchWriteItems: async operations => {
          for (const key of operations.deleteKeys || []) {
            const index = memory.findIndex(item => item.triggerScope === key.triggerScope && item.triggerId === key.triggerId);
            if (index !== -1) {
              memory.splice(index, 1);
            }
          }
          for (const item of operations.putItems || []) {
            memory.push(JSON.parse(JSON.stringify(item)));
          }
          return { ok: true };
        }
      }
    );

    await store.replaceTriggerRows('community-1', [], '7');
    const listed = await store.listTriggerRows('community-1', '7');
    assert.deepEqual(listed, {
      initialized: true,
      rows: []
    });
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
