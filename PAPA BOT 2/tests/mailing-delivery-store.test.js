const assert = require('node:assert/strict');

const {
  buildMailingDeliveryScope,
  createMailingDeliveryStore
} = require('../src/modules/mailing-delivery-store');

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
  await run('buildMailingDeliveryScope normalizes profile and community identifiers', async () => {
    assert.equal(buildMailingDeliveryScope('community-1', '7'), '7:community-1');
    assert.equal(buildMailingDeliveryScope('', ''), '1:global');
  });

  await run('mailing delivery store creates and updates mailing runtime state', async () => {
    const items = new Map();
    const store = createMailingDeliveryStore(
      {
        mode: 'cloud',
        ydbDocApiEndpoint: 'https://example.test/docapi',
        ydbMailingDeliveriesTable: 'mailing_delivery_entries',
        awsAccessKeyId: 'key',
        awsSecretAccessKey: 'secret',
        ymqRegion: 'ru-central1'
      },
      {
        putItem: async item => {
          items.set(item.mailingScope + '|' + item.mailingId, JSON.parse(JSON.stringify(item)));
        },
        getItem: async key => {
          const item = items.get(key.mailingScope + '|' + key.mailingId);
          return item ? JSON.parse(JSON.stringify(item)) : null;
        }
      }
    );

    const result = await store.updateMailingState('community-1', '5', draft => {
      draft['Статус'] = 'В обработке';
      draft['Ошибка'] = '';
      return { value: draft };
    }, '7');

    assert.equal(result.changed, true);
    const state = await store.getMailingState('community-1', '5', '7');
    assert.equal(state['№'], '5');
    assert.equal(state['Статус'], 'В обработке');

    await store.updateMailingState('community-1', '5', draft => {
      draft['Статус'] = 'Отправлено';
      draft['Ошибка'] = '';
      draft['Факт. время отправки (по мск.)'] = '2026-04-23 12:00:00';
      return { value: draft };
    }, '7');

    const updatedState = await store.getMailingState('community-1', '5', '7');
    assert.equal(updatedState['Статус'], 'Отправлено');
    assert.equal(updatedState['Факт. время отправки (по мск.)'], '2026-04-23 12:00:00');
  });
  await run('mailing delivery store replaces and lists full mailing rows with initialized marker', async () => {
    const items = new Map();
    const store = createMailingDeliveryStore(
      {
        mode: 'cloud',
        ydbDocApiEndpoint: 'https://example.test/docapi',
        ydbMailingDeliveriesTable: 'mailing_delivery_entries',
        awsAccessKeyId: 'key',
        awsSecretAccessKey: 'secret',
        ymqRegion: 'ru-central1'
      },
      {
        putItem: async item => {
          items.set(item.mailingScope + '|' + item.mailingId, JSON.parse(JSON.stringify(item)));
        },
        getItem: async key => {
          const item = items.get(key.mailingScope + '|' + key.mailingId);
          return item ? JSON.parse(JSON.stringify(item)) : null;
        },
        deleteItem: async key => {
          items.delete(key.mailingScope + '|' + key.mailingId);
        },
        queryItems: async request => ({
          Items: Array.from(items.values())
            .filter(item => item.mailingScope === request.mailingScope)
            .map(item => JSON.parse(JSON.stringify(item)))
        })
      }
    );

    await store.replaceMailingRows('community-1', [
      { '№': '5', 'Сообщение Рассылки': 'Новость дня', 'Статус': 'Ожидает' },
      { '№': '8', 'Сообщение Рассылки': 'Вторая новость', 'Статус': 'Ожидает' }
    ], '7');

    const listed = await store.listRows('community-1', '7');
    assert.equal(listed.initialized, true);
    assert.equal(listed.rows.length, 2);
    assert.equal(listed.rows[0]['Сообщение Рассылки'], 'Новость дня');
    assert.equal(listed.rows[1]['Сообщение Рассылки'], 'Вторая новость');

    const row = await store.getMailingRow('community-1', '8', '7');
    assert.equal(row['Сообщение Рассылки'], 'Вторая новость');
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
