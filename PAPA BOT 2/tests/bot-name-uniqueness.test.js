const assert = require('node:assert/strict');

const { __testOnly } = require('../src/handler');

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
  await run('handleSaveSheet rejects duplicate bot names across messages and comments sheets', async () => {
    const response = await __testOnly.handleSaveSheetWithDependencies(
      {
        queryStringParameters: {
          save: 'СООБЩЕНИЯ',
          communityId: 'community-1',
          profileId: '7'
        },
        body: JSON.stringify([
          { 'Бот': 'bot-1', 'Шаг': 'step-a', 'Ответ': 'hello' }
        ])
      },
      {
        getRequestProfileId: () => '7',
        getActiveCommunityId: () => 'community-1',
        isProfileScopedSheet: () => false,
        loadBotConfig: async () => {},
        getFullConfig: () => ({ communities: {} }),
        invalidateCache: () => {},
        getSheetData: async sheetName => {
          if (sheetName === 'КОММЕНТАРИИ В ПОСТАХ') {
            return [{ 'Бот': 'bot-1', 'Шаг': 'comment-step', 'Ответ': 'reply' }];
          }
          return [];
        },
        saveSheetData: async () => {
          throw new Error('saveSheetData should not be called');
        },
        log: () => {}
      }
    );

    assert.equal(response.statusCode, 400);
    const payload = JSON.parse(response.body);
    assert.equal(payload.success, false);
    assert.match(payload.error, /bot-1/i);
  });
})().then(() => {
  process.exit(0);
}).catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
