const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const settings = require('../src/modules/attachment-upload-settings');

function memoryStore(initial) {
  let value = initial;
  return {
    async loadJsonObject() { return { value }; },
    async saveJsonObject(_key, next) { value = next; },
    get value() { return value; }
  };
}

test('initial global attachment sources match the configured service policy', async () => {
  const store = memoryStore(undefined);
  const result = await settings.getAttachmentUploadSettings('profile-1', { hotStateStore: store });
  assert.deepEqual(result.effective, { image: 'community', document: 'user', video: 'community' });
  assert.equal(result.userVideoPrivacy.effective, 'all');
});

test('storage failure falls back to the global attachment policy instead of breaking profile loading', async () => {
  const store = { async loadJsonObject() { throw new Error('temporary storage failure'); } };
  const result = await settings.getAttachmentUploadSettings('profile-1', { hotStateStore: store });
  assert.deepEqual(result.effective, { image: 'community', document: 'user', video: 'community' });
});

test('global change clears profile overrides and becomes effective for every profile', async () => {
  const store = memoryStore({ global: { image: 'community', document: 'user', video: 'community' }, profileOverrides: { one: { image: 'user' } }, globalUserVideoPrivacy: 'nobody', profileUserVideoPrivacyOverrides: { one: 'friends' } });
  await settings.saveGlobalAttachmentUploadSettings({ image: 'user', document: 'community', video: 'user', userVideoPrivacy: 'all' }, {}, { hotStateStore: store });
  const result = await settings.getAttachmentUploadSettings('one', { hotStateStore: store });
  assert.deepEqual(result.overrides, {});
  assert.deepEqual(result.effective, { image: 'user', document: 'community', video: 'user' });
  assert.equal(result.userVideoPrivacy.effective, 'all');
});

test('individual profile override wins until a later global reset', async () => {
  const store = memoryStore({ global: { image: 'community', document: 'user', video: 'community' }, profileOverrides: {} });
  await settings.saveProfileAttachmentUploadOverrides('one', { image: 'user', userVideoPrivacy: 'friends' }, {}, { hotStateStore: store });
  const result = await settings.getAttachmentUploadSettings('one', { hotStateStore: store });
  assert.equal(result.effective.image, 'user');
  assert.equal(result.effective.document, 'user');
  assert.equal(result.userVideoPrivacy.effective, 'friends');
});

test('attachment upload settings save uses an authenticated session and temporary notices', () => {
  const handler = fs.readFileSync(path.join(__dirname, '..', 'src', 'handler.js'), 'utf8');
  const panel = fs.readFileSync(path.join(__dirname, '..', 'adminPanelHTML.js'), 'utf8');
  const routeIndex = handler.indexOf('q.saveProfileAttachmentUploadSettings !== undefined');
  const sessionIndex = handler.indexOf('const needsAdminSession');
  const saveIndex = panel.indexOf('window.saveProfileAttachmentUploadSettings = async function()');
  const saveBlock = panel.slice(saveIndex, panel.indexOf('window.selectProfileDailyLimitPackage', saveIndex));

  assert.ok(routeIndex > sessionIndex, 'save route must be covered by the session guard');
  assert.match(saveBlock, /fetchAdminJson\(baseUrl \+ '\?saveProfileAttachmentUploadSettings=1'/);
  assert.match(saveBlock, /setInlineNoticeWithTimeout\(statusEl, 'error',[\s\S]*5000\)/);
});
