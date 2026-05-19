const test = require('node:test');
const assert = require('node:assert/strict');

const { adminPanelHTML } = require('../adminPanelHTML');

test('groups tab action buttons share the same compact action class', () => {
  assert.match(adminPanelHTML, /\.group-action-btn/);
  assert.match(adminPanelHTML, /openGroupForm\(/);
  assert.match(adminPanelHTML, /manageGroupMembers\([^,]+, true\)/);
  assert.match(adminPanelHTML, /manageGroupMembers\([^,]+, false\)/);
  assert.match(adminPanelHTML, /deleteGroupByIndex\(/);

  const groupCardActionLine = adminPanelHTML.split('\n').find((line) => line.includes('manageGroupMembers') && line.includes('deleteGroupByIndex')) || '';
  assert.match(groupCardActionLine, /btn-info group-action-btn/);
  assert.match(groupCardActionLine, /btn-save group-action-btn/);
  assert.match(groupCardActionLine, /btn-neutral group-action-btn/);
  assert.match(groupCardActionLine, /btn-delete group-action-btn/);
});

test('groups tab exposes Mini App visual settings', () => {
  assert.match(adminPanelHTML, /MiniApp включен/);
  assert.match(adminPanelHTML, /MiniApp скрыть из списка/);
  assert.match(adminPanelHTML, /MiniApp slug/);
  assert.match(adminPanelHTML, /MiniApp заголовок/);
  assert.match(adminPanelHTML, /MiniApp описание/);
  assert.match(adminPanelHTML, /MiniApp иконка URL/);
  assert.match(adminPanelHTML, /MiniApp баннер URL/);
  assert.match(adminPanelHTML, /MiniApp текст подписки/);
  assert.match(adminPanelHTML, /MiniApp текст отписки/);
});

test('groups tab renders generated Mini App links', () => {
  assert.match(adminPanelHTML, /buildMiniAppGroupLink/);
  assert.match(adminPanelHTML, /VK_MINIAPP_APP_URL/);
  assert.match(adminPanelHTML, /#c=/);
  assert.match(adminPanelHTML, /&g=/);
});

test('groups tab includes image upload controls for Mini App assets', () => {
  assert.match(adminPanelHTML, /uploadMiniAppGroupImage/);
  assert.match(adminPanelHTML, /miniappUploadAsset/);
});
