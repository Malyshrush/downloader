const test = require('node:test');
const assert = require('node:assert/strict');

const { adminPanelHTML } = require('../adminPanelHTML');

test('profile dashboard includes a community files block with filter and columns', () => {
  assert.match(adminPanelHTML, /profileFilesFilter/);
  assert.match(adminPanelHTML, /profileFilesTableBody/);
  assert.match(adminPanelHTML, /<h3 class="profile-manager-title">Файлы<\/h3>/);
  assert.match(adminPanelHTML, /Название/);
  assert.match(adminPanelHTML, /Тип/);
  assert.match(adminPanelHTML, /Размер/);
  assert.match(adminPanelHTML, /Аттачмент/);
});
