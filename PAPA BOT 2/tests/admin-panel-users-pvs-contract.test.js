const assert = require('node:assert/strict');

const { adminPanelHTML } = require('../adminPanelHTML');

assert.doesNotMatch(
  adminPanelHTML,
  /delete\s+clone\[['"]Переменная ПВС['"]\]/
);
assert.doesNotMatch(
  adminPanelHTML,
  /delete\s+clone\[['"]Значение ПВС['"]\]/
);
assert.match(adminPanelHTML, /'Users': 'ПОЛЬЗОВАТЕЛИ'/);
assert.match(adminPanelHTML, /'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ'/);
assert.match(adminPanelHTML, /'Shared_Variables': \[/);
assert.match(adminPanelHTML, /name: 'Значение ПВС'/);
assert.match(adminPanelHTML, /'Значение ПВС': row\['Значение ПВС'\] \|\| ''/);

process.stdout.write('PASS admin panel keeps user PVS columns on save\n');
