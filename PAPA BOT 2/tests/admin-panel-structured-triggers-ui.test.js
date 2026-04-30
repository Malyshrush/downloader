const test = require('node:test');
const assert = require('node:assert/strict');

const { adminPanelHTML } = require('../adminPanelHTML');

test('structured trigger builder exposes send bot answer action with bot and step selectors', () => {
    assert.match(adminPanelHTML, /value: 'send_bot_answer'/);
    assert.match(adminPanelHTML, /label: 'Отправить ответ с бота'/);
    assert.match(adminPanelHTML, /actionCode === 'add_to_bot' \|\| actionCode === 'send_bot_answer' \|\| actionCode === 'remove_from_bot'/);
    assert.match(adminPanelHTML, /actionCode === 'add_to_bot' \|\| actionCode === 'send_bot_answer'/);
    assert.match(adminPanelHTML, /!shouldStructuredTriggerActionShowStep\(nextAction\.action\)/);
});
