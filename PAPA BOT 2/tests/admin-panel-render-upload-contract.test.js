const test = require('node:test');
const assert = require('node:assert/strict');

const { adminPanelHTML } = require('../adminPanelHTML');

test('admin panel uses 60 second Render wake window for large attachment uploads', () => {
    assert.match(adminPanelHTML, /var RENDER_INITIAL_UPLOAD_TIMEOUT_MS = 20000;/);
    assert.match(adminPanelHTML, /var RENDER_WAKE_TIMEOUT_MS = 60000;/);
    assert.match(adminPanelHTML, /var RENDER_WAKE_POLL_INTERVAL_MS = 5000;/);
    assert.match(adminPanelHTML, /setTimeout\(tryWakeUp, Math\.min\(RENDER_WAKE_POLL_INTERVAL_MS, currentRemainingWakeMs\)\);/);
    assert.match(adminPanelHTML, /Не удалось разбудить Render в течение 60 секунд/);
});

test('admin panel no longer hardcodes three wake attempts for Render uploads', () => {
    assert.doesNotMatch(adminPanelHTML, /var maxWakeUpAttempts = 3;/);
    assert.doesNotMatch(adminPanelHTML, /Не удалось разбудить Render после '\s*\+\s*maxWakeUpAttempts/);
});
