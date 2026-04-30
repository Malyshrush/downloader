const test = require('node:test');
const assert = require('node:assert/strict');

const { adminPanelHTML } = require('../adminPanelHTML');

test('admin panel uses multi-step Render retry timings for large attachment uploads', () => {
    assert.match(adminPanelHTML, /var RENDER_INITIAL_UPLOAD_TIMEOUT_MS = 20000;/);
    assert.match(adminPanelHTML, /var RENDER_RETRY_UPLOAD_TIMEOUT_MS = 120000;/);
    assert.match(adminPanelHTML, /var RENDER_FINAL_RETRY_DELAY_MS = 10000;/);
    assert.match(adminPanelHTML, /message\.includes\('timed out'\)/);
});

test('admin panel no longer hardcodes wake-up messaging for Render uploads', () => {
    assert.doesNotMatch(adminPanelHTML, /Пытаемся разбудить сервис Render/);
    assert.doesNotMatch(adminPanelHTML, /Не удалось разбудить Render/);
});

test('admin panel surfaces a distinct final upload failure after retries', () => {
    assert.match(adminPanelHTML, /Render всё ещё обрабатывает файл, восстанавливаем результат или делаем последнюю попытку/);
    assert.match(adminPanelHTML, /Render загрузил файл, но браузер не получил ответ:/);
});

test('admin panel sends render upload_id and can recover completed uploads', () => {
    assert.match(adminPanelHTML, /formData\.append\('upload_id', uploadId\)/);
    assert.match(adminPanelHTML, /\/upload-result\?upload_id=/);
    assert.match(adminPanelHTML, /recoverRenderResult\(60000\)/);
});
