const test = require('node:test');
const assert = require('node:assert/strict');

const { adminPanelHTML } = require('../adminPanelHTML');

test('admin panel uses multi-step Render retry timings for large attachment uploads', () => {
    assert.match(adminPanelHTML, /var useRenderService = fileSizeMB > 3\.5;/g);
    assert.equal((adminPanelHTML.match(/var useRenderService = fileSizeMB > 3\.5;/g) || []).length, 2);
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
    assert.match(adminPanelHTML, /action: 'recover_render_upload'/);
    assert.doesNotMatch(adminPanelHTML, /\/upload-result\?upload_id=/);
    assert.match(adminPanelHTML, /recoverRenderResult\(60000\)/);
});

test('admin panel throttles automatic session captcha refreshes', () => {
    assert.match(adminPanelHTML, /window\.sessionCaptchaLastRefreshAt = 0;/);
    assert.match(adminPanelHTML, /refreshSessionCaptcha\(true\)/);
    assert.match(adminPanelHTML, /Date\.now\(\) - Number\(window\.sessionCaptchaLastRefreshAt \|\| 0\) < 10000/);
});

test('admin panel keeps upload community context available after render response', () => {
    assert.match(adminPanelHTML, /var communityId = window\.currentCommunityId \|\| '';/);
    assert.match(adminPanelHTML, /communityId: communityId,\s*groupId: groupId/s);
    assert.doesNotMatch(adminPanelHTML, /const communityId = window\.currentCommunityId;/);
});

test('admin panel fully reloads after successful session captcha', () => {
    assert.match(adminPanelHTML, /function reloadAdminPanelAfterSessionCaptcha\(\)/);
    assert.match(adminPanelHTML, /window\.location\.reload\(\)/);
    assert.match(adminPanelHTML, /reloadAdminPanelAfterSessionCaptcha\(\);/);
    assert.doesNotMatch(adminPanelHTML, /reloadActiveTabAfterSessionCaptcha\(\);/);
});

test('admin panel shows user token upload notice with direct settings navigation', () => {
    assert.match(adminPanelHTML, /id="userTokenSettingsBlock"/);
    assert.match(adminPanelHTML, /function navigateToUserTokenSettings\(\)/);
    assert.match(adminPanelHTML, /function showUserTokenRequiredNotice\(statusEl\)/);
    assert.match(adminPanelHTML, /onclick="navigateToUserTokenSettings\(\); return false;"/);
    assert.doesNotMatch(adminPanelHTML, /throw new Error\('User Token не настроен\. Проверьте НАСТРОЙКА сообщества\.'\)/);
});

test('admin panel blocks bot and step creation until a community exists', () => {
    assert.match(adminPanelHTML, /function ensureCommunityReadyForBotEditing\(tab\)/);
    assert.match(adminPanelHTML, /navigateToCommunitySettings\(\); return false;/);
    assert.match(adminPanelHTML, /if \(!ensureCommunityReadyForBotEditing\(tab\)\) return;/);
});

test('admin panel accepts the currently selected configured community before adding bot steps', () => {
    assert.match(adminPanelHTML, /function communityConfigMatchesId\(config, communityId\)/);
    assert.match(adminPanelHTML, /communities\[communityId\]/);
    assert.match(adminPanelHTML, /communityConfigMatchesId\(communities\[id\], communityId\)/);
    assert.match(adminPanelHTML, /querySelector\('#communityButtons \.community-btn\.active\[data-community-id\]'\)/);
    assert.match(adminPanelHTML, /activeCommunityLabel-Messages/);
});

test('admin panel action notices are shown in the visible viewport', () => {
    assert.match(adminPanelHTML, /id = 'panelActionToast'/);
    assert.match(adminPanelHTML, /position:fixed/);
    assert.match(adminPanelHTML, /top:18px/);
    assert.match(adminPanelHTML, /z-index:10050/);
});

test('attachment modal has explicit upload button and profile files picker', () => {
    assert.match(adminPanelHTML, /attachUploadSelectedFile/);
    assert.match(adminPanelHTML, /textContent = 'Загрузить'/);
    assert.match(adminPanelHTML, /renderProfileFileAttachmentPicker/);
    assert.match(adminPanelHTML, /insertProfileFileAttachment/);
    assert.match(adminPanelHTML, /Добавить файл из ПРОФИЛЯ/);
});

test('attachment modal orders profile document, profile file, then local upload block', () => {
    const consentIndex = adminPanelHTML.indexOf('var consentDocumentPicker = renderConsentDocumentAttachmentPicker(tab, idx, col);');
    const profileFileIndex = adminPanelHTML.indexOf('var profileFilePicker = renderProfileFileAttachmentPicker(tab, idx, col);');
    const localFileIndex = adminPanelHTML.indexOf('modal.appendChild(fileDiv);');
    assert.ok(consentIndex > 0, 'consent document picker should be rendered');
    assert.ok(profileFileIndex > 0, 'profile file picker should be rendered');
    assert.ok(localFileIndex > 0, 'local upload block should be rendered');
    assert.ok(consentIndex < profileFileIndex, 'documents block should be above profile files block');
    assert.ok(profileFileIndex < localFileIndex, 'profile files block should be above local file upload block');
    assert.match(adminPanelHTML, /fileDiv\.style\.cssText = 'margin:0 0 15px 0; padding:12px; border:1px solid var\(--section-border\); border-radius:8px; background:var\(--surface-muted\);'/);
});

test('admin panel uses larger visual scrollbars with hover affordance', () => {
    assert.match(adminPanelHTML, /scrollbar-width: auto/);
    assert.match(adminPanelHTML, /::-webkit-scrollbar \{ width: 18px; height: 18px; \}/);
    assert.match(adminPanelHTML, /::-webkit-scrollbar-thumb:hover/);
    assert.match(adminPanelHTML, /border: 4px solid transparent/);
});

test('admin panel adds readable help modal for every main tab header', () => {
    assert.match(adminPanelHTML, /const ADMIN_TAB_HELP = \{/);
    assert.match(adminPanelHTML, /function installTabHelpButtons\(\)/);
    assert.match(adminPanelHTML, /window\.openTabHelp = function\(tabId\)/);
    assert.match(adminPanelHTML, /className = 'tab-help-btn'/);
    assert.match(adminPanelHTML, /textContent = 'СПРАВКА'/);
    assert.match(adminPanelHTML, /\.tab-help-overlay/);
    assert.match(adminPanelHTML, /\.tab-panel-kicker-row/);
    [
        'Messages',
        'Comments',
        'Users',
        'Groups',
        'Variables',
        'Mailing',
        'Delayed',
        'Triggers',
        'Profile',
        'Settings',
        'Admin'
    ].forEach(tab => {
        assert.match(adminPanelHTML, new RegExp(tab + ': \\{'));
    });
});

test('admin panel help documents columns, controls, JSON copy/import and consent setup', () => {
    assert.match(adminPanelHTML, /const ADMIN_TAB_HELP_DETAILS = \{/);
    assert.match(adminPanelHTML, /function renderTabHelpColumns\(tabId\)/);
    assert.match(adminPanelHTML, /renderTabHelpColumns\(tabId\)/);
    assert.match(adminPanelHTML, /Колонки и поля/);
    assert.match(adminPanelHTML, /Триггер: КНОПКА/);
    assert.match(adminPanelHTML, /Триггер: ФАЙЛ/);
    assert.match(adminPanelHTML, /Дублирует строку в таблице и одновременно копирует JSON-код этого шага в буфер обмена/);
    assert.match(adminPanelHTML, /Импорт JSON/);
    assert.match(adminPanelHTML, /Бот "Согласия"/);
    assert.match(adminPanelHTML, /Автонастройка сервера ВК/);
    assert.match(adminPanelHTML, /Развернутые примеры настройки/);
});
