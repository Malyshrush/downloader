/**
 * Исправление админ-панели:
 * 1. При переключении сообщества - перезагрузка ВСЕХ вкладок
 * 2. Убрать "..." из вкладок
 * 3. Добавить название активного сообщества под заголовком каждой вкладки
 */

const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'adminPanelHTML.js');
let content = fs.readFileSync(filePath, 'utf8');

// =============================================
// 1. Убираем "..." из HTML вкладок
// =============================================
content = content.replace(
    /<table id="table-Variables"><tbody><\/tbody>\.\.\.<\/table>/g,
    '<table id="table-Variables"><tbody></tbody></table>'
);
content = content.replace(
    /<table id="table-Mailing"><tbody><\/tbody>\.\.\.<\/table>/g,
    '<table id="table-Mailing"><tbody></tbody></table>'
);
content = content.replace(
    /<table id="table-Delayed"><tbody><\/tbody>\.\.\.<\/table>/g,
    '<table id="table-Delayed"><tbody></tbody></table>'
);

// =============================================
// 2. Добавляем блок для отображения активного сообщества в каждую вкладку
// =============================================

// Messages
content = content.replace(
    /(<div id="Messages" class="tabcontent">\s*<!-- <h3>[^<]*<\/h3> -->\s*<div id="loading-Messages">[^<]*<\/div>)/g,
    '$1\n<div id="activeCommunityLabel-Messages" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>'
);

// Comments
content = content.replace(
    /(<div id="Comments" class="tabcontent">\s*<!-- <h3>[^<]*<\/h3> -->\s*<div id="loading-Comments">[^<]*<\/div>)/g,
    '$1\n<div id="activeCommunityLabel-Comments" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>'
);

// Users
content = content.replace(
    /(<div id="Users" class="tabcontent">\s*<!-- <h3>[^<]*<\/h3> -->\s*<div id="loading-Users">[^<]*<\/div>)/g,
    '$1\n<div id="activeCommunityLabel-Users" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>'
);

// Variables
content = content.replace(
    /(<div id="Variables" class="tabcontent">\s*<!-- <h3>[^<]*<\/h3> -->\s*<div id="loading-Variables">[^<]*<\/div>)/g,
    '$1\n<div id="activeCommunityLabel-Variables" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>'
);

// Mailing
content = content.replace(
    /(<div id="Mailing" class="tabcontent">\s*<!-- <h3>[^<]*<\/h3> -->\s*<div id="loading-Mailing">[^<]*<\/div>)/g,
    '$1\n<div id="activeCommunityLabel-Mailing" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>'
);

// Delayed
content = content.replace(
    /(<div id="Delayed" class="tabcontent">\s*<!-- <h3>[^<]*<\/h3> -->\s*<div id="loading-Delayed">[^<]*<\/div>)/g,
    '$1\n<div id="activeCommunityLabel-Delayed" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>'
);

// =============================================
// 3. Исправляем switchCommunity - теперь перезагружает ВСЕ вкладки
// =============================================

const oldSwitchCommunity = `window.switchCommunity = async function(communityId) {
    // \u2705 1. \u041E\u0431\u043D\u043E\u0432\u043B\u044F\u0435\u043C \u0442\u0435\u043A\u0443\u0449\u0438\u0439 ID
    window.currentCommunityId = communityId;
    debug('Switching to community: ' + communityId);

    // \u2705 2. \u041E\u0431\u043D\u043E\u0432\u043B\u044F\u0435\u043C \u0432\u0438\u0437\u0443\u0430\u043B\u044C\u043D\u043E\u0435 \u0441\u043E\u0441\u0442\u043E\u044F\u043D\u0438\u0435 \u043A\u043D\u043E\u043F\u043E\u043A
    const buttons = document.querySelectorAll('#communityButtons .btn');
    buttons.forEach(btn => {
        // \u0421\u0431\u0440\u0430\u0441\u044B\u0432\u0430\u0435\u043C \u0432\u0441\u0435 \u043A\u043D\u043E\u043F\u043A\u0438
        btn.style.background = '#e0e0e0';
        btn.style.color = '#333';
        btn.style.border = '1px solid #ccc';
        btn.textContent = btn.textContent.replace(' \\u2705', '');
    });

    // \u2705 3. \u041D\u0430\u0445\u043E\u0434\u0438\u043C \u0438 \u043F\u043E\u0434\u0441\u0432\u0435\u0447\u0438\u0432\u0430\u0435\u043C \u0430\u043A\u0442\u0438\u0432\u043D\u0443\u044E \u043A\u043D\u043E\u043F\u043A\u0443 \u043F\u043E data-community-id
    const activeBtn = Array.from(buttons).find(b => b.dataset.communityId === communityId);
    if (activeBtn) {
        activeBtn.style.background = '#4CAF50';
        activeBtn.style.color = 'white';
        activeBtn.style.border = '2px solid #2e7d32';
        activeBtn.textContent = activeBtn.textContent.replace(' \\u2705', '') + ' \\u2705';
    }

    // \\u2705 Step 4: Load community settings
    await loadCommunitySettings(communityId);

    // \\u2705 \\u2705 \\u041F\\u0415\\u0420\\u0415\\u0417\\u0410\\u0413\\u0420\\u0423\\u0417\\u041A\\u0410 \\u0414\\u0410\\u041D\\u041D\\u042B\\u0425 \\u0412\\u0421\\u0415\\u0425 \\u0412\\u041A\\u041B\\u0410\\u0414\\u041E\\u041A \\u0434\\u043B\\u044F \\u044D\\u0442\\u043E\\u0433\\u043E \\u0441\\u043E\\u043E\\u0431\\u0449\\u0435\\u0441\\u0442\\u0432\\u0430
    // \\u0418\\u043D\\u0432\\u0430\\u043B\\u0438\\u0434\\u0438\\u0440\\u0443\\u0435\\u043C \\u043A\\u044D\\u0448 \\u043D\\u0430 \\u0441\\u0435\\u0440\\u0432\\u0435\\u0440\\u0435 + \\u043F\\u0435\\u0440\\u0435\\u0437\\u0430\\u0433\\u0440\\u0443\\u0436\\u0430\\u0435\\u043C \\u043B\\u043E\\u043A\\u0430\\u043B\\u044C\\u043D\\u043E
    const activeTabEl = document.querySelector('.tablinks.active');
    if (activeTabEl) {
        const tabName = activeTabEl.getAttribute('onclick')?.match(/'(\\w+)'/)?.[1];
        if (tabName && sheetMap[tabName]) {
            // \\u2705 \\u041F\\u0440\\u0438\\u043D\\u0443\\u0434\\u0438\\u0442\\u0435\\u043B\\u044C\\u043D\\u043E \\u043F\\u0435\\u0440\\u0435\\u0437\\u0430\\u0433\\u0440\\u0443\\u0436\\u0430\\u0435\\u043C \\u0434\\u0430\\u043D\\u043D\\u044B\\u0435 \\u0430\\u043A\\u0442\\u0438\\u0432\\u043D\\u043E\\u0439 \\u0432\\u043A\\u043B\\u0430\\u0434\\u043A\\u0438
            await loadData(tabName);
            debug('?\\u2705 Reloaded data for tab: ' + tabName + ' after community switch');
        }
    }

    // \\u2705 6. \\u041E\\u0431\\u043D\\u043E\\u0432\\u043B\\u044F\\u0435\\u043C \\u0437\\u0430\\u0433\\u043E\\u043B\\u043E\\u0432\\u043E\\u043A/\\u0441\\u0442\\u0430\\u0442\\u0443\\u0441
    const settingsDebug = document.getElementById('settings-debug');
    if (settingsDebug) {
        settingsDebug.innerHTML = '<div style="background:#e8f5e9;padding:10px;border-radius:4px;">\\u2705 \\u0410\\u043A\\u0442\\u0438\\u0432\\u043D\\u043E: <strong>' + communityId + '</strong></div>';
    }
};`;

const newSwitchCommunity = `window.switchCommunity = async function(communityId) {
    // 1. Обновляем текущий ID
    window.currentCommunityId = communityId;
    debug('Switching to community: ' + communityId);

    // 2. Обновляем визуальное состояние кнопок
    const buttons = document.querySelectorAll('#communityButtons .btn');
    buttons.forEach(btn => {
        btn.style.background = '#e0e0e0';
        btn.style.color = '#333';
        btn.style.border = '1px solid #ccc';
        btn.textContent = btn.textContent.replace(' \\u2705', '');
    });

    // 3. Подсвечиваем активную кнопку
    const activeBtn = Array.from(buttons).find(b => b.dataset.communityId === communityId);
    if (activeBtn) {
        activeBtn.style.background = '#4CAF50';
        activeBtn.style.color = 'white';
        activeBtn.style.border = '2px solid #2e7d32';
        activeBtn.textContent = activeBtn.textContent.replace(' \\u2705', '') + ' \\u2705';
    }

    // 4. Загружаем настройки сообщества
    await loadCommunitySettings(communityId);

    // 5. \\u2705 \\u041F\\u0415\\u0420\\u0415\\u0417\\u0410\\u0413\\u0420\\u0423\\u0416\\u0410\\u0415\\u041C \\u0412\\u0421\\u0415 \\u0412\\u041A\\u041B\\u0410\\u0414\\u041A\\u0418 \\u0434\\u043B\\u044F \\u0432\\u044B\\u0431\\u0440\\u0430\\u043D\\u043D\\u043E\\u0433\\u043E \\u0441\\u043E\\u043E\\u0431\\u0449\\u0435\\u0441\\u0442\\u0432\\u0430!
    const allTabs = ['Messages', 'Comments', 'Users', 'Variables', 'Mailing', 'Delayed'];
    for (const tabName of allTabs) {
        // Очищаем кэш
        dataStore[tabName] = [];
        // Перезагружаем данные
        await loadData(tabName);
        debug('\\u2705 Reloaded tab: ' + tabName + ' for community: ' + communityId);
    }

    // 6. Обновляем метки активного сообщества на всех вкладках
    updateCommunityLabels(communityId);

    // 7. Обновляем статус
    const settingsDebug = document.getElementById('settings-debug');
    if (settingsDebug) {
        settingsDebug.innerHTML = '<div style="background:#e8f5e9;padding:10px;border-radius:4px;">\\u2705 \\u0410\\u043A\\u0442\\u0438\\u0432\\u043D\\u043E: <strong>' + communityId + '</strong></div>';
    }
};

// \\u2705 \\u041D\\u041E\\u0412\\u0410\\u042F \\u0424\\u0423\\u041D\\u041A\\u0426\\u0418\\u042F: \\u041E\\u0431\\u043D\\u043E\\u0432\\u043B\\u0435\\u043D\\u0438\\u0435 \\u043C\\u0435\\u0442\\u043E\\u043A \\u0430\\u043A\\u0442\\u0438\\u0432\\u043D\\u043E\\u0433\\u043E \\u0441\\u043E\\u043E\\u0431\\u0449\\u0435\\u0441\\u0442\\u0432\\u0430 \\u043D\\u0430 \\u0432\\u043A\\u043B\\u0430\\u0434\\u043A\\u0430\\u0445
window.updateCommunityLabels = async function(communityId) {
    // Получаем имя сообщества из настроек
    const baseUrl = window.location.href.split('?')[0];
    let communityName = communityId;
    try {
        const res = await fetch(baseUrl + '?getBotSettings');
        const data = await res.json();
        const config = data.communities?.[communityId] || {};
        communityName = config.group_name || communityId;
    } catch(e) {}

    const tabs = ['Messages', 'Comments', 'Users', 'Variables', 'Mailing', 'Delayed'];
    for (const tab of tabs) {
        const labelEl = document.getElementById('activeCommunityLabel-' + tab);
        if (labelEl) {
            labelEl.style.display = 'block';
            labelEl.innerHTML = '\\uD83D\\uDCCC \\u0410\\u043A\\u0442\\u0438\\u0432\\u043D\\u043E\\u0435 \\u0441\\u043E\\u043E\\u0431\\u0449\\u0435\\u0441\\u0442\\u0432\\u043E: <strong>' + communityName + '</strong> (ID: ' + communityId + ')';
        }
    }
};`;

content = content.replace(oldSwitchCommunity, newSwitchCommunity);

// =============================================
// 4. Добавляем вызов updateCommunityLabels в openTab
// =============================================

content = content.replace(
    /(if \(name === 'Settings'\) {\s*loadSettings\(\);)/g,
    `$1
    } else {
        // Показываем метку активного сообщества
        if (window.currentCommunityId) {
            updateCommunityLabels(window.currentCommunityId);
        }`
);

// =============================================
// 5. Вызываем updateCommunityLabels при onload
// =============================================

content = content.replace(
    /(await loadData\('Messages'\);)/g,
    `$1
        // Показываем метку активного сообщества
        if (window.currentCommunityId) {
            updateCommunityLabels(window.currentCommunityId);
        }`
);

// Сохраняем
fs.writeFileSync(filePath, content, 'utf8');

console.log('✅ Admin panel updated!');
console.log('   - Убраны "..." из вкладок');
console.log('   - При переключении сообщества - ВСЕ вкладки перезагружаются');
console.log('   - Добавлены метки активного сообщества на каждой вкладке');
