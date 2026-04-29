const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'adminPanelHTML.js');
let content = fs.readFileSync(filePath, 'utf8');

// Добавляем функцию updateCommunityLabels перед loadCommunitySettings
const addFunction = `
// &#x2705; НОВАЯ ФУНКЦИЯ: Обновление меток активного сообщества на всех вкладках
window.updateCommunityLabels = async function(communityId) {
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
            labelEl.innerHTML = '&#x1F4CC; &#x1F4E6; &#x1F4E6;: <strong>' + communityName + '</strong> (ID: ' + communityId + ')';
        }
    }
};

`;

content = content.replace(
    /\nwindow\.loadCommunitySettings = async function\(communityId\) {/g,
    addFunction + '\nwindow.loadCommunitySettings = async function(communityId) {'
);

fs.writeFileSync(filePath, content, 'utf8');
console.log('✅ updateCommunityLabels добавлена!');
