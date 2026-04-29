/**
 * Скрипт исправления adminPanelHTML.js:
 * 1. Заменяет эмодзи на HTML-сущности (&#x...;)
 * 2. Исправляет CSS для корректного отображения вкладок
 * 3. Исправляет '? вместо нормальных символов
 */

const fs = require('fs');
const path = require('path');

const filePath = path.join(__dirname, '..', 'adminPanelHTML.js');
let content = fs.readFileSync(filePath, 'utf8');

// Карта замен эмодзи → HTML-сущности
const emojiMap = {
    '🤖': '&#x1F916;',
    '🛡️': '&#x1F6E1;&#xFE0F;',
    '🔑': '&#x1F511;',
    '🔒': '&#x1F512;',
    '🔓': '&#x1F513;',
    '🔗': '&#x1F517;',
    '💬': '&#x1F4AC;',
    '📝': '&#x1F4DD;',
    '👥': '&#x1F465;',
    '🧮': '&#x1F9EE;',
    '📨': '&#x1F4E8;',
    '⏳': '&#x23F3;',
    '⚙️': '&#x2699;&#xFE0F;',
    'ℹ️': '&#x2139;&#xFE0F;',
    '💾': '&#x1F4BE;',
    '✅': '&#x2705;',
    '❌': '&#x274C;',
    '⚠️': '&#x26A0;&#xFE0F;',
    '🔄': '&#x1F504;',
    '📋': '&#x1F4CB;',
    '🗑️': '&#x1F5D1;&#xFE0F;',
    '🛠️': '&#x1F6E0;&#xFE0F;',
    '📖': '&#x1F4D6;',
    '❓': '&#x2753;',
    '🏷️': '&#x1F3F7;&#xFE0F;',
    '📊': '&#x1F4CA;',
    '📌': '&#x1F4CC;',
    '➕': '&#x2795;',
    '🗝️': '&#x1F5DD;&#xFE0F;',
    '🆔': '&#x1F194;',
    '🔍': '&#x1F50D;',
    '💥': '&#x1F4A5;',
    '🚀': '&#x1F680;',
    '📦': '&#x1F4E6;',
    '🔨': '&#x1F528;',
    '⏰': '&#x23F0;',
    '📎': '&#x1F4CE;',
    '🎬': '&#x1F3AC;',
    '🖼️': '&#x1F5BC;&#xFE0F;',
    '📹': '&#x1F4F9;',
    '📄': '&#x1F4C4;',
    '📁': '&#x1F4C1;',
    '🔧': '&#x1F527;',
    '🗒️': '&#x1F5D2;&#xFE0F;',
    '📢': '&#x1F4E2;',
    '📧': '&#x1F4E7;',
    '🎉': '&#x1F389;',
    '💡': '&#x1F4A1;',
    '🔔': '&#x1F514;',
    '🎯': '&#x1F3AF;',
    '❤️': '&#x2764;&#xFE0F;',
    '👍': '&#x1F44D;',
    '👎': '&#x1F44E;',
    '✨': '&#x2728;',
    '🌟': '&#x1F31F;',
    '🔥': '&#x1F525;',
    '💯': '&#x1F4AF;',
    '☑️': '&#x2611;&#xFE0F;',
    '☝️': '&#x261D;&#xFE0F;',
};

// Заменяем эмодзи на HTML-сущности
for (const [emoji, entity] of Object.entries(emojiMap)) {
    const regex = new RegExp(emoji.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&'), 'g');
    content = content.replace(regex, entity);
}

// Исправляем CSS для вкладок - УБИРАЕМ !important который ломает display
content = content.replace(
    /\.tabcontent\s*{\s*display:\s*none\s*!important;\s*}/g,
    '.tabcontent { display: none; }'
);

content = content.replace(
    /\.tabcontent\[style\*="display:\s*block"\]\s*{\s*display:\s*block\s*!important;\s*}/g,
    '.tabcontent.active { display: block !important; }'
);

// Исправляем JS для вкладок - добавляем/убираем класс active
content = content.replace(
    /document\.querySelectorAll\('\.tabcontent'\)\.forEach\(el\s*=>\s*el\.style\.display\s*=\s*'none'\);/g,
    "document.querySelectorAll('.tabcontent').forEach(el => { el.style.display = 'none'; el.classList.remove('active'); });"
);

content = content.replace(
    /document\.getElementById\(name\)\.style\.display\s*=\s*'block';/g,
    "document.getElementById(name).style.display = 'block'; document.getElementById(name).classList.add('active');"
);

// Исправляем "? на нормальные символы (остаточные)
content = content.replace(/\?\?\? Название сообщества/g, '&#x1F3F7;&#xFE0F; Название сообщества');
content = content.replace(/\?\? Код подтверждения/g, '&#x1F511; Код подтверждения');
content = content.replace(/\?\? Зачем:/g, '&#x2753; Зачем:');
content = content.replace(/\?\? Как получить:/g, '&#x1F4D6; Как получить:');
content = content.replace(/\? Сохранение/g, '&#x1F4BE; Сохранение');
content = content.replace(/\? Ошибка/g, '&#x274C; Ошибка');
content = content.replace(/\? Настройки/g, '&#x2705; Настройки');
content = content.replace(/\? Токен/g, '&#x1F511; Токен');
content = content.replace(/\? Сервер УСПЕШНО/g, '&#x2705; Сервер УСПЕШНО');
content = content.replace(/\? Сообщество/g, '&#x1F4E6; Сообщество');
content = content.replace(/\? Новое сообщество/g, '&#x2728; Новое сообщество');
content = content.replace(/\? Активно:/g, '&#x2705; Активно:');
content = content.replace(/\? Reloaded/g, '&#x2705; Reloaded');
content = content.replace(/\? Page loaded/g, '&#x2705; Page loaded');
content = content.replace(/\?\? Page loaded/g, '&#x2705; Page loaded');
content = content.replace(/\?\? URL:/g, '&#x1F517; URL:');
content = content.replace(/\?\? Origin:/g, '&#x1F310; Origin:');
content = content.replace(/\? Auth required/g, '&#x1F6E1; Auth required');
content = content.replace(/\? Auth confirmed/g, '&#x2705; Auth confirmed');
content = content.replace(/\? Loading data/g, '&#x1F4E6; Loading data');
content = content.replace(/\? Error loading/g, '&#x274C; Error loading');

// Убираем дублирующееся поле "Переменные ВК" в DEFAULT_DATA (если есть в JS рендере)
// Это исправляется на стороне сервера в storage.js

// Сохраняем результат
fs.writeFileSync(filePath, content, 'utf8');

console.log('✅ adminPanelHTML.js исправлен!');
console.log('   - Эмодзи заменены на HTML-сущности');
console.log('   - CSS вкладок исправлен');
console.log('   - Остаточные "?" заменены на нормальные символы');
