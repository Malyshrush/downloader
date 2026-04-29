/**
 * Модуль создания клавиатур VK
 */

const { log } = require('../utils/logger');

/**
 * Создать клавиатуру из строки конфигурации
 */
function createKeyboard(row, prefix = 'Кнопка Ответа', linkPrefix = 'Цвет/Ссылка Ответа', maxButtons = 10) {
    try {
        // ✅ Новый формат: _keyboard поле (JSON)
        if (row._keyboard) {
            try {
                const kb = typeof row._keyboard === 'string' ? JSON.parse(row._keyboard) : row._keyboard;
                log('debug', '🎹 _keyboard loaded:', JSON.stringify(kb));
                
                // Sanitize для VK API:
                // Для inline open_link требует label (макс 40 символов)!
                // Для non-inline open_link не поддерживается — конвертируем в text
                if (kb.buttons) {
                    for (const btnRow of kb.buttons) {
                        if (!Array.isArray(btnRow)) continue;
                        for (const btn of btnRow) {
                            if (btn.action && btn.action.type === 'open_link') {
                                if (!kb.inline) {
                                    // Для non-inline конвертируем open_link в text
                                    btn.action.type = 'text';
                                    btn.action.label = (btn.action.link || 'Ссылка').substring(0, 40);
                                    btn.color = btn.color || 'primary';
                                    delete btn.action.link;
                                    if (!btn.action.payload) btn.action.payload = {};
                                } else {
                                    // Для inline open_link: label ОБЯЗАТЕЛЕН для VK API, макс 40 символов
                                    if (!btn.action.label) {
                                        // Берём короткую версию ссылки или "Открыть"
                                        try {
                                            const urlObj = new URL(btn.action.link);
                                            btn.action.label = (urlObj.hostname || 'Открыть').substring(0, 40);
                                        } catch(e) {
                                            btn.action.label = 'Открыть';
                                        }
                                    } else {
                                        btn.action.label = btn.action.label.substring(0, 40);
                                    }
                                    // Удаляем payload и color из open_link (не поддерживаются)
                                    delete btn.action.payload;
                                    delete btn.action.color;
                                }
                            } else if (btn.action && btn.action.type === 'text') {
                                let payloadObj = {};
                                if (typeof btn.action.payload === 'string') {
                                    try {
                                        payloadObj = JSON.parse(btn.action.payload);
                                    } catch(e) {
                                        payloadObj = {};
                                    }
                                } else if (btn.action.payload && typeof btn.action.payload === 'object') {
                                    payloadObj = { ...btn.action.payload };
                                }
                                payloadObj.buttonLabel = btn.action.label || payloadObj.buttonLabel || '';
                                payloadObj.source = payloadObj.source || 'keyboard';
                                btn.action.payload = JSON.stringify(payloadObj);
                            }
                        }
                    }
                }
                return kb;
            } catch(e) {
                log('debug', '⚠️ _keyboard parse error, fallback to old format:', e.message);
            }
        }
        
        log('debug', '⚠️ No _keyboard found, using old format with prefix: ' + prefix);

        // ✅ Старый формат: Кнопка-N, Цвет/Ссылка-N
        const buttons = [];

        for (let i = 1; i <= maxButtons; i++) {
            const btnText = (row[`${prefix}-${i}`] || '').trim();
            if (!btnText) continue;

            const btnLinkOrColor = (row[`${linkPrefix}-${i}`] || '');
            const parts = (btnLinkOrColor || '').split('||');
            let colorValue = parts[0] || '';
            const linkValue = parts[1] || '';

            // Очистка цвета
            colorValue = colorValue.replace(/[\r\n\t]/g, '').replace(/\s+/g, ' ').trim();
            const normalizedColor = colorValue.toLowerCase().replace(/ё/g, 'е').trim();

            let button = null;

            // Проверка: ссылка или цвет
            if (normalizedColor === 'ссылка...' || linkValue.trim().startsWith('http')) {
                // Кнопка-ссылка: НЕ содержит label в action!
                button = {
                    action: {
                        type: 'open_link',
                        link: linkValue.trim() || colorValue.trim()
                    }
                };
            } else {
                const color = mapColorToVK(normalizedColor);
                button = {
                    action: {
                        type: 'text',
                        label: btnText,
                        payload: JSON.stringify({ button: i, buttonLabel: btnText, source: 'keyboard' })
                    },
                    color: color
                };
            }

            if (button) {
                buttons.push([button]);
            }
        }

        if (buttons.length === 0) return null;

        return {
            one_time: false,
            inline: true,
            buttons: buttons
        };
    } catch (error) {
        log('error', 'Error creating keyboard:', error);
        return null;
    }
}

/**
 * Сопоставить цвет с VK API
 */
function mapColorToVK(normalizedColor) {
    if (/^красн/.test(normalizedColor) || normalizedColor === 'negative' || normalizedColor === 'red') {
        return 'negative';
    }
    if (/^зелен/.test(normalizedColor) || /^зелён/.test(normalizedColor) || 
        normalizedColor === 'positive' || normalizedColor === 'green') {
        return 'positive';
    }
    if (/^син/.test(normalizedColor) || normalizedColor === 'primary' || normalizedColor === 'blue') {
        return 'primary';
    }
    // По умолчанию secondary (белый/серый)
    return 'secondary';
}

/**
 * Создать клавиатуру для рассылки
 */
function createMailingKeyboard(row) {
    try {
        // ✅ Новый формат: _keyboard поле (JSON)
        if (row._keyboard) {
            try {
                const kb = typeof row._keyboard === 'string' ? JSON.parse(row._keyboard) : row._keyboard;
                // Sanitize для VK API:
                if (kb.buttons) {
                    for (const btnRow of kb.buttons) {
                        if (!Array.isArray(btnRow)) continue;
                        for (const btn of btnRow) {
                            if (btn.action && btn.action.type === 'open_link') {
                                if (!kb.inline) {
                                    // Для non-inline конвертируем open_link в text
                                    btn.action.type = 'text';
                                    btn.action.label = btn.action.link || 'Ссылка';
                                    btn.color = btn.color || 'primary';
                                    delete btn.action.link;
                                    if (!btn.action.payload) btn.action.payload = {};
                                }
                                // Для inline open_link: label ОБЯЗАТЕЛЕН для VK API
                                if (!btn.action.label) {
                                    btn.action.label = btn.action.link || 'Открыть';
                                }
                                // Удаляем payload и color из open_link
                                delete btn.action.payload;
                                delete btn.action.color;
                            } else if (btn.action && btn.action.type === 'text') {
                                let payloadObj = {};
                                if (typeof btn.action.payload === 'string') {
                                    try {
                                        payloadObj = JSON.parse(btn.action.payload);
                                    } catch(e) {
                                        payloadObj = {};
                                    }
                                } else if (btn.action.payload && typeof btn.action.payload === 'object') {
                                    payloadObj = { ...btn.action.payload };
                                }
                                payloadObj.buttonLabel = btn.action.label || payloadObj.buttonLabel || '';
                                payloadObj.source = payloadObj.source || 'keyboard';
                                btn.action.payload = JSON.stringify(payloadObj);
                            }
                        }
                    }
                }
                return kb;
            } catch(e) {
                log('debug', '⚠️ _keyboard parse error, fallback to old format:', e.message);
            }
        }

        // ✅ Старый формат: Кнопка-N, Цвет/Ссылка-N
        const buttons = [];

        for (let i = 1; i <= 10; i++) {
            const btnText = (row['Кнопка-' + i] || '').trim();
            if (!btnText) continue;

            const colorOrLink = (row['Цвет/Ссылка-' + i] || '');
            const parts = (colorOrLink || '').split('||');
            let colorValue = parts[0] || '';
            const linkValue = parts[1] || '';

            colorValue = colorValue.replace(/[\r\n\t]/g, '').replace(/\s+/g, ' ').trim();
            const normalizedColor = colorValue.toLowerCase().replace(/ё/g, 'е').trim();

            let button = null;

            if (normalizedColor === 'ссылка...' || linkValue.trim().startsWith('http')) {
                button = {
                    action: {
                        type: 'open_link',
                        link: linkValue.trim() || colorValue.trim()
                    }
                };
            } else {
                const color = mapColorToVK(normalizedColor);
                button = {
                    action: { type: 'text', label: btnText, payload: JSON.stringify({ button: i, buttonLabel: btnText, source: 'keyboard' }) },
                    color: color
                };
            }

            if (button) buttons.push([button]);
        }

        if (buttons.length === 0) return null;

        return { one_time: false, inline: true, buttons: buttons };
    } catch (error) {
        log('error', 'Error creating mailing keyboard:', error);
        return null;
    }
}

/**
 * Генерация колонок для кнопок ответа (для админ-панели)
 */
function generateAnswerButtonColumns(startIdx, endIdx) {
    const cols = [];
    const colors = ['th-blue-1','th-blue-2','th-blue-3','th-purple-1','th-purple-2','th-teal-1','th-teal-2','th-red-1','th-red-2','th-cyan'];

    for (let i = startIdx; i <= endIdx; i++) {
        const colorIdx = (i - 1) % colors.length;
        cols.push({
            name: `Кнопка Ответа-${i}`,
            class: colors[colorIdx],
            hint: 'Текст кнопки ' + i,
            section: 'КНОПКИ В ОТВЕТЕ'
        });
        cols.push({
            name: `Цвет/Ссылка Ответа-${i}`,
            class: colors[colorIdx],
            type: 'select',
            options: ['красный','зелёный','синий','белый','ССЫЛКА...'],
            hint: 'Цвет или ссылка',
            section: 'КНОПКИ В ОТВЕТЕ'
        });
    }
    return cols;
}

/**
 * Генерация колонок для кнопок ЗО (для админ-панели)
 */
function generateFallbackButtonColumns(startIdx, endIdx) {
    const cols = [];
    const colors = ['th-indigo','th-purple-1','th-purple-2','th-teal-1','th-teal-2','th-red-1','th-red-2','th-cyan','th-blue-1','th-blue-2'];

    for (let i = startIdx; i <= endIdx; i++) {
        const colorIdx = (i - 1) % colors.length;
        cols.push({
            name: `Кнопка ЗО-${i}`,
            class: colors[colorIdx],
            hint: 'Текст кнопки ЗО ' + i,
            section: 'КНОПКИ В ЗАГОТОВЛЕННОМ ОТВЕТЕ'
        });
        cols.push({
            name: `Цвет/Ссылка ЗО-${i}`,
            class: colors[colorIdx],
            type: 'select',
            options: ['красный','зелёный','синий','белый','ССЫЛКА...'],
            hint: 'Цвет или ссылка',
            section: 'КНОПКИ В ЗАГОТОВЛЕННОМ ОТВЕТЕ'
        });
    }
    return cols;
}

/**
 * Получить колонку удаления (для админ-панели)
 */
function getDeleteColumn() {
    return { name: 'Удалить строку', class: 'th-red-1', hint: 'Удалить строку', section: 'УДАЛЕНИЕ' };
}

module.exports = {
    createKeyboard,
    createMailingKeyboard,
    generateAnswerButtonColumns,
    generateFallbackButtonColumns,
    getDeleteColumn
};
