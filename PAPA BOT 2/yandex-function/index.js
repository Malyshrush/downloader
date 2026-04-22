/**
 * VK Bot - Главная точка входа
 * 
 * Объединяет все модули и экспортирует обработчик для Yandex Cloud Functions.
 * 
 * НОВАЯ МОДУЛЬНАЯ СТРУКТУРА:
 * - src/modules/       - бизнес-логика (хранилище, VK API, пользователи и т.д.)
 * - src/utils/         - утилиты (логирование, валидация, ошибки)
 * - src/handler.js     - роутер и обработчик HTTP запросов
 * 
 * Для локального запуска: node src/local-server.js
 */

// Загрузка переменных окружения (ручная, без dotenv для Yandex Functions)
if (require('fs').existsSync('.env')) {
    require('dotenv').config();
}

const { log } = require('./src/utils/logger');
const { logValidationResult, logConfigSummary } = require('./src/utils/config-validator');
const { setupGlobalErrorHandlers } = require('./src/utils/error-handler');

// Настройка глобальных обработчиков ошибок
setupGlobalErrorHandlers();

// Валидация конфигурации при запуске
log('info', '🚀 VK Bot starting...');
logValidationResult();
logConfigSummary();

// Экспорт основного обработчика
const { handler, workerHandler } = require('./src/handler');

module.exports.handler = handler;
module.exports.workerHandler = workerHandler;

// Для локального запуска
if (require.main === module) {
    log('info', '⚠️  Для локального запуска используйте: node src/local-server.js');
    log('info', '⚠️  Этот файл предназначен для деплоя в Yandex Cloud Functions');
}
