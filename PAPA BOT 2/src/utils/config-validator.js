/**
 * Утилита для валидации конфигурации
 */

const { log } = require('./logger');

/**
 * Проверить все необходимые переменные окружения
 */
function validateEnv() {
    const required = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY'
    ];

    const optional = [
        'VK_TOKEN',
        'VK_GROUP_ID',
        'USER_TOKEN',
        'APP_URL',
        'ADMIN_USERNAME',
        'ADMIN_PASSWORD',
        'ADMIN_EMAIL',
        'BUCKET_NAME',
        'LOG_LEVEL',
        'PORT'
    ];

    const missing = [];
    const warnings = [];

    // Проверка обязательных
    for (const key of required) {
        if (!process.env[key]) {
            missing.push(key);
        }
    }

    // Проверка необязательных
    for (const key of optional) {
        if (!process.env[key]) {
            warnings.push(key);
        }
    }

    // Валидация значений
    if (process.env.LOG_LEVEL && !['debug', 'info', 'warn', 'error', 'none'].includes(process.env.LOG_LEVEL)) {
        warnings.push(`Недопустимый LOG_LEVEL: ${process.env.LOG_LEVEL} (допустимы: debug, info, warn, error, none)`);
    }

    if (process.env.PORT && isNaN(parseInt(process.env.PORT))) {
        warnings.push(`Недопустимый PORT: ${process.env.PORT}`);
    }

    if (process.env.VK_GROUP_ID && isNaN(parseInt(process.env.VK_GROUP_ID))) {
        warnings.push(`VK_GROUP_ID должен быть числом`);
    }

    return {
        valid: missing.length === 0,
        missing,
        warnings
    };
}

/**
 * Логировать результат валидации
 */
function logValidationResult() {
    const result = validateEnv();

    if (result.valid) {
        log('info', '✅ Конфигурация валидна');
    } else {
        log('error', '❌ Ошибки конфигурации:');
        for (const key of result.missing) {
            log('error', `   • Отсутствует: ${key}`);
        }
    }

    if (result.warnings.length > 0) {
        log('warn', '⚠️ Предупреждения конфигурации:');
        for (const warning of result.warnings) {
            log('warn', `   • ${warning}`);
        }
    }

    return result;
}

/**
 * Маскировать секретные значения для логирования
 */
function maskSecret(secret, visibleChars = 4) {
    if (!secret || secret.length <= visibleChars) {
        return '***';
    }
    return secret.substring(0, visibleChars) + '...' + '*'.repeat(Math.min(8, secret.length - visibleChars));
}

/**
 * Логировать конфигурацию (без секретов)
 */
function logConfigSummary() {
    log('info', '📋 Конфигурация:');
    log('info', `   • BUCKET_NAME: ${process.env.BUCKET_NAME || 'bot-data-storage'}`);
    log('info', `   • VK_TOKEN: ${process.env.VK_TOKEN ? maskSecret(process.env.VK_TOKEN) : 'не задан'}`);
    log('info', `   • VK_GROUP_ID: ${process.env.VK_GROUP_ID || 'не задан'}`);
    log('info', `   • USER_TOKEN: ${process.env.USER_TOKEN ? maskSecret(process.env.USER_TOKEN) : 'не задан'}`);
    log('info', `   • APP_URL: ${process.env.APP_URL || 'не задан'}`);
    log('info', `   • LOG_LEVEL: ${process.env.LOG_LEVEL || 'debug'}`);
    log('info', `   • PORT: ${process.env.PORT || 3000}`);
}

module.exports = {
    validateEnv,
    logValidationResult,
    maskSecret,
    logConfigSummary
};
