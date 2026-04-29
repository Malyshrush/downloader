/**
 * Модуль логирования с уровнями и форматированием
 */

const LOG_LEVEL = process.env.LOG_LEVEL || 'debug';

const LEVELS = ['debug', 'info', 'warn', 'error', 'none'];
const CURRENT_LEVEL_INDEX = LEVELS.indexOf(LOG_LEVEL);

/**
 * Логирование с уровнем и временной меткой
 */
function log(level, ...args) {
    const messageLevelIndex = LEVELS.indexOf(level);
    if (messageLevelIndex < CURRENT_LEVEL_INDEX) return;

    const timestamp = new Date().toISOString();
    const method = level === 'error' ? console.error :
                   level === 'warn'  ? console.warn :
                   level === 'info'  ? console.info :
                   level === 'debug' ? console.debug : console.log;
    
    method(`[${timestamp}] [${level.toUpperCase()}]`, ...args);
}

/**
 * Логирование с контекстом (для отслеживания сообщества, пользователя и т.д.)
 */
function logWithContext(context, level, message, ...args) {
    const contextStr = Object.entries(context)
        .map(([k, v]) => `${k}=${v}`)
        .join(', ');
    
    log(level, `[${contextStr}] ${message}`, ...args);
}

module.exports = {
    log,
    logWithContext
};
