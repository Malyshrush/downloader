/**
 * Модуль обработки ошибок с retry логикой
 */

const { log } = require('./logger');

/**
 * Выполнить функцию с повторными попытками
 * 
 * @param {Function} fn - функция для выполнения
 * @param {Object} options - настройки
 * @param {number} options.maxRetries - максимальное количество попыток (по умолчанию 3)
 * @param {number} options.retryDelay - задержка между попытками в мс (по умолчанию 1000)
 * @param {number} options.backoffMultiplier - множитель экспоненциальной задержки (по умолчанию 2)
 * @param {Array} options.retryableErrors - коды ошибок для повтора (по умолчанию [6, 912])
 * @param {string} options.operationName - имя операции для логирования
 * @returns {Promise<*>} результат выполнения функции
 */
async function withRetry(fn, options = {}) {
    const {
        maxRetries = 3,
        retryDelay = 1000,
        backoffMultiplier = 2,
        retryableErrors = [6, 912], // Rate limit, Chat bot feature
        operationName = 'Operation'
    } = options;

    let lastError = null;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const result = await fn();
            
            if (attempt > 1) {
                log('info', `✅ ${operationName} succeeded on attempt ${attempt}`);
            }
            
            return result;
        } catch (error) {
            lastError = error;
            const errorCode = error.response?.data?.error?.error_code || error.code;
            const errorMsg = error.response?.data?.error?.error_msg || error.message;

            // Проверяем, стоит ли повторять
            const shouldRetry = retryableErrors.includes(errorCode) && attempt < maxRetries;

            if (shouldRetry) {
                const delay = retryDelay * Math.pow(backoffMultiplier, attempt - 1);
                log('warn', `⚠️ ${operationName} failed (attempt ${attempt}/${maxRetries}): ${errorMsg}`);
                log('info', `🔄 Retrying in ${delay / 1000}s...`);
                
                await new Promise(resolve => setTimeout(resolve, delay));
            } else {
                log('error', `❌ ${operationName} failed after ${attempt} attempts: ${errorMsg}`);
                throw error;
            }
        }
    }

    throw lastError;
}

/**
 * Обработчик ошибок для HTTP запросов
 */
function handleHttpError(error, context = '') {
    const statusCode = error.response?.status || error.statusCode || 500;
    const message = error.response?.data?.error || error.message;

    log('error', `❌ HTTP Error ${context}:`, {
        statusCode,
        message,
        stack: error.stack
    });

    return {
        statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify({
            success: false,
            error: message,
            context
        })
    };
}

/**
 * Глобальный обработчик необработанных ошибок
 */
function setupGlobalErrorHandlers() {
    // Необработанные исключения
    process.on('uncaughtException', (error) => {
        log('error', '💥 UNCAUGHT EXCEPTION:', error);
        log('error', error.stack);
        
        // Graceful shutdown
        process.exit(1);
    });

    // Необработанные отклонения промисов
    process.on('unhandledRejection', (reason, promise) => {
        log('error', '💥 UNHANDLED PROMISE REJECTION:', reason);
    });

    // Предупреждения Node.js
    process.on('warning', (warning) => {
        log('warn', '⚠️ NODE WARNING:', warning.message);
    });
}

module.exports = {
    withRetry,
    handleHttpError,
    setupGlobalErrorHandlers
};
