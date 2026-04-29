/**
 * Локальный сервер для тестирования VK Bot
 * 
 * Использование:
 *   node src/local-server.js
 *   node src/local-server.js --port 3000
 */

require('dotenv').config();

const http = require('http');
const { handler } = require('./handler');

const PORT = process.env.PORT || 3000;

/**
 * Преобразовать HTTP запрос в формат Yandex Cloud Functions
 */
function createEventFromRequest(req, body) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const query = Object.fromEntries(url.searchParams.entries());
    
    return {
        httpMethod: req.method,
        path: url.pathname,
        queryStringParameters: query,
        query: query,
        headers: req.headers,
        body: body
    };
}

/**
 * Преобразовать ответ обработчика в HTTP ответ
 */
function sendResponse(res, handlerResponse) {
    const statusCode = handlerResponse.statusCode || 200;
    const headers = handlerResponse.headers || {};
    const body = handlerResponse.body || '';

    res.writeHead(statusCode, headers);
    res.end(body);
}

/**
 * Создать сервер
 */
const server = http.createServer(async (req, res) => {
    let body = '';

    req.on('data', chunk => {
        body += chunk.toString();
    });

    req.on('end', async () => {
        try {
            console.log(`\n${'='.repeat(60)}`);
            console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
            console.log(`${'='.repeat(60)}`);

            const event = createEventFromRequest(req, body);
            const response = await handler(event);
            sendResponse(res, response);

            console.log(`↳ Status: ${response.statusCode}`);
        } catch (error) {
            console.error('❌ Server error:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: error.message }));
        }
    });
});

/**
 * Graceful shutdown
 */
function gracefulShutdown(signal) {
    console.log(`\n📡 Received ${signal}. Shutting down gracefully...`);
    server.close(() => {
        console.log('✅ Server closed');
        process.exit(0);
    });

    // Принудительное завершение через 10 секунд
    setTimeout(() => {
        console.log('⚠️  Force shutdown');
        process.exit(1);
    }, 10000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

/**
 * Запуск сервера
 */
server.listen(PORT, () => {
    console.log(`
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║   🤖  VK Bot Local Server                                 ║
║                                                           ║
║   📡 Port:    ${PORT}                                       ║
║   🌐 URL:     http://localhost:${PORT}                      ║
║                                                           ║
║   📋 Endpoints:                                           ║
║      GET  /              - Admin panel                    ║
║      GET  /?sheet=...    - Get sheet data                 ║
║      POST /?save=...     - Save sheet data                ║
║      POST /?verifyAuth   - Verify auth                    ║
║                                                           ║
║   Press Ctrl+C to stop                                    ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
    `);
});
