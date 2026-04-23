/**
 * 🚀 VK Bot - Скрипт деплоя в Yandex Cloud Functions
 * 
 * Что делает:
 * 1. Создаёт резервную копию текущей версии
 * 2. Устанавливает зависимости (npm install)
 * 3. Подготавливает dist/ папку для деплоя
 * 4. Деплоит новую версию функции
 * 5. Сохраняет логи деплоя
 * 
 * Использование:
 *   node scripts/deploy.js
 *   node scripts/deploy.js --skip-backup  (пропустить бэкап)
 *   node scripts/deploy.js --prepare-only (только подготовить dist/ без деплоя)
 *   node scripts/deploy.js --only-logs    (только скачать логи)
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { execSync } = require('child_process');

// ============================================
// КОНФИГУРАЦИЯ
// ============================================
const CONFIG = {
    functionTargets: [
        {
            name: 'vk-bot-2',
            entrypoint: 'index.handler',
            logsFile: 'last_deploy_logs.txt'
        },
        {
            name: 'vk-bot-2-worker',
            entrypoint: 'index.workerHandler',
            logsFile: 'last_deploy_worker_logs.txt'
        },
        {
            name: 'vk-bot-2-sender',
            entrypoint: 'index.senderHandler',
            logsFile: 'last_deploy_sender_logs.txt'
        }
    ],
    runtime: 'nodejs18',
    memory: '512m',
    timeout: '360s',
    serviceAccountId: 'aje2phgc3cdo22nuen04',
    bucketName: 'bot-data-storage2',
    ycPath: path.join(process.env.USERPROFILE, 'yandex-cloud', 'bin', 'yc.exe'),
    projectRoot: path.join(__dirname, '..'),
    distDir: path.join(__dirname, '..', 'dist'),
    backupsDir: path.join(__dirname, '..', 'backups'),
    
    // Белый список переменных для деплоя
    envWhitelist: [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'BUCKET_NAME',
        'APP_URL',
        'LOG_LEVEL',
        'ADMIN_EMAIL',
        'ADMIN_USERNAME',
        'ADMIN_PASSWORD',
        'VK_TOKEN',
        'VK_GROUP_ID',
        'USER_TOKEN',
        'CALLBACK_PROXY_URL',
        'CALLBACK_SECRET',
        'EVENT_QUEUE_MODE',
        'EVENT_RUNTIME_MODE',
        'YMQ_ENDPOINT',
        'YMQ_REGION',
        'YMQ_INCOMING_QUEUE_URL',
        'YMQ_OUTBOUND_QUEUE_URL',
        'YDB_DOCAPI_ENDPOINT',
        'YDB_IDEMPOTENCY_TABLE',
        'YDB_HOT_STATE_TABLE',
        'YDB_APP_LOGS_TABLE',
        'YDB_USER_STATE_TABLE',
        'YDB_COMMUNITY_VARIABLES_TABLE',
        'YDB_DELAYED_DELIVERIES_TABLE',
        'YDB_PROFILE_USER_SHARED_TABLE',
        'YDB_SHARED_VARIABLES_TABLE',
        'EVENT_IDEMPOTENCY_LEASE_SECONDS',
        'EVENT_IDEMPOTENCY_RETENTION_DAYS'
    ]
};

// ============================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// ============================================

/**
 * Логирование с цветом
 */
function log(message, color = 'white') {
    const colors = {
        cyan: '\x1b[36m',
        yellow: '\x1b[33m',
        green: '\x1b[32m',
        red: '\x1b[31m',
        white: '\x1b[0m'
    };
    console.log(`${colors[color] || colors.white}${message}\x1b[0m`);
}

/**
 * Автосинхронизация displayVersion из parts
 */
function syncBotVersionFile() {
    const versionFile = path.join(CONFIG.projectRoot, 'bot-version.json');
    if (!fs.existsSync(versionFile)) return;

    const raw = JSON.parse(fs.readFileSync(versionFile, 'utf8'));
    const parts = Array.isArray(raw.parts) ? raw.parts : [];
    const computed = 'version ' + parts.map(part => String(part?.value || '0000').trim() || '0000').join('.');

    if (raw.displayVersion !== computed) {
        raw.displayVersion = computed;
        fs.writeFileSync(versionFile, JSON.stringify(raw, null, 2), 'utf8');
        log(`🔢 displayVersion синхронизирована: ${computed}`, 'cyan');
    } else {
        log(`🔢 displayVersion уже синхронизирована: ${computed}`, 'cyan');
    }
}

/**
 * Создать резервную копию
 */
function createBackup() {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupPath = path.join(CONFIG.backupsDir, timestamp);
    
    log(`\n📦 Создание резервной копии...`, 'cyan');
    
    // Создаём папку бэкапа
    if (!fs.existsSync(CONFIG.backupsDir)) {
        fs.mkdirSync(CONFIG.backupsDir, { recursive: true });
    }
    fs.mkdirSync(backupPath, { recursive: true });
    
    // Копируем важные файлы
    const filesToBackup = [
        'index.js',
        'package.json',
        'adminPanelHTML.js',
        'bot-version.json',
        'README.md'
    ];
    
    const srcModules = [
        'src/modules',
        'src/utils',
        'src/handler.js',
        'src/local-server.js'
    ];
    
    let copiedCount = 0;
    
    // Копируем корневые файлы
    for (const file of filesToBackup) {
        const src = path.join(CONFIG.projectRoot, file);
        const dest = path.join(backupPath, file);
        if (fs.existsSync(src)) {
            fs.copyFileSync(src, dest);
            copiedCount++;
        }
    }
    
    // Копируем src/
    for (const srcPath of srcModules) {
        const src = path.join(CONFIG.projectRoot, srcPath);
        const dest = path.join(backupPath, srcPath);
        if (fs.existsSync(src)) {
            copyRecursive(src, dest);
            copiedCount++;
        }
    }
    
    log(`✅ Бэкап сохранён: ${path.relative(CONFIG.projectRoot, backupPath)}`, 'green');
    log(`   Скопировано файлов: ${copiedCount}`, 'white');
    
    return backupPath;
}

/**
 * Рекурсивное копирование папок
 */
function copyRecursive(src, dest) {
    const stats = fs.statSync(src);
    
    if (stats.isDirectory()) {
        if (!fs.existsSync(dest)) {
            fs.mkdirSync(dest, { recursive: true });
        }
        
        const entries = fs.readdirSync(src);
        for (const entry of entries) {
            const srcPath = path.join(src, entry);
            const destPath = path.join(dest, entry);
            copyRecursive(srcPath, destPath);
        }
    } else {
        fs.copyFileSync(src, dest);
    }
}

function createPackageArchive() {
    const zipPath = path.join(CONFIG.distDir, 'function.zip');
    if (fs.existsSync(zipPath)) {
        fs.unlinkSync(zipPath);
    }

    log(`\n🗜️ Упаковка dist/ в ZIP...`, 'cyan');
    const packageEntries = ['adminPanelHTML.js', 'bot-version.json', 'index.js', 'package.json', 'src', 'node_modules'];
    execSync(`tar.exe -a -cf "${zipPath}" ${packageEntries.join(' ')}`, {
        cwd: CONFIG.distDir,
        stdio: 'inherit'
    });

    const sha256 = crypto.createHash('sha256').update(fs.readFileSync(zipPath)).digest('hex');
    const sizeMb = Math.round((fs.statSync(zipPath).size / 1024 / 1024) * 100) / 100;
    log(`📦 function.zip: ${sizeMb} MB`, 'cyan');
    return { zipPath, sha256 };
}

function uploadPackageArtifact(packageInfo) {
    const objectName = `deployments/function_${Date.now()}.zip`;
    log(`\n☁️ Загрузка пакета в Object Storage...`, 'cyan');
    execSync(`"${CONFIG.ycPath}" storage s3 cp "${packageInfo.zipPath}" "s3://${CONFIG.bucketName}/${objectName}" --only-show-errors`, {
        cwd: CONFIG.projectRoot,
        stdio: 'inherit'
    });
    log(`✅ Пакет загружен: s3://${CONFIG.bucketName}/${objectName}`, 'green');
    return {
        objectName,
        sha256: packageInfo.sha256
    };
}

/**
 * Подготовка dist/ папки для деплоя
 */
function prepareDist() {
    log(`\n🔨 Подготовка dist/ папки...`, 'cyan');

    // Очищаем dist/
    if (fs.existsSync(CONFIG.distDir)) {
        fs.rmSync(CONFIG.distDir, { recursive: true, force: true });
    }
    fs.mkdirSync(CONFIG.distDir, { recursive: true });

    // Копируем файлы для деплоя
    const filesToDeploy = [
        'yandex-function/index.js',
        'yandex-function/package.json',
        'adminPanelHTML.js',
        'bot-version.json'
    ];

    const dirsToDeploy = [
        'src'
        // node_modules устанавливается отдельно через npm install в dist/
    ];
    
    let copiedCount = 0;
    
    // Копируем корневые файлы
    for (const file of filesToDeploy) {
        const src = path.join(CONFIG.projectRoot, file);
        let destName = file;
        if (file === 'yandex-function/index.js') destName = 'index.js';
        else if (file === 'yandex-function/package.json') destName = 'package.json';
        const dest = path.join(CONFIG.distDir, destName);
        if (fs.existsSync(src)) {
            fs.copyFileSync(src, dest);
            copiedCount++;
            log(`   ✓ ${destName}`, 'white');
        }
    }
    
    // Копируем src/
    for (const dir of dirsToDeploy) {
        const src = path.join(CONFIG.projectRoot, dir);
        const dest = path.join(CONFIG.distDir, dir);
        if (fs.existsSync(src)) {
            copyRecursive(src, dest);
            copiedCount++;
            log(`   ✓ ${dir}/`, 'white');
        }
    }
    
    log(`✅ Подготовлено файлов: ${copiedCount}`, 'green');
    
    // 📦 Устанавливаем зависимости в dist/ ТОЛЬКО если node_modules не существует
    const nmPath = path.join(CONFIG.distDir, 'node_modules');
    if (!fs.existsSync(nmPath) || !fs.existsSync(path.join(nmPath, '@aws-sdk'))) {
        log(`\n📦 Установка зависимостей в dist/...`, 'cyan');
        try {
            execSync('npm install --omit=dev', {
                cwd: CONFIG.distDir,
                stdio: 'inherit'
            });
            log('✅ Зависимости установлены', 'green');
        } catch (error) {
            log(`⚠️ Ошибка установки: ${error.message}`, 'yellow');
        }
    } else {
        log('✅ node_modules уже существует, пропускаем установку', 'green');
    }
    
    // 🗑️ Агрессивная очистка node_modules в dist
    if (fs.existsSync(nmPath)) {
        log('🗑️ Агрессивная очистка node_modules...', 'yellow');
        try {
            // Удаляем текстовые файлы и ненужные пакеты
            execSync(`powershell -Command "Get-ChildItem -Path '${nmPath}' -Recurse -Include '*.md','CHANGELOG*','LICENSE*','README*','*.txt' -File | Remove-Item -Force -ErrorAction SilentlyContinue; Get-ChildItem -Path '${nmPath}' -Recurse -Directory -Include '__tests__','__test__','test','tests','coverage','examples','docs','doc','.github' -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue; Remove-Item '${nmPath}\\multer' -Recurse -Force -ErrorAction SilentlyContinue"`, {
                stdio: 'inherit'
            });
            
            // Проверяем размер
            const result = execSync(`powershell -Command "[math]::Round((Get-ChildItem -Path '${nmPath}' -Recurse | Measure-Object -Property Length -Sum).Sum / 1MB, 2)"`, { encoding: 'utf8' });
            const sizeMB = result.trim();
            log(`📊 node_modules: ${sizeMB} MB`, 'cyan');
        } catch (e) {
            log(`⚠️ Ошибка очистки: ${e.message}`, 'yellow');
        }
    }
}

/**
 * Подготовить переменные окружения для деплоя
 */
function prepareEnvVars() {
    const envFile = path.join(CONFIG.projectRoot, '.env');
    
    if (!fs.existsSync(envFile)) {
        log('⚠️  .env файл не найден, деплой без переменных', 'yellow');
        return '';
    }
    
    const lines = fs.readFileSync(envFile, 'utf8')
        .split('\n')
        .map(l => l.trim())
        .filter(l => l && !l.startsWith('#') && !l.startsWith('//'));
    
    const envVars = {};
    
    for (const line of lines) {
        const [key, ...valueParts] = line.split('=');
        const value = valueParts.join('=').trim();
        
        if (CONFIG.envWhitelist.includes(key.trim())) {
            envVars[key.trim()] = value;
        }
    }
    
    return Object.entries(envVars)
        .map(([k, v]) => `${k}=${v}`)
        .join(',');
}

function provisionEventInfra() {
    const envFile = path.join(CONFIG.projectRoot, '.env');
    if (!fs.existsSync(envFile)) {
        log('ℹ️ .env не найден, пропускаем auto-provision event infra', 'yellow');
        return;
    }

    const envContent = fs.readFileSync(envFile, 'utf8');
    if (!/YDB_DOCAPI_ENDPOINT\s*=/.test(envContent) || !/AWS_ACCESS_KEY_ID\s*=/.test(envContent) || !/AWS_SECRET_ACCESS_KEY\s*=/.test(envContent)) {
        log('ℹ️ Недостаточно cloud env для auto-provision event infra, пропускаем шаг', 'yellow');
        return;
    }

    const provisionScript = path.join(CONFIG.projectRoot, 'yandex-function', 'scripts', 'provision-event-infra.js');
    if (!fs.existsSync(provisionScript)) {
        log('ℹ️ provision-event-infra.js не найден, пропускаем шаг', 'yellow');
        return;
    }

    log(`\n☁️ Проверка очередей и YDB таблиц...`, 'cyan');
    execSync('node scripts/provision-event-infra.js', {
        cwd: path.join(CONFIG.projectRoot, 'yandex-function'),
        stdio: 'inherit'
    });
    log('✅ Event infra синхронизирована', 'green');
}

/**
 * Выполнить деплой
 */
function ensureFunctionExists(target) {
    const getCmd = `"${CONFIG.ycPath}" serverless function get ${target.name}`;
    try {
        execSync(getCmd, {
            cwd: CONFIG.projectRoot,
            stdio: 'ignore'
        });
        return true;
    } catch (error) {
        log(`ℹ️ Функция ${target.name} не найдена, создаю...`, 'yellow');
        const createCmd = `"${CONFIG.ycPath}" serverless function create --name ${target.name}`;
        execSync(createCmd, {
            cwd: CONFIG.projectRoot,
            stdio: 'inherit'
        });
        return true;
    }
}

function deployFunction(target, envArg, packageArtifact) {
    log(`\n🚀 Деплой в Yandex Cloud Functions...`, 'cyan');
    log(`   Функция: ${target.name}`, 'white');
    log(`   Runtime: ${CONFIG.runtime}`, 'white');
    log(`   Memory: ${CONFIG.memory}`, 'white');
    log(`   Timeout: ${CONFIG.timeout}`, 'white');
    log(`   Entrypoint: ${target.entrypoint}`, 'white');

    const ycCmd = [
        `"${CONFIG.ycPath}"`,
        'serverless function version create',
        `--function-name ${target.name}`,
        `--runtime ${CONFIG.runtime}`,
        `--entrypoint ${target.entrypoint}`,
        `--memory ${CONFIG.memory}`,
        `--execution-timeout ${CONFIG.timeout}`,
        `--package-bucket-name ${CONFIG.bucketName}`,
        `--package-object-name ${packageArtifact.objectName}`,
        `--package-sha256 ${packageArtifact.sha256}`,
        `--service-account-id ${CONFIG.serviceAccountId}`,
        envArg ? `--environment ${envArg}` : ''
    ].filter(Boolean).join(' ');
    
    try {
        log(`\n⏳ Выполняется деплой...`, 'yellow');
        execSync(ycCmd, { 
            cwd: CONFIG.projectRoot,
            stdio: 'inherit'
        });
        
        log(`\n✅ Деплой успешно завершён!`, 'green');
        return true;
    } catch (error) {
        log(`\n❌ Ошибка деплоя:`, 'red');
        const details =
            error.stderr?.toString() ||
            error.stdout?.toString() ||
            error.message;
        log(details, 'red');
        return false;
    }
}

/**
 * Скачать логи после деплоя
 */
function saveLogs(target) {
    log(`\n📋 Сохранение логов ${target.name}...`, 'cyan');
    
    const logsCmd = `"${CONFIG.ycPath}" serverless function logs ${target.name} --limit 50`;
    
    try {
        const logs = execSync(logsCmd, { 
            cwd: CONFIG.projectRoot,
            encoding: 'utf8'
        });
        
        const logsFile = path.join(CONFIG.projectRoot, target.logsFile);
        fs.writeFileSync(logsFile, logs, 'utf8');
        
        log(`✅ Логи сохранены в ${target.logsFile}`, 'green');
        return true;
    } catch (error) {
        log(`⚠️  Не удалось получить логи: ${error.message}`, 'yellow');
        return false;
    }
}

/**
 * Проверить наличие yc CLI
 */
function checkYC() {
    if (!fs.existsSync(CONFIG.ycPath)) {
        log(`❌ Yandex Cloud CLI не найден: ${CONFIG.ycPath}`, 'red');
        log(`   Установите: https://yandex.cloud/ru/docs/cli/quickstart`, 'yellow');
        return false;
    }
    return true;
}

// ============================================
// ГЛАВНАЯ ФУНКЦИЯ
// ============================================

async function main() {
    const args = process.argv.slice(2);
    const skipBackup = args.includes('--skip-backup');
    const prepareOnly = args.includes('--prepare-only');
    const onlyLogs = args.includes('--only-logs');
    
    log(`\n${'═'.repeat(60)}`, 'cyan');
    log(`  🤖 VK Bot - Деплой в Yandex Cloud Functions`, 'cyan');
    log(`${'═'.repeat(60)}\n`, 'cyan');
    
    // Проверка yc CLI
    if (!checkYC()) {
        process.exit(1);
    }

    // 0. Синхронизация версии из parts
    syncBotVersionFile();
    
    // Только логи
    if (onlyLogs) {
        for (const target of CONFIG.functionTargets) {
            saveLogs(target);
        }
        return;
    }
    
    // 1. Бэкап
    if (!skipBackup) {
        createBackup();
    }

    provisionEventInfra();
    
    // 2. Подготовка dist/
    prepareDist();
    const packageInfo = createPackageArchive();
    const packageArtifact = uploadPackageArtifact(packageInfo);
    
    // 3. Переменные окружения
    const envArg = prepareEnvVars();
    if (envArg) {
        log(`\n🔑 Переменные окружения: ${envArg.split(',').length} шт.`, 'white');
    }

    for (const target of CONFIG.functionTargets) {
        ensureFunctionExists(target);
    }

    if (prepareOnly) {
        log(`\n${'═'.repeat(60)}`, 'green');
        log(`  ✅ ПОДГОТОВКА DIST ЗАВЕРШЕНА`, 'green');
        log(`  dist/ готова к деплою без выката в облако`, 'green');
        log(`${'═'.repeat(60)}\n`, 'green');
        return;
    }
     
    // 4. Деплой
    const success = CONFIG.functionTargets.every(target => deployFunction(target, envArg, packageArtifact));
    
    if (success) {
        // 5. Логи
        setTimeout(() => {
            for (const target of CONFIG.functionTargets) {
                saveLogs(target);
            }
            
            log(`\n${'═'.repeat(60)}`, 'green');
            log(`  ✅ ДЕПЛОЙ ЗАВЕРШЁН УСПЕШНО!`, 'green');
            log(`${'═'.repeat(60)}\n`, 'green');
        }, 5000); // Ждём 5 сек чтобы логи успели появиться
    } else {
        log(`\n❌ ДЕПЛОЙ НЕ УДАЛСЯ`, 'red');
        process.exit(1);
    }
}

// Запуск
main().catch(error => {
    log(`\n💥 Критическая ошибка:`, 'red');
    log(error.stack || error.message, 'red');
    process.exit(1);
});
