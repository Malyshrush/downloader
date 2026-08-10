'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');

const projectRoot = path.resolve(__dirname, '..');
require('dotenv').config({ path: path.join(projectRoot, '.env'), quiet: true });
const { getPublicProfiles, updateAdminProfileCredentials } = require('../src/modules/admin-profiles');
const { clearLoginLock } = require('../src/modules/admin-security');
const { loadServiceLimits, saveServiceLimits, normalizeServiceLimits } = require('../src/modules/service-limits');
const {
    getServiceLimitProfiles,
    createServiceLimitProfile,
    saveServiceLimitProfile,
    activateServiceLimitProfile
} = require('../src/modules/service-limits-profiles');
const {
    getAttachmentUploadSettings,
    saveGlobalAttachmentUploadSettings,
    saveProfileAttachmentUploadOverrides
} = require('../src/modules/attachment-upload-settings');
const logsDir = path.join(projectRoot, 'operation-logs');
const historyFile = path.join(logsDir, 'history.json');
const registryFile = path.join(projectRoot, 'PROJECT_COMMANDS.json');
const host = '127.0.0.1';
const preferredPort = Number(process.env.PAPA_BOT_CONTROL_PORT) || 3210;
const accessToken = crypto.randomBytes(24).toString('hex');
const maxPortAttempts = 21;

function testFiles(names) {
    return names.map(name => path.join(projectRoot, 'tests', name));
}

function allTestFiles() {
    return fs.readdirSync(path.join(projectRoot, 'tests'))
        .filter(name => name.endsWith('.test.js'))
        .sort()
        .map(name => path.join(projectRoot, 'tests', name));
}

const syncVersionCode = [
    "require('dotenv').config();",
    "const data=require('./bot-version.json');",
    "require('./src/modules/bot-version-store').saveBotVersionData(data)",
    ".then(v=>console.log('Production version:',v.displayVersion))",
    ".catch(e=>{console.error(e.stack||e.message);process.exit(1)})"
].join('');

const smokeCode = [
    "const url='https://functions.yandexcloud.net/d4eg37ikm3vl5tm1mjld';",
    "fetch(url).then(async r=>{const t=await r.text();",
    "console.log('HTTP status:',r.status);",
    "console.log('delete_comment:',t.includes(\"value: 'delete_comment'\"));",
    "console.log('like_comment:',t.includes(\"value: 'like_comment'\"));",
    "if(!r.ok||!t.includes(\"value: 'delete_comment'\")||!t.includes(\"value: 'like_comment'\"))process.exit(1)",
    "}).catch(e=>{console.error(e.message);process.exit(1)})"
].join('');

const COMMANDS = {
    quick_tests: {
        title: 'Быстрые тесты текущих изменений',
        description: 'Проверяет триггеры комментариев, списки, VK API, интерфейс и синтаксис встроенного JavaScript. Запускайте перед каждым deployment.',
        success: 'Ключевые сценарии изменения работают, синтаксис и интерфейс не повреждены.',
        steps: () => [{
            file: process.execPath,
            args: ['--test', ...testFiles([
                'structured-trigger-comment-actions.test.js',
                'structured-trigger-comment-text-lists.test.js',
                'comment-event-snapshot-store.test.js',
                'structured-triggers-runtime.test.js',
                'event-worker.test.js',
                'admin-panel-structured-triggers-ui.test.js',
                'admin-panel-inline-script-syntax.test.js',
                'vk-api-post.test.js'
            ])],
            label: 'Целевые тесты'
        }]
    },
    full_tests: {
        title: 'Все тесты проекта',
        description: 'Запускает каждый файл tests/*.test.js. Это самая полная проверка и обычно занимает дольше быстрых тестов.',
        success: 'Полный набор автоматических тестов завершён без ошибок.',
        steps: () => [{
            file: process.execPath,
            args: ['--test', ...allTestFiles()],
            label: 'Полная регрессия'
        }]
    },
    syntax: {
        title: 'Проверка синтаксиса',
        description: 'Компилирует основные серверные модули и отдельно проверяет JavaScript, встроенный в HTML админ-панели.',
        success: 'Основные файлы и встроенный скрипт админ-панели синтаксически корректны.',
        steps: () => [
            { file: process.execPath, args: ['--check', 'src/modules/structured-triggers.js'], label: 'structured-triggers.js' },
            { file: process.execPath, args: ['--check', 'src/modules/vk-api.js'], label: 'vk-api.js' },
            { file: process.execPath, args: ['--check', 'adminPanelHTML.js'], label: 'adminPanelHTML.js' },
            { file: process.execPath, args: ['--test', 'tests/admin-panel-inline-script-syntax.test.js'], label: 'Встроенный JavaScript' }
        ]
    },
    version: {
        title: 'Проверка версии и уведомлений',
        description: 'Проверяет 13 сегментов bot-version.json, историю обновлений, временные метки и интерфейс уведомлений.',
        success: 'Версия, история и уведомления имеют согласованную структуру.',
        steps: () => [{
            file: process.execPath,
            args: ['--test', ...testFiles([
                'bot-version-file-contract.test.js',
                'bot-version-store.test.js',
                'admin-panel-version-notifications-contract.test.js'
            ])],
            label: 'Контракты версии'
        }]
    },
    production_status: {
        title: 'Статусы production',
        description: 'Только читает состояния main, worker и sender в Yandex Cloud. Ничего не изменяет.',
        success: 'Состояния всех production-функций получены. Для рабочего сервиса ожидается status: ACTIVE.',
        steps: () => ['vk-bot-2', 'vk-bot-2-worker', 'vk-bot-2-sender'].map(name => ({
            file: 'yc',
            args: ['serverless', 'function', 'get', name, '--format', 'json'],
            label: name
        }))
    },
    production_logs: {
        title: 'Свежие production-логи',
        description: 'Показывает последние журналы main, worker и sender за 15 минут. Ничего не изменяет.',
        success: 'Свежие журналы трёх функций загружены.',
        steps: () => ['vk-bot-2', 'vk-bot-2-worker', 'vk-bot-2-sender'].map(name => ({
            file: 'yc',
            args: ['serverless', 'function', 'logs', name, '--since', '15m', '--limit', '150'],
            label: name
        }))
    },
    smoke: {
        title: 'Production smoke-проверка',
        description: 'Открывает публичную страницу production и проверяет HTTP 200 и наличие последних элементов интерфейса. Данные не изменяет.',
        success: 'Production отвечает и содержит ожидаемые элементы интерфейса.',
        steps: () => [{ file: process.execPath, args: ['-e', smokeCode], label: 'HTTP smoke' }]
    },
    deploy: {
        title: 'Полный deployment',
        description: 'Создаёт обязательный backup, собирает dist, проверяет импорт, обновляет main/worker/sender, проверяет очереди, синхронизирует bot-version и выполняет smoke-проверку.',
        success: 'Deployment завершён, версия синхронизирована, production прошёл smoke-проверку.',
        dangerous: true,
        steps: () => [
            { file: process.execPath, args: ['scripts/deploy.js'], label: 'Deployment Yandex Cloud' },
            { file: process.execPath, args: ['-e', syncVersionCode], label: 'Синхронизация production-версии' },
            { file: process.execPath, args: ['-e', smokeCode], label: 'Production smoke' }
        ]
    }
};

const BUILTIN_COMMAND_IDS = new Set(Object.keys(COMMANDS));
let registryCommandIds = new Set();
let registryDocumentation = [];
let registryStatus = {
    loadedAt: '',
    documented: 0,
    runnable: 0,
    warnings: [],
    error: ''
};

function hasUnsafeArgument(value) {
    const text = String(value || '');
    return !text || /[\r\n\0]/.test(text) || text.includes('..');
}

function compileRegistryStep(step) {
    if (!step || typeof step !== 'object') throw new Error('Шаг команды должен быть объектом');
    const executable = String(step.executable || '').trim().toLowerCase();
    const args = Array.isArray(step.args) ? step.args.map(value => String(value)) : [];
    if (!args.length || args.some(hasUnsafeArgument)) throw new Error('Шаг содержит пустые или небезопасные аргументы');

    let file = '';
    if (executable === 'node') {
        if (!['--test', '--check'].includes(args[0])) {
            throw new Error('Из реестра Node разрешён только для --test и --check');
        }
        if (args.slice(1).some(value => path.isAbsolute(value))) {
            throw new Error('Пути Node из реестра должны быть относительными');
        }
        file = process.execPath;
    } else if (executable === 'git') {
        if (!['status', 'diff'].includes(args[0])) {
            throw new Error('Из реестра Git разрешён только для status и diff');
        }
        file = 'git';
    } else if (executable === 'yc') {
        const prefix = args.slice(0, 2).join(' ');
        const operation = args[2];
        const isReadOnlyOperation = prefix === 'serverless function' &&
            (operation === 'get' || operation === 'logs' ||
                (operation === 'version' && args[3] === 'list'));
        if (!isReadOnlyOperation) {
            throw new Error('Из реестра YC разрешены только get, logs и version list');
        }
        file = 'yc';
    } else {
        throw new Error('Исполняемый файл не входит в allowlist: ' + executable);
    }

    return {
        file,
        args,
        label: String(step.label || step.explanation || `Шаг ${step.order || ''}`).trim(),
        explanation: String(step.explanation || '').trim(),
        relation: String(step.relation || '').trim(),
        order: Number(step.order) || 0
    };
}

function compileRegistryCommands(data) {
    if (!data || Number(data.schemaVersion) !== 1 || !Array.isArray(data.commands)) {
        throw new Error('PROJECT_COMMANDS.json имеет неподдерживаемую схему');
    }
    const compiled = {};
    const warnings = [];

    for (const item of data.commands) {
        if (!item || !item.runnable) continue;
        const id = String(item.id || '').trim();
        if (!/^[a-z][a-z0-9_]{2,64}$/.test(id)) {
            warnings.push(`Пропущена команда с некорректным id: ${id || '(пусто)'}`);
            continue;
        }
        if (BUILTIN_COMMAND_IDS.has(id)) {
            warnings.push(`Команда ${id} не может переопределить встроенную`);
            continue;
        }
        try {
            const steps = (Array.isArray(item.sequence) ? item.sequence : [])
                .slice()
                .sort((a, b) => Number(a.order) - Number(b.order))
                .map(compileRegistryStep);
            if (!steps.length) throw new Error('нет запускаемых шагов');
            compiled[id] = {
                title: String(item.title || id).trim(),
                description: String(item.description || item.purpose || '').trim(),
                success: String(item.success || 'Команда из реестра успешно завершена.').trim(),
                dangerous: false,
                source: 'registry',
                purpose: String(item.purpose || '').trim(),
                whenToUse: String(item.whenToUse || '').trim(),
                sequence: steps,
                steps: () => steps.map(step => ({ ...step, args: [...step.args] }))
            };
        } catch (error) {
            warnings.push(`Команда ${id} пропущена: ${error.message}`);
        }
    }
    return {
        commands: compiled,
        documented: data.commands.length,
        warnings
    };
}

function loadProjectCommandRegistry(filePath = registryFile) {
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const compiled = compileRegistryCommands(parsed);
    for (const id of registryCommandIds) delete COMMANDS[id];
    registryCommandIds = new Set(Object.keys(compiled.commands));
    Object.assign(COMMANDS, compiled.commands);
    registryDocumentation = parsed.commands
        .filter(item => item && !item.runnable)
        .map(item => ({
            id: `documentation_${String(item.id || '').replace(/[^a-z0-9_]/gi, '_')}`,
            title: String(item.title || item.id || 'Описание команды').trim(),
            description: String(item.description || item.purpose || '').trim(),
            purpose: String(item.purpose || '').trim(),
            whenToUse: String(item.whenToUse || '').trim(),
            sequence: (Array.isArray(item.sequence) ? item.sequence : [])
                .slice()
                .sort((a, b) => Number(a.order) - Number(b.order))
                .map((step, index) => ({
                    order: Number(step.order) || index + 1,
                    label: String(step.label || step.commandTemplate || step.explanation || `Шаг ${index + 1}`).trim(),
                    explanation: String(step.explanation || '').trim(),
                    relation: String(step.relation || '').trim()
                }))
        }));
    registryStatus = {
        loadedAt: new Date().toISOString(),
        documented: compiled.documented,
        runnable: registryCommandIds.size,
        warnings: compiled.warnings,
        error: ''
    };
    return registryStatus;
}

function publicCommands() {
    const executable = Object.entries(COMMANDS).map(([id, command]) => ({
        id,
        title: command.title,
        description: command.description,
        dangerous: Boolean(command.dangerous),
        source: command.source || 'builtin',
        purpose: command.purpose || '',
        whenToUse: command.whenToUse || '',
        sequence: command.steps().map((step, index) => ({
            order: step.order || index + 1,
            label: step.label,
            explanation: step.explanation || '',
            relation: step.relation || ''
        }))
    }));
    return [...executable, ...registryDocumentation.map(command => ({
        ...command,
        dangerous: false,
        source: 'documentation',
        documentedOnly: true
    }))];
}

function registryCommandSnapshot(commands = publicCommands()) {
    return commands
        .filter(command => command.source === 'registry')
        .map(command => ({
            id: command.id,
            title: command.title,
            signature: JSON.stringify(command)
        }))
        .sort((a, b) => a.id.localeCompare(b.id));
}

function diffRegistryCommandSnapshots(previous, next) {
    const before = new Map((previous || []).map(command => [command.id, command]));
    const after = new Map((next || []).map(command => [command.id, command]));
    return {
        added: [...after.values()].filter(command => !before.has(command.id)).map(command => command.title),
        updated: [...after.values()]
            .filter(command => before.has(command.id) && before.get(command.id).signature !== command.signature)
            .map(command => command.title),
        removed: [...before.values()].filter(command => !after.has(command.id)).map(command => command.title),
        total: after.size
    };
}

let activeRun = null;
let currentChild = null;
let history = loadHistory();

function ensureLogsDir() {
    fs.mkdirSync(logsDir, { recursive: true });
}

function loadHistory() {
    try {
        return JSON.parse(fs.readFileSync(historyFile, 'utf8'));
    } catch {
        return [];
    }
}

function saveHistory() {
    ensureLogsDir();
    fs.writeFileSync(historyFile, JSON.stringify(history.slice(0, 50), null, 2), 'utf8');
}

function safeLogName(id) {
    return String(id || '').replace(/[^a-zA-Z0-9_.-]/g, '');
}

function redactSensitive(value) {
    const text = String(value || '');
    if (!/(password|secret|token|access_key|yookassa_api)/i.test(text)) return text;
    return text.replace(
        /^(\s*["']?[^:=]*(?:password|secret|token|access_key|yookassa_api)[^:=]*["']?\s*[:=]\s*).*$/i,
        '$1***'
    );
}

function writeLine(run, line, stream = 'stdout') {
    const clean = redactSensitive(String(line || '').replace(/\u001b\[[0-9;]*m/g, ''));
    const entry = { at: new Date().toISOString(), stream, text: clean };
    run.output.push(entry);
    if (run.output.length > 2500) run.output.splice(0, run.output.length - 2500);
    fs.appendFileSync(run.logPath, `[${entry.at}] [${stream}] ${clean}\n`, 'utf8');
}

function pipeLines(stream, run, streamName) {
    let pending = '';
    stream.setEncoding('utf8');
    stream.on('data', chunk => {
        pending += chunk;
        const lines = pending.split(/\r?\n/);
        pending = lines.pop() || '';
        lines.forEach(line => writeLine(run, line, streamName));
    });
    stream.on('end', () => {
        if (pending) writeLine(run, pending, streamName);
    });
}

function runStep(step, run) {
    return new Promise(resolve => {
        if (run.cancelRequested) return resolve({ code: 130 });
        writeLine(run, `\n=== ${step.label} ===`, 'system');
        writeLine(run, `Запуск: ${step.file} ${step.args.join(' ')}`, 'system');
        const child = spawn(step.file, step.args, {
            cwd: projectRoot,
            env: process.env,
            shell: false,
            windowsHide: true
        });
        currentChild = child;
        pipeLines(child.stdout, run, 'stdout');
        pipeLines(child.stderr, run, 'stderr');
        child.on('error', error => {
            writeLine(run, error.stack || error.message, 'stderr');
            currentChild = null;
            resolve({ code: 1 });
        });
        child.on('close', code => {
            writeLine(run, `Шаг завершён с кодом ${code}.`, code === 0 ? 'system' : 'stderr');
            currentChild = null;
            resolve({ code: Number(code) || 0 });
        });
    });
}

async function startCommand(commandId) {
    const command = COMMANDS[commandId];
    if (!command) throw new Error('Неизвестная команда');
    if (activeRun && activeRun.status === 'running') throw new Error('Другая команда уже выполняется');

    ensureLogsDir();
    const id = `${Date.now()}-${safeLogName(commandId)}`;
    const run = {
        id,
        commandId,
        title: command.title,
        description: command.description,
        status: 'running',
        startedAt: new Date().toISOString(),
        finishedAt: '',
        durationMs: 0,
        exitCode: null,
        result: 'Выполняется…',
        output: [],
        cancelRequested: false,
        logFile: `${id}.log`,
        logPath: path.join(logsDir, `${id}.log`)
    };
    activeRun = run;
    writeLine(run, `${command.title}: начало выполнения.`, 'system');

    let exitCode = 0;
    for (const step of command.steps()) {
        const result = await runStep(step, run);
        if (result.code !== 0) {
            exitCode = result.code;
            break;
        }
    }

    run.finishedAt = new Date().toISOString();
    run.durationMs = Date.now() - new Date(run.startedAt).getTime();
    run.exitCode = exitCode;
    run.status = run.cancelRequested ? 'cancelled' : (exitCode === 0 ? 'success' : 'failed');
    run.result = run.cancelRequested
        ? 'Команда остановлена пользователем.'
        : (exitCode === 0 ? command.success : `Команда завершилась с ошибкой (код ${exitCode}). Смотрите последние красные строки вывода.`);
    writeLine(run, `\nИТОГ: ${run.result}`, run.status === 'success' ? 'system' : 'stderr');

    history.unshift({
        id: run.id,
        commandId: run.commandId,
        title: run.title,
        status: run.status,
        startedAt: run.startedAt,
        finishedAt: run.finishedAt,
        durationMs: run.durationMs,
        exitCode: run.exitCode,
        result: run.result,
        logFile: run.logFile
    });
    saveHistory();
}

function publicRun(run) {
    if (!run) return null;
    const { logPath, cancelRequested, ...safe } = run;
    return safe;
}

function json(res, statusCode, value) {
    res.writeHead(statusCode, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store'
    });
    res.end(JSON.stringify(value));
}

function readBody(req) {
    return new Promise((resolve, reject) => {
        let body = '';
        req.on('data', chunk => {
            body += chunk;
            if (body.length > 10000) reject(new Error('Слишком большой запрос'));
        });
        req.on('end', () => resolve(body));
        req.on('error', reject);
    });
}

function authorized(req) {
    return req.headers['x-control-token'] === accessToken;
}

async function updateControlCenterProfileCredentials(profileId, payload = {}, overrides = {}) {
    const changes = {
        username: String(payload.username || '').trim(),
        password: String(payload.password || '').trim(),
        recoveryEmail: String(payload.recoveryEmail || '').trim()
    };
    if (!changes.username && !changes.password && !changes.recoveryEmail) {
        throw new Error('Укажите новый логин, пароль или почту');
    }

    const updateProfile = overrides.updateAdminProfileCredentials || updateAdminProfileCredentials;
    const resetLoginLock = overrides.clearLoginLock || clearLoginLock;
    const profile = await updateProfile(profileId, changes);
    await resetLoginLock(profile.username);
    return {
        success: true,
        profile,
        changed: {
            username: Boolean(changes.username),
            password: Boolean(changes.password),
            recoveryEmail: Boolean(changes.recoveryEmail)
        }
    };
}

function renderHtml() {
    const cards = publicCommands().map(command => `
        <article class="command-card ${command.dangerous ? 'danger' : ''}">
            <div>
                <h3>${escapeHtml(command.title)}</h3>
                <p>${escapeHtml(command.description)}</p>
                <div class="command-source">${command.source === 'documentation' ? 'Документация из PROJECT_COMMANDS.json' : (command.source === 'registry' ? 'Из PROJECT_COMMANDS.json' : 'Встроенная команда')}</div>
                <ol>${command.sequence.map(step => `<li><strong>${escapeHtml(step.label)}</strong>${step.explanation ? ` — ${escapeHtml(step.explanation)}` : ''}${step.relation ? `<small>${escapeHtml(step.relation)}</small>` : ''}</li>`).join('')}</ol>
            </div>
            <button ${command.documentedOnly ? 'disabled' : `data-command="${command.id}" data-dangerous="${command.dangerous ? 'true' : 'false'}"`}>
                ${command.documentedOnly ? 'Только описание' : (command.dangerous ? 'Запустить deployment' : 'Запустить')}
            </button>
        </article>`).join('');

    return `<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Центр управления PAPA BOT</title>
<style>
:root{color-scheme:dark;--bg:#07111f;--panel:#101d31;--line:#29405e;--text:#edf6ff;--muted:#9fb2c8;--blue:#38bdf8;--green:#34d399;--red:#fb7185;--amber:#fbbf24}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at top,#17365c 0,#07111f 52%);color:var(--text);font:15px/1.5 system-ui,sans-serif}
.wrap{max-width:1500px;margin:auto;padding:24px}.hero{display:flex;justify-content:space-between;gap:20px;align-items:center;margin-bottom:20px}.hero h1{margin:0;font-size:30px}.hero p{margin:5px 0;color:var(--muted)}.hero-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.hero-actions button{margin:0;background:linear-gradient(135deg,#059669,#0f766e)}
.badge{padding:8px 13px;border:1px solid var(--line);border-radius:999px;background:#0c192a;color:var(--green);font-weight:700}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:13px}.command-group{margin-top:22px}.command-group:first-child{margin-top:0}.command-group h2{margin:0 0 10px;font-size:20px}.command-group.registry-group{padding:16px;border:1px solid #0f766e;border-radius:18px;background:#064e3b22}.command-group-note{margin:-5px 0 12px;color:var(--muted)}.command-card{display:flex;flex-direction:column;justify-content:space-between;min-height:220px;padding:18px;background:linear-gradient(145deg,#13233a,#0c1829);border:1px solid var(--line);border-radius:17px;box-shadow:0 14px 35px #0005}.command-card.registry-command{border-color:#0f766e}.command-card.danger{border-color:#7c4428}.command-card h3{margin:0 0 8px;font-size:18px}.command-card p{margin:0;color:var(--muted)}.command-card ol{padding-left:20px;color:#c5d6e8;font-size:13px}.command-card li{margin:7px 0}.command-card li small{display:block;color:var(--muted)}.command-source{display:inline-block;margin-top:10px;padding:3px 8px;border-radius:999px;background:#1d2d43;color:#7dd3fc;font-size:11px}
button{border:0;border-radius:11px;padding:11px 15px;background:linear-gradient(135deg,#0284c7,#2563eb);color:white;font-weight:800;cursor:pointer;margin-top:16px}button:hover{filter:brightness(1.13)}button:disabled{opacity:.45;cursor:not-allowed}.danger button{background:linear-gradient(135deg,#d97706,#dc2626)}
.workspace{display:grid;grid-template-columns:minmax(0,2fr) minmax(300px,1fr);gap:16px;margin-top:20px}.panel{background:#0b1728eF;border:1px solid var(--line);border-radius:17px;padding:16px;min-width:0}.panel h2{margin:0 0 12px}.run-summary{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}.pill{padding:6px 10px;border-radius:999px;background:#1d2d43;color:var(--muted)}.pill.success{color:var(--green)}.pill.failed,.pill.cancelled{color:var(--red)}pre{height:520px;overflow:auto;margin:0;padding:14px;border-radius:12px;background:#020711;color:#d8e8f8;white-space:pre-wrap;word-break:break-word;font:13px/1.45 Consolas,monospace}
.stderr{color:#fda4af}.system{color:#7dd3fc;font-weight:700}.history{display:flex;flex-direction:column;gap:9px;max-height:600px;overflow:auto}.history-item{padding:11px;border:1px solid var(--line);border-radius:11px;background:#101d31}.history-item strong{display:block}.history-item small{color:var(--muted)}.result{margin:10px 0;color:var(--muted)}.refresh-notice{margin:10px 0 16px;padding:12px 14px;border:1px solid #0f766e;border-radius:12px;background:#064e3b44;color:#d1fae5}.refresh-notice.no-change{border-color:#475569;background:#1e293b99;color:#dbeafe}.refresh-notice[hidden]{display:none}a{color:var(--blue)}#cancelBtn{background:#7f1d1d;display:none;margin:0 0 12px}
.profile-admin{margin:0 0 20px;padding:18px;border:1px solid #2563eb;border-radius:17px;background:#0b1728ef}.profile-admin h2{margin:0}.profile-admin p{margin:5px 0 15px;color:var(--muted)}.profile-form{display:grid;grid-template-columns:minmax(180px,1.1fr) repeat(3,minmax(160px,1fr)) auto;gap:11px;align-items:end}.field{display:flex;flex-direction:column;gap:6px}.field label{font-size:12px;color:#c5d6e8;font-weight:800}.field input,.field select{width:100%;min-height:43px;padding:9px 11px;border:1px solid var(--line);border-radius:10px;background:#07111f;color:var(--text)}.profile-form button{min-height:43px;margin:0}.profile-notice{margin-top:12px;padding:10px 12px;border-radius:10px;background:#064e3b55;color:#d1fae5}.profile-notice.error{background:#7f1d1d55;color:#fecdd3}.profile-notice[hidden]{display:none}
.limits-admin{margin:0 0 20px;padding:18px;border:1px solid #0f766e;border-radius:17px;background:#064e3b1f}.limits-admin h2{margin:0}.limits-admin p{margin:5px 0 12px;color:var(--muted)}.limits-toolbar{display:grid;grid-template-columns:minmax(210px,1fr) minmax(210px,1fr) auto;gap:11px;align-items:end}.limits-actions{display:flex;gap:10px;flex-wrap:wrap}.limits-actions button{min-width:190px}.limits-notice{margin-top:12px;padding:10px 12px;border-radius:10px;background:#064e3b55;color:#d1fae5}.limits-notice.error{background:#7f1d1d55;color:#fecdd3}.limits-notice[hidden]{display:none}.limits-form{display:grid;gap:13px;margin-top:16px}.limit-section{padding:14px;border:1px solid var(--line);border-radius:12px;background:#07111f}.limit-section h3{margin:0 0 4px}.limit-section p{font-size:13px}.limit-fields{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px}.limit-field{display:flex;flex-direction:column;gap:5px}.limit-field label{font-size:12px;font-weight:800;color:#c5d6e8}.limit-field small{font-size:11px;color:var(--muted)}.limit-field input,.limits-toolbar input,.limits-toolbar select{width:100%;min-height:42px;padding:9px 11px;border:1px solid var(--line);border-radius:10px;background:#07111f;color:var(--text)}.limit-table{width:100%;border-collapse:collapse;margin-top:9px}.limit-table th,.limit-table td{padding:7px;text-align:left;border-bottom:1px solid #29405e}.limit-table input{width:100%;min-height:38px;padding:7px;border:1px solid var(--line);border-radius:8px;background:#020711;color:var(--text)}.limit-table button{margin:0;padding:7px 10px;background:#7f1d1d;min-width:0}.secondary-btn{background:#334155!important}.limits-description{font-size:12px;color:var(--muted);margin:0 0 6px}
@media(max-width:900px){.workspace{grid-template-columns:1fr}.hero{align-items:flex-start;flex-direction:column}pre{height:420px}}
@media(max-width:1200px){.profile-form{grid-template-columns:repeat(2,minmax(220px,1fr))}}
</style></head>
<body><main class="wrap">
<section class="hero"><div><h1>Центр управления PAPA BOT</h1><p>Тесты, диагностика, production и deployment с живым выводом и историей.</p></div><div class="hero-actions"><button id="refreshCommandsBtn">↻ Обновить команды</button><div id="serverBadge" class="badge">● Готов к работе</div></div></section>
<div id="registryInfo" class="result">Команды проекта загружаются…</div>
<div id="refreshNotice" class="refresh-notice" hidden></div>
<section class="profile-admin">
  <h2>Данные входа профилей</h2>
  <p>Выберите профиль и заполните только те значения, которые нужно заменить. Пустые поля сохраняют текущие данные. Текущий пароль никогда не показывается.</p>
  <div class="profile-form">
    <div class="field"><label for="profileCredentialId">Профиль</label><select id="profileCredentialId"><option>Загрузка…</option></select></div>
    <div class="field"><label for="profileNewUsername">Новый логин</label><input id="profileNewUsername" autocomplete="off" placeholder="Не менять"></div>
    <div class="field"><label for="profileNewPassword">Новый пароль</label><input id="profileNewPassword" type="password" autocomplete="new-password" placeholder="Не менять"></div>
    <div class="field"><label for="profileNewEmail">Новая почта</label><input id="profileNewEmail" type="email" autocomplete="off" placeholder="Не менять"></div>
    <button id="saveProfileCredentialsBtn">Сохранить данные</button>
  </div>
  <div id="profileCredentialNotice" class="profile-notice" hidden></div>
</section>
<section class="limits-admin">
  <h2>Профили лимитов, тарифов и бонусов</h2>
  <p>Выберите профиль значений или создайте новый. Все поля подписаны по-русски: изменяйте только нужные значения — остальные остаются такими, как сохранены в выбранном профиле. «Загрузить профиль в сервис» применяет его к новым операциям; уже купленные пакеты не меняются.</p>
  <div class="limits-toolbar"><div class="field"><label for="serviceLimitProfile">Профиль значений</label><select id="serviceLimitProfile"><option>Загрузка…</option></select></div><div class="field"><label for="newServiceLimitProfileName">Новый профиль</label><input id="newServiceLimitProfileName" placeholder="Например: Летняя акция"></div><button id="createServiceLimitProfileBtn" type="button">+ Создать профиль</button></div>
  <div id="serviceLimitProfileMeta" class="limits-description"></div>
  <div id="serviceLimitsForm" class="limits-form" aria-live="polite">Загрузка настроек…</div>
  <div class="limits-actions"><button id="saveServiceLimitProfileBtn" type="button">Сохранить изменения профиля</button><button id="applyServiceLimitProfileBtn" type="button">Загрузить профиль в сервис</button><button id="reloadServiceLimitsBtn" class="secondary-btn" type="button">↻ Обновить список</button></div>
  <div id="serviceLimitsNotice" class="limits-notice" hidden></div>
</section>
<section class="limits-admin" id="attachmentUploadSettings"><h2>Источники загрузки вложений</h2><p>Глобальная настройка применяется ко всем текущим и будущим профилям и сбрасывает их личные настройки. Она действует для Сообщений, Комментариев, Рассылки и Отложенных; фотографии в комментариях VK технически загружаются как вложение сообщества.</p><div class="limits-toolbar"><div class="field"><label>Изображения</label><select id="globalAttachmentImage"><option value="community">Сообщество</option><option value="user">Пользователь</option></select></div><div class="field"><label>Документы</label><select id="globalAttachmentDocument"><option value="user">Пользователь</option><option value="community">Сообщество</option></select></div><div class="field"><label>Видео / клипы</label><select id="globalAttachmentVideo"><option value="community">Сообщество</option><option value="user">Пользователь</option></select></div></div><div class="limits-actions"><button id="saveGlobalAttachmentSettingsBtn" type="button">Применить глобально ко всем</button></div><div class="limits-toolbar" style="margin-top:14px"><div class="field"><label>Профиль</label><select id="attachmentProfileId"></select></div><div class="field"><label>Изображения</label><select id="profileAttachmentImage"><option value="">Глобально</option><option value="community">Сообщество</option><option value="user">Пользователь</option></select></div><div class="field"><label>Документы</label><select id="profileAttachmentDocument"><option value="">Глобально</option><option value="user">Пользователь</option><option value="community">Сообщество</option></select></div><div class="field"><label>Видео / клипы</label><select id="profileAttachmentVideo"><option value="">Глобально</option><option value="community">Сообщество</option><option value="user">Пользователь</option></select></div></div><div class="limits-actions"><button id="saveProfileAttachmentSettingsBtn" type="button">Сохранить индивидуально</button></div><div id="attachmentSettingsNotice" class="limits-notice" hidden></div></section>
<section id="commandsGrid" class="grid">${cards}</section>
<section class="workspace">
  <div class="panel"><h2>Текущая команда</h2><button id="cancelBtn">Остановить команду</button><div id="summary" class="run-summary"><span class="pill">Команды ещё не запускались</span></div><div id="result" class="result">Здесь появится понятный итог выполнения.</div><pre id="output">Ожидание команды…</pre></div>
  <aside class="panel"><h2>История</h2><div id="history" class="history">Загрузка…</div></aside>
</section></main>
<script>
setTimeout(function(){
  function addUserVideoPrivacyField(anchorId, selectId, includeGlobal){const anchor=document.getElementById(anchorId);if(!anchor||document.getElementById(selectId))return;const field=document.createElement('div');field.className='field';field.innerHTML='<label>Приватность видео от пользователя</label><select id="'+selectId+'">'+(includeGlobal?'<option value="">Глобально</option>':'')+'<option value="all">Все пользователи</option><option value="friends">Только друзья</option><option value="friends_of_friends">Друзья и друзья друзей</option><option value="nobody">Только я</option></select><small>Используется, когда источник видео/клипов — «Пользователь».</small>';anchor.closest('.limits-toolbar').appendChild(field)}
  addUserVideoPrivacyField('globalAttachmentVideo','globalAttachmentUserVideoPrivacy',false);
  addUserVideoPrivacyField('profileAttachmentVideo','profileAttachmentUserVideoPrivacy',true);
  document.getElementById('saveGlobalAttachmentSettingsBtn').onclick=saveGlobalAttachmentSettings;document.getElementById('saveProfileAttachmentSettingsBtn').onclick=saveProfileAttachmentSettings;document.getElementById('attachmentProfileId').onchange=loadAttachmentProfileSettings;loadAttachmentSettings()
},0);
const TOKEN=${JSON.stringify(accessToken)};
let lastOutputLength=0;
let commandsSignature='';
function esc(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function duration(ms){if(!ms)return '—';const s=Math.round(ms/1000);return s<60?s+' сек.':Math.floor(s/60)+' мин. '+(s%60)+' сек.'}
async function api(url,options={}){options.headers=Object.assign({'X-Control-Token':TOKEN},options.headers||{});const r=await fetch(url,options);const data=await r.json();if(!r.ok)throw new Error(data.error||'Ошибка запроса');return data}
async function loadProfiles(){const select=document.getElementById('profileCredentialId');const selected=select.value;try{const data=await api('/api/profiles');select.innerHTML=data.profiles.map(p=>'<option value="'+esc(p.id)+'">'+esc(p.id+' — '+p.name+' ('+p.username+')')+'</option>').join('');if(data.profiles.some(p=>p.id===selected))select.value=selected}catch(e){select.innerHTML='<option value="">Не удалось загрузить профили</option>';showProfileNotice(e.message,true)}}
function showProfileNotice(message,isError){const notice=document.getElementById('profileCredentialNotice');notice.textContent=message;notice.className='profile-notice'+(isError?' error':'');notice.hidden=false}
async function saveProfileCredentials(){const button=document.getElementById('saveProfileCredentialsBtn');const profileId=document.getElementById('profileCredentialId').value;const payload={username:document.getElementById('profileNewUsername').value,password:document.getElementById('profileNewPassword').value,recoveryEmail:document.getElementById('profileNewEmail').value};if(!profileId)return showProfileNotice('Выберите профиль',true);if(!payload.username.trim()&&!payload.password.trim()&&!payload.recoveryEmail.trim())return showProfileNotice('Укажите новый логин, пароль или почту',true);if(!confirm('Сохранить новые данные входа выбранного профиля?'))return;button.disabled=true;try{const result=await api('/api/profiles/'+encodeURIComponent(profileId)+'/credentials',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});document.getElementById('profileNewUsername').value='';document.getElementById('profileNewPassword').value='';document.getElementById('profileNewEmail').value='';showProfileNotice('Данные профиля '+result.profile.name+' сохранены. Незаполненные поля не изменялись.',false);await loadProfiles()}catch(e){showProfileNotice(e.message,true)}finally{button.disabled=false}}
function attachmentValues(prefix){return {image:document.getElementById(prefix+'Image').value,document:document.getElementById(prefix+'Document').value,video:document.getElementById(prefix+'Video').value,userVideoPrivacy:(document.getElementById(prefix+'UserVideoPrivacy')||{value:''}).value}}
function attachmentNotice(message,isError){const el=document.getElementById('attachmentSettingsNotice');el.textContent=message;el.className='limits-notice'+(isError?' error':'');el.hidden=false}
async function loadAttachmentSettings(){try{const data=await api('/api/attachment-upload-settings');const g=data.global||{};const privacy=data.userVideoPrivacy||{};document.getElementById('globalAttachmentImage').value=g.image||'community';document.getElementById('globalAttachmentDocument').value=g.document||'user';document.getElementById('globalAttachmentVideo').value=g.video||'community';document.getElementById('globalAttachmentUserVideoPrivacy').value=privacy.global||'all';const profiles=(await api('/api/profiles')).profiles||[];const select=document.getElementById('attachmentProfileId');const selected=select.value;select.innerHTML=profiles.map(p=>'<option value="'+esc(p.id)+'">'+esc(p.id+' — '+p.name)+'</option>').join('');if(profiles.some(p=>p.id===selected))select.value=selected;await loadAttachmentProfileSettings()}catch(e){attachmentNotice(e.message,true)}}
async function loadAttachmentProfileSettings(){const id=document.getElementById('attachmentProfileId').value;if(!id)return;try{const data=await api('/api/attachment-upload-settings?profileId='+encodeURIComponent(id));const o=data.overrides||{};const privacy=data.userVideoPrivacy||{};document.getElementById('profileAttachmentImage').value=o.image||'';document.getElementById('profileAttachmentDocument').value=o.document||'';document.getElementById('profileAttachmentVideo').value=o.video||'';document.getElementById('profileAttachmentUserVideoPrivacy').value=privacy.override||''}catch(e){attachmentNotice(e.message,true)}}
async function saveGlobalAttachmentSettings(){if(!confirm('Применить глобальные источники загрузки ко всем профилям и сбросить индивидуальные настройки?'))return;try{await api('/api/attachment-upload-settings/global',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({values:attachmentValues('globalAttachment')})});attachmentNotice('Глобальные настройки применены ко всем профилям.',false);await loadAttachmentSettings()}catch(e){attachmentNotice(e.message,true)}}
async function saveProfileAttachmentSettings(){const profileId=document.getElementById('attachmentProfileId').value;if(!profileId)return;try{await api('/api/attachment-upload-settings/profiles/'+encodeURIComponent(profileId),{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({overrides:attachmentValues('profileAttachment')})});attachmentNotice('Индивидуальная настройка сохранена.',false);await loadAttachmentProfileSettings()}catch(e){attachmentNotice(e.message,true)}}
let serviceLimitProfiles=[];let activeServiceLimitProfileId='';
function showServiceLimitsNotice(message,isError){const notice=document.getElementById('serviceLimitsNotice');notice.textContent=message;notice.className='limits-notice'+(isError?' error':'');notice.hidden=false}
function limitField(id,label,value,description){return '<div class="limit-field"><label for="'+id+'">'+esc(label)+'</label><input id="'+id+'" type="number" min="0" step="1" value="'+esc(value)+'"><small>'+esc(description)+'</small></div>'}
function limitTable(kind,rows,withDuration){const headers=withDuration?'<tr><th>Стоимость, ₽</th><th>Запросы</th><th>Срок, минут</th><th></th></tr>':'<tr><th>Стоимость, ₽</th><th>Запросы</th><th></th></tr>';const body=(rows||[]).map((row,index)=>'<tr data-row-kind="'+kind+'"><td><input data-key="cost" type="number" min="1" value="'+esc(row.cost)+'"></td><td><input data-key="requests" type="number" min="1" value="'+esc(row.requests)+'"></td>'+(withDuration?'<td><input data-key="durationMinutes" type="number" min="1" value="'+esc(row.durationMinutes)+'"></td>':'')+'<td><button type="button" data-remove-limit-row="'+kind+'">Удалить</button></td></tr>').join('');return '<table class="limit-table"><thead>'+headers+'</thead><tbody id="'+kind+'Rows">'+body+'</tbody></table><button type="button" class="secondary-btn" data-add-limit-row="'+kind+'">+ Добавить позицию</button>'}
function bonusTable(rows){return '<table class="limit-table"><thead><tr><th>Сумма от, ₽</th><th>Бонус, %</th><th></th></tr></thead><tbody id="bonusRows">'+(rows||[]).map(row=>'<tr data-row-kind="bonus"><td><input data-key="from" type="number" min="1" value="'+esc(row.from)+'"></td><td><input data-key="percent" type="number" min="0" max="100" value="'+esc(row.percent)+'"></td><td><button type="button" data-remove-limit-row="bonus">Удалить</button></td></tr>').join('')+'</tbody></table><button type="button" class="secondary-btn" data-add-limit-row="bonus">+ Добавить уровень бонуса</button>'}
function renderServiceLimitForm(values){const v=values||{};const top=v.balanceTopUp||{};const reports=v.reports||{};document.getElementById('serviceLimitsForm').innerHTML='<section class="limit-section"><h3>Бесплатный тариф</h3><p>Сколько запросов в сутки получает профиль без платной подписки.</p><div class="limit-fields">'+limitField('freeDailyRequests','Бесплатные запросы в сутки',v.freeDailyRequests,'Минимум 1 запрос')+'</div></section><section class="limit-section"><h3>Пополнение баланса и бонусы</h3><p>Границы ручного пополнения и бонус, который добавляется при достижении суммы.</p><div class="limit-fields">'+limitField('balanceTopUpMin','Минимальная сумма пополнения, ₽',top.min,'Минимум 1 ₽')+limitField('balanceTopUpMax','Максимальная сумма пополнения, ₽',top.max,'Не меньше минимальной суммы')+'</div>'+bonusTable(top.bonuses)+'</section><section class="limit-section"><h3>Тарифы «Подписка»</h3><p>Стоимость, число запросов и срок подписки. Срок задаётся в минутах: 1 440 = сутки, 43 200 = 30 дней.</p>'+limitTable('subscription',(v.subscriptions||[]),true)+'</section><section class="limit-section"><h3>Тарифы «Пакеты»</h3><p>Разовые пакеты запросов вне суточного лимита профиля.</p>'+limitTable('extraPackage',(v.extraPackages||[]),false)+'</section><section class="limit-section"><h3>Ошибки и предложения</h3><p>Суточные ограничения отправки и бонусы, начисляемые после итогового статуса.</p><div class="limit-fields">'+limitField('bugDailyLimit','Ошибок от профиля в сутки',reports.bugDailyLimit,'Минимум 1')+limitField('suggestionDailyLimit','Предложений от профиля в сутки',reports.suggestionDailyLimit,'Минимум 1')+limitField('bugFixedReward','Бонус за исправленную ошибку',reports.bugFixedReward,'Можно указать 0')+limitField('suggestionImplementedReward','Бонус за реализованное предложение',reports.suggestionImplementedReward,'Можно указать 0')+'</div></section>'}
function numberValue(id){const el=document.getElementById(id);return el?el.value:''}
function tableValues(kind,withDuration){return Array.from(document.querySelectorAll('tr[data-row-kind="'+kind+'"]')).map(row=>{const item={};row.querySelectorAll('[data-key]').forEach(input=>item[input.dataset.key]=input.value);if(!withDuration)delete item.durationMinutes;return item})}
function readServiceLimitValues(){return {freeDailyRequests:numberValue('freeDailyRequests'),balanceTopUp:{min:numberValue('balanceTopUpMin'),max:numberValue('balanceTopUpMax'),bonuses:tableValues('bonus',false).map(item=>({from:item.from,percent:item.percent}))},subscriptions:tableValues('subscription',true),extraPackages:tableValues('extraPackage',false),reports:{bugDailyLimit:numberValue('bugDailyLimit'),suggestionDailyLimit:numberValue('suggestionDailyLimit'),bugFixedReward:numberValue('bugFixedReward'),suggestionImplementedReward:numberValue('suggestionImplementedReward')}}}
function selectedServiceLimitProfile(){return serviceLimitProfiles.find(profile=>profile.id===activeServiceLimitProfileId)||null}
function renderServiceLimitProfiles(data,selectedId){serviceLimitProfiles=data.profiles||[];activeServiceLimitProfileId=selectedId||data.activeProfileId||serviceLimitProfiles[0]?.id||'';const select=document.getElementById('serviceLimitProfile');select.innerHTML=serviceLimitProfiles.map(profile=>'<option value="'+esc(profile.id)+'">'+esc(profile.name)+(profile.isActive?' — загружен в сервис':'')+'</option>').join('');select.value=activeServiceLimitProfileId;const profile=selectedServiceLimitProfile();document.getElementById('serviceLimitProfileMeta').textContent=profile?(profile.isDefault?'Профиль по умолчанию: ':'')+(profile.description||'Без описания'):'Профиль не найден';renderServiceLimitForm(profile?.values||{})}
async function loadServiceLimitProfiles(selectedId){const button=document.getElementById('reloadServiceLimitsBtn');if(button)button.disabled=true;try{const data=await api('/api/service-limit-profiles');renderServiceLimitProfiles(data,selectedId);showServiceLimitsNotice('Профили значений загружены. Выберите профиль и измените только нужные поля.',false)}catch(e){showServiceLimitsNotice(e.message,true)}finally{if(button)button.disabled=false}}
async function createLimitProfile(){const input=document.getElementById('newServiceLimitProfileName');const name=input.value.trim();if(!name)return showServiceLimitsNotice('Введите название нового профиля значений',true);const button=document.getElementById('createServiceLimitProfileBtn');button.disabled=true;try{const profile=await api('/api/service-limit-profiles',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:name,values:readServiceLimitValues()})});input.value='';await loadServiceLimitProfiles(profile.id);showServiceLimitsNotice('Создан профиль «'+profile.name+'». Он пока не загружен в сервис.',false)}catch(e){showServiceLimitsNotice(e.message,true)}finally{button.disabled=false}}
async function saveLimitProfile(){const profile=selectedServiceLimitProfile();if(!profile)return;const button=document.getElementById('saveServiceLimitProfileBtn');button.disabled=true;try{const result=await api('/api/service-limit-profiles/'+encodeURIComponent(profile.id),{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({values:readServiceLimitValues()})});await loadServiceLimitProfiles(result.id);showServiceLimitsNotice('Изменения сохранены в профиле «'+result.name+'». Сервис пока использует прежний загруженный профиль.',false)}catch(e){showServiceLimitsNotice(e.message,true)}finally{button.disabled=false}}
async function applyLimitProfile(){const profile=selectedServiceLimitProfile();if(!profile)return;if(!confirm('Загрузить профиль «'+profile.name+'» в PAPA BOT? Новые операции будут использовать эти значения.'))return;const button=document.getElementById('applyServiceLimitProfileBtn');button.disabled=true;try{const result=await api('/api/service-limit-profiles/'+encodeURIComponent(profile.id)+'/activate',{method:'POST'});await loadServiceLimitProfiles(result.profile.id);showServiceLimitsNotice('Профиль «'+result.profile.name+'» загружен в сервис. Новые операции используют его значения.',false)}catch(e){showServiceLimitsNotice(e.message,true)}finally{button.disabled=false}}
document.getElementById('serviceLimitsForm').onclick=e=>{const add=e.target.closest('[data-add-limit-row]');if(add){const kind=add.dataset.addLimitRow;const target=document.getElementById(kind+'Rows');if(target){const withDuration=kind==='subscription';const row=document.createElement('tr');row.dataset.rowKind=kind;row.innerHTML=kind==='bonus'?'<td><input data-key="from" type="number" min="1"></td><td><input data-key="percent" type="number" min="0" max="100"></td><td><button type="button" data-remove-limit-row="bonus">Удалить</button></td>':'<td><input data-key="cost" type="number" min="1"></td><td><input data-key="requests" type="number" min="1"></td>'+(withDuration?'<td><input data-key="durationMinutes" type="number" min="1"></td>':'')+'<td><button type="button" data-remove-limit-row="'+kind+'">Удалить</button></td>';target.appendChild(row)}return}const remove=e.target.closest('[data-remove-limit-row]');if(remove){const row=remove.closest('tr');if(row)row.remove()}};
async function runCommand(id,dangerous){if(dangerous&&!confirm('Запустить полный deployment в production? Перед ним будет создан backup.'))return;try{await api('/api/run/'+id,{method:'POST'});lastOutputLength=0;poll()}catch(e){alert(e.message)}}
async function cancel(){if(!confirm('Остановить текущую команду?'))return;try{await api('/api/cancel',{method:'POST'})}catch(e){alert(e.message)}}
function refreshMessage(changes){const changed=changes.added.length+changes.updated.length+changes.removed.length;if(!changed)return 'Список не изменился: новых, обновлённых и удалённых команд нет. Всего доступно из PROJECT_COMMANDS.json: '+changes.total+'.';const parts=[];if(changes.added.length)parts.push('новых '+changes.added.length+' ('+changes.added.join(', ')+')');if(changes.updated.length)parts.push('обновлено '+changes.updated.length+' ('+changes.updated.join(', ')+')');if(changes.removed.length)parts.push('удалено '+changes.removed.length+' ('+changes.removed.join(', ')+')');return 'Реестр перечитан: '+parts.join('; ')+'. Всего доступно: '+changes.total+'.'}
async function refreshCommands(){const btn=document.getElementById('refreshCommandsBtn');const notice=document.getElementById('refreshNotice');btn.disabled=true;try{const result=await api('/api/refresh-commands',{method:'POST'});commandsSignature='';renderCommands(result.commands);renderRegistry(result.registry);const changed=result.changes.added.length+result.changes.updated.length+result.changes.removed.length;notice.textContent=refreshMessage(result.changes);notice.className='refresh-notice'+(changed?'':' no-change');notice.hidden=false;if(changed){const registryGroup=document.getElementById('registryCommandsGroup');if(registryGroup)registryGroup.scrollIntoView({behavior:'smooth',block:'start'})}}catch(e){notice.textContent=e.message;notice.className='refresh-notice';notice.hidden=false}finally{btn.disabled=false}}
function commandCard(c){const documentation=c.source==='documentation';return '<article class="command-card '+(c.source==='registry'?'registry-command ':'')+(c.dangerous?'danger':'')+'" data-command-card="'+esc(c.id)+'"><div><h3>'+esc(c.title)+'</h3><p>'+esc(c.description)+'</p><div class="command-source">'+(documentation?'Документация из PROJECT_COMMANDS.json':(c.source==='registry'?'Из PROJECT_COMMANDS.json':'Встроенная команда'))+'</div><ol>'+c.sequence.map(s=>'<li><strong>'+esc(s.label)+'</strong>'+(s.explanation?' — '+esc(s.explanation):'')+(s.relation?'<small>'+esc(s.relation)+'</small>':'')+'</li>').join('')+'</ol></div>'+(documentation?'<button disabled>Только описание</button>':'<button data-command="'+esc(c.id)+'" data-dangerous="'+(c.dangerous?'true':'false')+'">'+(c.dangerous?'Запустить deployment':'Запустить')+'</button>')+'</article>'}
function renderCommands(commands){const signature=JSON.stringify(commands);if(signature===commandsSignature)return;commandsSignature=signature;const builtin=commands.filter(c=>c.source==='builtin');const registry=commands.filter(c=>c.source==='registry');const documentation=commands.filter(c=>c.source==='documentation');document.getElementById('commandsGrid').className='';document.getElementById('commandsGrid').innerHTML='<section class="command-group"><h2>Встроенные команды</h2><div class="grid">'+builtin.map(commandCard).join('')+'</div></section><section class="command-group registry-group" id="registryCommandsGroup"><h2>Команды из PROJECT_COMMANDS.json — '+registry.length+'</h2><p class="command-group-note">Именно этот раздел изменяется кнопкой «Обновить команды».</p><div class="grid">'+registry.map(commandCard).join('')+'</div></section><section class="command-group registry-group"><h2>Документация команд — '+documentation.length+'</h2><p class="command-group-note">Команды здесь не запускаются из Центра управления: карточки показывают безопасную последовательность, файлы и назначение.</p><div class="grid">'+documentation.map(commandCard).join('')+'</div></section>'}
function renderRegistry(registry){const warnings=(registry.warnings||[]).length?' Предупреждения: '+registry.warnings.join('; '):'';document.getElementById('registryInfo').textContent='PROJECT_COMMANDS.json: документировано '+registry.documented+', доступно для запуска '+registry.runnable+'.'+warnings}
document.getElementById('commandsGrid').onclick=e=>{const b=e.target.closest('[data-command]');if(b)runCommand(b.dataset.command,b.dataset.dangerous==='true')};document.getElementById('cancelBtn').onclick=cancel;document.getElementById('refreshCommandsBtn').onclick=refreshCommands;document.getElementById('saveProfileCredentialsBtn').onclick=saveProfileCredentials;document.getElementById('reloadServiceLimitsBtn').onclick=()=>loadServiceLimitProfiles(activeServiceLimitProfileId);document.getElementById('createServiceLimitProfileBtn').onclick=createLimitProfile;document.getElementById('saveServiceLimitProfileBtn').onclick=saveLimitProfile;document.getElementById('applyServiceLimitProfileBtn').onclick=applyLimitProfile;document.getElementById('serviceLimitProfile').onchange=e=>{activeServiceLimitProfileId=e.target.value;const profile=selectedServiceLimitProfile();document.getElementById('serviceLimitProfileMeta').textContent=profile?(profile.isDefault?'Профиль по умолчанию: ':'')+(profile.description||'Без описания'):'';renderServiceLimitForm(profile?.values||{})};loadProfiles();loadServiceLimitProfiles();
function renderState(run){const buttons=document.querySelectorAll('[data-command]');const running=run&&run.status==='running';buttons.forEach(b=>b.disabled=running);document.getElementById('cancelBtn').style.display=running&&run.commandId!=='deploy'?'inline-block':'none';document.getElementById('serverBadge').textContent=running?'● Выполняется':'● Готов к работе';if(!run)return;
 document.getElementById('summary').innerHTML='<span class="pill '+esc(run.status)+'">'+esc(run.status)+'</span><span class="pill">'+esc(run.title)+'</span><span class="pill">'+duration(run.durationMs||(Date.now()-new Date(run.startedAt)))+'</span><span class="pill">код: '+esc(run.exitCode??'—')+'</span>';
 document.getElementById('result').textContent=run.result;const out=document.getElementById('output');if(run.output.length!==lastOutputLength){out.innerHTML=run.output.map(x=>'<span class="'+esc(x.stream)+'">'+esc(x.text)+'</span>').join('\\n');out.scrollTop=out.scrollHeight;lastOutputLength=run.output.length}}
function renderHistory(items){document.getElementById('history').innerHTML=items.length?items.map(x=>'<div class="history-item"><strong>'+esc(x.title)+'</strong><small>'+new Date(x.startedAt).toLocaleString('ru-RU')+' · '+duration(x.durationMs)+' · код '+esc(x.exitCode)+'</small><div class="'+esc(x.status)+'">'+esc(x.result)+'</div><a href="/api/log/'+encodeURIComponent(x.logFile)+'?token='+encodeURIComponent(TOKEN)+'" target="_blank">Открыть полный лог</a></div>').join(''):'История пока пуста.'}
async function poll(){try{const data=await api('/api/state');renderCommands(data.commands);renderRegistry(data.registry);renderState(data.active);renderHistory(data.history)}catch(e){document.getElementById('serverBadge').textContent='● Нет связи'}setTimeout(poll,800)}poll();
</script></body></html>`;
}

function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, char => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[char]));
}

const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://${host}`);
    if (req.method === 'GET' && url.pathname === '/api/health') {
        const address = server.address();
        return json(res, 200, {
            app: 'papa-bot-control-center',
            status: 'ok',
            port: address && typeof address === 'object' ? address.port : null
        });
    }
    if (req.method === 'GET' && url.pathname === '/') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
        return res.end(renderHtml());
    }
    if (req.method === 'GET' && url.pathname === '/api/state') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        return json(res, 200, { active: publicRun(activeRun), history, commands: publicCommands(), registry: registryStatus });
    }
    if (req.method === 'GET' && url.pathname === '/api/profiles') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            const result = await getPublicProfiles();
            return json(res, 200, result);
        } catch (error) {
            return json(res, 500, { error: 'Не удалось загрузить профили: ' + error.message });
        }
    }
    if (req.method === 'GET' && url.pathname === '/api/attachment-upload-settings') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            return json(res, 200, await getAttachmentUploadSettings(url.searchParams.get('profileId') || ''));
        } catch (error) {
            return json(res, 500, { error: error.message });
        }
    }
    if (req.method === 'PUT' && url.pathname === '/api/attachment-upload-settings/global') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            const body = JSON.parse(await readBody(req) || '{}');
            return json(res, 200, await saveGlobalAttachmentUploadSettings(body.values || {}, { updatedBy: 'control-center' }));
        } catch (error) {
            return json(res, 400, { error: error.message });
        }
    }
    const attachmentProfileSettingsMatch = req.method === 'PUT'
        ? url.pathname.match(/^\/api\/attachment-upload-settings\/profiles\/([^/]+)$/)
        : null;
    if (attachmentProfileSettingsMatch) {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            const body = JSON.parse(await readBody(req) || '{}');
            return json(res, 200, await saveProfileAttachmentUploadOverrides(decodeURIComponent(attachmentProfileSettingsMatch[1]), body.overrides || {}, { updatedBy: 'control-center' }));
        } catch (error) {
            return json(res, 400, { error: error.message });
        }
    }
    if (req.method === 'GET' && url.pathname === '/api/service-limits') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            return json(res, 200, await loadServiceLimits());
        } catch (error) {
            return json(res, 500, { error: 'Не удалось загрузить лимиты: ' + error.message });
        }
    }
    if (req.method === 'GET' && url.pathname === '/api/service-limit-profiles') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            return json(res, 200, await getServiceLimitProfiles({ defaultValues: await loadServiceLimits() }));
        } catch (error) {
            return json(res, 500, { error: 'Не удалось загрузить профили значений: ' + error.message });
        }
    }
    if (req.method === 'POST' && url.pathname === '/api/service-limit-profiles') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            const rawBody = await readBody(req);
            return json(res, 201, await createServiceLimitProfile(rawBody ? JSON.parse(rawBody) : {}, { defaultValues: await loadServiceLimits() }));
        } catch (error) {
            return json(res, 400, { error: error.message || 'Не удалось создать профиль значений' });
        }
    }
    const serviceLimitProfileMatch = req.method === 'PUT'
        ? url.pathname.match(/^\/api\/service-limit-profiles\/([^/]+)$/)
        : null;
    if (serviceLimitProfileMatch) {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            const rawBody = await readBody(req);
            return json(res, 200, await saveServiceLimitProfile(
                decodeURIComponent(serviceLimitProfileMatch[1]),
                rawBody ? JSON.parse(rawBody) : {}
            ));
        } catch (error) {
            return json(res, 400, { error: error.message || 'Не удалось сохранить профиль значений' });
        }
    }
    const activateServiceLimitProfileMatch = req.method === 'POST'
        ? url.pathname.match(/^\/api\/service-limit-profiles\/([^/]+)\/activate$/)
        : null;
    if (activateServiceLimitProfileMatch) {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            return json(res, 200, await activateServiceLimitProfile(
                decodeURIComponent(activateServiceLimitProfileMatch[1]),
                { saveLimits: saveServiceLimits }
            ));
        } catch (error) {
            return json(res, 400, { error: error.message || 'Не удалось загрузить профиль значений в сервис' });
        }
    }
    if (req.method === 'PUT' && url.pathname === '/api/service-limits') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            const rawBody = await readBody(req);
            const limits = normalizeServiceLimits(rawBody ? JSON.parse(rawBody) : {});
            return json(res, 200, await saveServiceLimits(limits));
        } catch (error) {
            return json(res, 400, { error: error.message || 'Не удалось сохранить лимиты' });
        }
    }
    const profileCredentialsMatch = req.method === 'POST'
        ? url.pathname.match(/^\/api\/profiles\/([^/]+)\/credentials$/)
        : null;
    if (profileCredentialsMatch) {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        try {
            const rawBody = await readBody(req);
            const payload = rawBody ? JSON.parse(rawBody) : {};
            const result = await updateControlCenterProfileCredentials(
                decodeURIComponent(profileCredentialsMatch[1]),
                payload
            );
            return json(res, 200, result);
        } catch (error) {
            return json(res, 400, { error: error.message || 'Не удалось сохранить данные профиля' });
        }
    }
    if (req.method === 'POST' && url.pathname === '/api/refresh-commands') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        if (activeRun && activeRun.status === 'running') {
            return json(res, 409, { error: 'Дождитесь завершения текущей команды' });
        }
        try {
            const previousCommands = registryCommandSnapshot();
            loadProjectCommandRegistry();
            const commands = publicCommands();
            const changes = diffRegistryCommandSnapshots(previousCommands, registryCommandSnapshot(commands));
            return json(res, 200, { success: true, commands, registry: registryStatus, changes });
        } catch (error) {
            registryStatus = { ...registryStatus, error: error.message };
            return json(res, 400, { error: 'Не удалось обновить команды: ' + error.message });
        }
    }
    if (req.method === 'POST' && url.pathname.startsWith('/api/run/')) {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        await readBody(req);
        const commandId = decodeURIComponent(url.pathname.slice('/api/run/'.length));
        const command = COMMANDS[commandId];
        if (!command) return json(res, 404, { error: 'Команда не найдена' });
        startCommand(commandId).catch(error => {
            if (activeRun) {
                activeRun.status = 'failed';
                activeRun.result = error.message;
                writeLine(activeRun, error.stack || error.message, 'stderr');
            }
        });
        return json(res, 202, { success: true, runId: activeRun.id });
    }
    if (req.method === 'POST' && url.pathname === '/api/cancel') {
        if (!authorized(req)) return json(res, 403, { error: 'Нет доступа' });
        if (!activeRun || activeRun.status !== 'running') return json(res, 409, { error: 'Нет выполняемой команды' });
        if (activeRun.commandId === 'deploy') {
            return json(res, 409, { error: 'Deployment нельзя прерывать из панели. Дождитесь его итогового статуса.' });
        }
        activeRun.cancelRequested = true;
        if (currentChild && !currentChild.killed) currentChild.kill();
        return json(res, 200, { success: true });
    }
    if (req.method === 'GET' && url.pathname.startsWith('/api/log/')) {
        if (url.searchParams.get('token') !== accessToken) {
            res.writeHead(403);
            return res.end('Нет доступа');
        }
        const fileName = safeLogName(decodeURIComponent(url.pathname.slice('/api/log/'.length)));
        const filePath = path.join(logsDir, fileName);
        if (!fileName || !fs.existsSync(filePath)) {
            res.writeHead(404);
            return res.end('Лог не найден');
        }
        res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
        return fs.createReadStream(filePath).pipe(res);
    }
    json(res, 404, { error: 'Маршрут не найден' });
});

function probeControlCenter(port, timeoutMs = 700) {
    return new Promise(resolve => {
        const request = http.get({ host, port, path: '/api/health', timeout: timeoutMs }, response => {
            let body = '';
            response.setEncoding('utf8');
            response.on('data', chunk => {
                body += chunk;
                if (body.length > 4096) request.destroy();
            });
            response.on('end', () => {
                try {
                    const data = JSON.parse(body);
                    resolve(response.statusCode === 200 && data.app === 'papa-bot-control-center'
                        ? { port: Number(data.port) || port }
                        : null);
                } catch {
                    resolve(null);
                }
            });
        });
        request.on('timeout', () => request.destroy());
        request.on('error', () => resolve(null));
    });
}

function listenOnce(serverInstance, port) {
    return new Promise((resolve, reject) => {
        const onError = error => {
            serverInstance.off('listening', onListening);
            if (error && (error.code === 'EADDRINUSE' || error.code === 'EACCES')) return resolve(false);
            reject(error);
        };
        const onListening = () => {
            serverInstance.off('error', onError);
            resolve(true);
        };
        serverInstance.once('error', onError);
        serverInstance.once('listening', onListening);
        serverInstance.listen(port, host);
    });
}

async function listenOnAvailablePort(serverInstance, firstPort, attempts = maxPortAttempts) {
    for (let offset = 0; offset < attempts; offset += 1) {
        const port = firstPort + offset;
        if (await listenOnce(serverInstance, port)) return port;
    }
    const error = new Error(`No free local port found in range ${firstPort}-${firstPort + attempts - 1}`);
    error.code = 'NO_FREE_CONTROL_CENTER_PORT';
    throw error;
}

function openBrowser(url) {
    if (process.platform !== 'win32' || process.env.PAPA_BOT_NO_BROWSER) return;
    const opener = spawn('explorer.exe', [url], {
        detached: true,
        stdio: 'ignore',
        windowsHide: true
    });
    opener.on('error', error => {
        console.error('Не удалось автоматически открыть браузер:', error.message);
        console.error('Откройте адрес вручную:', url);
    });
    opener.unref();
}

async function startControlCenter(options = {}) {
    const requestedPort = Number(options.port) || preferredPort;
    try {
        loadProjectCommandRegistry();
    } catch (error) {
        registryStatus = { ...registryStatus, error: error.message };
        console.error('PROJECT_COMMANDS.json не загружен:', error.message);
    }

    const existing = await probeControlCenter(requestedPort);
    if (existing) {
        const existingUrl = `http://${host}:${existing.port}`;
        console.log(`Центр управления PAPA BOT уже работает: ${existingUrl}`);
        openBrowser(existingUrl);
        return { reused: true, port: existing.port, url: existingUrl };
    }

    const selectedPort = await listenOnAvailablePort(server, requestedPort);
    const url = `http://${host}:${selectedPort}`;
    if (selectedPort !== requestedPort) {
        console.warn(`Порт ${requestedPort} занят другой программой. Используется свободный порт ${selectedPort}.`);
    }
    console.log(`\nЦентр управления PAPA BOT запущен: ${url}`);
    console.log('Не закрывайте это окно, пока работаете с панелью.\n');
    openBrowser(url);
    return { reused: false, port: selectedPort, url, server };
}

if (require.main === module) {
    startControlCenter().catch(error => {
        console.error('Не удалось запустить Центр управления PAPA BOT.');
        console.error(error.stack || error.message);
        process.exitCode = 1;
    });
}

module.exports = {
    COMMANDS,
    projectRoot,
    registryFile,
    safeLogName,
    redactSensitive,
    compileRegistryCommands,
    loadProjectCommandRegistry,
    publicCommands,
    registryCommandSnapshot,
    diffRegistryCommandSnapshots,
    updateControlCenterProfileCredentials,
    renderHtml,
    probeControlCenter,
    listenOnAvailablePort,
    openBrowser,
    startControlCenter
};
