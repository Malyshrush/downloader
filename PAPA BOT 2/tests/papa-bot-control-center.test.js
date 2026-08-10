const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');

const { COMMANDS, projectRoot, safeLogName, redactSensitive } = require('../scripts/papa-bot-control-center');

test('control center exposes only predefined operational commands', () => {
  assert.deepEqual(Object.keys(COMMANDS), [
    'quick_tests',
    'full_tests',
    'syntax',
    'version',
    'production_status',
    'production_logs',
    'smoke',
    'deploy'
  ]);
  assert.equal(COMMANDS.deploy.dangerous, true);
  assert.match(COMMANDS.deploy.description, /backup/);
  assert.equal(typeof COMMANDS.deploy.steps, 'function');
});

test('control center exposes project registry refresh helpers', () => {
  const controlCenter = require('../scripts/papa-bot-control-center');
  assert.equal(typeof controlCenter.compileRegistryCommands, 'function');
  assert.equal(typeof controlCenter.loadProjectCommandRegistry, 'function');
  assert.equal(typeof controlCenter.publicCommands, 'function');
  assert.equal(typeof controlCenter.registryCommandSnapshot, 'function');
  assert.equal(typeof controlCenter.diffRegistryCommandSnapshots, 'function');
  assert.match(controlCenter.registryFile, /PROJECT_COMMANDS\.json$/);
  assert.equal(typeof controlCenter.probeControlCenter, 'function');
  assert.equal(typeof controlCenter.listenOnAvailablePort, 'function');
  assert.equal(typeof controlCenter.openBrowser, 'function');
  assert.equal(typeof controlCenter.updateControlCenterProfileCredentials, 'function');
});

test('profile credential editor forwards only entered values and never returns a password', async () => {
  const { updateControlCenterProfileCredentials } = require('../scripts/papa-bot-control-center');
  let received;
  let clearedUsername;
  const result = await updateControlCenterProfileCredentials('2', {
    username: '',
    password: 'new-secret',
    recoveryEmail: ''
  }, {
    updateAdminProfileCredentials: async (profileId, changes) => {
      received = { profileId, changes };
      return { id: profileId, name: 'Profile 2', username: 'saved-login', recoveryEmail: 'old@example.com' };
    },
    clearLoginLock: async username => { clearedUsername = username; }
  });

  assert.deepEqual(received, {
    profileId: '2',
    changes: { username: '', password: 'new-secret', recoveryEmail: '' }
  });
  assert.equal(clearedUsername, 'saved-login');
  assert.equal(result.changed.password, true);
  assert.equal(result.changed.username, false);
  assert.equal(Object.hasOwn(result.profile, 'password'), false);
});

test('profile credential editor rejects an entirely empty update', async () => {
  const { updateControlCenterProfileCredentials } = require('../scripts/papa-bot-control-center');
  await assert.rejects(updateControlCenterProfileCredentials('1', {}), /Укажите новый логин/);
});

test('control center UI contains profile selector and three optional replacement fields', () => {
  const source = require('node:fs').readFileSync(
    require('node:path').join(projectRoot, 'scripts', 'papa-bot-control-center.js'),
    'utf8'
  );
  assert.match(source, /id="profileCredentialId"/);
  assert.match(source, /id="profileNewUsername"/);
  assert.match(source, /id="profileNewPassword" type="password"/);
  assert.match(source, /id="profileNewEmail" type="email"/);
  assert.match(source, /Пустые поля сохраняют текущие данные/);
  assert.match(source, /\/api\/profiles\/([^/]+)\/credentials/);
});

test('control center exposes Russian editable limit profiles and Mini App command documentation', () => {
  const source = require('node:fs').readFileSync(
    require('node:path').join(projectRoot, 'scripts', 'papa-bot-control-center.js'),
    'utf8'
  );
  assert.match(source, /id="serviceLimitProfile"/);
  assert.match(source, /id="newServiceLimitProfileName"/);
  assert.match(source, /Профили лимитов, тарифов и бонусов/);
  assert.match(source, /\/api\/service-limit-profiles/);
  assert.match(source, /Загрузить профиль в сервис/);
  assert.doesNotMatch(source, /id="serviceLimitsJson"/);
  assert.match(source, /Документация команд/);
  assert.match(source, /Только описание/);
  assert.match(source, /registryDocumentation/);
});

test('rendered Control Center browser script remains syntactically valid', () => {
  const { renderHtml } = require('../scripts/papa-bot-control-center');
  const html = renderHtml();
  const inlineScript = html.match(/<script>([\s\S]*?)<\/script>/)?.[1] || '';
  assert.ok(inlineScript.length > 100);
  assert.doesNotThrow(() => new vm.Script(inlineScript));
});

test('registry refresh reports actual additions, edits and removals instead of total runnable count', () => {
  const { diffRegistryCommandSnapshots } = require('../scripts/papa-bot-control-center');
  const changes = diffRegistryCommandSnapshots(
    [
      { id: 'unchanged', title: 'Без изменений', signature: 'same' },
      { id: 'edited', title: 'Старая команда', signature: 'before' },
      { id: 'removed', title: 'Удалённая команда', signature: 'removed' }
    ],
    [
      { id: 'unchanged', title: 'Без изменений', signature: 'same' },
      { id: 'edited', title: 'Обновлённая команда', signature: 'after' },
      { id: 'added', title: 'Новая команда', signature: 'new' }
    ]
  );
  assert.deepEqual(changes, {
    added: ['Новая команда'],
    updated: ['Обновлённая команда'],
    removed: ['Удалённая команда'],
    total: 3
  });
});

test('refresh UI clearly distinguishes no changes from newly added commands', () => {
  const source = require('node:fs').readFileSync(
    require('node:path').join(projectRoot, 'scripts', 'papa-bot-control-center.js'),
    'utf8'
  );
  assert.match(source, /Список не изменился: новых, обновлённых и удалённых команд нет/);
  assert.match(source, /Команды из PROJECT_COMMANDS\.json/);
  assert.match(source, /Именно этот раздел изменяется кнопкой/);
  assert.doesNotMatch(source, /Добавлено запускаемых команд/);
});

test('control center health endpoint and fixed-port recovery are present', () => {
  const source = require('node:fs').readFileSync(
    require('node:path').join(projectRoot, 'scripts', 'papa-bot-control-center.js'),
    'utf8'
  );
  assert.match(source, /\/api\/health/);
  assert.match(source, /EADDRINUSE/);
  assert.match(source, /listenOnAvailablePort\(server, requestedPort\)/);
  assert.match(source, /spawn\('explorer\.exe', \[url\]/);
});

test('Windows launcher keeps startup failures visible and writes a startup log', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const launcher = fs.readFileSync(path.join(projectRoot, 'ЗАПУСТИТЬ ЦЕНТР УПРАВЛЕНИЯ.cmd'), 'utf8');
  const powershell = fs.readFileSync(path.join(projectRoot, 'scripts', 'start-control-center.ps1'), 'utf8');
  assert.match(launcher, /start-control-center\.ps1/);
  assert.match(launcher, /%~dp0/);
  assert.match(launcher, /%SystemRoot%\\System32\\WindowsPowerShell\\v1\.0\\powershell\.exe/);
  assert.doesNotMatch(launcher, /if errorlevel/i);
  assert.doesNotMatch(launcher, /[^\x00-\x7F]/);
  assert.match(powershell, /control-center-startup\.log/);
  assert.match(powershell, /Read-Host 'Press ENTER after reading the error'/);
  assert.match(powershell, /Get-Command node\.exe/);
});

test('occupied preferred port falls back to another free local port', async () => {
  const http = require('node:http');
  const blocker = http.createServer((request, response) => response.end('occupied'));
  await new Promise((resolve, reject) => {
    blocker.once('error', reject);
    blocker.listen(0, '127.0.0.1', resolve);
  });
  const occupiedPort = blocker.address().port;
  const candidate = http.createServer((request, response) => response.end('control-center'));

  try {
    const { listenOnAvailablePort } = require('../scripts/papa-bot-control-center');
    const selectedPort = await listenOnAvailablePort(candidate, occupiedPort, 10);
    assert.notEqual(selectedPort, occupiedPort);
    assert.equal(candidate.address().port, selectedPort);
  } finally {
    await Promise.all([
      new Promise(resolve => blocker.close(resolve)),
      new Promise(resolve => candidate.listening ? candidate.close(resolve) : resolve())
    ]);
  }
});

test('deployment includes version synchronization and production smoke after cloud deployment', () => {
  const steps = COMMANDS.deploy.steps();
  assert.equal(steps.length, 3);
  assert.deepEqual(steps[0].args, ['scripts/deploy.js']);
  assert.match(steps[1].args.join(' '), /saveBotVersionData/);
  assert.match(steps[2].args.join(' '), /functions\.yandexcloud\.net/);
  assert.match(COMMANDS.deploy.description, /main\/worker\/sender/);
});

test('control center exposes global and per-profile user video privacy controls', () => {
  const source = require('node:fs').readFileSync(
    require('node:path').join(projectRoot, 'scripts', 'papa-bot-control-center.js'),
    'utf8'
  );
  assert.match(source, /globalAttachmentUserVideoPrivacy/);
  assert.match(source, /profileAttachmentUserVideoPrivacy/);
  assert.match(source, /userVideoPrivacy/);
});

test('full tests are discovered from the project tests directory without shell execution', () => {
  const step = COMMANDS.full_tests.steps()[0];
  assert.equal(step.file, process.execPath);
  assert.equal(step.args[0], '--test');
  assert.ok(step.args.some(file => file.endsWith('papa-bot-control-center.test.js')));
  assert.equal(projectRoot.endsWith('PAPA BOT 2'), true);
});

test('saved log names cannot escape the operation log directory', () => {
  assert.equal(safeLogName('../../secret.txt'), '....secret.txt');
  assert.doesNotMatch(safeLogName('..\\..\\secret.txt'), /[\\/]/);
});

test('live output and saved logs redact deployment credentials', () => {
  assert.equal(redactSensitive('  USER_TOKEN: vk1.private-value'), '  USER_TOKEN: ***');
  assert.equal(redactSensitive('  "AWS_SECRET_ACCESS_KEY": "secret-value",'), '  "AWS_SECRET_ACCESS_KEY": ***');
  assert.equal(redactSensitive('status: ACTIVE'), 'status: ACTIVE');
});
