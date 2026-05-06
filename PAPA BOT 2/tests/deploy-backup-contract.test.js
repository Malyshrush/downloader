const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const deployScript = fs.readFileSync(path.join(__dirname, '..', 'scripts', 'deploy.js'), 'utf8');

assert.match(deployScript, /backupsDir:\s*path\.join\(__dirname,\s*'\.\.',\s*'backup_papa_bot'\)/);
assert.match(deployScript, /--skip-backup ignored: mandatory project backup is required before deploy\./);
assert.match(deployScript, /createBackup\(\);\s*\n\s*provisionEventInfra\(\);/);
assert.doesNotMatch(deployScript, /if\s*\(!skipBackup\)\s*{\s*createBackup\(\);\s*}/);
assert.match(deployScript, /'backup_papa_bot'/);
assert.match(deployScript, /'node_modules'/);
assert.match(deployScript, /'dist'/);

const gitignore = fs.readFileSync(path.join(__dirname, '..', '.gitignore'), 'utf8');
assert.match(gitignore, /^backup_papa_bot\/$/m);

const deploy = require('../scripts/deploy');
const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'papa-deploy-backup-'));
const projectRoot = path.join(tmpRoot, 'PAPA BOT 2');
const backupRoot = path.join(projectRoot, 'backup_papa_bot');
fs.mkdirSync(path.join(projectRoot, 'src'), { recursive: true });
fs.mkdirSync(path.join(projectRoot, 'node_modules', 'pkg'), { recursive: true });
fs.mkdirSync(path.join(projectRoot, 'dist'), { recursive: true });
fs.mkdirSync(path.join(projectRoot, '.git'), { recursive: true });
fs.mkdirSync(path.join(projectRoot, 'backup_papa_bot', 'old'), { recursive: true });
fs.writeFileSync(path.join(projectRoot, 'adminPanelHTML.js'), 'admin panel');
fs.writeFileSync(path.join(projectRoot, 'src', 'handler.js'), 'handler');
fs.writeFileSync(path.join(projectRoot, '.env'), 'SECRET=kept-in-local-backup');
fs.writeFileSync(path.join(projectRoot, 'node_modules', 'pkg', 'ignored.js'), 'ignored');
fs.writeFileSync(path.join(projectRoot, 'dist', 'ignored.js'), 'ignored');
fs.writeFileSync(path.join(projectRoot, '.git', 'ignored'), 'ignored');

const originalProjectRoot = deploy.CONFIG.projectRoot;
const originalBackupsDir = deploy.CONFIG.backupsDir;
try {
    deploy.CONFIG.projectRoot = projectRoot;
    deploy.CONFIG.backupsDir = backupRoot;
    const backupPath = deploy.createBackup();

    assert.equal(path.basename(backupPath), 'PAPA BOT 2');
    assert.equal(fs.readFileSync(path.join(backupPath, 'adminPanelHTML.js'), 'utf8'), 'admin panel');
    assert.equal(fs.readFileSync(path.join(backupPath, 'src', 'handler.js'), 'utf8'), 'handler');
    assert.equal(fs.readFileSync(path.join(backupPath, '.env'), 'utf8'), 'SECRET=kept-in-local-backup');
    assert.ok(fs.existsSync(path.join(path.dirname(backupPath), 'backup-manifest.json')));
    assert.equal(fs.existsSync(path.join(backupPath, 'node_modules')), false);
    assert.equal(fs.existsSync(path.join(backupPath, 'dist')), false);
    assert.equal(fs.existsSync(path.join(backupPath, '.git')), false);
    assert.equal(fs.existsSync(path.join(backupPath, 'backup_papa_bot')), false);
} finally {
    deploy.CONFIG.projectRoot = originalProjectRoot;
    deploy.CONFIG.backupsDir = originalBackupsDir;
    fs.rmSync(tmpRoot, { recursive: true, force: true });
}

console.log('PASS deploy creates mandatory project backup before deploy');
