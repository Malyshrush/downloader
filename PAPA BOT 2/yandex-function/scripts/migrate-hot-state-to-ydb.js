const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { buildEventRuntimeConfig } = require('../../src/modules/event-runtime-config');

require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const part = argv[index];
    if (!part.startsWith('--')) continue;
    const [rawKey, inlineValue] = part.slice(2).split('=');
    if (inlineValue !== undefined) {
      args[rawKey] = inlineValue;
      continue;
    }
    const next = argv[index + 1];
    args[rawKey] = next && !next.startsWith('--') ? next : 'true';
    if (next && !next.startsWith('--')) {
      index += 1;
    }
  }
  return args;
}

function createDocumentClient(config) {
  const client = new DynamoDBClient({
    region: config.ymqRegion,
    endpoint: config.ydbDocApiEndpoint,
    credentials: {
      accessKeyId: config.awsAccessKeyId,
      secretAccessKey: config.awsSecretAccessKey
    }
  });

  return DynamoDBDocumentClient.from(client, {
    marshallOptions: {
      removeUndefinedValues: true
    }
  });
}

function getLatestBackupDir() {
  const backupsRoot = path.join(__dirname, '..', '..', 'backups');
  const entries = fs.readdirSync(backupsRoot, { withFileTypes: true })
    .filter(entry => entry.isDirectory() && entry.name.startsWith('cloud-state-'))
    .map(entry => ({
      name: entry.name,
      fullPath: path.join(backupsRoot, entry.name),
      mtimeMs: fs.statSync(path.join(backupsRoot, entry.name)).mtimeMs
    }))
    .sort((left, right) => right.mtimeMs - left.mtimeMs);

  if (!entries.length) {
    throw new Error('No cloud-state backup directories found');
  }

  return entries[0].fullPath;
}

function loadManifest(backupDir) {
  const manifestPath = path.join(backupDir, 'manifest.json');
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`Backup manifest not found: ${manifestPath}`);
  }
  const raw = fs.readFileSync(manifestPath, 'utf8').replace(/^\uFEFF/, '');
  return JSON.parse(raw);
}

function isHotStateEntry(entry) {
  return entry && typeof entry.key === 'string' && entry.key.endsWith('.json');
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const runtimeConfig = buildEventRuntimeConfig({
    ...process.env,
    YDB_DOCAPI_ENDPOINT: args['ydb-docapi-endpoint'] || process.env.YDB_DOCAPI_ENDPOINT || '',
    YMQ_INCOMING_QUEUE_URL: process.env.YMQ_INCOMING_QUEUE_URL || 'pending',
    YMQ_OUTBOUND_QUEUE_URL: process.env.YMQ_OUTBOUND_QUEUE_URL || 'pending'
  });

  if (!runtimeConfig.awsAccessKeyId || !runtimeConfig.awsSecretAccessKey) {
    throw new Error('AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required');
  }
  if (!runtimeConfig.ydbDocApiEndpoint) {
    throw new Error('YDB_DOCAPI_ENDPOINT or --ydb-docapi-endpoint is required');
  }

  const backupDir = args['backup-dir'] || getLatestBackupDir();
  const tableName = args['hot-state-table'] || runtimeConfig.ydbHotStateTable;
  const manifest = loadManifest(backupDir);
  const hotEntries = manifest.filter(isHotStateEntry);
  const documentClient = createDocumentClient(runtimeConfig);

  let migratedCount = 0;
  for (const entry of hotEntries) {
    const objectPath = path.join(backupDir, entry.file);
    const jsonText = fs.readFileSync(objectPath, 'utf8');
    JSON.parse(jsonText);

    await documentClient.send(new PutCommand({
      TableName: tableName,
      Item: {
        objectKey: entry.key,
        jsonText,
        objectHash: crypto.createHash('sha256').update(jsonText).digest('hex'),
        sourceEtag: entry.etag || '',
        sourceLastModified: entry.last_modified || '',
        migratedAt: new Date().toISOString()
      }
    }));
    migratedCount += 1;
  }

  process.stdout.write(JSON.stringify({
    backupDir,
    tableName,
    migratedCount
  }, null, 2) + '\n');
}

main().catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
