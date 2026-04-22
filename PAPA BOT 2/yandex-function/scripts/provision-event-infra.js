const fs = require('fs');
const path = require('path');
const { CreateTableCommand, DescribeTableCommand, DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { CreateQueueCommand, GetQueueUrlCommand, SQSClient } = require('@aws-sdk/client-sqs');
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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function createSqsClient(config) {
  return new SQSClient({
    region: config.ymqRegion,
    endpoint: config.ymqEndpoint,
    credentials: {
      accessKeyId: config.awsAccessKeyId,
      secretAccessKey: config.awsSecretAccessKey
    }
  });
}

function createDynamoClient(config) {
  return new DynamoDBClient({
    region: config.ymqRegion,
    endpoint: config.ydbDocApiEndpoint,
    credentials: {
      accessKeyId: config.awsAccessKeyId,
      secretAccessKey: config.awsSecretAccessKey
    }
  });
}

async function ensureQueue(client, queueName, visibilityTimeoutSeconds) {
  try {
    const existing = await client.send(new GetQueueUrlCommand({ QueueName: queueName }));
    return {
      queueName,
      queueUrl: existing.QueueUrl
    };
  } catch (error) {
    if (error.name !== 'QueueDoesNotExist') {
      throw error;
    }
  }

  const created = await client.send(new CreateQueueCommand({
    QueueName: queueName,
    Attributes: {
      VisibilityTimeout: String(visibilityTimeoutSeconds),
      MessageRetentionPeriod: String(4 * 24 * 60 * 60)
    }
  }));

  return {
    queueName,
    queueUrl: created.QueueUrl
  };
}

async function describeTable(client, tableName) {
  try {
    const response = await client.send(new DescribeTableCommand({ TableName: tableName }));
    return response.Table || null;
  } catch (error) {
    if (error.name === 'ResourceNotFoundException') {
      return null;
    }
    throw error;
  }
}

async function waitForTableActive(client, tableName) {
  for (;;) {
    const table = await describeTable(client, tableName);
    if (table && table.TableStatus === 'ACTIVE') {
      return table;
    }
    await sleep(3000);
  }
}

async function ensureTable(client, tableName, keySchema, attributeDefinitions) {
  const existing = await describeTable(client, tableName);
  if (existing) {
    return {
      tableName,
      tableStatus: existing.TableStatus
    };
  }

  await client.send(new CreateTableCommand({
    TableName: tableName,
    BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: attributeDefinitions,
    KeySchema: keySchema
  }));

  const active = await waitForTableActive(client, tableName);
  return {
    tableName,
    tableStatus: active.TableStatus
  };
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

  const incomingQueueName = args['incoming-queue-name'] || 'vk-bot-2-incoming';
  const outboundQueueName = args['outbound-queue-name'] || 'vk-bot-2-outbound';
  const idempotencyTableName = args['idempotency-table'] || runtimeConfig.ydbIdempotencyTable;
  const hotStateTableName = args['hot-state-table'] || runtimeConfig.ydbHotStateTable;
  const appLogsTableName = args['app-logs-table'] || runtimeConfig.ydbAppLogsTable;
  const userStateTableName = args['user-state-table'] || runtimeConfig.ydbUserStateTable;

  const sqsClient = createSqsClient(runtimeConfig);
  const dynamoClient = createDynamoClient(runtimeConfig);

  const incomingQueue = await ensureQueue(sqsClient, incomingQueueName, runtimeConfig.idempotencyLeaseSeconds);
  const outboundQueue = await ensureQueue(sqsClient, outboundQueueName, runtimeConfig.idempotencyLeaseSeconds);
  const idempotencyTable = await ensureTable(
    dynamoClient,
    idempotencyTableName,
    [
      {
        AttributeName: 'eventId',
        KeyType: 'HASH'
      }
    ],
    [
      {
        AttributeName: 'eventId',
        AttributeType: 'S'
      }
    ]
  );
  const hotStateTable = await ensureTable(
    dynamoClient,
    hotStateTableName,
    [
      {
        AttributeName: 'objectKey',
        KeyType: 'HASH'
      }
    ],
    [
      {
        AttributeName: 'objectKey',
        AttributeType: 'S'
      }
    ]
  );
  const appLogsTable = await ensureTable(
    dynamoClient,
    appLogsTableName,
    [
      {
        AttributeName: 'logScope',
        KeyType: 'HASH'
      },
      {
        AttributeName: 'logId',
        KeyType: 'RANGE'
      }
    ],
    [
      {
        AttributeName: 'logScope',
        AttributeType: 'S'
      },
      {
        AttributeName: 'logId',
        AttributeType: 'S'
      }
    ]
  );
  const userStateTable = await ensureTable(
    dynamoClient,
    userStateTableName,
    [
      {
        AttributeName: 'userScope',
        KeyType: 'HASH'
      },
      {
        AttributeName: 'userId',
        KeyType: 'RANGE'
      }
    ],
    [
      {
        AttributeName: 'userScope',
        AttributeType: 'S'
      },
      {
        AttributeName: 'userId',
        AttributeType: 'S'
      }
    ]
  );

  const output = {
    createdAt: new Date().toISOString(),
    ymqEndpoint: runtimeConfig.ymqEndpoint,
    ydbDocApiEndpoint: runtimeConfig.ydbDocApiEndpoint,
    incomingQueue,
    outboundQueue,
    idempotencyTable,
    hotStateTable,
    appLogsTable,
    userStateTable
  };

  const outputPath = path.join(__dirname, '..', 'event-infra.generated.json');
  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2), 'utf8');
  process.stdout.write(JSON.stringify(output, null, 2) + '\n');
}

main().catch(error => {
  process.stderr.write(String(error && error.stack ? error.stack : error) + '\n');
  process.exit(1);
});
