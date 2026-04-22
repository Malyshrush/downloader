const crypto = require('crypto');
const { buildEventRuntimeConfig } = require('./event-runtime-config');
const { log } = require('../utils/logger');

const DEFAULT_BUCKET_NAME = 'bot-data-storage';

function cloneValue(value) {
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function isNotFoundError(error) {
  return Boolean(
    error &&
    (
      error.name === 'NoSuchKey' ||
      error.name === 'ResourceNotFoundException' ||
      error.code === 'NoSuchKey' ||
      error.$metadata?.httpStatusCode === 404
    )
  );
}

function createS3Client(config) {
  const { S3Client } = require('@aws-sdk/client-s3');
  return new S3Client({
    region: config.ymqRegion || 'ru-central1',
    endpoint: 'https://storage.yandexcloud.net',
    credentials: {
      accessKeyId: config.awsAccessKeyId,
      secretAccessKey: config.awsSecretAccessKey
    }
  });
}

function createDocumentClient(config) {
  const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
  const { DynamoDBDocumentClient } = require('@aws-sdk/lib-dynamodb');
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

function buildHotStateConfig(env = process.env) {
  const runtimeConfig = buildEventRuntimeConfig(env);
  return {
    ...runtimeConfig,
    bucketName: String(env.BUCKET_NAME || DEFAULT_BUCKET_NAME).trim() || DEFAULT_BUCKET_NAME
  };
}

function isHotStateEnabled(config) {
  return Boolean(
    config &&
    config.mode === 'cloud' &&
    config.ydbDocApiEndpoint &&
    config.ydbHotStateTable &&
    config.awsAccessKeyId &&
    config.awsSecretAccessKey
  );
}

function createHotStateStore(config = buildHotStateConfig(process.env), overrides = {}) {
  const logger = typeof overrides.log === 'function' ? overrides.log : log;
  let documentClient = overrides.documentClient || null;
  let s3Client = overrides.s3Client || null;
  function getDocumentClient() {
    if (!documentClient && isHotStateEnabled(config)) {
      documentClient = createDocumentClient(config);
    }
    return documentClient;
  }
  function getS3Client() {
    if (!s3Client) {
      s3Client = createS3Client(config);
    }
    return s3Client;
  }
  const readHotStateItem = overrides.readHotStateItem || (async objectKey => {
    const { GetCommand } = require('@aws-sdk/lib-dynamodb');
    const client = getDocumentClient();
    if (!client) return null;
    const response = await client.send(new GetCommand({
      TableName: config.ydbHotStateTable,
      Key: { objectKey }
    }));
    return response.Item || null;
  });
  const writeHotStateItem = overrides.writeHotStateItem || (async (objectKey, jsonText, meta = {}) => {
    const { PutCommand } = require('@aws-sdk/lib-dynamodb');
    const client = getDocumentClient();
    if (!client) return null;
    await client.send(new PutCommand({
      TableName: config.ydbHotStateTable,
      Item: {
        objectKey,
        jsonText,
        objectHash: crypto.createHash('sha256').update(jsonText).digest('hex'),
        updatedAt: new Date().toISOString(),
        sourceEtag: String(meta.sourceEtag || ''),
        sourceLastModified: String(meta.sourceLastModified || ''),
        sourceObjectKey: String(meta.sourceObjectKey || objectKey)
      }
    }));
    return { objectKey };
  });
  const readS3Object = overrides.readS3Object || (async objectKey => {
    const { GetObjectCommand } = require('@aws-sdk/client-s3');
    const response = await getS3Client().send(new GetObjectCommand({
      Bucket: config.bucketName,
      Key: objectKey
    }));
    return {
      objectKey,
      jsonText: await response.Body.transformToString(),
      etag: response.ETag || '',
      lastModified: response.LastModified ? response.LastModified.toISOString() : ''
    };
  });
  const writeS3Object = overrides.writeS3Object || (async (objectKey, jsonText) => {
    const { PutObjectCommand } = require('@aws-sdk/client-s3');
    await getS3Client().send(new PutObjectCommand({
      Bucket: config.bucketName,
      Key: objectKey,
      Body: jsonText,
      ContentType: 'application/json'
    }));
    return { objectKey };
  });

  async function loadJsonObject(objectKey, options = {}) {
    const normalizedKey = String(objectKey || '').trim();
    if (!normalizedKey) {
      throw new Error('objectKey is required');
    }

    const defaultValue = options.defaultValue;
    const legacyKeys = Array.isArray(options.legacyKeys)
      ? options.legacyKeys.map(key => String(key || '').trim()).filter(Boolean)
      : [];

    if (isHotStateEnabled(config)) {
      try {
        const item = await readHotStateItem(normalizedKey);
        if (item && typeof item.jsonText === 'string' && item.jsonText.trim()) {
          return {
            value: JSON.parse(item.jsonText),
            source: 'ydb',
            objectKey: normalizedKey,
            jsonText: item.jsonText
          };
        }
      } catch (error) {
        logger('warn', `Hot state YDB read failed for ${normalizedKey}: ${error.message}`);
      }
    }

    for (const candidateKey of [normalizedKey, ...legacyKeys]) {
      try {
        const entry = await readS3Object(candidateKey);
        const value = JSON.parse(entry.jsonText);

        if (isHotStateEnabled(config)) {
          try {
            await writeHotStateItem(normalizedKey, entry.jsonText, {
              sourceEtag: entry.etag,
              sourceLastModified: entry.lastModified,
              sourceObjectKey: candidateKey
            });
          } catch (backfillError) {
            logger('warn', `Hot state YDB backfill failed for ${normalizedKey}: ${backfillError.message}`);
          }
        }

        return {
          value,
          source: 's3',
          objectKey: candidateKey,
          jsonText: entry.jsonText
        };
      } catch (error) {
        if (!isNotFoundError(error)) {
          logger('warn', `Hot state S3 read failed for ${candidateKey}: ${error.message}`);
        }
      }
    }

    return {
      value: cloneValue(defaultValue),
      source: 'default',
      objectKey: normalizedKey,
      jsonText: defaultValue === undefined ? '' : JSON.stringify(defaultValue, null, 2)
    };
  }

  async function saveJsonObject(objectKey, value) {
    const normalizedKey = String(objectKey || '').trim();
    if (!normalizedKey) {
      throw new Error('objectKey is required');
    }

    const jsonText = JSON.stringify(value, null, 2);
    if (isHotStateEnabled(config)) {
      try {
        await writeHotStateItem(normalizedKey, jsonText);
        let backupError = '';
        try {
          await writeS3Object(normalizedKey, jsonText);
        } catch (error) {
          backupError = error.message;
          logger('warn', `Hot state S3 backup write failed for ${normalizedKey}: ${error.message}`);
        }

        return {
          primary: 'ydb',
          backupAttempted: true,
          backupError
        };
      } catch (error) {
        logger('warn', `Hot state YDB write failed for ${normalizedKey}, falling back to S3: ${error.message}`);
      }
    }

    await writeS3Object(normalizedKey, jsonText);
    return {
      primary: 's3',
      backupAttempted: false,
      backupError: ''
    };
  }

  async function ensureJsonObject(objectKey, defaultValue) {
    const current = await loadJsonObject(objectKey, { defaultValue });
    if (current.source === 'default') {
      await saveJsonObject(objectKey, current.value);
    }
    return current.value;
  }

  return {
    loadJsonObject,
    saveJsonObject,
    ensureJsonObject
  };
}

module.exports = {
  buildHotStateConfig,
  createHotStateStore,
  isHotStateEnabled
};
