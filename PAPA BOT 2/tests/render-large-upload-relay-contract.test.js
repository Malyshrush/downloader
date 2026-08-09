const test = require('node:test');
const assert = require('node:assert/strict');

process.env.CALLBACK_SECRET = 'render-relay-test-secret';

const { __testOnly } = require('../src/handler');

function makeEvent(body, secret = process.env.CALLBACK_SECRET) {
  return {
    headers: { 'x-render-upload-secret': secret },
    body: JSON.stringify(body)
  };
}

test('Render relay keeps User Token server-side and assembles chunks before VK upload', async () => {
  const objects = new Map();
  const uploaded = [];
  const s3Client = {
    async send(command) {
      const input = command.input;
      const name = command.constructor.name;
      if (name === 'PutObjectCommand') {
        objects.set(input.Key, Buffer.from(input.Body));
        return {};
      }
      if (name === 'GetObjectCommand') {
        return { Body: { transformToByteArray: async () => objects.get(input.Key) } };
      }
      if (name === 'DeleteObjectCommand') {
        objects.delete(input.Key);
        return {};
      }
      throw new Error('Unexpected S3 command: ' + name);
    }
  };
  const overrides = {
    s3Client,
    getBucketName: () => 'test-bucket',
    loadBotConfig: async () => {},
    getUserToken: async () => 'server-only-user-token',
    uploadToVK: async (buffer, fileName, fileType, target, groupId) => {
      uploaded.push({ buffer, fileName, fileType, target, groupId });
      return 'doc-1_2';
    },
    persistUploadedCommunityFileRecord: async () => {}
  };
  const common = {
    upload_id: 'upload_test_relay_1',
    total_chunks: 2,
    fileName: 'large.pdf',
    fileType: 'application/pdf',
    fileSize: 10,
    target: 'message',
    groupId: '229445618',
    communityId: 'community-1',
    profileId: '1'
  };
  const grant = __testOnly.createRenderRelayGrant({
    profileId: '1',
    communityId: 'community-1',
    groupId: '229445618',
    target: 'message',
    uploadId: common.upload_id,
    exp: Date.now() + 60000
  });

  const first = await __testOnly.handleUploadAttachmentChunk(
    makeEvent({ ...common, render_grant: grant, chunk_index: 0, chunk_base64: Buffer.from('hello ').toString('base64') }),
    overrides
  );
  assert.equal(first.statusCode, 200);
  assert.equal(JSON.parse(first.body).pending, true);

  const second = await __testOnly.handleUploadAttachmentChunk(
    makeEvent({ ...common, render_grant: grant, chunk_index: 1, chunk_base64: Buffer.from('world').toString('base64') }),
    overrides
  );
  assert.equal(second.statusCode, 200);
  assert.equal(JSON.parse(second.body).attachment, 'doc-1_2');
  assert.equal(uploaded[0].buffer.toString(), 'hello world');
  assert.equal(uploaded[0].target, 'message');
  assert.equal(objects.size, 0);
});

test('Render relay rejects requests without its server-to-server secret', async () => {
  const response = await __testOnly.handleUploadAttachmentChunk(
    makeEvent({ upload_id: 'upload_test_relay_2', chunk_index: 0, total_chunks: 1, chunk_base64: 'YQ==' }, 'wrong-secret'),
    { s3Client: { send: async () => { throw new Error('must not access storage'); } } }
  );
  assert.equal(response.statusCode, 400);
});
