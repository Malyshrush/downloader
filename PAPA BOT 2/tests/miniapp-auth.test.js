const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');

const {
  buildVkLaunchSignPayload,
  signVkLaunchParams,
  verifyVkLaunchParams
} = require('../src/modules/miniapp-auth');

test('buildVkLaunchSignPayload sorts vk params and excludes sign and non-vk params', () => {
  const payload = buildVkLaunchSignPayload({
    sign: 'ignored',
    vk_user_id: '123',
    vk_app_id: '999',
    c: '229445618'
  });

  assert.equal(payload, 'vk_app_id=999&vk_user_id=123');
});

test('verifyVkLaunchParams accepts valid signature', () => {
  const secret = 'miniapp-secret';
  const params = {
    vk_app_id: '999',
    vk_user_id: '123',
    vk_group_id: '229445618'
  };
  const sign = signVkLaunchParams(params, secret);
  const result = verifyVkLaunchParams({ ...params, sign }, { secret });

  assert.equal(result.ok, true);
  assert.equal(result.userId, '123');
  assert.equal(result.groupId, '229445618');
});

test('verifyVkLaunchParams rejects invalid signature', () => {
  const result = verifyVkLaunchParams({
    vk_app_id: '999',
    vk_user_id: '123',
    sign: 'bad'
  }, { secret: 'miniapp-secret' });

  assert.equal(result.ok, false);
  assert.equal(result.error, 'invalid_vk_sign');
});

test('verifyVkLaunchParams rejects missing user id', () => {
  const secret = 'miniapp-secret';
  const params = { vk_app_id: '999' };
  const sign = crypto.createHmac('sha256', secret).update('vk_app_id=999').digest('base64url');
  const result = verifyVkLaunchParams({ ...params, sign }, { secret });

  assert.equal(result.ok, false);
  assert.equal(result.error, 'missing_vk_user_id');
});
