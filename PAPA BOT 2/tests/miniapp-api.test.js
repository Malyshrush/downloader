const test = require('node:test');
const assert = require('node:assert/strict');

const { __testOnly } = require('../src/handler');
const { signVkLaunchParams } = require('../src/modules/miniapp-auth');

function parse(response) {
    return JSON.parse(response.body);
}

test('Mini App group list returns only visible enabled groups', async () => {
    const response = await __testOnly.handleMiniAppRequestWithDependencies({
        httpMethod: 'GET',
        queryStringParameters: { miniapp: 'groups', c: '229445618' }
    }, {
        getSheetData: async () => [
            { 'Группа': 'vip', 'MiniApp включен': 'да', 'MiniApp slug': 'vip', 'MiniApp заголовок': 'VIP' },
            { 'Группа': 'secret', 'MiniApp включен': 'да', 'MiniApp скрыть из списка': 'да', 'MiniApp slug': 'secret', 'MiniApp заголовок': 'Secret' }
        ],
        resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' })
    });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(parse(response).groups.map(item => item.slug), ['vip']);
});

test('Mini App detail returns hidden enabled group by direct slug', async () => {
    const response = await __testOnly.handleMiniAppRequestWithDependencies({
        httpMethod: 'GET',
        queryStringParameters: { miniapp: 'group', c: '229445618', g: 'secret' }
    }, {
        getSheetData: async () => [
            { 'Группа': 'secret', 'MiniApp включен': 'да', 'MiniApp скрыть из списка': 'да', 'MiniApp slug': 'secret', 'MiniApp заголовок': 'Secret' }
        ],
        resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' })
    });

    assert.equal(response.statusCode, 200);
    assert.equal(parse(response).group.slug, 'secret');
});

test('Mini App detail treats disabled group as not found', async () => {
    const response = await __testOnly.handleMiniAppRequestWithDependencies({
        httpMethod: 'GET',
        queryStringParameters: { miniapp: 'group', c: '229445618', g: 'off' }
    }, {
        getSheetData: async () => [
            { 'Группа': 'off', 'MiniApp включен': '', 'MiniApp slug': 'off', 'MiniApp заголовок': 'Off' }
        ],
        resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' })
    });

    assert.equal(response.statusCode, 404);
    assert.equal(parse(response).error, 'group_not_found');
});

test('Mini App group list rejects unknown community', async () => {
    const response = await __testOnly.handleMiniAppRequestWithDependencies({
        httpMethod: 'GET',
        queryStringParameters: { miniapp: 'groups', c: '404' }
    }, {
        resolveCommunity: async () => null
    });

    assert.equal(response.statusCode, 404);
    assert.equal(parse(response).error, 'community_not_found');
});

test('Mini App subscribe verifies launch params and adds group', async () => {
    const secret = 'miniapp-secret';
    const launch = { vk_app_id: '999', vk_user_id: '123', vk_group_id: '229445618' };
    const calls = [];
    const response = await __testOnly.handleMiniAppRequestWithDependencies({
        httpMethod: 'POST',
        queryStringParameters: { miniapp: 'subscribe', c: '229445618', g: 'vip' },
        body: JSON.stringify({ launchParams: { ...launch, sign: signVkLaunchParams(launch, secret) } })
    }, {
        miniAppSecret: secret,
        getSheetData: async () => [
            { 'Группа': 'vip', 'MiniApp включен': 'да', 'MiniApp slug': 'vip', 'MiniApp заголовок': 'VIP' }
        ],
        resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' }),
        ensureMiniAppUser: async (userId, communityId, profileId) => calls.push(['ensure', userId, communityId, profileId]),
        updateUserGroups: async (userId, add, remove, communityId, profileId) => calls.push(['groups', userId, add, remove, communityId, profileId])
    });

    assert.equal(response.statusCode, 200);
    assert.equal(parse(response).subscribed, true);
    assert.deepEqual(calls, [
        ['ensure', '123', '229445618', '1'],
        ['groups', '123', 'vip', '', '229445618', '1']
    ]);
});

test('Mini App subscribe rejects invalid launch signature', async () => {
    const response = await __testOnly.handleMiniAppRequestWithDependencies({
        httpMethod: 'POST',
        queryStringParameters: { miniapp: 'subscribe', c: '229445618', g: 'vip' },
        body: JSON.stringify({ launchParams: { vk_user_id: '123', sign: 'bad' } })
    }, {
        miniAppSecret: 'miniapp-secret',
        getSheetData: async () => [
            { 'Группа': 'vip', 'MiniApp включен': 'да', 'MiniApp slug': 'vip', 'MiniApp заголовок': 'VIP' }
        ],
        resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' })
    });

    assert.equal(response.statusCode, 401);
    assert.equal(parse(response).error, 'invalid_vk_sign');
});

test('Mini App unsubscribe removes group idempotently', async () => {
    const secret = 'miniapp-secret';
    const launch = { vk_app_id: '999', vk_user_id: '123', vk_group_id: '229445618' };
    const calls = [];
    const response = await __testOnly.handleMiniAppRequestWithDependencies({
        httpMethod: 'POST',
        queryStringParameters: { miniapp: 'unsubscribe', c: '229445618', g: 'vip' },
        body: JSON.stringify({ launchParams: { ...launch, sign: signVkLaunchParams(launch, secret) } })
    }, {
        miniAppSecret: secret,
        getSheetData: async () => [
            { 'Группа': 'vip', 'MiniApp включен': 'да', 'MiniApp slug': 'vip', 'MiniApp заголовок': 'VIP' }
        ],
        resolveCommunity: async () => ({ communityId: '229445618', profileId: '1' }),
        updateUserGroups: async (userId, add, remove, communityId, profileId) => calls.push(['groups', userId, add, remove, communityId, profileId])
    });

    assert.equal(response.statusCode, 200);
    assert.equal(parse(response).subscribed, false);
    assert.deepEqual(calls, [
        ['groups', '123', '', 'vip', '229445618', '1']
    ]);
});
