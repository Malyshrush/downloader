const test = require('node:test');
const assert = require('node:assert/strict');

const { __testOnly } = require('../src/handler');

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
