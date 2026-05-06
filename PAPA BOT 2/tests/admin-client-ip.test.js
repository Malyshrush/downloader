const test = require('node:test');
const assert = require('node:assert/strict');

const { getClientIpFromHeaders, extractForwardedIp } = require('../src/modules/client-ip');

test('admin client IP prefers x-real-ip over proxy x-forwarded-for', () => {
    const ip = getClientIpFromHeaders({
        'x-forwarded-for': '10.10.0.5, 144.31.1.42',
        'x-real-ip': '144.31.1.42'
    });

    assert.equal(ip, '144.31.1.42');
});

test('admin client IP extracts first public address from forwarded chain', () => {
    assert.equal(extractForwardedIp('10.0.0.1, 172.16.0.1, 144.31.1.42, 203.0.113.10'), '144.31.1.42');
});

test('admin client IP handles case-insensitive cloud headers', () => {
    const ip = getClientIpFromHeaders({
        'X-Forwarded-For': '100.64.1.10, 198.51.100.25',
        'X-Real-IP': '198.51.100.25'
    });

    assert.equal(ip, '198.51.100.25');
});
