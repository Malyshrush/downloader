const assert = require('node:assert/strict');
const crypto = require('node:crypto');

const {
    createCaptchaChallenge,
    verifyCaptchaAnswer,
    appendSecurityEvent,
    checkCaptchaRateLimit,
    registerCaptchaRateLimitHit
} = require('../src/modules/admin-security');

function run(name, fn) {
    try {
        fn();
        process.stdout.write('PASS ' + name + '\n');
    } catch (error) {
        process.stderr.write('FAIL ' + name + '\n');
        throw error;
    }
}

function hashCaptchaAnswer(answer) {
    return crypto.createHash('sha256').update(String(answer || '').trim().toUpperCase()).digest('hex');
}

run('createCaptchaChallenge returns svg and hashed answer state', () => {
    const challenge = createCaptchaChallenge({
        mode: 'session',
        answer: 'AB12C',
        now: new Date('2026-04-21T10:00:00.000Z')
    });

    assert.match(challenge.captchaSvg, /<svg/);
    assert.equal(challenge.hash, hashCaptchaAnswer('AB12C'));
    assert.equal(challenge.answer, undefined);
    assert.equal(challenge.mode, 'session');
});

run('verifyCaptchaAnswer fails after 3 bad attempts', () => {
    const challenge = createCaptchaChallenge({
        mode: 'session',
        answer: 'AB12C',
        now: new Date('2026-04-21T10:00:00.000Z')
    });
    const state = { captchaChallenge: challenge, captchaFailCount: 0 };

    verifyCaptchaAnswer(state, 'wrong', new Date('2026-04-21T10:00:10.000Z'));
    verifyCaptchaAnswer(state, 'wrong', new Date('2026-04-21T10:00:20.000Z'));
    const result = verifyCaptchaAnswer(state, 'wrong', new Date('2026-04-21T10:00:30.000Z'));

    assert.equal(result.ok, false);
    assert.equal(result.terminateSession, true);
    assert.equal(state.captchaFailCount, 3);
});

run('appendSecurityEvent stores most recent event first', () => {
    const data = { securityEvents: [] };
    appendSecurityEvent(data, { type: 'session_created', profileId: '2' });

    assert.equal(Array.isArray(data.securityEvents), true);
    assert.equal(data.securityEvents.length, 1);
    assert.equal(data.securityEvents[0].type, 'session_created');
});

run('checkCaptchaRateLimit blocks rapid repeated actions after registration', () => {
    const data = { captchaRateLimits: {} };
    const first = checkCaptchaRateLimit({
        data,
        sessionId: 'sess_1',
        ip: '203.0.113.10',
        action: 'submit',
        now: Date.parse('2026-04-21T10:00:00.000Z')
    });

    assert.equal(first.blocked, false);
    registerCaptchaRateLimitHit({
        data,
        key: first.key,
        bucket: first.bucket,
        now: Date.parse('2026-04-21T10:00:00.000Z')
    });

    const second = checkCaptchaRateLimit({
        data,
        sessionId: 'sess_1',
        ip: '203.0.113.10',
        action: 'submit',
        now: Date.parse('2026-04-21T10:00:01.000Z')
    });

    assert.equal(second.blocked, true);
    assert.equal(second.cooldownMs > 0, true);
});
