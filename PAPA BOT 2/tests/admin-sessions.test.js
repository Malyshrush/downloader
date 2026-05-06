const assert = require('node:assert/strict');

const {
    createSessionRecord,
    normalizeSessionStore,
    computeSessionRisk,
    isSessionExpired,
    touchSession
} = require('../src/modules/admin-sessions');

function run(name, fn) {
    try {
        fn();
        process.stdout.write('PASS ' + name + '\n');
    } catch (error) {
        process.stderr.write('FAIL ' + name + '\n');
        throw error;
    }
}

run('createSessionRecord seeds verified client context', () => {
    const session = createSessionRecord({
        sessionId: 'sess_1',
        profileId: '2',
        ip: '203.0.113.10',
        userAgent: 'Mozilla/5.0',
        now: '2026-04-21T10:00:00.000Z'
    });

    assert.equal(session.profileId, '2');
    assert.equal(session.lastVerifiedIp, '203.0.113.10');
    assert.equal(session.lastUserAgent, 'Mozilla/5.0');
    assert.equal(session.captchaRequired, false);
    assert.equal(session.terminatedAt, null);
});

run('normalizeSessionStore falls back to empty sessions container', () => {
    assert.deepEqual(normalizeSessionStore(null), { sessions: {} });
    assert.deepEqual(normalizeSessionStore({ sessions: {} }), { sessions: {} });
});

run('isSessionExpired returns true after 12 hours of inactivity', () => {
    const session = createSessionRecord({
        sessionId: 'sess_2',
        profileId: '2',
        ip: '203.0.113.10',
        userAgent: 'Mozilla/5.0',
        now: '2026-04-21T00:00:00.000Z'
    });

    session.lastSeenAt = '2026-04-21T00:00:00.000Z';
    assert.equal(isSessionExpired(session, new Date('2026-04-21T12:00:01.000Z')), true);
    assert.equal(isSessionExpired(session, new Date('2026-04-21T11:59:59.000Z')), false);
});

run('computeSessionRisk requires captcha for suspicious fast ip change with ua drift', () => {
    const session = createSessionRecord({
        sessionId: 'sess_3',
        profileId: '2',
        ip: '203.0.113.10',
        userAgent: 'Mozilla/5.0',
        now: '2026-04-21T10:00:00.000Z'
    });

    const risk = computeSessionRisk(session, {
        ip: '198.51.100.25',
        userAgent: 'curl/8.0',
        now: new Date('2026-04-21T10:05:00.000Z')
    });

    assert.equal(risk.total >= 3, true);
    assert.equal(risk.requiresCaptcha, true);
});

run('computeSessionRisk allows stable context without captcha', () => {
    const session = createSessionRecord({
        sessionId: 'sess_4',
        profileId: '2',
        ip: '203.0.113.10',
        userAgent: 'Mozilla/5.0',
        now: '2026-04-21T10:00:00.000Z'
    });

    const risk = computeSessionRisk(session, {
        ip: '203.0.113.10',
        userAgent: 'Mozilla/5.0',
        now: new Date('2026-04-21T11:00:00.000Z')
    });

    assert.equal(risk.total, 0);
    assert.equal(risk.requiresCaptcha, false);
});

run('touchSession clears suspicious security state after verified captcha pass', () => {
    const session = createSessionRecord({
        sessionId: 'sess_5',
        profileId: '2',
        ip: '203.0.113.10',
        userAgent: 'Mozilla/5.0',
        now: '2026-04-21T10:00:00.000Z'
    });

    session.captchaRequired = true;
    session.captchaFailCount = 2;
    session.suspiciousChangeCount = 3;
    session.captchaReason = 'ip_changed_suspicious';
    session.captchaChallenge = { hash: 'x', expiresAt: '2026-04-21T10:05:00.000Z', attempts: 1, mode: 'session', captchaSvg: '<svg />' };

    touchSession(session, {
        ip: '198.51.100.25',
        userAgent: 'Mozilla/5.0',
        now: '2026-04-21T10:06:00.000Z',
        verified: true
    });

    assert.equal(session.lastVerifiedIp, '198.51.100.25');
    assert.equal(session.captchaRequired, false);
    assert.equal(session.captchaFailCount, 0);
    assert.equal(session.suspiciousChangeCount, 0);
    assert.equal(session.captchaReason, '');
    assert.equal(session.captchaChallenge, null);
    assert.equal(Boolean(session.securityVerifiedUntil), true);
});

run('computeSessionRisk allows temporary VPN drift after verified captcha pass', () => {
    const session = createSessionRecord({
        sessionId: 'sess_6',
        profileId: '2',
        ip: '203.0.113.10',
        userAgent: 'Mozilla/5.0',
        now: '2026-04-21T10:00:00.000Z'
    });

    touchSession(session, {
        ip: '198.51.100.25',
        userAgent: 'Mozilla/5.0',
        now: '2026-04-21T10:06:00.000Z',
        verified: true
    });

    const graceRisk = computeSessionRisk(session, {
        ip: '198.51.100.99',
        userAgent: 'Mozilla/5.0',
        now: new Date('2026-04-21T10:10:00.000Z')
    });
    assert.equal(graceRisk.requiresCaptcha, false);
    assert.equal(graceRisk.trustedGrace, true);

    const expiredGraceRisk = computeSessionRisk(session, {
        ip: '198.51.100.100',
        userAgent: 'curl/8.0',
        now: new Date('2026-04-21T10:37:00.000Z')
    });
    assert.equal(expiredGraceRisk.requiresCaptcha, true);
});
