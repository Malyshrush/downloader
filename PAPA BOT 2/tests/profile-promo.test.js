const assert = require('node:assert/strict');

const {
    MAX_PROMO_ATTEMPTS,
    getMoscowDayKey,
    getNextMoscowMidnightTimestamp,
    normalizeProfilePromoActivationState
} = require('../src/modules/admin-security');
const {
    buildProfilePromoActivationUpdate
} = require('../src/modules/admin-profiles');

function run(name, fn) {
    try {
        fn();
        process.stdout.write('PASS ' + name + '\n');
    } catch (error) {
        process.stderr.write('FAIL ' + name + '\n');
        throw error;
    }
}

run('normalizeProfilePromoActivationState resets attempts on a new Moscow day', () => {
    const now = new Date('2026-04-19T10:15:00+03:00');
    const state = normalizeProfilePromoActivationState({
        attempts: 3,
        dayKey: '2026-04-18',
        lastAttemptAt: Date.parse('2026-04-18T22:00:00+03:00')
    }, now);

    assert.equal(state.dayKey, '2026-04-19');
    assert.equal(state.attempts, 0);
    assert.equal(state.blocked, false);
    assert.equal(state.remainingAttempts, MAX_PROMO_ATTEMPTS);
    assert.equal(state.nextResetAt, Date.parse('2026-04-20T00:00:00+03:00'));
});

run('normalizeProfilePromoActivationState blocks input after three attempts within the same Moscow day', () => {
    const now = new Date('2026-04-19T23:20:00+03:00');
    const state = normalizeProfilePromoActivationState({
        attempts: 3,
        dayKey: '2026-04-19',
        lastAttemptAt: Date.parse('2026-04-19T20:00:00+03:00')
    }, now);

    assert.equal(state.dayKey, getMoscowDayKey(now));
    assert.equal(state.attempts, 3);
    assert.equal(state.blocked, true);
    assert.equal(state.remainingAttempts, 0);
    assert.equal(state.nextResetAt, getNextMoscowMidnightTimestamp(now));
});

run('buildProfilePromoActivationUpdate extends profile lifetime and adds daily request limit', () => {
    const now = new Date('2026-04-19T12:00:00+03:00');
    const currentExpiresAt = '2026-04-19T13:00:00+03:00';

    const updated = buildProfilePromoActivationUpdate({
        id: '7',
        active: true,
        expiresAt: currentExpiresAt,
        requestsLimit: 1000
    }, {
        code: 'PAPA-BOOST',
        durationMinutes: 120,
        dailyRequestsLimit: 500
    }, now);

    assert.equal(updated.active, true);
    assert.equal(updated.promoCodeUsed, 'PAPA-BOOST');
    assert.equal(updated.requestsLimit, 1500);
    assert.equal(updated.expiresAt, '2026-04-19T12:00:00.000Z');
});
