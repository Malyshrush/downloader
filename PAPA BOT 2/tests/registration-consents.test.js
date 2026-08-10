const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const {
  REGISTRATION_CONSENT_DOCUMENTS,
  createRegistrationConsentsRecord,
  hasRequiredRegistrationConsents,
  normalizeAdminAuth,
  toPublicProfile
} = require('../src/modules/admin-profiles');

const adminPanelHTML = fs.readFileSync(path.join(__dirname, '..', 'adminPanelHTML.js'), 'utf8');
const handlerSource = fs.readFileSync(path.join(__dirname, '..', 'src', 'handler.js'), 'utf8');

test('registration requires all separate legal confirmations and stores the published document set', () => {
  assert.equal(hasRequiredRegistrationConsents({}), false);
  assert.equal(hasRequiredRegistrationConsents({
    personalDataConsent: true,
    privacyPolicy: true,
    publicOffer: false
  }), false);
  assert.equal(hasRequiredRegistrationConsents({
    personalDataConsent: true,
    privacyPolicy: true,
    publicOffer: true
  }), true);

  const record = createRegistrationConsentsRecord('2026-08-10T12:00:00.000Z');
  assert.equal(record.acceptedAt, '2026-08-10T12:00:00.000Z');
  assert.deepEqual(record.documents.map(document => document.key), [
    'personalDataConsent',
    'privacyPolicy',
    'publicOffer'
  ]);
  assert.equal(record.documents.length, REGISTRATION_CONSENT_DOCUMENTS.length);
  assert.ok(record.documents.every(document => document.url.startsWith('https://malyshrush.github.io/papa-bot-vk-miniapp/legal/')));
});

test('registration consent record is retained in stored and public profile data', () => {
  const record = createRegistrationConsentsRecord('2026-08-10T12:00:00.000Z');
  const auth = normalizeAdminAuth({
    profiles: {
      2: {
        id: '2', name: 'Test', username: 'test', password: 'secret', recoveryEmail: 'test@example.com',
        createdAt: '2026-08-10T11:00:00.000Z', registrationConsents: record
      }
    }
  });
  const publicProfile = toPublicProfile(auth.profiles['2'], '2');
  assert.equal(publicProfile.createdAt, '2026-08-10T11:00:00.000Z');
  assert.equal(publicProfile.registrationConsents.acceptedAt, '2026-08-10T12:00:00.000Z');
  assert.equal(publicProfile.registrationConsents.documents[2].title, 'Публичная оферта');
  assert.equal(Object.hasOwn(publicProfile, 'password'), false);
});

test('registration form, backend guard and profile views expose the required consent audit trail', () => {
  assert.match(adminPanelHTML, /registerConsentPersonalData/);
  assert.match(adminPanelHTML, /registerConsentPrivacyPolicy/);
  assert.match(adminPanelHTML, /registerConsentPublicOffer/);
  assert.match(adminPanelHTML, /registrationConsents:/);
  assert.match(handlerSource, /hasRequiredRegistrationConsents\(body\.registrationConsents\)/);
  assert.match(adminPanelHTML, /profileRegistrationInfo/);
  assert.match(adminPanelHTML, /Согласия при регистрации/);
});
