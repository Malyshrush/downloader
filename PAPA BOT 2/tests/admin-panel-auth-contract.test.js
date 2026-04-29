const test = require('node:test');
const assert = require('node:assert/strict');

const { adminPanelHTML } = require('../adminPanelHTML');

test('admin panel fetches with credentials include', () => {
    assert.match(adminPanelHTML, /credentials:\s*'include'/);
    assert.match(adminPanelHTML, /fetchAdminJson/);
});

test('admin panel contains session captcha overlay hooks', () => {
    assert.match(adminPanelHTML, /captcha-lock/);
    assert.match(adminPanelHTML, /sessionCaptchaOverlay/);
    assert.match(adminPanelHTML, /verifyCaptcha/);
    assert.match(adminPanelHTML, /getCaptcha/);
});

test('admin panel login flow uses loginAdmin and login captcha hooks', () => {
    assert.match(adminPanelHTML, /\?loginAdmin/);
    assert.match(adminPanelHTML, /refreshLoginCaptcha/);
    assert.match(adminPanelHTML, /loginCaptchaAnswer/);
});

test('admin panel validates existing session through shared auth wrapper', () => {
    assert.match(adminPanelHTML, /\?validateSession/);
    assert.match(adminPanelHTML, /fetchAdminJson\(baseUrl \+ '\?validateSession'/);
});

test('admin panel stores and sends header-based session token', () => {
    assert.match(adminPanelHTML, /adminSessionToken/);
    assert.match(adminPanelHTML, /X-Admin-Session/);
});

test('admin panel session captcha overlay fully blocks background and shows explicit prompt', () => {
    assert.match(adminPanelHTML, /body\.captcha-lock\s*\.container/);
    assert.match(adminPanelHTML, /for="sessionCaptchaAnswer"/);
    assert.match(adminPanelHTML, /sessionCaptchaImage" style="[^"]*min-height/);
});

test('admin panel does not recursively refresh session captcha when loading captcha image', () => {
    assert.match(adminPanelHTML, /isSessionCaptchaChallengeResponse/);
    assert.match(adminPanelHTML, /data && data\.captchaRequired && !isSessionCaptchaChallengeResponse\(url, data\)/);
});

test('admin panel keeps current session captcha visible when refresh is rate limited', () => {
    assert.match(adminPanelHTML, /window\.sessionCaptchaRefreshInFlight/);
    assert.match(adminPanelHTML, /captcha_rate_limited/);
    assert.match(adminPanelHTML, /previousMarkup/);
    assert.match(adminPanelHTML, /Подожди.*каптч/i);
});
