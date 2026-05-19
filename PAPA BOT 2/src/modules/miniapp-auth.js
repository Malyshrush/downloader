const crypto = require('node:crypto');

function clean(value) {
    return String(value || '').trim();
}

function buildVkLaunchSignPayload(params) {
    return Object.keys(params || {})
        .filter(key => key.startsWith('vk_'))
        .filter(key => key !== 'sign')
        .sort()
        .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(clean(params[key]))}`)
        .join('&');
}

function signVkLaunchParams(params, secret) {
    return crypto
        .createHmac('sha256', clean(secret))
        .update(buildVkLaunchSignPayload(params))
        .digest('base64url');
}

function timingSafeEqualString(leftValue, rightValue) {
    const left = Buffer.from(clean(leftValue));
    const right = Buffer.from(clean(rightValue));
    if (left.length !== right.length) return false;
    return crypto.timingSafeEqual(left, right);
}

function trimLaunchParams(params) {
    return Object.fromEntries(
        Object.entries(params || {}).map(([key, value]) => [key, clean(value)])
    );
}

function verifyVkLaunchParams(params, options = {}) {
    const secret = clean(options.secret || process.env.VK_MINIAPP_SECRET);
    if (!secret) return { ok: false, error: 'missing_miniapp_secret' };

    const expectedSign = signVkLaunchParams(params, secret);
    if (!timingSafeEqualString(params && params.sign, expectedSign)) {
        return { ok: false, error: 'invalid_vk_sign' };
    }

    const trimmedParams = trimLaunchParams(params);
    const userId = trimmedParams.vk_user_id;
    if (!userId) return { ok: false, error: 'missing_vk_user_id' };

    return {
        ok: true,
        userId,
        appId: trimmedParams.vk_app_id,
        groupId: trimmedParams.vk_group_id,
        params: trimmedParams
    };
}

module.exports = {
    buildVkLaunchSignPayload,
    signVkLaunchParams,
    verifyVkLaunchParams
};
