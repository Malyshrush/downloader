function getHeaderValue(headers = {}, name = '') {
    if (!headers || !name) return '';
    const direct = headers[name] || headers[name.toLowerCase()] || headers[name.toUpperCase()];
    if (direct) return String(direct).trim();
    const target = name.toLowerCase();
    for (const [key, value] of Object.entries(headers)) {
        if (String(key || '').toLowerCase() === target) {
            return String(value || '').trim();
        }
    }
    return '';
}

function isPrivateIpAddress(ip = '') {
    const value = String(ip || '').trim();
    const ipv4 = value.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
    if (ipv4) {
        const parts = ipv4.slice(1).map(part => Number(part));
        if (parts.some(part => !Number.isInteger(part) || part < 0 || part > 255)) return true;
        const [a, b] = parts;
        return (
            a === 10 ||
            a === 127 ||
            (a === 172 && b >= 16 && b <= 31) ||
            (a === 192 && b === 168) ||
            (a === 169 && b === 254) ||
            (a === 100 && b >= 64 && b <= 127)
        );
    }
    const lower = value.toLowerCase();
    return lower === '::1' || lower.startsWith('fc') || lower.startsWith('fd') || lower.startsWith('fe80:');
}

function extractForwardedIp(forwarded = '') {
    return String(forwarded || '')
        .split(',')
        .map(part => part.trim())
        .filter(Boolean)
        .find(ip => !isPrivateIpAddress(ip)) || '';
}

function getClientIpFromHeaders(headers = {}) {
    const realIp = getHeaderValue(headers, 'x-real-ip');
    if (realIp && !isPrivateIpAddress(realIp)) {
        return realIp;
    }
    const forwardedIp = extractForwardedIp(getHeaderValue(headers, 'x-forwarded-for'));
    if (forwardedIp) return forwardedIp;
    return realIp || getHeaderValue(headers, 'x-real-ip');
}

module.exports = {
    getHeaderValue,
    isPrivateIpAddress,
    extractForwardedIp,
    getClientIpFromHeaders
};
