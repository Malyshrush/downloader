const FIELD_GROUP = 'Группа';
const FIELD_ADMIN_DESCRIPTION = 'Описание';
const FIELD_ENABLED = 'MiniApp включен';
const FIELD_HIDDEN = 'MiniApp скрыть из списка';
const FIELD_SLUG = 'MiniApp slug';
const FIELD_TITLE = 'MiniApp заголовок';
const FIELD_DESCRIPTION = 'MiniApp описание';
const FIELD_ICON_URL = 'MiniApp иконка URL';
const FIELD_ICON_FILE = 'MiniApp иконка файл';
const FIELD_BANNER_URL = 'MiniApp баннер URL';
const FIELD_BANNER_FILE = 'MiniApp баннер файл';
const FIELD_SUBSCRIBE_TEXT = 'MiniApp текст подписки';
const FIELD_UNSUBSCRIBE_TEXT = 'MiniApp текст отписки';

const TRUE_VALUES = new Set([
    '1',
    'true',
    'yes',
    'y',
    'да',
    'вкл',
    'включен',
    'включено'
]);

const RU_TRANSLIT = {
    а: 'a',
    б: 'b',
    в: 'v',
    г: 'g',
    д: 'd',
    е: 'e',
    ё: 'e',
    ж: 'zh',
    з: 'z',
    и: 'i',
    й: 'y',
    к: 'k',
    л: 'l',
    м: 'm',
    н: 'n',
    о: 'o',
    п: 'p',
    р: 'r',
    с: 's',
    т: 't',
    у: 'u',
    ф: 'f',
    х: 'h',
    ц: 'c',
    ч: 'ch',
    ш: 'sh',
    щ: 'sch',
    ъ: '',
    ы: 'y',
    ь: '',
    э: 'e',
    ю: 'yu',
    я: 'ya'
};

function toText(value) {
    if (value === null || value === undefined) {
        return '';
    }
    return String(value).trim();
}

function isTruthy(value) {
    return TRUE_VALUES.has(toText(value).toLowerCase());
}

function transliterateBasicRussian(value) {
    return value.replace(/[а-яё]/g, (char) => RU_TRANSLIT[char] || '');
}

function normalizeMiniAppSlug(value, fallbackValue = '') {
    const source = toText(value) || toText(fallbackValue) || 'group';
    const normalized = transliterateBasicRussian(source.toLowerCase())
        .replace(/[^a-z0-9_-]+/g, '-')
        .replace(/-+/g, '-')
        .replace(/^[-_]+|[-_]+$/g, '');

    return normalized || 'group';
}

function pickUrl(manualUrl, uploadedFileUrl) {
    return toText(manualUrl) || toText(uploadedFileUrl);
}

function normalizeMiniAppGroupRows(rows = []) {
    const usedEnabledSlugs = new Set();

    return (Array.isArray(rows) ? rows : []).map((row) => {
        const source = row && typeof row === 'object' ? row : {};
        const name = toText(source[FIELD_GROUP]);
        const enabled = isTruthy(source[FIELD_ENABLED]);
        const slug = normalizeMiniAppSlug(source[FIELD_SLUG], name);

        if (enabled) {
            if (usedEnabledSlugs.has(slug)) {
                throw new Error(`Duplicate MiniApp slug: ${slug}`);
            }
            usedEnabledSlugs.add(slug);
        }

        return {
            name,
            adminDescription: toText(source[FIELD_ADMIN_DESCRIPTION]),
            enabled,
            hidden: isTruthy(source[FIELD_HIDDEN]),
            slug,
            title: toText(source[FIELD_TITLE]) || name,
            description: toText(source[FIELD_DESCRIPTION]) || toText(source[FIELD_ADMIN_DESCRIPTION]),
            iconUrl: pickUrl(source[FIELD_ICON_URL], source[FIELD_ICON_FILE]),
            bannerUrl: pickUrl(source[FIELD_BANNER_URL], source[FIELD_BANNER_FILE]),
            subscribeText: toText(source[FIELD_SUBSCRIBE_TEXT]) || 'Подписаться',
            unsubscribeText: toText(source[FIELD_UNSUBSCRIBE_TEXT]) || 'Отписаться',
            source
        };
    });
}

function normalizeSubscribedNames(subscribedNames = []) {
    if (!Array.isArray(subscribedNames)) {
        return new Set();
    }

    return new Set(subscribedNames.map((name) => toText(name).toLowerCase()).filter(Boolean));
}

function toListDto(group, subscribedNamesSet) {
    return {
        slug: group.slug,
        title: group.title,
        description: group.description,
        iconUrl: group.iconUrl,
        subscribed: subscribedNamesSet.has(group.name.toLowerCase())
    };
}

function listVisibleMiniAppGroups(groups = [], options = {}) {
    const subscribedNames = Array.isArray(options) ? options : options.subscribedNames;
    const subscribedNamesSet = normalizeSubscribedNames(subscribedNames);

    return (Array.isArray(groups) ? groups : [])
        .filter((group) => group && group.enabled && !group.hidden)
        .map((group) => toListDto(group, subscribedNamesSet));
}

function findMiniAppGroupBySlug(groups = [], slug) {
    const normalizedSlug = normalizeMiniAppSlug(slug);
    return (Array.isArray(groups) ? groups : []).find((group) => (
        group && group.enabled && group.slug === normalizedSlug
    )) || null;
}

function toDetailDto(group, subscribed = false) {
    if (!group) {
        return null;
    }

    return {
        slug: group.slug,
        title: group.title,
        description: group.description,
        iconUrl: group.iconUrl,
        bannerUrl: group.bannerUrl,
        subscribeText: group.subscribeText,
        unsubscribeText: group.unsubscribeText,
        subscribed: Boolean(subscribed)
    };
}

module.exports = {
    normalizeMiniAppSlug,
    normalizeMiniAppGroupRows,
    listVisibleMiniAppGroups,
    findMiniAppGroupBySlug,
    toDetailDto
};
