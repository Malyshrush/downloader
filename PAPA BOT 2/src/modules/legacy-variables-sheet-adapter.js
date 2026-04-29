const { getSheetData, invalidateCache } = require('./storage');

const PROFILE_USER_SHARED_SHEET = 'ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ';
const SHARED_VARIABLES_SHEET = 'ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ';
const SHARED_VARIABLE_NAME_COLUMNS = [
    'Переменная ПВС',
    'РџРµСЂРµРјРµРЅРЅР°СЏ РџР’РЎ',
    'Р СџР ВµРЎР‚Р ВµР СР ВµР Р…Р Р…Р В°РЎРЏ Р СџР вЂ™Р РЋ'
];
const SHARED_VARIABLE_VALUE_COLUMNS = [
    'Значение ПВС',
    'Р—РЅР°С‡РµРЅРёРµ РџР’РЎ',
    'Р вЂ”Р Р…Р В°РЎвЂЎР ВµР Р…Р С‘Р Вµ Р СџР вЂ™Р РЋ'
];

function getFirstDefined(row, columns) {
    for (const column of columns) {
        if (row && Object.prototype.hasOwnProperty.call(row, column) && row[column]) {
            return row[column];
        }
    }
    return '';
}

function createLegacyVariablesSheetAdapter(dependencies = {}) {
    const sheetGetter = dependencies.getSheetData || getSheetData;
    const cacheInvalidator = dependencies.invalidateCache || invalidateCache;

    return {
        async getProfileUserSharedVariableRows(profileId = '1') {
            const rows = await sheetGetter(PROFILE_USER_SHARED_SHEET, null, profileId);
            return Array.isArray(rows) ? rows : [];
        },

        async getSharedVariables(profileId = '1') {
            const rows = await sheetGetter(SHARED_VARIABLES_SHEET, null, profileId);
            if (!Array.isArray(rows) || rows.length === 0) {
                return {};
            }

            const sharedVars = {};
            for (const row of rows) {
                const name = String(getFirstDefined(row, SHARED_VARIABLE_NAME_COLUMNS) || '').trim();
                if (!name) {
                    continue;
                }
                sharedVars[name.toLowerCase()] = String(getFirstDefined(row, SHARED_VARIABLE_VALUE_COLUMNS) || '').trim();
            }

            return sharedVars;
        },

        invalidateRuntimeCaches(communityId, profileId = '1') {
            cacheInvalidator('ПОЛЬЗОВАТЕЛИ', communityId, profileId);
            cacheInvalidator('ПЕРЕМЕННЫЕ', communityId, profileId);
        }
    };
}

module.exports = {
    createLegacyVariablesSheetAdapter,
    PROFILE_USER_SHARED_SHEET,
    SHARED_VARIABLES_SHEET
};
