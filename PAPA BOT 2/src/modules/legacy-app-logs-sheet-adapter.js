const { getSheetData, updateSheetData, invalidateCache } = require('./storage');
const { assertLegacySheetAccessAllowed, buildLegacySheetAccessPolicy } = require('./legacy-sheet-access-policy');

const APP_LOG_SHEET = 'ЛОГИ ПРИЛОЖЕНИЯ';

function createLegacyAppLogsSheetAdapter(dependencies = {}) {
    const sheetGetter = dependencies.getSheetData || getSheetData;
    const sheetUpdater = dependencies.updateSheetData || updateSheetData;
    const cacheInvalidator = dependencies.invalidateCache || invalidateCache;
    const accessPolicy = dependencies.legacySheetAccessPolicy || buildLegacySheetAccessPolicy();

    return {
        async listRows(communityId, profileId = '1') {
            assertLegacySheetAccessAllowed('app_logs', accessPolicy);
            const rows = await sheetGetter(APP_LOG_SHEET, communityId, profileId);
            return Array.isArray(rows) ? rows : [];
        },

        async replaceRows(communityId, profileId = '1', updater) {
            assertLegacySheetAccessAllowed('app_logs', accessPolicy);
            return sheetUpdater(APP_LOG_SHEET, communityId, profileId, updater);
        },

        invalidateRowsCache(communityId, profileId = '1') {
            assertLegacySheetAccessAllowed('app_logs', accessPolicy);
            cacheInvalidator(APP_LOG_SHEET, communityId, profileId);
        }
    };
}

module.exports = {
    APP_LOG_SHEET,
    createLegacyAppLogsSheetAdapter
};
