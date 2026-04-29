const { getSheetData, saveSheetData, updateSheetData, invalidateCache } = require('./storage');

const DELAYED_SHEET = 'ОТЛОЖЕННЫЕ';
const MAILING_SHEET = 'РАССЫЛКА';
const MESSAGE_SHEET = 'СООБЩЕНИЯ';
const COMMENT_SHEET = 'КОММЕНТАРИИ В ПОСТАХ';

function createLegacySchedulerSheetAdapter(dependencies = {}) {
    const sheetGetter = dependencies.getSheetData || getSheetData;
    const sheetSaver = dependencies.saveSheetData || saveSheetData;
    const sheetUpdater = dependencies.updateSheetData || updateSheetData;
    const cacheInvalidator = dependencies.invalidateCache || invalidateCache;

    return {
        async listMessageRows(communityId, profileId = '1') {
            const rows = await sheetGetter(MESSAGE_SHEET, communityId, profileId);
            return Array.isArray(rows) ? rows : [];
        },

        async listCommentRows(communityId, profileId = '1') {
            const rows = await sheetGetter(COMMENT_SHEET, communityId, profileId);
            return Array.isArray(rows) ? rows : [];
        },

        async listDelayedRows(communityId, profileId = '1') {
            const rows = await sheetGetter(DELAYED_SHEET, communityId, profileId);
            return Array.isArray(rows) ? rows : [];
        },

        async replaceDelayedRows(communityId, profileId = '1', rows = []) {
            await sheetSaver(DELAYED_SHEET, rows, communityId, profileId);
            cacheInvalidator(DELAYED_SHEET, communityId, profileId);
        },

        async appendDelayedRow(communityId, profileId = '1', row = {}) {
            let nextRows = [];
            await sheetUpdater(DELAYED_SHEET, communityId, profileId, async currentRows => {
                nextRows = Array.isArray(currentRows) ? currentRows.slice() : [];
                nextRows.push(row);
                return nextRows;
            });
            cacheInvalidator(DELAYED_SHEET, communityId, profileId);
            return nextRows;
        },

        async listMailingRows(communityId, profileId = '1') {
            const rows = await sheetGetter(MAILING_SHEET, communityId, profileId);
            return Array.isArray(rows) ? rows : [];
        },

        async replaceMailingRows(communityId, profileId = '1', rows = []) {
            await sheetSaver(MAILING_SHEET, rows, communityId, profileId);
            cacheInvalidator(MAILING_SHEET, communityId, profileId);
        }
    };
}

module.exports = {
    createLegacySchedulerSheetAdapter,
    DELAYED_SHEET,
    MAILING_SHEET,
    MESSAGE_SHEET,
    COMMENT_SHEET
};
