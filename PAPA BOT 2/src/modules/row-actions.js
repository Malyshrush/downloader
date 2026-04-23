/**
 * Модуль выполнения действий строки
 */

const { log } = require('../utils/logger');
const { updateUserBotAndStep, updateUserGroups } = require('./users');
const { performVariableActions } = require('./variables');
const { addAppLog } = require('./app-logs');
const { createDelayedDeliveryStore } = require('./delayed-delivery-store');

const delayedDeliveryStore = createDelayedDeliveryStore();

function getDelayedDeliveryStore(overrides = {}) {
    return overrides.delayedDeliveryStore || delayedDeliveryStore;
}

function isDelayedDeliveryStoreEnabled(overrides = {}) {
    const store = getDelayedDeliveryStore(overrides);
    return Boolean(store && typeof store.isEnabled === 'function' && store.isEnabled());
}

/**
 * Выполнить все действия строки конфигурации
 */
async function performRowActions(row, userId, groupId, isComment = false, communityId = null, profileId = '1') {
    try {
        log('debug', `🛠️ Performing FULL row actions for user ${userId}`);

        // Конвертируем communityId → vk_group_id для правильного имени файла
        let fileCommunityId = communityId;
        try {
            const { getCommunityConfig } = require('./config');
            const config = await getCommunityConfig(communityId, profileId);
            if (config && config.vk_group_id) {
                fileCommunityId = config.vk_group_id.toString();
            }
        } catch(e) {}

        log('debug', `🛠️ row-actions: communityId=${communityId} → fileCommunityId=${fileCommunityId}`);

        // 1. Обновление бота и шага
        const bot = (row['Бот'] || '').trim();
        const step = (row['Шаг'] || '').trim();

        if (bot && step) {
            log('debug', `🤖 Setting bot: "${bot}", step: "${step}"`);
            await updateUserBotAndStep(userId, bot, step, fileCommunityId, profileId);
        }

        // 2. Добавление групп
        const addGroups = (row['ДОБАВИТЬ ГРУППУ'] || row['ГРУППА'] || '').trim();
        if (addGroups) {
            log('debug', `➕ Adding groups: "${addGroups}"`);
            await updateUserGroups(userId, addGroups, '', fileCommunityId, profileId);
        }
        
        // 3. Удаление групп
        const removeGroups = (row['УДАЛИТЬ ГРУППУ'] || '').trim();
        if (removeGroups) {
            log('debug', `➖ Removing groups: "${removeGroups}"`);
            await updateUserGroups(userId, '', removeGroups, fileCommunityId, profileId);
        }
        
        // 4. Действия с переменными — раздельно для ПП и ГП
        const ppActions = (row['Действия с ПП'] || '').trim();
        const gpActions = (row['Действия с ГП'] || '').trim();
        const pvsActions = (row['Действия с ПВС'] || '').trim();
        // Старый формат для обратной совместимости
        const oldVarActions = (row['Действия с ПП/ГП/ПВК'] || '').trim();

        if (ppActions) {
            log('debug', `🔧 Performing PP (user) variable actions: "${ppActions}"`);
            await performVariableActions(ppActions, userId, groupId, true, fileCommunityId, 'user', profileId);
        }
        if (gpActions) {
            log('debug', `🔧 Performing GP (global) variable actions: "${gpActions}"`);
            await performVariableActions(gpActions, userId, groupId, true, fileCommunityId, 'global', profileId);
        }
        if (pvsActions) {
            log('debug', `🔧 Performing PVS (shared) variable actions: "${pvsActions}"`);
            await performVariableActions(pvsActions, userId, groupId, true, fileCommunityId, 'shared', profileId);
        }
        if (oldVarActions && !ppActions && !gpActions && !pvsActions) {
            log('debug', `🔧 Performing old-format variable actions: "${oldVarActions}"`);
            await performVariableActions(oldVarActions, userId, groupId, true, fileCommunityId, 'auto', profileId);
        }
        
        // 5. Отправка на шаг с задержкой
        const sendToStep = (row['Отправить на Шаг'] || '').trim();
        const delayStr = (row['Задержка отправки на Шаг'] || '').trim();

        if (sendToStep && delayStr) {
            const stepData = sendToStep + ',' + delayStr;
            log('debug', `⏰ Scheduling step: "${stepData}"`);
            await scheduleStepMessage(userId, groupId, stepData, isComment, fileCommunityId, profileId);
        } else if (sendToStep) {
            const stepData = sendToStep + ',0';
            log('debug', `⏰ Scheduling step without delay: "${stepData}"`);
            await scheduleStepMessage(userId, groupId, stepData, isComment, fileCommunityId, profileId);
        }

        log('debug', `✅ All row actions completed for user ${userId}`);
    } catch (error) {
        log('error', '❌ Error performing row actions:', error);
    }
}

/**
 * Запланировать отложенное сообщение
 */
async function scheduleStepMessageLegacy(userId, groupId, stepData, isComment = false, communityId = null, profileId = '1', overrides = {}) {
    try {
        const [step, delayStr] = stepData.split(',').map(s => s.trim());
        const delay = parseDelay(delayStr);

        log('debug', `⏰ Scheduling step: "${stepData}" → step="${step}", delay=${delay}sec`);

        if (delay > 0 && step) {
            const { getSheetData, updateSheetData, invalidateCache } = require('./storage');
            const getSheetDataImpl = overrides.getSheetData || getSheetData;
            const updateSheetDataImpl = overrides.updateSheetData || updateSheetData;
            const invalidateCacheImpl = overrides.invalidateCache || invalidateCache;
            const getCommunityConfig = overrides.getCommunityConfig || require('./config').getCommunityConfig;
            const addAppLogImpl = overrides.addAppLog || addAppLog;

            // Московское время = UTC + 3 часа
            const mskOffset = 3 * 60 * 60 * 1000;
            const inputNow = overrides.now instanceof Date ? overrides.now : new Date();
            const scheduledTimeMsk = new Date(inputNow.getTime() + delay * 1000 + mskOffset);
            const mskTimeStr = scheduledTimeMsk.toISOString().replace('T', ' ').substring(0, 19);

            // Получаем vk_group_id для корректного имени файла
            let fileCommunityId = communityId;
            try {
                const config = await getCommunityConfig(communityId, profileId);
                if (config && config.vk_group_id) {
                    fileCommunityId = config.vk_group_id.toString();
                }
            } catch(e) {}

            const delayed = await getSheetDataImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);

            delayed.push({
                '№': (delayed.length + 1).toString(),
                'Шаг': step,
                'ID Пользователя': userId.toString(),
                'Группа': '',
                'Тип': isComment ? 'comment' : 'message',
                'Дата и время отправки': mskTimeStr,
                'Дата и время отправки (по мск.)': mskTimeStr,
                'Статус': 'Ожидает',
                'Фактическое время отправки': '',
                'Факт. время отправки (по мск.)': '',
                'Ошибка': ''
            });

            await updateSheetDataImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId, function() { return delayed; });
            invalidateCacheImpl('ОТЛОЖЕННЫЕ', fileCommunityId, profileId);
            log('debug', `✅ Step ${step} saved to delayed for ${userId} at ${mskTimeStr} мск. (file: ${fileCommunityId})`);
            await addAppLogImpl({
                tab: 'DELAYED',
                title: 'Запланировано отложенное сообщение',
                summary: 'Шаг ' + step + ' будет отправлен позже',
                details: ['Пользователь: ' + userId, 'Время: ' + mskTimeStr],
                communityId: fileCommunityId,
                profileId
            });
        } else {
            log('warn', `⚠️ Step NOT scheduled: delay=${delay}, step="${step}"`);
        }
    } catch (error) {
        log('error', '❌ Error scheduling step message:', error);
    }
}

/**
 * Распарсить задержку
 */
async function scheduleStepMessageWithDependencies(userId, groupId, stepData, isComment = false, communityId = null, profileId = '1', overrides = {}) {
    if (!isDelayedDeliveryStoreEnabled(overrides)) {
        return scheduleStepMessageLegacy(userId, groupId, stepData, isComment, communityId, profileId, overrides);
    }

    try {
        const [step, delayStr] = stepData.split(',').map(s => s.trim());
        const delay = parseDelay(delayStr);
        if (delay <= 0 || !step) {
            log('warn', `вљ пёЏ Step NOT scheduled: delay=${delay}, step="${step}"`);
            return;
        }

        const inputNow = overrides.now instanceof Date ? overrides.now : new Date();
        const mskOffset = 3 * 60 * 60 * 1000;
        const scheduledTimeMsk = new Date(inputNow.getTime() + delay * 1000 + mskOffset);
        const mskTimeStr = scheduledTimeMsk.toISOString().replace('T', ' ').substring(0, 19);
        const getCommunityConfigImpl = overrides.getCommunityConfig || require('./config').getCommunityConfig;
        const addAppLogImpl = overrides.addAppLog || addAppLog;

        let fileCommunityId = communityId;
        try {
            const config = await getCommunityConfigImpl(communityId, profileId);
            if (config && config.vk_group_id) {
                fileCommunityId = config.vk_group_id.toString();
            }
        } catch (error) {}

        const delayedRow = {
            'Шаг': step,
            'РЁР°Рі': step,
            'ID Пользователя': userId.toString(),
            'ID РџРѕР»СЊР·РѕРІР°С‚РµР»СЏ': userId.toString(),
            'Группа': '',
            'Р“СЂСѓРїРїР°': '',
            'Тип': isComment ? 'comment' : 'message',
            'РўРёРї': isComment ? 'comment' : 'message',
            'Дата и время отправки': mskTimeStr,
            'Р”Р°С‚Р° Рё РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё': mskTimeStr,
            'Дата и время отправки (по мск.)': mskTimeStr,
            'Р”Р°С‚Р° Рё РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)': mskTimeStr,
            'Статус': 'Ожидает',
            'РЎС‚Р°С‚СѓСЃ': 'РћР¶РёРґР°РµС‚',
            'Фактическое время отправки': '',
            'Р¤Р°РєС‚РёС‡РµСЃРєРѕРµ РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё': '',
            'Факт. время отправки (по мск.)': '',
            'Р¤Р°РєС‚. РІСЂРµРјСЏ РѕС‚РїСЂР°РІРєРё (РїРѕ РјСЃРє.)': '',
            'Ошибка': '',
            'РћС€РёР±РєР°': ''
        };

        await getDelayedDeliveryStore(overrides).appendDelayedRow(fileCommunityId, delayedRow, profileId);
        await addAppLogImpl({
            tab: 'DELAYED',
            title: 'Запланировано отложенное сообщение',
            summary: 'Шаг ' + step + ' будет отправлен позже',
            details: ['Пользователь: ' + userId, 'Время: ' + mskTimeStr],
            communityId: fileCommunityId,
            profileId
        });
    } catch (error) {
        log('error', 'вќЊ Error scheduling step message:', error);
    }
}

async function scheduleStepMessage(userId, groupId, stepData, isComment = false, communityId = null, profileId = '1') {
    return scheduleStepMessageWithDependencies(userId, groupId, stepData, isComment, communityId, profileId);
}

function parseDelay(delayStr) {
    if (!delayStr) return 0;
    
    const str = delayStr.trim().toLowerCase();
    const value = parseInt(str) || 0;
    
    if (str.includes('ч.') || str.includes('часов') || str.includes('часа') || str.includes('час')) {
        return value * 3600;
    }
    if (str.includes('мин.') || str.includes('минут') || str.includes('минуту') || str.includes('мин')) {
        return value * 60;
    }
    
    return value; // секунды по умолчанию
}

module.exports = {
    performRowActions,
    scheduleStepMessage,
    parseDelay,
    __testOnly: {
        scheduleStepMessageWithDependencies
    }
};
