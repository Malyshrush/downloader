require('dotenv').config();

const { initializeStorage, getSheetData, saveSheetData } = require('../src/modules/storage');
const { processStructuredTriggers } = require('../src/modules/structured-triggers');

const PROFILE_ID = '1';
const COMMUNITY_ID = '219331507';
const GROUP_ID = 219331507;
const TEST_USER_ID = 900000001;

function clone(value) {
    return JSON.parse(JSON.stringify(value));
}

function makeBaseUserRow() {
    return {
        'ID': String(TEST_USER_ID),
        'ИМЯ': 'Автотест Триггеров',
        'ГРУППА': '',
        'Пользовательская': '',
        'Значения ПП': '',
        'Текущий Бот': '',
        'Текущий Шаг': '',
        'Отправленные Шаги': ''
    };
}

function buildUsersForTrigger(originalUsers, row) {
    const rows = clone(originalUsers).filter(user => String(user['ID'] || '').trim() !== String(TEST_USER_ID));
    const user = makeBaseUserRow();
    const actionCode = String(row['Код действия'] || '').trim();

    if (actionCode === 'remove_group' && row['Группа']) {
        user['ГРУППА'] = String(row['Группа']).trim();
    }

    if (actionCode === 'remove_from_bot' && row['Бот']) {
        user['Текущий Бот'] = String(row['Бот']).trim();
        user['Текущий Шаг'] = 'BASE_STEP';
    }

    rows.push(user);
    return rows;
}

function normalizeList(value) {
    return String(value || '')
        .split(/[\r\n,]+/)
        .map(item => item.trim())
        .filter(Boolean);
}

function buildMessagePayload(row, type) {
    const conditionCode = String(row['Код условия'] || '').trim();
    const value = String(row['Значение'] || '').trim();
    let text = 'автотест';
    let attachments = [];
    let payload = null;

    if (conditionCode === 'text_equals') text = value;
    else if (conditionCode === 'text_not_equals') text = 'другое значение';
    else if (conditionCode === 'text_contains') text = 'начало ' + value + ' конец';
    else if (conditionCode === 'text_not_contains') text = 'другое значение';
    else if (conditionCode === 'text_regex') text = 'тест 123';
    else if (conditionCode === 'phone_ru') text = '+79991234567';
    else if (conditionCode === 'email') text = 'test@example.com';
    else if (conditionCode === 'number') text = '42';
    else if (conditionCode === 'number_less_than') text = String(Math.max(1, Number(value || 10) - 1));
    else if (conditionCode === 'number_greater_than') text = String(Number(value || 10) + 1);
    else if (conditionCode === 'message_has_photo') attachments = [{ type: 'photo' }];
    else if (conditionCode === 'message_has_video') attachments = [{ type: 'video' }];
    else if (conditionCode === 'message_has_audio') attachments = [{ type: 'audio' }];
    else if (conditionCode === 'message_has_document') attachments = [{ type: 'doc' }];
    else if (conditionCode === 'message_has_voice') attachments = [{ type: 'audio_message' }];
    else if (conditionCode === 'message_has_product') attachments = [{ type: 'market' }];

    if (type === 'message_event') {
        payload = { buttonLabel: value || 'Кнопка 1' };
        text = value || 'Кнопка 1';
    }

    if (type === 'message_event') {
        return {
            group_id: GROUP_ID,
            type: 'message_event',
            object: {
                user_id: TEST_USER_ID,
                peer_id: TEST_USER_ID,
                payload
            }
        };
    }

    return {
        group_id: GROUP_ID,
        type,
        object: {
            message: {
                from_id: TEST_USER_ID,
                peer_id: TEST_USER_ID,
                text,
                payload,
                attachments
            }
        }
    };
}

function buildPostPayload(row, type) {
    const value = String(row['Значение'] || '').trim();
    const links = normalizeList(value);
    const link = links[0] || 'https://vk.com/wall-219331507_1';
    const match = link.match(/wall(-?\d+)_(\d+)/i);
    const ownerId = match ? Number(match[1]) : -GROUP_ID;
    const postId = match ? Number(match[2]) : 1;

    if (type === 'wall_repost') {
        return {
            group_id: GROUP_ID,
            type,
            object: {
                from_id: TEST_USER_ID,
                copy_history: [{ owner_id: ownerId, id: postId }]
            }
        };
    }

    if (type === 'like_add') {
        return {
            group_id: GROUP_ID,
            type,
            object: {
                liker_id: TEST_USER_ID,
                object_type: 'post',
                object_owner_id: ownerId,
                object_id: postId
            }
        };
    }

    const extraCode = String(row['Код доп. условия'] || '').trim();
    let text = 'комментарий автотест';
    if (extraCode === 'comment_text_contains') {
        text = 'найдено ' + String(row['Доп. значение'] || '').trim();
    }

    return {
        group_id: GROUP_ID,
        type,
        object: {
            from_id: TEST_USER_ID,
            post_owner_id: ownerId,
            post_id: postId,
            text
        }
    };
}

function buildMembershipPayload(type) {
    return {
        group_id: GROUP_ID,
        type,
        object: {
            user_id: TEST_USER_ID
        }
    };
}

function buildEventForRow(row) {
    const eventCode = String(row['Код события'] || '').trim();

    if (eventCode === 'incoming_message') return buildMessagePayload(row, 'message_new');
    if (eventCode === 'outgoing_message') return buildMessagePayload(row, 'message_reply');
    if (eventCode === 'message_button_click') return buildMessagePayload(row, 'message_event');
    if (eventCode === 'wall_repost') return buildPostPayload(row, 'wall_repost');
    if (eventCode === 'wall_like') return buildPostPayload(row, 'like_add');
    if (eventCode === 'wall_comment_add') return buildPostPayload(row, 'wall_reply_new');
    if (eventCode === 'wall_comment_delete') return buildPostPayload(row, 'wall_reply_delete');
    if (eventCode === 'user_group_join') return buildMembershipPayload('group_join');
    if (eventCode === 'user_group_leave') return buildMembershipPayload('group_leave');

    throw new Error('Unsupported event code: ' + eventCode);
}

function validateAction(row, usersAfter) {
    const actionCode = String(row['Код действия'] || '').trim();
    const user = usersAfter.find(item => String(item['ID'] || '').trim() === String(TEST_USER_ID));
    if (!user) return { ok: false, message: 'Тестовый пользователь не найден после срабатывания' };

    if (actionCode === 'add_group') {
        const groups = normalizeList(user['ГРУППА']);
        return groups.includes(String(row['Группа'] || '').trim())
            ? { ok: true }
            : { ok: false, message: 'Группа не добавилась' };
    }

    if (actionCode === 'remove_group') {
        const groups = normalizeList(user['ГРУППА']);
        return !groups.includes(String(row['Группа'] || '').trim())
            ? { ok: true }
            : { ok: false, message: 'Группа не удалилась' };
    }

    if (actionCode === 'add_to_bot') {
        const bots = normalizeList(user['Текущий Бот']);
        const steps = normalizeList(user['Текущий Шаг']);
        const botIndex = bots.indexOf(String(row['Бот'] || '').trim());
        return botIndex !== -1 && String(steps[botIndex] || '').includes(String(row['Шаг'] || '').trim())
            ? { ok: true }
            : { ok: false, message: 'Бот/шаг не добавились' };
    }

    if (actionCode === 'remove_from_bot') {
        const bots = normalizeList(user['Текущий Бот']);
        return !bots.includes(String(row['Бот'] || '').trim())
            ? { ok: true }
            : { ok: false, message: 'Бот не удалился' };
    }

    return { ok: true };
}

async function run() {
    await initializeStorage();

    const originalTriggers = await getSheetData('ТРИГГЕРЫ', COMMUNITY_ID, PROFILE_ID);
    const originalUsers = await getSheetData('ПОЛЬЗОВАТЕЛИ', COMMUNITY_ID, PROFILE_ID);

    const results = [];

    try {
        for (let i = 0; i < originalTriggers.length; i++) {
            const row = clone(originalTriggers[i]);
            const isolatedRows = clone(originalTriggers).map((item, idx) => {
                item['Активен'] = idx === i ? 'ДА' : 'НЕТ';
                return item;
            });

            await saveSheetData('ТРИГГЕРЫ', isolatedRows, COMMUNITY_ID, PROFILE_ID);
            await saveSheetData('ПОЛЬЗОВАТЕЛИ', buildUsersForTrigger(originalUsers, row), COMMUNITY_ID, PROFILE_ID);

            const event = buildEventForRow(row);
            const outcome = await processStructuredTriggers(event, PROFILE_ID);
            const usersAfter = await getSheetData('ПОЛЬЗОВАТЕЛИ', COMMUNITY_ID, PROFILE_ID);
            const validation = validateAction(row, usersAfter);

            const ok = !!outcome.matched && !!outcome.handled && validation.ok;
            results.push({
                index: i + 1,
                title: row['Название'] || ('Триггер #' + (i + 1)),
                ok,
                details: ok ? 'ok' : ((outcome.matched && outcome.handled) ? validation.message : 'Триггер не сработал')
            });
            console.log((ok ? 'PASS' : 'FAIL') + ' [' + (i + 1) + '/' + originalTriggers.length + '] ' + results[results.length - 1].title + ' -> ' + results[results.length - 1].details);
        }
    } finally {
        await saveSheetData('ТРИГГЕРЫ', originalTriggers, COMMUNITY_ID, PROFILE_ID);
        await saveSheetData('ПОЛЬЗОВАТЕЛИ', originalUsers, COMMUNITY_ID, PROFILE_ID);
    }

    const failed = results.filter(item => !item.ok);
    console.log('TOTAL=' + results.length + ' PASSED=' + (results.length - failed.length) + ' FAILED=' + failed.length);
    if (failed.length) {
        failed.forEach(item => {
            console.log('FAILED_ITEM #' + item.index + ': ' + item.title + ' -> ' + item.details);
        });
        process.exitCode = 1;
    }
}

run().catch(error => {
    console.error(error);
    process.exit(1);
});
