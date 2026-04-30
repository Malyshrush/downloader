require('module').Module._initPaths();
require('dotenv').config();

const axios = require('axios');
const { initializeStorage, getSheetData, saveSheetData } = require('../src/modules/storage');
const structuredTriggers = require('../src/modules/structured-triggers');
const { getVkToken } = require('../src/modules/config');
const {
  getUserVariables,
  listUsers,
  removeUserBotAndStep,
  updateUserData,
  updateUserGroups,
  updateUserVariables
} = require('../src/modules/users');
const { getGlobalVariables, getProfileUserSharedVariables } = require('../src/modules/variables');

const PROFILE = '1';
const C_MAIN = '229445618';
const G_MAIN = 229445618;
const C_CLOSED = '219331507';
const G_CLOSED = 219331507;
const USER_ADMIN = 27894453;
const USER_TEST = 787794248;

const S = {
  messages: '\u0421\u041e\u041e\u0411\u0429\u0415\u041d\u0418\u042f',
  comments: '\u041a\u041e\u041c\u041c\u0415\u041d\u0422\u0410\u0420\u0418\u0418 \u0412 \u041f\u041e\u0421\u0422\u0410\u0425',
  users: '\u041f\u041e\u041b\u042c\u0417\u041e\u0412\u0410\u0422\u0415\u041b\u0418',
  variables: '\u041f\u0415\u0420\u0415\u041c\u0415\u041d\u041d\u042b\u0415'
};

const K = {
  title: '\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435',
  event: '\u041a\u043e\u0434 \u0441\u043e\u0431\u044b\u0442\u0438\u044f',
  cond: '\u041a\u043e\u0434 \u0443\u0441\u043b\u043e\u0432\u0438\u044f',
  value: '\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435',
  extraCond: '\u041a\u043e\u0434 \u0434\u043e\u043f. \u0443\u0441\u043b\u043e\u0432\u0438\u044f',
  extraValue: '\u0414\u043e\u043f. \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u0435',
  action: '\u041a\u043e\u0434 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044f',
  actionCommunityId: 'ID \u0441\u043e\u043e\u0431\u0449\u0435\u0441\u0442\u0432\u0430 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044f',
  actionVarName: '\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043f\u0435\u0440\u0435\u043c\u0435\u043d\u043d\u043e\u0439',
  actionVarValue: '\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043f\u0435\u0440\u0435\u043c\u0435\u043d\u043d\u043e\u0439',
  group: '\u0413\u0440\u0443\u043f\u043f\u0430',
  bot: '\u0411\u043e\u0442',
  step: '\u0428\u0430\u0433',
  answer: '\u041e\u0442\u0432\u0435\u0442',
  addGroup: '\u0414\u041e\u0411\u0410\u0412\u0418\u0422\u042c \u0413\u0420\u0423\u041f\u041f\u0423',
  ppActions: '\u0414\u0435\u0439\u0441\u0442\u0432\u0438\u044f \u0441 \u041f\u041f',
  id: 'ID',
  groupsCol: '\u0413\u0420\u0423\u041f\u041f\u0410',
  curBot: '\u0422\u0435\u043a\u0443\u0449\u0438\u0439 \u0411\u043e\u0442',
  curStep: '\u0422\u0435\u043a\u0443\u0449\u0438\u0439 \u0428\u0430\u0433'
};

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function yes() {
  return '\u0414\u0410';
}

function list(value) {
  return String(value || '').split(/[\r\n,]+/).map(item => item.trim()).filter(Boolean);
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function userRow(rows, userId) {
  return rows.find(row => String(row[K.id] || '').trim() === String(userId));
}

async function runtimeUserRow(userId, communityId = C_MAIN) {
  const users = await listUsers(communityId, PROFILE);
  return userRow(users, userId);
}

function trigger(name, eventCode, conditionCode, value, actionCode, extra = {}) {
  return Object.assign({
    [K.title]: name,
    [K.event]: eventCode,
    [K.cond]: conditionCode || 'any_message',
    [K.value]: value || '',
    [K.action]: actionCode,
    '\u0410\u043a\u0442\u0438\u0432\u0435\u043d': yes(),
    '\u041d\u0435 \u043f\u0440\u0438\u043c\u0435\u043d\u044f\u0442\u044c \u043e\u0441\u0442\u0430\u043b\u044c\u043d\u044b\u0435 \u043f\u0440\u0430\u0432\u0438\u043b\u0430': yes()
  }, extra);
}

function storeWith(row) {
  return { isEnabled: () => true, listTriggerRows: async () => ({ initialized: true, rows: [row] }) };
}

async function runTrigger(row, event, overrides = {}) {
  return structuredTriggers.__testOnly.processStructuredTriggersWithDependencies(
    event,
    PROFILE,
    Object.assign({ structuredTriggerStore: storeWith(row) }, overrides)
  );
}

function message(text, userId = USER_TEST, groupId = G_MAIN, attachments = []) {
  return { type: 'message_new', group_id: groupId, object: { message: { from_id: userId, peer_id: userId, text, attachments } } };
}

function button(label) {
  return { type: 'message_event', group_id: G_MAIN, object: { user_id: USER_TEST, peer_id: USER_TEST, payload: { buttonLabel: label } } };
}

function comment(text) {
  return { type: 'wall_reply_new', group_id: G_MAIN, object: { id: Date.now() % 100000, post_id: 1, from_id: USER_TEST, text } };
}

function group(type, userId, groupId, request = false) {
  return { type, group_id: groupId, object: { user_id: userId, join_type: request ? 'request' : 'join', joined_by_request: request || undefined } };
}

async function vk(method, params, token) {
  const response = await axios.post('https://api.vk.com/method/' + method, null, {
    params: Object.assign({}, params, { access_token: token, v: '5.199' }),
    timeout: 25000
  });
  return response.data;
}

async function isMember(communityId, userId) {
  const token = await getVkToken(0, communityId, PROFILE);
  const data = await vk('groups.isMember', { group_id: communityId, user_id: userId }, token);
  return data.response;
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const results = [];

async function test(name, fn) {
  const started = Date.now();
  try {
    const details = await fn();
    results.push({ name, ok: true, ms: Date.now() - started, details });
    console.log('PASS ' + name + ' ' + JSON.stringify(details || {}));
  } catch (error) {
    results.push({ name, ok: false, ms: Date.now() - started, error: error.message });
    console.log('FAIL ' + name + ' ' + error.message);
  }
}

async function withRestoredMainState(fn) {
  const users = await getSheetData(S.users, C_MAIN, PROFILE);
  try {
    return await fn();
  } finally {
    await saveSheetData(S.users, users, C_MAIN, PROFILE);
  }
}

async function ensureRuntimeUser(userId, communityId = C_MAIN) {
  await updateUserData(userId, communityId, PROFILE);
}

async function removeRuntimeGroups(userId, groups, communityId = C_MAIN) {
  const value = Array.isArray(groups) ? groups.join('\n') : String(groups || '');
  if (!value.trim()) return;
  await updateUserGroups(userId, '', value, communityId, PROFILE);
}

async function removeRuntimeBots(userId, bots, communityId = C_MAIN) {
  for (const bot of bots) {
    if (String(bot || '').trim()) {
      await removeUserBotAndStep(userId, bot, communityId, PROFILE);
    }
  }
}

async function withRestoredUserVariables(userId, communityId, fn) {
  const before = await getUserVariables(userId, communityId, PROFILE);
  try {
    return await fn();
  } finally {
    await updateUserVariables(userId, before, true, communityId, PROFILE);
  }
}

async function runPreflight() {
  await test('preflight memberships', async () => ({
    main278: await isMember(C_MAIN, USER_ADMIN),
    main787: await isMember(C_MAIN, USER_TEST),
    closed278: await isMember(C_CLOSED, USER_ADMIN),
    closed787: await isMember(C_CLOSED, USER_TEST)
  }));
}

async function runConditions() {
  await withRestoredMainState(async () => {
    await ensureRuntimeUser(USER_TEST);
    const cases = [
      ['incoming any', trigger('rt-any', 'incoming_message', 'any_message', '', 'add_group', { [K.group]: 'rt_any' }), message('hello')],
      ['text equals', trigger('rt-eq', 'incoming_message', 'text_equals', 'rt_eq', 'add_group', { [K.group]: 'rt_eq' }), message('rt_eq')],
      ['text contains', trigger('rt-contains', 'incoming_message', 'text_contains', 'needle', 'add_group', { [K.group]: 'rt_contains' }), message('has needle')],
      ['regex', trigger('rt-regex', 'incoming_message', 'text_regex', 'rt-[0-9]+', 'add_group', { [K.group]: 'rt_regex' }), message('rt-123')],
      ['email', trigger('rt-email', 'incoming_message', 'email', '', 'add_group', { [K.group]: 'rt_email' }), message('test@example.com')],
      ['phone', trigger('rt-phone', 'incoming_message', 'phone_ru', '', 'add_group', { [K.group]: 'rt_phone' }), message('+79991234567')],
      ['photo attachment', trigger('rt-photo', 'incoming_message', 'message_has_photo', '', 'add_group', { [K.group]: 'rt_photo' }), message('', USER_TEST, G_MAIN, [{ type: 'photo' }])],
      ['button label', trigger('rt-button', 'message_button_click', 'button_label_equals', 'rt_button', 'add_group', { [K.group]: 'rt_button' }), button('rt_button')],
      ['wall comment', trigger('rt-comment', 'wall_comment_add', 'any_post', '', 'add_group', { [K.extraCond]: 'comment_text_contains', [K.extraValue]: 'ok-comment', [K.group]: 'rt_comment' }), comment('text ok-comment')],
      ['group join synthetic real user', trigger('rt-join', 'user_group_join', 'any_message', '', 'add_group', { [K.group]: 'rt_join' }), group('group_join', USER_TEST, G_MAIN)]
    ];
    for (const [name, row, event] of cases) {
      await test('condition ' + name, async () => {
        const result = await runTrigger(row, event);
        const current = await runtimeUserRow(USER_TEST);
        assert(result.matched && result.handled, 'not matched/handled');
        assert(current, 'runtime user row missing');
        assert(list(current[K.groupsCol]).includes(row[K.group]), 'action group not added');
        await removeRuntimeGroups(USER_TEST, row[K.group]);
        return { group: row[K.group] };
      });
    }
  });
}

async function runBotActions() {
  await withRestoredMainState(async () => {
    await ensureRuntimeUser(USER_ADMIN);
    await withRestoredUserVariables(USER_ADMIN, C_MAIN, async () => {
      await test('action add_to_bot full cycle real message', async () => {
      const bot = 'RT Full Bot';
      const step = 'Step1';
      const row = trigger('rt-add-to-bot', 'incoming_message', 'text_equals', 'rt_add_to_bot', 'add_to_bot', { [K.bot]: bot, [K.step]: step });
      const messageRows = [{ [K.bot]: bot, [K.step]: step, [K.answer]: 'RT add_to_bot real answer ' + Date.now(), [K.addGroup]: 'rt_full_group', [K.ppActions]: 'rt_full_pp=ok' }];
      const result = await runTrigger(row, message('rt_add_to_bot', USER_ADMIN), { loadMessageRows: async () => messageRows, loadCommentRows: async () => [] });
      await sleep(1000);
      const current = await runtimeUserRow(USER_ADMIN);
      const vars = await getUserVariables(USER_ADMIN, C_MAIN, PROFILE);
      assert(result.matched && result.handled, 'not handled');
      assert(current, 'runtime user row missing');
      assert(list(current[K.groupsCol]).includes('rt_full_group'), 'group action missing');
      assert(String(current[K.curBot] || '').includes(bot), 'bot not assigned');
      assert(String(current[K.curStep] || '').includes(step), 'step not assigned');
      assert(vars.rt_full_pp === 'ok', 'PP action missing');
      await removeRuntimeGroups(USER_ADMIN, 'rt_full_group');
      await removeRuntimeBots(USER_ADMIN, [bot]);
      return { sentTo: USER_ADMIN, bot, step };
      });

      await test('action send_bot_answer real message answer only', async () => {
      const bot = 'RT Answer Bot';
      const step = 'Only';
      const row = trigger('rt-send-answer', 'incoming_message', 'text_equals', 'rt_send_answer', 'send_bot_answer', { [K.bot]: bot, [K.step]: step });
      const messageRows = [{ [K.bot]: bot, [K.step]: step, [K.answer]: 'RT answer-only real answer ' + Date.now(), [K.addGroup]: 'rt_forbidden_group', [K.ppActions]: 'rt_forbidden_pp=bad' }];
      const result = await runTrigger(row, message('rt_send_answer', USER_ADMIN), { loadMessageRows: async () => messageRows, loadCommentRows: async () => [] });
      await sleep(1000);
      const current = await runtimeUserRow(USER_ADMIN);
      const vars = await getUserVariables(USER_ADMIN, C_MAIN, PROFILE);
      assert(result.matched && result.handled, 'not handled');
      assert(current, 'runtime user row missing');
      assert(!list(current[K.groupsCol]).includes('rt_forbidden_group'), 'forbidden group was added');
      assert(vars.rt_forbidden_pp !== 'bad', 'forbidden PP changed');
      assert(!String(current[K.curBot] || '').includes(bot), 'answer-only assigned bot');
      return { sentTo: USER_ADMIN, actionsSkipped: true };
      });
    });
  });
}

async function runVariablesAndDelete() {
  await withRestoredMainState(async () => {
    await ensureRuntimeUser(USER_TEST);
    await withRestoredUserVariables(USER_TEST, C_MAIN, async () => {
      await test('action user/global/shared variables', async () => {
      await runTrigger(trigger('user-var', 'incoming_message', 'text_equals', 'rt_user_var', 'user_var_add', { [K.actionVarName]: 'rt_user_var', [K.actionVarValue]: 'v1' }), message('rt_user_var'));
      await runTrigger(trigger('global-var', 'incoming_message', 'text_equals', 'rt_global_var', 'global_var_add', { [K.actionVarName]: 'rt_global_var', [K.actionVarValue]: 'v1' }), message('rt_global_var'));
      await runTrigger(trigger('shared-var', 'incoming_message', 'text_equals', 'rt_shared_var', 'shared_var_add', { [K.actionVarName]: 'rt_shared_var', [K.actionVarValue]: 'v1' }), message('rt_shared_var'));
      const userVars = await getUserVariables(USER_TEST, C_MAIN, PROFILE);
      const globalVars = (await getGlobalVariables(C_MAIN, PROFILE)).globalVars;
      const sharedVars = await getProfileUserSharedVariables(USER_TEST, PROFILE);
      assert(userVars.rt_user_var === 'v1', 'user var missing');
      assert(globalVars.rt_global_var === 'v1', 'global var missing');
      assert(sharedVars.rt_shared_var === 'v1', 'shared var missing');
      await runTrigger(trigger('user-var-delete', 'incoming_message', 'text_equals', 'rt_user_var_delete', 'user_var_delete', { [K.actionVarName]: 'rt_user_var' }), message('rt_user_var_delete'));
      await runTrigger(trigger('global-var-delete', 'incoming_message', 'text_equals', 'rt_global_var_delete', 'global_var_delete', { [K.actionVarName]: 'rt_global_var' }), message('rt_global_var_delete'));
      await runTrigger(trigger('shared-var-delete', 'incoming_message', 'text_equals', 'rt_shared_var_delete', 'shared_var_delete', { [K.actionVarName]: 'rt_shared_var' }), message('rt_shared_var_delete'));
      return { user: userVars.rt_user_var, global: globalVars.rt_global_var, shared: sharedVars.rt_shared_var };
      });
    });

    await test('action delete_user_data', async () => {
      const result = await runTrigger(trigger('delete-data', 'incoming_message', 'text_equals', 'rt_delete_data', 'delete_user_data'), message('rt_delete_data', USER_TEST));
      const current = await runtimeUserRow(USER_TEST);
      assert(result.matched && result.handled, 'not handled');
      assert(!current, 'runtime user row still exists');
      await ensureRuntimeUser(USER_TEST);
      return { deletedUser: USER_TEST };
    });
  });
}

async function runMembership() {
  await test('action approve_group_request closed community', async () => {
    const before = await isMember(C_CLOSED, USER_TEST);
    if (before === 1) {
      return { before, after: before, skipped: 'already_member' };
    }
    const row = trigger('approve-request', 'user_group_request', 'any_request_condition', '', 'approve_group_request', { [K.actionCommunityId]: C_CLOSED });
    const result = await runTrigger(row, group('group_join', USER_TEST, G_CLOSED, true));
    await sleep(5000);
    const after = await isMember(C_CLOSED, USER_TEST);
    assert(result.matched && result.handled, 'not handled');
    assert(after === 1, 'membership not approved');
    return { before, after };
  });

  await test('action delete_user_conversation', async () => {
    const result = await runTrigger(trigger('delete-conversation', 'incoming_message', 'text_equals', 'rt_delete_conversation', 'delete_user_conversation'), message('rt_delete_conversation', USER_ADMIN));
    assert(result.matched && result.handled, 'not handled');
    return { requestedFor: USER_ADMIN };
  });
}

async function main() {
  process.env.LOG_LEVEL = process.env.LOG_LEVEL || 'error';
  const block = process.argv[2] || 'preflight';
  if (block === 'preflight') await runPreflight();
  else if (block === 'conditions') await runConditions();
  else if (block === 'bot-actions') await runBotActions();
  else if (block === 'vars-delete') await runVariablesAndDelete();
  else if (block === 'membership') await runMembership();
  else throw new Error('Unknown block: ' + block);
  const failed = results.filter(item => !item.ok);
  console.log('REAL_TRIGGER_TEST_REPORT_JSON=' + JSON.stringify({ block, total: results.length, passed: results.length - failed.length, failed: failed.length, results }, null, 2));
  process.exit(failed.length ? 1 : 0);
}

main().catch(error => {
  console.error(error && error.stack || error);
  process.exit(1);
});
