const test = require('node:test');
const assert = require('node:assert/strict');

const { adminPanelHTML } = require('../adminPanelHTML');

function getProfileRenderComposition() {
  const start = adminPanelHTML.indexOf('container.innerHTML = foreignProfileBanner');
  const end = adminPanelHTML.indexOf('window.selectProfileDailyLimitPackage');
  assert.ok(start > 0, 'profile dashboard composition is present');
  assert.ok(end > start, 'profile dashboard composition end is present');
  return adminPanelHTML.slice(start, end);
}

test('profile dashboard places connected communities above balance and promo below balance', () => {
  const composition = getProfileRenderComposition();
  const communitiesIndex = composition.indexOf('id="profileConnectedCommunities"');
  const balanceIndex = composition.indexOf('balanceSectionHtml');
  const promoIndex = composition.indexOf('promoSectionHtml');

  assert.ok(communitiesIndex >= 0, 'connected communities block is rendered');
  assert.ok(balanceIndex > communitiesIndex, 'balance is rendered below connected communities');
  assert.ok(promoIndex > balanceIndex, 'promo activation is rendered below balance');
  assert.doesNotMatch(composition, /requestSelectedProfileLimit\(\)/);
  assert.doesNotMatch(composition, /limitHistorySectionHtml/);
});

test('profile community cards expose clickable profile section metrics', () => {
  assert.match(adminPanelHTML, /function renderProfileCommunityMetricLink/);
  assert.match(adminPanelHTML, /class="profile-community-metric-btn"/);
  assert.match(adminPanelHTML, /scrollToProfileSection/);
  assert.match(adminPanelHTML, /profileAiIntegrations/);
  assert.match(adminPanelHTML, /profilePaymentIntegrations/);
  assert.match(adminPanelHTML, /profilePaymentOperations/);
  assert.match(adminPanelHTML, /profileErrorReports/);
  assert.match(adminPanelHTML, /profileSuggestionReports/);
  assert.match(adminPanelHTML, /profileConsentDocuments/);
  assert.match(adminPanelHTML, /profileFiles/);
});

test('profile dashboard renders filterable payment operations after payment integrations', () => {
  const composition = getProfileRenderComposition();
  const paymentIntegrationsIndex = composition.indexOf('paymentSectionHtml');
  const paymentOperationsIndex = composition.indexOf('paymentOperationsSectionHtml');
  const documentsIndex = composition.indexOf('documentsSectionHtml');

  assert.match(adminPanelHTML, /var paymentOperations = Array\.isArray\(data\.paymentOperations\)/);
  assert.match(adminPanelHTML, /id="profilePaymentOperations"/);
  assert.match(adminPanelHTML, /filterProfilePaymentOperations/);
  assert.match(adminPanelHTML, /paymentOperationsIntegrationFilter/);
  assert.match(adminPanelHTML, /paymentOperationsStatusFilter/);
  assert.match(adminPanelHTML, /paymentOperationsBotFilter/);
  assert.match(adminPanelHTML, /paymentOperationsUserFilter/);
  assert.match(adminPanelHTML, /paymentOperationStatusOptions/);
  assert.match(adminPanelHTML, /__missing__/);
  assert.match(adminPanelHTML, /data-payment-id=/);
  assert.match(adminPanelHTML, /id="paymentOperationsVisibleCount"/);
  assert.match(adminPanelHTML, /id="paymentOperationsTotalCount"/);
  assert.match(adminPanelHTML, /id="deleteFilteredPaymentOperationsButton"/);
  assert.match(adminPanelHTML, /Удалить отфильтрованные/);
  assert.match(adminPanelHTML, /window\.deleteFilteredProfilePaymentOperations/);
  assert.match(adminPanelHTML, /\?deleteProfilePaymentOperations=1/);
  assert.match(adminPanelHTML, /Удалить ' \+ paymentIds\.length \+ ' операций без возможности восстановления\?/);
  assert.match(
    adminPanelHTML,
    /setInlineNoticeWithTimeout\(freshStatusEl,\s*'success',\s*'Удалено операций: ' \+ String\(result\.removedCount \|\| 0\) \+ '\.',\s*3000\)/
  );
  assert.match(adminPanelHTML, /principalProfileId:\s*getPrincipalProfileId\(\)/);
  assert.match(adminPanelHTML, /paymentIds:\s*paymentIds/);
  assert.match(adminPanelHTML, /\.profile-payment-operations-table-wrap\s*\{[\s\S]*max-height:/);
  assert.match(adminPanelHTML, /\.profile-payment-operations-table-wrap thead th\s*\{[\s\S]*position:\s*sticky/);
  assert.match(adminPanelHTML, /Дата и время/);
  assert.match(adminPanelHTML, /Пользователь/);
  assert.match(adminPanelHTML, /Назначение/);
  assert.ok(paymentOperationsIndex > paymentIntegrationsIndex, 'payment operations follow payment integrations');
  assert.ok(documentsIndex > paymentOperationsIndex, 'documents follow payment operations');
});

test('community card places payment operations directly after payment systems', () => {
  assert.match(
    adminPanelHTML,
    /renderProfileCommunityMetricLink\('Платежные системы'[\s\S]{0,700}renderProfileCommunityMetricLink\('Платежные операции'/
  );
  assert.match(adminPanelHTML, /communityPaymentOperationsCount/);
  const paymentOperationsIndex = adminPanelHTML.indexOf("renderProfileCommunityMetricLink('Платежные операции'");
  const attachmentSettingsIndex = adminPanelHTML.indexOf("renderProfileCommunityMetricLink('Источник загрузки вложений'");
  assert.ok(attachmentSettingsIndex > paymentOperationsIndex, 'attachment upload settings follow payment operations');
});

test('profile community card transitions center the selected block heading', () => {
  assert.match(adminPanelHTML, /window\.scrollToProfileSection = function\(sectionId\)/);
  assert.match(adminPanelHTML, /querySelector\('\.profile-manager-title, h3, h2'\)/);
  assert.match(adminPanelHTML, /heading\.scrollIntoView\(\{ behavior: 'smooth', block: 'center' \}\)/);
  assert.match(adminPanelHTML, /profile-section-title-focus/);
});

test('profile balance renders three vertical cards with approved controls and benefit badges', () => {
  assert.match(adminPanelHTML, /\.profile-balance-stack\s*\{/);
  assert.match(adminPanelHTML, /\.profile-balance-card\s*\{/);
  assert.match(adminPanelHTML, /\.profile-benefit-badge\s*\{[\s\S]*top:\s*-13px;/);
  assert.match(adminPanelHTML, /profile-balance-heading-row/);
  assert.match(adminPanelHTML, /profile-balance-action/);
  assert.match(adminPanelHTML, /Текущий баланс/);
  assert.match(adminPanelHTML, /Подписка/);
  assert.match(adminPanelHTML, /Пакеты/);
  assert.match(adminPanelHTML, /Действует 30 дней с ежедневным суточным лимитом запросов к сервису PAPA BOT/);
  assert.match(adminPanelHTML, /Приобретенные Пакеты не сгорают/);
  assert.match(adminPanelHTML, />Пополнить<\/button>/);
  assert.doesNotMatch(adminPanelHTML, /Пополнить через YooKassa/);
  assert.match(adminPanelHTML, /btn btn-accent profile-balance-action/);
  assert.match(adminPanelHTML, /profile-benefit-badge/);
  assert.match(adminPanelHTML, /requests \+ ' = ' \+ cost \+ '₽'/);
});

test('profile community card shows extra requests directly after daily limit', () => {
  assert.match(
    adminPanelHTML,
    /Суточный лимит:<\/span>[\s\S]{0,240}Вне суточного лимита:<\/span>/
  );
});

test('profile balance top-up explains and previews the applied bonus', () => {
  assert.match(adminPanelHTML, /class="profile-topup-help"/);
  assert.match(adminPanelHTML, /50–999 ₽: без бонуса/);
  assert.match(adminPanelHTML, /1000–4999 ₽: \+10%/);
  assert.match(adminPanelHTML, /5000–50 000 ₽: \+20%/);
  assert.match(adminPanelHTML, /Допустимая сумма пополнения: от 50 до 50 000 ₽/);
  assert.match(adminPanelHTML, /profile-topup-bonus--zero/);
  assert.match(adminPanelHTML, /profile-topup-bonus--ten/);
  assert.match(adminPanelHTML, /profile-topup-bonus--twenty/);
  assert.match(adminPanelHTML, /window\.updateProfileTopUpBonus = function\(\)/);
  assert.match(adminPanelHTML, /oninput="updateProfileTopUpBonus\(\)"/);
  assert.match(adminPanelHTML, /profileTopUpBonus/);
  assert.match(adminPanelHTML, /#profileTopUpAmount::-webkit-inner-spin-button/);
  assert.match(adminPanelHTML, /-moz-appearance:\s*textfield/);
});

test('admin panel has a shared floating back to top button for scrolled tabs', () => {
  assert.match(adminPanelHTML, /id="adminBackToTopButton"/);
  assert.match(adminPanelHTML, /id="adminBackToTopButtonRight"/);
  assert.match(adminPanelHTML, /\^<br>\^<br>\^/);
  assert.match(adminPanelHTML, /\.admin-back-to-top-btn--right\s*\{/);
  assert.match(adminPanelHTML, /function updateAdminBackToTopButton/);
  assert.match(adminPanelHTML, /window\.scrollToAdminTabTop/);
  assert.match(adminPanelHTML, /querySelectorAll\('\.admin-back-to-top-btn'\)/);
  assert.match(adminPanelHTML, /window\.addEventListener\('scroll', updateAdminBackToTopButton/);
  assert.match(adminPanelHTML, /\.admin-back-to-top-btn\s*\{[\s\S]*top:\s*76px;/);
});

test('settings user token help recommends admin rights and a non-primary VK account', () => {
  assert.match(adminPanelHTML, /Пользователь должен быть Администратором в сообществе или сообществах/);
  assert.match(adminPanelHTML, /Желательно завести Второй аккаунт в ВК/);
  assert.match(adminPanelHTML, /желательно выбрать свой НЕ основной аккаунт/);
});
