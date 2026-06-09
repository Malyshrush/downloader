# Admin Financial Operations Design

## Goal

Replace the current main-admin `Пополнения балансов` card list with a read-only,
filterable financial operations table covering every profile. Hide
`Увеличение лимитов` from the Admin UI without removing its stored data or API
behavior.

## Scope

The table includes:

- profile balance top-ups in all statuses;
- daily limit subscription purchases;
- extra limit package purchases;
- promo-code balance and extra-limit grants;
- rewards for fixed bug reports;
- rewards for implemented suggestions;
- manual admin balance or extra-limit adjustments;
- unknown historical balance operations as `Прочая операция`.

The table is available only through the existing main-admin dashboard.
It is read-only and does not support bulk deletion.

## Architecture

The server builds one normalized financial journal from the existing
`balanceTopUps` collection and each profile's `balanceOperations` collection.
No new persistent journal and no data migration are introduced.

The normalized rows are returned by `getAdminDashboard` as
`financialOperations`. Existing raw data may remain in the response where
needed for compatibility, but the new UI consumes only the normalized list.

Each row has a stable display contract:

- `id`
- `createdAt`
- `profileId`
- `profileName`
- `type`
- `typeLabel`
- `status`
- `statusLabel`
- `amountRub`
- `balanceDelta`
- `limitDelta`
- `communityId`
- `communityName`
- `source`
- `description`
- `providerPaymentId`

Rows are sorted newest first.

## Deduplication

A confirmed top-up usually has both a `balanceTopUps` record and a related
`balanceOperations` credit record. The server emits one row for that financial
event.

The primary association uses the top-up ID stored on the balance operation.
Provider payment ID and matching profile/time/amount may be used only as
fallbacks when old data lacks the primary association. Unrelated operations
must not be merged.

Pending, canceled, and failed top-ups remain visible even when they have no
balance operation.

## Admin UI

The Admin tab no longer renders the `Увеличение лимитов` block. Its server data,
resolution endpoints, and stored history are unchanged.

The existing `Пополнения балансов` area becomes `Финансовые операции` and uses
the same table language as Profile `Платежные операции`:

- internal vertical and horizontal scrolling;
- approximately ten visible rows;
- sticky header;
- `Отображается: N из M`;
- no deletion control.

Columns:

1. Date and time
2. Profile
3. Operation type
4. Status
5. Monetary amount
6. Balance change
7. Limit change
8. Community
9. Source or reason
10. Operation ID

Filters:

- profile;
- operation type;
- status;
- date from;
- date to;
- free-text search across profile, community, promo code or description,
  provider payment ID, and operation ID.

All filters combine with logical AND. Date boundaries are inclusive in the
browser's local calendar interpretation.

## Compatibility And Errors

Missing fields are rendered as `Не указано` or an empty numeric change rather
than dropping a row. Unknown types use `Прочая операция`. Malformed dates sort
last and remain searchable.

If the admin dashboard fails to load, the existing Admin error handling remains
responsible for the visible error. An empty journal renders a dedicated empty
state.

## Verification

Automated coverage must verify:

- normalized top-ups in pending, succeeded, canceled, and error states;
- purchases, promo grants, rewards, manual adjustments, and unknown operations;
- successful top-up deduplication;
- newest-first sorting;
- Admin API exposure to the main admin;
- hidden limit-request block;
- table columns, filters, count, scrolling, sticky header, and absence of a
  deletion action;
- generated inline browser JavaScript syntax.

`FUNCTIONALITY.md` must be updated. After focused and broader regression tests,
deploy the verified version according to repository policy.
