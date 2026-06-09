# Payment Operations and Bot Selection Design

## Scope

This change improves two admin-panel areas:

1. The selected bot control is visually distinct: its name button is blue and contains a small white circle instead of a check mark.
2. The profile dashboard gains a `Платежные операции` section that shows payment-button operations from every bot and community in the profile.

The existing bot enabled/disabled control remains separate and keeps its green/red status colors and check/cross symbols.

## Data Model

`paymentButtonPayments` remains the single source of truth. No duplicate payment journal or migration is introduced.

New payment records store:

- `sourceBot`: the bot whose row generated the payment keyboard.

Existing fields continue to provide:

- payment and provider identifiers;
- integration identifier and name;
- status;
- community and user identifiers;
- amount and description;
- creation, update, and resolution timestamps;
- configured success/fail routes.

Existing records are not migrated. If `sourceBot` is absent, the UI displays `Не указан`.

The payment creation context receives the source bot from the message, delayed-delivery, or mailing row that generated the keyboard.

## Dashboard Data

The profile dashboard returns up to the existing 500 stored payment-button records, ordered newest first.

Each returned operation is enriched from local profile/community data:

- user display name from the relevant community user table, with VK ID always retained;
- community display name from the loaded profile configuration.

No VK or payment-provider API call is made while rendering the dashboard. Missing enrichment data must not hide an operation:

- missing user name: show VK ID only;
- missing source bot: show `Не указан`;
- missing community name: show the stored community ID;
- unknown status: show its original stored value.

## Bot Switcher UI

The active bot name button uses a blue gradient and white text.

The active marker is a small white circle rendered as a dedicated element inside the name button. The bot name no longer appends a textual check mark.

The status toggle remains unchanged in meaning:

- green check: bot enabled;
- red cross: bot disabled.

## Payment Operations UI

The new `Платежные операции` section is rendered immediately after `Платежные системы` and before `Документы`.

It contains one profile-wide table, ordered newest first, with these columns:

- date and time;
- status;
- payment integration;
- source bot;
- user name and VK ID;
- payment description;
- amount;
- community;
- payment ID.

Human-readable status labels are determined from the lowercased stored status:

- `pending`, `waiting`, `created`, or an empty status: `Ожидает`;
- `succeeded`, `success`, or `paid`: `Успешно`;
- `canceled`, `cancelled`, or `failed`: `Отменено`;
- `error`: `Ошибка`.

Unknown statuses preserve their stored text.

Local filters are provided for:

- payment integration;
- status;
- source bot;
- user, searchable by name or VK ID.

Filtering does not refetch dashboard data. If no operation matches, the table shows a clear empty-state row.

## Community Cards

Each community card adds `Платежные операции` immediately after `Платежные системы`.

The metric value is the number of stored payment operations whose `communityId` matches that community. Clicking it scrolls to the profile-wide payment operations section.

## Compatibility and Limits

- Existing provider webhook and routing behavior is unchanged.
- Existing pending, succeeded, canceled, failed, and unknown records remain visible.
- The current storage cap of 500 payment-button records per profile remains unchanged.
- No external-user migration is required during pre-release testing.

## Verification

Automated coverage must verify:

- payment creation stores `sourceBot`;
- dashboard output includes payment operations, newest first, with user/community enrichment and safe fallbacks;
- the active bot is blue and uses a white circle instead of a textual check;
- the status toggle still uses check/cross symbols;
- the payment operations section follows payment integrations;
- the community metric follows payment systems and counts operations for that community;
- filters and empty-state behavior are present;
- generated inline browser JavaScript compiles.

Because `adminPanelHTML.js` embedded JavaScript changes, run:

`node --test tests/admin-panel-inline-script-syntax.test.js`

Update `FUNCTIONALITY.md` in the same implementation change.
