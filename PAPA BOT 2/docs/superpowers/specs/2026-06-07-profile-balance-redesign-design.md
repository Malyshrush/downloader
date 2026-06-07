# Profile Balance Redesign

## Goal

Rebuild the profile Balance section so balance top-up, 30-day subscriptions, and non-expiring request packages are clearly separated. Keep the existing purchase flows and storage model while updating tariff values, labels, placement, and YooKassa credential selection.

## Balance Layout

The `Баланс` surface contains three full-width cards stacked vertically:

1. `Текущий баланс`
2. `Подписка`
3. `Пакеты`

### Current Balance

- Show the current amount on the same row, to the right of `Текущий баланс`.
- Remove the separate `Баланс: N ₽` row.
- Place the top-up amount input below the heading row.
- Place the purple `Пополнить` button to the right of the input.
- Replace `Пополнить через YooKassa` with `Пополнить`.
- Keep the existing top-up validation, redirect payment creation, status notice, and payment history.

### Subscription

Show this explanation:

`Действует 30 дней с ежедневным суточным лимитом запросов к сервису PAPA BOT.`

Use these price and daily-limit pairs:

| Price | Daily requests | Benefit |
| ---: | ---: | ---: |
| 100 ₽ | 500 | none |
| 200 ₽ | 1050 | +50 |
| 300 ₽ | 1650 | +150 |
| 400 ₽ | 2300 | +300 |
| 500 ₽ | 3000 | +500 |
| 1000 ₽ | 7000 | +2000 |
| 2000 ₽ | 15000 | +5000 |
| 5000 ₽ | 35000 | +10000 |

Button text uses `requests = price`, for example `1050 = 200₽`.

The purchase button remains `Купить суточный лимит`, but uses the same purple action style and visual dimensions as `Пополнить`.

### Packages

Show this explanation:

`Приобретенные Пакеты не сгорают, если они не используются, и расходуются после окончания суточного лимита в вашей Подписке.`

Use these price and request-credit pairs:

| Price | Package requests | Benefit |
| ---: | ---: | ---: |
| 100 ₽ | 1000 | none |
| 200 ₽ | 2100 | +100 |
| 300 ₽ | 3300 | +300 |
| 400 ₽ | 4600 | +600 |
| 500 ₽ | 6000 | +1000 |
| 1000 ₽ | 13000 | +3000 |
| 2000 ₽ | 28000 | +8000 |
| 5000 ₽ | 70000 | +20000 |

Button text uses `requests = price`, for example `2100 = 200₽`.

The purchase button remains `Купить вне суточного лимита`, but uses the same purple action style and visual dimensions as `Пополнить`.

## Benefit Badges

- Every tariff except the base `100 ₽` tariff has a separate gold `Выгода +N` badge.
- The badge sits at the upper-right of its tariff button and extends beyond the right edge.
- Use approved visual variant A2: retain the original horizontal position and move the badge upward by 4 px from the original mockup, represented by `top: -13px`.
- Keep enough spacing between tariff controls and rows so badges do not collide with neighboring controls.
- The badge must not cover the tariff text.

## Community Card

- Remove `Вне суточного лимита: N` from the Current Balance card.
- Add `Вне суточного лимита: N` directly after the `Суточный лимит: ...` row in each connected-community card.
- This value remains profile-wide, so all community cards display the same current package remainder.
- Keep the clickable `Баланс` community metric unchanged unless required for layout compatibility.

## YooKassa

- Profile balance top-ups use the global shop credentials in `.env`.
- `YOOKASSA_SHOP_ID` supplies the shop ID.
- `YOOKASSA_API` supplies the API secret.
- These production variables take precedence over legacy aliases and test variables.
- Deployment must continue transferring both variables to the deployed function.
- Existing payment metadata, idempotence, webhook verification, pending-payment reconciliation, and duplicate-credit protection remain unchanged.

## Testing

- Add an admin-panel contract test for the three-card structure, labels, action colors, tariff labels, benefit badges, and community-card placement.
- Update profile balance tests for the new subscription and package tariff arrays.
- Add or update YooKassa tests proving `YOOKASSA_SHOP_ID` and `YOOKASSA_API` take precedence over legacy/test values.
- Run focused profile UI, profile balance, YooKassa, handler, and deploy-contract tests.
- Render the profile page in the local browser and visually verify desktop and narrow layouts.

## Documentation And Deployment

- Update `FUNCTIONALITY.md` in the same change.
- Update version-facing metadata only if required by the repository's existing version contract.
- After tests and browser verification pass, deploy using the repository deployment script and verify the deployment result before reporting completion.
