# Balance Top-Up Hint Design

## Goal

Explain profile balance top-up bonuses directly beside the amount field and show the currently applicable bonus while the administrator types.

## UI

- Keep the amount field on the left and the purple `Пополнить` action on the right.
- Add a circular `?` help control between the field and the action.
- Reuse the existing `data-hint` tooltip system.
- Tooltip text:
  - `50–999 ₽: без бонуса`
  - `1000–4999 ₽: +10%`
  - `5000–50 000 ₽: +20%`
  - `Допустимая сумма пополнения: от 50 до 50 000 ₽.`
- Wrap the input in a positioned container and show a non-editable percentage badge inside its right edge.
- Bonus badge colors:
  - `+0%`: gray
  - `+10%`: green
  - `+20%`: pink
- Update the badge immediately on the input event.
- Empty, non-numeric, and values below 1000 display `+0%`.
- Keep `type="number"` and numeric keyboard behavior, but hide browser increment/decrement spinner controls with scoped CSS.

## Testing And Deployment

- Add admin-panel contract coverage for the help control, tooltip text, spinner hiding, bonus classes, thresholds, and input handler.
- Keep server-side bonus calculation unchanged.
- Update `FUNCTIONALITY.md`.
- Run affected UI, profile balance, handler, and deploy tests.
- Deploy all PAPA BOT functions and verify the health endpoint.
