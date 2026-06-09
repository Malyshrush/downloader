# Payment Operations Bulk Delete Design

## Goal

Extend the profile payment operations journal with safe deletion of the currently filtered rows, a visible row count, an internally scrolling table, and a second bottom-right shortcut for scrolling the active admin tab to the top.

## Payment Operation Deletion

- The browser identifies the rows currently visible after all four existing filters are applied.
- Each rendered row carries its stored `paymentId`.
- Above the table, show `Отображается: N из M`, where `N` is the visible filtered count and `M` is the total count returned in the profile dashboard.
- Add a destructive button labeled `Удалить отфильтрованные (N)`.
- Disable the button when `N` is zero.
- Before deletion, show: `Удалить N операций без возможности восстановления?`
- On confirmation, send the unique visible payment IDs, current profile ID, and principal profile ID to a dedicated endpoint.
- The server validates profile access using the same profile access rules as other profile dashboard mutations and removes only matching records from that profile's `paymentButtonPayments`.
- Missing or already deleted IDs are ignored. The response returns the number removed.
- After success, reload the dashboard. Filters reset because the dashboard is rendered fresh.
- The operation permanently removes journal data and cannot be undone.

## Table Layout

- The payment operations table sits in a fixed-height scroll container sized for approximately ten body rows.
- Vertical scrolling occurs inside the table block.
- The table header remains sticky at the top of the scroll container.
- Horizontal scrolling remains available for narrow screens and wide columns.

## Additional Scroll-To-Top Button

- Keep the existing `Вверх` button unchanged.
- Add a separate button at the bottom-right of the viewport.
- Its visible text is three lines:

```text
^
^
^
```

- It uses the same appearance timing and scroll behavior as the existing button.
- Both buttons scroll the currently active admin tab to its top with smooth behavior.

## Error Handling

- Do not send a deletion request when no rows are visible.
- Reject an empty payment ID list on the server.
- Never delete records from another profile.
- Show a readable inline success or error notice in the payment operations block.

## Testing

- Module test: removes only requested payment IDs and preserves unmatched operations.
- Module test: ignores missing IDs and rejects an empty list.
- Handler/UI contract tests: endpoint wiring, profile/principal IDs, visible row IDs, confirmation text, count label, disabled zero-state, scroll container, sticky header, and second button.
- Run the embedded admin script syntax test after modifying `adminPanelHTML.js`.
- Update `FUNCTIONALITY.md`.
- Deploy after verification.
