# Robokassa Payment Buttons Design

Date: 2026-06-03
Project: PAPA BOT
Status: Draft for written review

## Goal

Add Robokassa as a payment provider for bot payment buttons.

Administrators will save Robokassa merchant credentials in the ПРОФИЛЬ payment integrations section, select that integration in an existing payment button, and route users to configured bot steps after successful or failed payment outcomes.

Robokassa will not be used for PAPA BOT profile balance top-ups.

## Decisions

- Robokassa is supported only for bot payment buttons.
- The first version supports production payments only. It does not pass `IsTest=1` or store Robokassa test passwords.
- The integration supports the hash algorithms `MD5`, `SHA-256`, and `SHA-512`, selected by the administrator to match the Robokassa merchant settings.
- The first version does not pass `Receipt`. Fiscalization must be configured outside this PAPA BOT integration.
- A successful payment is trusted only after a valid server notification to Robokassa `ResultURL`.
- A failed-payment bot route may run after the user returns through Robokassa `FailURL`.
- A later valid successful `ResultURL` notification takes priority over an earlier failed browser return.

## Recommended Robokassa Protocol

Use the standard Robokassa payment interface and standard `ResultURL`, with signed `Shp_*` custom parameters.

This approach matches the existing PAPA BOT payment-button model:

1. PAPA BOT creates and stores a pending payment record.
2. PAPA BOT generates a signed Robokassa payment URL.
3. Robokassa sends a server notification to the configured PAPA BOT `ResultURL`.
4. PAPA BOT verifies the notification and routes the user.

`ResultUrl2` is not used because it introduces a separate JWS notification flow and certificate verification without providing a benefit for the first version. Browser redirects alone are not sufficient to confirm successful payment.

## Profile Integration UI

Robokassa will become selectable alongside YooKassa and Prodamus in the ПРОФИЛЬ payment integration form.

Required fields:

- `Merchant login`
- `Password 1`
- `Password 2`
- `Signature algorithm`: `MD5`, `SHA-256`, or `SHA-512`

PAPA BOT will display an automatically generated `Result URL` that the administrator must copy into the Robokassa merchant technical settings.

The UI will explain:

- the integration supports production payments only;
- the selected signature algorithm must match Robokassa settings;
- PAPA BOT does not send `Receipt`;
- fiscalization must be configured separately;
- success routing is confirmed by `ResultURL`;
- fail routing depends on the user returning through the browser.

The existing payment button editor remains unchanged. Robokassa uses the existing amount, description, success bot/step, and fail bot/step fields.

## Stored Integration Data

The existing profile payment integration record will store:

- `provider: "robokassa"`
- `merchantLogin`
- `password1`
- `password2`
- `robokassaHashAlgorithm`
- `notificationUrl`
- existing common integration metadata

`robokassaHashAlgorithm` will normalize to one of:

- `md5`
- `sha256`
- `sha512`

Invalid or empty values will be rejected for Robokassa integrations.

## Payment Creation

When a payment button uses a Robokassa integration, PAPA BOT will:

1. Validate the integration and button amount.
2. Generate a unique positive numeric `InvId`.
3. Save a pending payment-button record before returning the payment link.
4. Build PAPA BOT browser return URLs for success and failure.
5. Build a signed Robokassa URL for:

   `https://auth.robokassa.ru/Merchant/Index.aspx`

The payment request will include:

- `MerchantLogin`
- `OutSum`
- `InvId`
- `Description`
- `Culture=ru`
- `SignatureValue`
- signed `Shp_*` values that identify the PAPA BOT profile, integration, and payment record
- `SuccessUrl2` and `SuccessUrl2Method=GET`
- `FailUrl2` and `FailUrl2Method=GET`

The dynamic `SuccessUrl2` and `FailUrl2` values will be included in the payment-link signature in the order required by Robokassa. The standard `ResultURL` remains configured once in the Robokassa merchant technical settings.

The payment request will not include:

- `IsTest`
- `Receipt`
- holding or saved-card parameters
- a forced payment method

The user will choose a payment method on the Robokassa payment page.

## Signature Rules

PAPA BOT will calculate Robokassa signatures using the algorithm selected in the integration.

Payment-link signatures use `Password 1`.

`ResultURL` notification signatures use `Password 2`.

Browser return signatures use `Password 1`.

All signed `Shp_*` parameters will:

- be included in the request;
- be included in signature calculation;
- use stable names and casing;
- be sorted alphabetically as required by Robokassa.

Signature comparison will be case-insensitive and timing-safe where practical.

## ResultURL Success Handling

PAPA BOT will expose a public Robokassa result endpoint.

The endpoint will accept Robokassa notifications sent by either `GET` or `POST`, then:

1. Parse `OutSum`, `InvId`, `SignatureValue`, and `Shp_*`.
2. Find the saved payment record and its Robokassa integration.
3. Verify the profile and integration identifiers.
4. Verify the notification signature using `Password 2`.
5. Verify that `OutSum` matches the saved expected amount.
6. Mark the payment as `succeeded`.
7. Move the user to the configured success bot and step.
8. Send the target step answer immediately.
9. Respond with plain text `OK{InvId}`.

Repeated valid notifications must not route or send the success answer again, but must continue returning `OK{InvId}`.

Invalid signatures, mismatched amounts, missing records, and mismatched profile or integration identifiers must not change payment state or route the user.

## Browser Returns

### Success Return

The success return redirects the browser back to the VK community dialog.

It does not independently mark the payment as successful or run the success route. Only a valid `ResultURL` notification can confirm success.

### Fail Return

The fail return verifies the Robokassa return signature using `Password 1`.

For a valid pending payment, PAPA BOT will:

1. Mark the payment as `canceled`.
2. Move the user to the configured fail bot and step.
3. Send the target step answer immediately.
4. Redirect the browser back to the VK community dialog.

If the user closes Robokassa without returning through `FailURL`, the payment remains pending.

If a valid successful `ResultURL` notification arrives after a fail return, success takes priority. The payment becomes `succeeded`, and the success route runs once.

## Idempotency And State Priority

Payment records use the existing payment-button storage and routing fields.

State priority:

1. `succeeded`
2. `canceled`
3. `pending`

Rules:

- A repeated success notification does not send duplicate messages.
- A repeated fail return does not send duplicate messages.
- A fail return cannot downgrade a succeeded payment.
- A valid success notification can upgrade a canceled payment to succeeded.
- Invalid requests never alter payment state.

## Integration Test Button

The ПРОФИЛЬ `TEST` action will not create a real Robokassa payment.

It will:

- validate required credentials;
- validate the selected hash algorithm;
- calculate a deterministic local sample signature;
- report that the integration fields are structurally valid.

The result message must make clear that no network request or real payment was performed.

## Error Handling

Payment-link creation will fail with a user-facing error when:

- required Robokassa credentials are missing;
- the signature algorithm is invalid;
- the amount is invalid;
- a numeric `InvId` cannot be generated;
- the payment record cannot be saved.

Webhook and return handlers will log rejected requests without exposing merchant passwords or signature bases containing passwords.

## Testing

Automated coverage will include:

- Robokassa integration normalization and validation;
- selection of `MD5`, `SHA-256`, and `SHA-512`;
- payment-link signature generation for all supported algorithms;
- positive numeric unique `InvId` generation;
- saved pending payment records;
- valid `ResultURL` handling and exact `OK{InvId}` response;
- repeated `ResultURL` idempotency;
- invalid signature rejection;
- amount mismatch rejection;
- profile and integration mismatch rejection;
- valid fail return routing;
- repeated fail return idempotency;
- success priority after an earlier fail return;
- success return redirect without success confirmation;
- admin-panel Robokassa field and warning contracts;
- handler routing for Robokassa result and return endpoints.

## Documentation

`FUNCTIONALITY.md` must be updated with the implemented Robokassa behavior when the feature is added.

The final implementation must keep the documentation aligned with:

- `src/handler.js`
- `src/modules/payment-integrations.js`
- the Robokassa payment handler module
- `adminPanelHTML.js`
- relevant tests

## Out Of Scope

- PAPA BOT profile balance top-ups through Robokassa
- Robokassa test mode
- storing both production and test credentials in one integration
- `Receipt` generation and fiscalization data
- refunds
- holding or two-stage payments
- saved cards and recurring payments
- forcing a specific Robokassa payment method
- `ResultUrl2` JWS notifications
