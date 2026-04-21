# Ingress To Worker Extraction Design

Date: 2026-04-22
Project: PAPA BOT 2
Status: Draft for review

## Goal

Extract incoming VK event processing out of the webhook request path in `PAPA BOT 2` while preserving current product behavior.

Stage 1 must:

- keep Yandex Cloud Functions as the runtime
- keep current admin panel and existing product contracts intact
- stop doing full business processing directly inside webhook ingress
- introduce a stable normalized event contract
- introduce a queue abstraction with a stub adapter
- introduce a separate worker processing path

Stage 1 must not yet:

- migrate hot operational state out of Object Storage
- redesign scheduler internals
- redesign message, comment, or trigger business logic
- integrate real Yandex Message Queue

The purpose of this stage is extraction, not rewrite.

## Current Problem

The current ingress path in [src/handler.js](/C:/PROJECT/GPT/PAPA%20BOT%202/src/handler.js) performs too much synchronous work inside one VK callback request.

For incoming message and comment events, the handler currently does the following inline:

- resolves profile and community
- updates daily usage via `recordProfileEventUsage`
- runs `processStructuredTriggers`
- runs `handleMessage` or `handleComment`
- runs `processDelayed`
- runs `processMailing`

This pulls into webhook ingress all downstream side effects produced by:

- [src/modules/messages.js](/C:/PROJECT/GPT/PAPA%20BOT%202/src/modules/messages.js)
- [src/modules/comments.js](/C:/PROJECT/GPT/PAPA%20BOT%202/src/modules/comments.js)
- [src/modules/structured-triggers.js](/C:/PROJECT/GPT/PAPA%20BOT%202/src/modules/structured-triggers.js)
- [src/modules/scheduler.js](/C:/PROJECT/GPT/PAPA%20BOT%202/src/modules/scheduler.js)

Those modules in turn perform:

- storage reads and writes
- app log writes
- user state updates
- variable updates
- direct VK API sends
- delayed and mailing execution

This is the bottleneck that Stage 1 must remove.

## Recommended Approach

Use a queue-first boundary with a stub adapter.

Ingress becomes a thin path that:

1. accepts the raw VK callback
2. performs lightweight validation and context resolution
3. builds a normalized event envelope
4. publishes that envelope into an abstract event queue
5. returns `ok`

Worker processing becomes a separate path that:

1. receives the normalized envelope
2. reconstructs the processing context
3. runs the existing trigger, message, comment, and scheduler logic
4. owns the heavy side effects outside webhook ingress

This is preferred over direct Yandex Message Queue integration in Stage 1 because it stabilizes architecture boundaries first and external infrastructure second.

## Scope

In scope:

- normalized event contract
- queue abstraction with stub adapter
- separate worker entrypoint
- ingress refactor to publish instead of process inline
- minimal idempotency boundary for Stage 1
- tests for envelope normalization and ingress publishing

Out of scope:

- YMQ integration
- YDB integration
- Redis integration
- migration of `profile_dashboard.json` hot writes
- migration of user and variable storage out of JSON blobs
- deep refactor of scheduler behavior
- deep refactor of trigger, message, and comment engines

## Design Summary

Stage 1 introduces three new modules and changes one existing module.

### New module: `src/modules/event-envelope.js`

Responsibility:

- convert raw VK callback data into a deterministic normalized event object

Non-responsibilities:

- no VK sending
- no storage writes
- no trigger logic
- no business processing

Required fields:

- `eventId`
- `eventType`
- `profileId`
- `communityId`
- `userId`
- `payload`
- `createdAt`
- `source`

Recommended additional metadata:

- `idempotencyKey`
- `receivedAt`
- `traceId`
- `rawMeta`

Recommended envelope shape:

```json
{
  "eventId": "vk:message_new:123456:987654321",
  "eventType": "message_new",
  "profileId": "1",
  "communityId": "123456",
  "userId": "987654321",
  "payload": {
    "type": "message_new",
    "group_id": 123456,
    "object": {}
  },
  "createdAt": "2026-04-22T10:00:00.000Z",
  "receivedAt": "2026-04-22T10:00:00.000Z",
  "source": "vk-callback",
  "idempotencyKey": "vk:message_new:123456:987654321",
  "traceId": "evt_abc123",
  "rawMeta": {
    "hasSecret": true,
    "httpMethod": "POST"
  }
}
```

Important Stage 1 rule:

`payload` should preserve the original VK callback shape closely enough that existing modules can continue working with minimal adaptation. The envelope is a transport contract first, not a final domain model.

### New module: `src/modules/event-queue.js`

Responsibility:

- define the stable publish and consume contract between ingress and worker

Stage 1 implementation:

- stub adapter

Required interface:

- `publishIncomingEvent(eventEnvelope)`
- `publishOutboundAction(actionEnvelope)` as a forward-looking placeholder
- `consumeIncomingEvent(handler)`
- `drainIncomingEvents()` for tests and local worker execution

Stage 1 queue behavior:

- default implementation may use an in-memory queue or explicit synchronous test drain
- external callers must not depend on internal storage details
- the interface must be replaceable later by YMQ without changing ingress or worker orchestration

### New module: `src/modules/event-worker.js`

Responsibility:

- receive normalized envelopes and run existing processing logic outside ingress

Stage 1 orchestration order for compatibility:

1. validate envelope
2. apply minimal worker idempotency check
3. call `processStructuredTriggers`
4. call `handleMessage` or `handleComment` depending on event type
5. call `processDelayed`
6. call `processMailing`

This order intentionally preserves current behavior as much as possible before any deeper optimization.

### Existing module: `src/handler.js`

Responsibility changes:

- keep admin, timer, and other existing routes working
- split ingress routing from worker processing
- stop calling heavy processing directly from webhook ingress

For supported incoming VK events, webhook ingress should now:

1. parse raw event
2. resolve `profileId` and `communityId`
3. run lightweight daily usage gate
4. build normalized envelope
5. publish the envelope
6. return `200 ok`

Worker processing must be reachable through a separate entrypoint. Stage 1 may implement this as:

- a separate handler function in the same file
- or a route/mode switch that clearly distinguishes worker execution from ingress

The key requirement is separation of responsibilities, not file count.

## Event Flow

The target Stage 1 flow is:

1. VK sends callback to ingress.
2. Ingress parses the callback and identifies event type.
3. Ingress resolves profile and community context.
4. Ingress performs lightweight idempotency and daily usage checks.
5. Ingress creates a normalized envelope.
6. Ingress publishes the envelope to the queue abstraction.
7. Ingress returns `ok`.
8. Worker consumes the envelope.
9. Worker runs structured triggers and classic handlers.
10. Worker runs delayed and mailing hooks as needed for compatibility.

After Stage 1, ingress must no longer directly invoke:

- `handleMessage`
- `handleComment`
- `processStructuredTriggers`
- `processDelayed`
- `processMailing`

## Failure Handling

### Envelope build failure

If required envelope fields cannot be derived safely:

- do not fall back to heavy inline processing
- log the reason
- return `ok` to VK if the event is malformed in a way that should not be retried indefinitely

This avoids a failure mode where bad callbacks trigger repeated retries and repeated partial processing attempts.

### Queue publish failure

If ingress cannot publish the event:

- treat it as a real ingress failure
- return `500`
- do not claim success to VK

If the system cannot enqueue the event, it has not accepted responsibility for processing it.

### Worker failure

If worker processing throws:

- log the failure with `eventId` and `traceId`
- keep the failure isolated to worker execution
- do not reintroduce synchronous fallback into ingress

Stage 1 does not require a full DLQ implementation yet, but the worker contract should make that addition straightforward later.

## Idempotency

Stage 1 needs minimal idempotency in two places.

### Ingress idempotency

Ingress should derive a deterministic `eventId` / `idempotencyKey` from the callback where possible.

Examples:

- message event: event type + community + message id or conversation message id + sender
- comment event: event type + community + comment id

Ingress idempotency at this stage may be lightweight and local. Its purpose is to reduce accidental duplicate publishes, not to be the final durable solution.

### Worker idempotency

Worker must check whether `eventId` was already processed before invoking side effects.

Stage 1 may implement this using in-memory tracking or queue-layer stub state as long as:

- the contract is explicit
- the check is centered on `eventId`
- the implementation can later move to Redis or YDB without changing worker orchestration

## Compatibility Rules

To keep Stage 1 safe:

- do not rewrite `messages.js`, `comments.js`, `structured-triggers.js`, or `scheduler.js` for new architecture concepts yet
- move existing logic behind the worker boundary first
- preserve the current order of execution unless a bug forces a targeted change
- preserve the raw VK callback data within the envelope payload

This stage is successful if behavior is preserved while the architectural boundary changes.

## Testing Strategy

Stage 1 requires new tests in addition to the current admin/auth coverage.

### Unit tests

Add tests for:

- normalized event creation for `message_new`
- normalized event creation for `wall_reply_new`
- deterministic `eventId` generation
- handling of malformed callback data

### Integration-oriented tests

Add tests for:

- ingress publishes to queue instead of calling heavy handlers inline
- worker consumes envelope and dispatches to the correct processing path
- unsupported event types are ignored safely

### Regression checks

Keep existing tests passing:

- [tests/admin-auth-flow.test.js](/C:/PROJECT/GPT/PAPA%20BOT%202/tests/admin-auth-flow.test.js)
- [tests/admin-panel-auth-contract.test.js](/C:/PROJECT/GPT/PAPA%20BOT%202/tests/admin-panel-auth-contract.test.js)
- [tests/admin-security-captcha.test.js](/C:/PROJECT/GPT/PAPA%20BOT%202/tests/admin-security-captcha.test.js)
- [tests/admin-sessions.test.js](/C:/PROJECT/GPT/PAPA%20BOT%202/tests/admin-sessions.test.js)
- [tests/profile-promo.test.js](/C:/PROJECT/GPT/PAPA%20BOT%202/tests/profile-promo.test.js)

## Acceptance Criteria

Stage 1 is considered complete only if all of the following are true:

- webhook ingress no longer directly runs message, comment, trigger, delayed, or mailing processing
- `event-envelope`, `event-queue`, and `event-worker` exist as separate modules
- ingress publishes normalized events instead of doing full inline processing
- worker owns the existing heavy processing path
- current admin/auth flows still pass their tests
- normalized event and ingress publishing tests exist
- the design remains compatible with later replacement of the stub queue by YMQ

## Files Expected To Change In Implementation

- [src/handler.js](/C:/PROJECT/GPT/PAPA%20BOT%202/src/handler.js)
- [src/modules/event-envelope.js](/C:/PROJECT/GPT/PAPA%20BOT%202/src/modules/event-envelope.js)
- [src/modules/event-queue.js](/C:/PROJECT/GPT/PAPA%20BOT%202/src/modules/event-queue.js)
- [src/modules/event-worker.js](/C:/PROJECT/GPT/PAPA%20BOT%202/src/modules/event-worker.js)
- tests for envelope and ingress behavior

## Rollout Sequence

1. Add normalized event module.
2. Add queue abstraction with stub adapter.
3. Add worker module that orchestrates existing business processing.
4. Rewire ingress in `src/handler.js` to publish envelopes.
5. Add tests for normalization and ingress publishing.
6. Verify current auth/admin tests still pass.
7. Only after Stage 1 is stable, write the implementation plan for the next migration steps.

## Spec Self-Review

Completed checks:

- no placeholders such as `TODO` or `TBD` remain
- scope is limited to Stage 1 extraction
- queue abstraction, worker boundary, and ingress responsibilities are consistent
- failure behavior is defined separately for malformed events and publish failures
- compatibility requirements are explicit to avoid accidental rewrite scope

Known intentional limitations:

- queue transport is still a stub in Stage 1
- idempotency is minimal and not yet durable
- scheduler internals remain unchanged and only move behind the worker boundary
