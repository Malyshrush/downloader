# PAPA BOT 2: Rebuild To 1000 Communities

## Purpose

`PAPA BOT 2` is the working branch-copy for a controlled architectural rebuild of the current PAPA BOT project.

The current codebase is functional and should be treated as a verified baseline. The goal in `PAPA BOT 2` is not to re-invent product behavior, but to preserve the existing user-facing functionality while redesigning the internals so the system can realistically scale toward roughly `1000 VK communities` in Yandex Cloud Functions.

`PAPA BOT 1` must remain the frozen baseline/reference copy. `PAPA BOT 2` is the only copy intended for architectural changes.

## What Must Stay The Same

The following product behavior must remain intact unless there is an explicit migration decision:

- Admin panel behavior and layout
- Existing auth/session/captcha behavior
- Message scenarios
- Comment scenarios
- Structured triggers
- Users, groups, variables, profile-shared variables
- Mailings
- Delayed actions
- Multi-community support
- Admin profiles, promo codes, recovery flows, logs
- Current release/versioning discipline

In short: rebuild the engine, not the product.

## What Must Change

The current architecture is too synchronous and too dependent on hot JSON files in Object Storage.

Current bottlenecks in the baseline project include:

- Incoming webhook processing does too much work inside one HTTP request
- Hot state is stored in JSON blobs via `src/modules/storage.js`
- User state mutations rewrite full community user datasets
- `profile_dashboard.json` is written too often
- `app_logs` are written synchronously in hot paths
- `processDelayed` and `processMailing` are triggered from incoming-event handling
- VK sending is tightly coupled to business decision logic

The rebuild must remove these bottlenecks.

## Target Outcome

Target for `PAPA BOT 2`:

- Keep Yandex Cloud Functions as runtime
- Introduce queue-based async processing
- Move hot state away from Object Storage JSON blobs
- Make webhook ingress fast and thin
- Make worker processing idempotent
- Isolate outbound VK API sending from decision logic
- Prepare the system for a practical load target of about `200 incoming events/sec`

Important:

- `1000 communities` here means an engineering target, not a sales promise
- The target is to make the architecture capable of this class of load, with proper measurement and staged rollout

## Hard Constraints

- Stay on Yandex Cloud Functions
- Preserve current product logic as much as possible
- Do not break the current admin panel contract without reason
- Avoid a full big-bang rewrite
- Use phased migration with measurable checkpoints

## Recommended Target Architecture

### Runtime Layers

1. `Ingress Function`
- Receives VK callbacks
- Validates event
- Resolves community/profile
- Performs idempotency check
- Pushes normalized event into queue
- Returns `200 ok` immediately

2. `Event Worker Function`
- Reads inbound events from queue
- Loads required config/state
- Evaluates classic rules and structured triggers
- Produces side effects as commands, not direct inline heavy actions

3. `Sender Worker Function`
- Reads outbound action queue
- Sends messages/comments to VK
- Applies token/community rate limiting
- Handles retries and failure routing

4. `Scheduler Function`
- Triggered by timer
- Scans delayed jobs and mailing jobs
- Emits outbound actions into queue
- Does not send directly in large loops

5. `Admin API Function`
- Serves admin-specific backend operations
- Works against the new state layer
- Keeps existing UI contract where possible

### Infrastructure Layers

- `Yandex Message Queue` for inbound and outbound queues
- `YDB` for hot operational state
- `Managed Redis` for cache, idempotency, rate limits, short locks
- `Object Storage` only for backups, exports, large blobs, archived logs

## Queue Topology

Recommended queue split:

- `inbound-messages`
- `inbound-comments`
- `inbound-system`
- `outbound-actions`
- `dlq-inbound`
- `dlq-outbound`

Reason:

- separates traffic classes
- improves scaling control
- isolates failure domains
- avoids one hot mixed queue for everything

## Data Model Direction

Move hot state from JSON files to structured storage.

Minimum operational entities:

- communities
- profiles
- admin sessions
- users
- user variables
- global variables
- profile shared variables
- message rules
- comment rules
- structured triggers
- delayed jobs
- mailing jobs
- daily usage counters
- idempotency records
- outbound actions

### Suggested Primary Keys

- users: `(profile_id, community_id, user_id)`
- structured_triggers: `(profile_id, community_id, trigger_id)`
- message_rules: `(profile_id, community_id, rule_id)`
- comment_rules: `(profile_id, community_id, rule_id)`
- delayed_jobs: `(profile_id, community_id, job_id)`
- mailing_jobs: `(profile_id, community_id, job_id)`
- daily_usage: `(profile_id, date_key)`
- idempotency: `(event_id)`

## What To Remove From The Hot Path First

These are the first architectural extractions to make:

1. Remove inline `processDelayed` / `processMailing` from incoming event handling.
Current baseline location:
- `src/handler.js`

2. Remove synchronous app-log writes from every hot path.
Current baseline locations:
- `src/modules/messages.js`
- `src/modules/comments.js`
- `src/modules/app-logs.js`

3. Remove per-event rewrites of shared JSON operational files.
Current baseline locations:
- `src/modules/storage.js`
- `src/modules/users.js`
- `src/modules/profile-dashboard.js`

4. Separate decision logic from outbound VK sending.
Current baseline locations:
- `src/modules/messages.js`
- `src/modules/comments.js`
- `src/modules/vk-api.js`

## Migration Strategy

This rebuild must be incremental.

### Phase 0: Freeze And Observe

Goal:
- keep baseline behavior intact
- document current contracts
- identify exact dependencies

Actions:
- treat `PAPA BOT 1` as frozen reference
- keep `PAPA BOT 2` as migration branch-copy
- preserve current tests
- add more contract tests where behavior is implicit

Definition of done:
- baseline behavior is documented well enough to compare old vs new

### Phase 1: Thin Ingress

Goal:
- make incoming callback fast

Actions:
- add event normalization layer
- add idempotency key generation
- push events to queue
- return `ok` without running full scenario logic inline

Definition of done:
- webhook no longer performs full processing synchronously
- end-to-end still works through queue + worker path

### Phase 2: Introduce Event Workers

Goal:
- move scenario evaluation out of ingress

Actions:
- create worker that consumes inbound events
- move classic rule processing there
- move structured trigger processing there
- keep output as commands/events where possible

Definition of done:
- incoming message/comment logic runs outside ingress function

### Phase 3: Introduce Outbound Action Queue

Goal:
- isolate VK API operations

Actions:
- sender worker consumes outbound commands
- centralize retry/rate-limit logic
- keep message send/comment send out of scenario engine

Definition of done:
- scenario engine decides
- sender engine sends

### Phase 4: Migrate Hot State To YDB

Goal:
- eliminate S3 JSON blobs from operational hot paths

Actions:
- move users state first
- move counters/idempotency second
- move triggers/rules/config gradually
- keep Object Storage only for cold/archive/export paths

Definition of done:
- hot request flow no longer depends on JSON blob rewrites in Object Storage

### Phase 5: Scheduler Rebuild

Goal:
- delayed and mailing execution become queue-driven

Actions:
- timer scans due jobs
- timer emits outbound actions
- sender workers perform delivery

Definition of done:
- no large synchronous send loops in timer logic

### Phase 6: Admin Backend Adaptation

Goal:
- preserve admin panel behavior on top of the new state model

Actions:
- adapt GET/POST admin data flows
- preserve response contracts used by current frontend
- migrate auth/session support carefully

Definition of done:
- admin panel still behaves the same from the user perspective

## Order Of Data Migration

Recommended migration order:

1. idempotency records
2. daily usage counters
3. users
4. user variables / shared variables
5. delayed jobs
6. mailing jobs
7. message/comment rules
8. structured triggers
9. community config
10. admin support data if still needed

This order minimizes risk because it removes the hottest write paths first.

## Testing Requirements

Every phase must end with verification.

Minimum required verification:

- current auth/session tests still pass
- current admin session/captcha behavior still passes
- message flow tests still pass
- structured trigger tests still pass
- delayed/mailing tests cover queue-based execution
- idempotency tests exist
- duplicate event tests exist
- worker retry tests exist
- failure-to-DLQ path exists

Load testing target checkpoints:

- 20 events/sec
- 50 events/sec
- 100 events/sec
- 200 events/sec

Only move upward after evidence.

## Operational Metrics To Add

At minimum:

- ingress request latency
- queue depth by queue
- worker processing latency
- sender success/error rate
- VK API error distribution
- duplicate event rate
- delayed backlog size
- mailing backlog size
- DLQ size

## Acceptance Criteria For The Rebuild

`PAPA BOT 2` is considered on-track only if:

- ingress path is queue-first and thin
- hot state no longer depends on Object Storage JSON rewrites
- message/comment processing is asynchronous
- outbound VK sending is isolated in dedicated workers
- delayed and mailing execution is queue-driven
- baseline product behavior remains intact
- the architecture can be load-tested toward ~200 incoming events/sec

## Non-Goals For The First Migration

Do not expand scope with unrelated work.

Not required in the first migration:

- redesigning the admin UI
- changing pricing logic
- adding brand-new product modules
- replacing all docs at once
- full enterprise multi-region architecture

## Working Rules For Future Chat

If a new implementation chat starts from `PAPA BOT 2`, the assistant should follow these rules:

- read this file first
- use `PAPA BOT 1` only as behavioral reference
- do not optimize cosmetically before removing hot-path bottlenecks
- preserve existing API/UI contracts unless migration explicitly changes them
- prioritize queue-first architecture and hot-state migration
- verify each phase before moving to the next one

## Immediate First Tasks

When implementation begins in `PAPA BOT 2`, start with:

1. map the current incoming event path in `src/handler.js`
2. identify all synchronous side effects triggered from incoming webhook handling
3. introduce a normalized event envelope
4. add inbound queue publishing
5. move classic message/comment processing into worker flow
6. stop calling delayed/mailing processing from incoming webhook flow

## Summary

`PAPA BOT 2` is the migration workspace for turning the current working PAPA BOT into a queue-driven, serverless, hot-state-optimized architecture that can scale toward `1000 communities` on Yandex Cloud Functions.

The correct approach is phased migration, not a rewrite from scratch and not local micro-optimizations on top of the current synchronous blob-based flow.
