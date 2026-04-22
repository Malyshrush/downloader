# PAPA BOT 2

`PAPA BOT 2` is the active migration workspace for rebuilding the PAPA BOT runtime into a queue-first architecture on Yandex Cloud Functions without redesigning the product.

This is a controlled migration, not a greenfield rewrite.

## Product Contract

The rebuild must preserve:

- admin panel behavior
- auth, sessions, and CAPTCHA flows
- messages and comments
- structured triggers
- delayed steps and mailings
- users, groups, and variables
- multi-community support
- promo codes, recovery flows, and profile limits

If behavior changes, it must be a deliberate migration decision, not an accidental side effect.

## Current Runtime Shape

The branch already runs through separated execution paths:

- `handler` accepts webhook traffic, normalizes events, and publishes inbound work
- `workerHandler` consumes inbound batches and runs decision logic
- `senderHandler` consumes outbound action batches and performs delivery work

Scheduler responsibilities are also split:

- scheduler scans delayed and mailing jobs
- scheduler publishes outbound delivery actions
- sender worker performs actual sends and post-send updates

## Infrastructure In Place

- Yandex Message Queue for inbound and outbound queues
- YDB Document API for durable idempotency
- dedicated sender cloud function `vk-bot-2-sender`
- shared YDB-backed hot-state layer with S3 fallback and backup

## Already Migrated

- inbound queue abstraction and cloud backend
- normalized event envelope
- inbound worker path
- outbound message and comment action queue
- dedicated sender function deployment
- scheduler delivery routing through outbound actions
- removal of scheduler scans from inbound event worker
- YDB-backed hot-state primary path for:
  - bot config
  - profile dashboard
  - admin auth
  - admin security
  - admin sessions
  - users
  - variables
  - app logs
  - bot version metadata

## What Still Remains

The project is not "fully done" yet. The current remaining work is narrower:

- remove leftover legacy S3-only code from modules already cut over to hot-state
- keep documentation aligned with the real runtime and deploy topology
- decide where whole-document hot-state mutation is still acceptable and where structured YDB entities are needed next

## Key Files

- [src/handler.js](</C:/PROJECT/GPT/PAPA BOT 2/src/handler.js>)
- [src/worker-handler.js](</C:/PROJECT/GPT/PAPA BOT 2/src/worker-handler.js>)
- [src/sender-handler.js](</C:/PROJECT/GPT/PAPA BOT 2/src/sender-handler.js>)
- [src/modules/event-worker.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/event-worker.js>)
- [src/modules/outbound-actions.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/outbound-actions.js>)
- [src/modules/scheduler.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/scheduler.js>)
- [src/modules/hot-state-store.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/hot-state-store.js>)
- [scripts/deploy.js](</C:/PROJECT/GPT/PAPA BOT 2/scripts/deploy.js>)

## Read Before Continuing

1. [START_HERE.md](</C:/PROJECT/GPT/PAPA BOT 2/START_HERE.md>)
2. [REBUILD_TO_1000_COMMUNITIES.md](</C:/PROJECT/GPT/PAPA BOT 2/REBUILD_TO_1000_COMMUNITIES.md>)
3. [NEXT_STEPS.md](</C:/PROJECT/GPT/PAPA BOT 2/NEXT_STEPS.md>)

## Working Rule

Use `PAPA BOT 1` only as a behavioral reference.
All architectural changes happen in `PAPA BOT 2`.
