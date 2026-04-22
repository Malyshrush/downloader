# PAPA BOT 2

`PAPA BOT 2` is the active migration workspace for rebuilding PAPA BOT toward a queue-driven architecture that can scale much further while staying on Yandex Cloud Functions.

This is not a greenfield project and not a product redesign. It is a controlled rebuild of the runtime model behind the existing VK bot.

## Product Contract That Must Stay Stable

The rebuild must preserve:

- admin panel behavior
- auth, sessions, and CAPTCHA flows
- messages and comments
- structured triggers
- delayed steps and mailings
- users, groups, and variables
- multi-community support
- promo codes, recovery, and profile limits

If behavior changes, it must be an explicit migration decision, not a side effect of refactoring.

## Current Runtime Shape

The branch already contains three separated execution paths:

- `handler` / ingress path:
  accepts webhook requests and publishes normalized incoming events
- `workerHandler`:
  consumes inbound event batches and runs decision logic
- `senderHandler`:
  consumes outbound action batches and performs delivery work

Timer-based scheduler work is also separated:

- delayed steps and mailings are scanned by scheduler logic
- scheduler publishes outbound delivery actions
- sender worker performs actual sends and post-send updates

## Infrastructure Already In Place

- `Yandex Message Queue` for inbound and outbound queues
- `YDB Document API` for durable idempotency
- dedicated sender cloud function `vk-bot-2-sender`
- YDB-backed hot-state layer for config/admin/profile-dashboard JSON objects with S3 fallback and backup

## What Was Already Migrated

- inbound queue abstraction and cloud backend
- normalized event envelope
- inbound worker path
- outbound message/comment action queue
- dedicated sender function deployment
- delayed and mailing delivery routing through outbound actions
- removal of scheduler scans from inbound event worker
- profile dashboard hot-state moved behind the shared YDB-backed hot-state store

## What Still Remains

The project is not fully migrated yet. Important unfinished areas:

- full hot-state cutover away from Object Storage JSON for the hottest runtime entities
- moving `users`, variable-heavy state, and counters to structured storage
- reducing synchronous `app_logs` writes in hot paths
- finishing admin/backend adaptation on top of the new state model

## Key Files

- [src/handler.js](</C:/PROJECT/GPT/PAPA BOT 2/src/handler.js>)
- [src/modules/event-worker.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/event-worker.js>)
- [src/modules/outbound-actions.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/outbound-actions.js>)
- [src/modules/scheduler.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/scheduler.js>)
- [src/modules/hot-state-store.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/hot-state-store.js>)
- [scripts/deploy.js](</C:/PROJECT/GPT/PAPA BOT 2/scripts/deploy.js>)

## Read Before Continuing

1. [START_HERE.md](</C:/PROJECT/GPT/PAPA BOT 2/START_HERE.md>)
2. [REBUILD_TO_1000_COMMUNITIES.md](</C:/PROJECT/GPT/PAPA BOT 2/REBUILD_TO_1000_COMMUNITIES.md>)
3. [NEXT_STEPS.md](</C:/PROJECT/GPT/PAPA BOT 2/NEXT_STEPS.md>)

## Working Rule For This Copy

Use `PAPA BOT 1` only as a behavioral reference.
All architectural work must happen in `PAPA BOT 2`.
