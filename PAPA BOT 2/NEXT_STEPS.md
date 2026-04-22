# NEXT STEPS

This file tracks the practical migration status of `PAPA BOT 2` after the queue/YDB/sender work that is already done.

It is not a full architecture spec. For that, read [REBUILD_TO_1000_COMMUNITIES.md](</C:/PROJECT/GPT/PAPA BOT 2/REBUILD_TO_1000_COMMUNITIES.md>).

## Current Status

### Already Done

- inbound webhook path is thin and queue-first
- normalized event envelope exists
- inbound worker exists
- outbound action queue exists
- dedicated sender cloud function exists and is wired to outbound YMQ
- message/comment sends are routed through outbound actions
- scheduler now queues delayed and mailing deliveries to sender
- event worker no longer triggers `processDelayed` / `processMailing`
- durable idempotency is implemented in YDB
- config/admin/profile-dashboard hot-state storage is moved to YDB primary with S3 fallback/backup

### Still Not Done

- users and other hottest operational entities are still blob-backed in Object Storage
- synchronous app-log writes still exist in hot paths
- runtime state is not fully cut over from JSON blobs to structured storage
- admin/backend adaptation is not finished end-to-end

## Immediate Next Technical Focus

The next correct focus is to keep removing the hottest synchronous JSON write paths from runtime processing.

### First Target

Move these write-heavy paths away from blob-first behavior:

- `src/modules/users.js`
- variable-related runtime mutations

The goal is:

- stop rewriting large per-community JSON objects on every hot event
- reduce lock contention and request amplification
- prepare the remaining state for structured YDB storage

## Recommended Order From Here

### Step 1

Map every hot write that still happens during:

- inbound event processing
- sender delivery processing
- scheduler delivery completion

Files to inspect first:

- [src/modules/users.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/users.js>)
- [src/modules/variables.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/variables.js>)
- [src/modules/row-actions.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/row-actions.js>)
- [src/modules/app-logs.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/app-logs.js>)

### Step 2

Separate hot operational state into two groups:

- must become structured hot state now
- can temporarily stay in S3 as backup/export/cold data

### Step 3

Pick one bounded migration slice and finish it completely.

Recommended bounded slice:

- user step/bot/group mutations
- profile usage counters tightly related to hot processing
- then variable synchronization paths that still rewrite large sheets

## What Not To Do Next

- do not start cosmetic UI work
- do not redesign admin panel screens
- do not mix three migrations into one huge commit
- do not re-introduce direct VK sends from timer or decision code
- do not expand product scope

## Definition Of Done For The Next Slice

The next slice is successful only if:

- one hot write path is removed or materially reduced
- product behavior stays compatible
- tests cover the new contract
- queue-driven split remains intact

## Practical Start For The Next Session

`Работаем в PAPA BOT 2. Queue-first ingress, outbound sender и scheduler queue path уже сделаны. Следующий шаг: разобрать оставшиеся hot JSON writes в users/profile-dashboard/variables и вынести очередной write-heavy path из blob-first модели.`
