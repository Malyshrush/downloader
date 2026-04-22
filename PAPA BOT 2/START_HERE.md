# START HERE

This is the shortest correct entrypoint for any new session working on `PAPA BOT 2`.

## What This Workspace Is

`PAPA BOT 2` is the active migration workspace for rebuilding the current PAPA BOT runtime without changing the product contract.

- baseline reference: [PAPA BOT 1](</C:/PROJECT/GPT/PAPA BOT 1>)
- active migration workspace: [PAPA BOT 2](</C:/PROJECT/GPT/PAPA BOT 2>)

The goal is to preserve current bot behavior while replacing synchronous blob-heavy execution with a queue-first architecture on Yandex Cloud.

## What Is Already In Place

At the current state of the branch:

- webhook ingress is thin and queue-first
- normalized inbound event envelope exists
- inbound worker path exists
- outbound action queue exists
- dedicated sender function exists and is deployed
- delayed and mailing deliveries are queued to sender instead of sending inline
- event worker no longer runs scheduler scans
- durable idempotency is stored in YDB
- hot-state storage is YDB-primary with S3 fallback and backup for:
  - bot config
  - profile dashboard
  - admin auth
  - admin security
  - admin sessions
  - users
  - variables
  - app logs
  - bot version metadata

## What Must Stay Stable

Do not break without an explicit migration decision:

- admin panel behavior
- auth, sessions, and CAPTCHA flows
- message and comment behavior
- structured triggers
- delayed jobs and mailings
- users, groups, and variables
- multi-community support
- promo, recovery, and profile-limit flows

## Read These Files Next

1. [REBUILD_TO_1000_COMMUNITIES.md](</C:/PROJECT/GPT/PAPA BOT 2/REBUILD_TO_1000_COMMUNITIES.md>)
2. [NEXT_STEPS.md](</C:/PROJECT/GPT/PAPA BOT 2/NEXT_STEPS.md>)
3. only then touch code

## Working Rules

- treat `PAPA BOT 1` only as behavior reference
- make architecture changes only in `PAPA BOT 2`
- prefer bounded migration slices
- keep ingress thin, workers explicit, sender isolated
- verify each slice before moving on
- do not reintroduce direct VK sends from decision logic or scheduler loops

## Recommended Start Prompt

`Work only in PAPA BOT 2. START_HERE.md, REBUILD_TO_1000_COMMUNITIES.md, and NEXT_STEPS.md are already read. Continue the queue-first migration while preserving current product behavior.`
