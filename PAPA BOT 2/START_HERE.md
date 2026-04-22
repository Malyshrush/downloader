# START HERE

This file is the shortest correct entrypoint for any new session working on `PAPA BOT 2`.

## What This Project Is

`PAPA BOT 2` is the rebuild workspace for the current PAPA BOT architecture.

- baseline reference: [PAPA BOT 1](</C:/PROJECT/GPT/PAPA BOT 1>)
- active migration workspace: [PAPA BOT 2](</C:/PROJECT/GPT/PAPA BOT 2>)

The rebuild goal is to preserve the current product behavior while replacing the synchronous blob-heavy runtime with a queue-first architecture on Yandex Cloud.

## What Was Already Done

At the moment of this file version, these migration steps are already in place:

- ingress is queue-first and no longer runs full processing inline
- inbound worker path exists
- outbound action queue exists
- dedicated sender function exists and is deployed
- delayed and mailing deliveries are queued to sender instead of sending inline from scheduler
- event worker no longer runs scheduler scans
- durable idempotency is backed by YDB
- part of hot-state storage is switched from S3 JSON to YDB with S3 fallback/backup, including profile dashboard state

## What Must Still Be Protected

Do not break without an explicit migration decision:

- admin panel
- auth, sessions, CAPTCHA
- message and comment behavior
- structured triggers
- delayed jobs and mailings
- users, groups, variables
- multi-community support
- promo / recovery / profile-limit flows

## Read These Files Next

1. [REBUILD_TO_1000_COMMUNITIES.md](</C:/PROJECT/GPT/PAPA BOT 2/REBUILD_TO_1000_COMMUNITIES.md>)
2. [NEXT_STEPS.md](</C:/PROJECT/GPT/PAPA BOT 2/NEXT_STEPS.md>)
3. then only after that touch code

## Working Rules

- treat `PAPA BOT 1` only as behavioral reference
- make architectural changes only in `PAPA BOT 2`
- prefer incremental migration slices
- verify each slice before moving on
- keep ingress thin, workers explicit, sender isolated

## Recommended Start Prompt For A New Chat

`Работаем только в PAPA BOT 2. Уже прочитаны START_HERE.md, REBUILD_TO_1000_COMMUNITIES.md и NEXT_STEPS.md. Продолжаем миграцию queue-first архитектуры с сохранением текущего продуктового поведения.`
