# PAPA BOT

PAPA BOT is a multi-community VK bot running on Yandex Cloud Functions.

This repository now contains two important working contexts:

- `PAPA BOT 1` - frozen behavioral baseline
- `PAPA BOT 2` - active rebuild workspace for the queue-driven architecture

The product behavior is still the same bot: admin panel, sessions, CAPTCHA, messages, comments, delayed steps, mailings, variables, multi-community support, promo codes, recovery flows, and profile limits. What is being rebuilt is the runtime architecture, not the product contract.

## Current Architecture Direction

The project is moving from a synchronous webhook-centric model to a queue-first model:

1. `Ingress Function`
   receives VK callbacks, normalizes them, publishes incoming events, returns `ok`
2. `Event Worker Function`
   consumes inbound events, runs decision logic and structured triggers
3. `Sender Worker Function`
   consumes outbound actions and performs VK delivery
4. `Scheduler / Timer Path`
   scans delayed jobs and mailings, enqueues delivery actions instead of sending inline
5. `YDB + YMQ`
   used for durable idempotency, queues, and hot-state migration

## What Is Already Done In `PAPA BOT 2`

- inbound webhook path is thin and queue-first
- normalized event envelope and queue abstraction exist
- Yandex Message Queue is used for inbound and outbound flows
- durable idempotency is backed by YDB Document API
- dedicated sender function `vk-bot-2-sender` is deployed and wired to outbound YMQ
- outbound message/comment sends are moved to action queue processing
- scheduler now queues delayed and mailing deliveries instead of calling VK inline
- event worker no longer triggers `processDelayed` / `processMailing`
- hot-state primary storage for config/admin/profile-dashboard paths is switched to YDB with S3 fallback/backup

## What Is Not Finished Yet

- not all hot JSON operational paths are cut over from Object Storage to YDB
- `users`, variable-heavy paths, counters, and some runtime modules still depend on JSON blobs
- app logs are still written synchronously in hot paths
- the full admin/backend migration to the new state model is not complete

## Repository Layout

- [PAPA BOT 2](</C:/PROJECT/GPT/PAPA BOT 2>) - active rebuild workspace
- [PAPA BOT 1](</C:/PROJECT/GPT/PAPA BOT 1>) - frozen behavioral reference
- [render-uploader](</C:/PROJECT/GPT/PAPA BOT 2/render-uploader>) - helper service for large VK uploads
- [callback-proxy](</C:/PROJECT/GPT/PAPA BOT 2/callback-proxy>) - helper path for VK flows that need user-token mediated actions
- [vk-token-extension](</C:/PROJECT/GPT/PAPA BOT 2/vk-token-extension>) - browser helper for VK user token extraction

## Where To Start

If the work continues in `PAPA BOT 2`, read these files in order:

1. [START_HERE.md](</C:/PROJECT/GPT/PAPA BOT 2/START_HERE.md>)
2. [REBUILD_TO_1000_COMMUNITIES.md](</C:/PROJECT/GPT/PAPA BOT 2/REBUILD_TO_1000_COMMUNITIES.md>)
3. [NEXT_STEPS.md](</C:/PROJECT/GPT/PAPA BOT 2/NEXT_STEPS.md>)

## Deploy

Main deployment entrypoint:

```bash
node scripts/deploy.js
```

The deployment currently maintains these serverless functions:

- `vk-bot-2` - ingress/admin/timer function
- `vk-bot-2-worker` - inbound event worker
- `vk-bot-2-sender` - outbound sender worker

## Working Rule

When changing `PAPA BOT 2`, preserve product behavior unless a migration step explicitly says otherwise.
The correct direction is:

- thinner ingress
- more queue-driven processing
- less Object Storage in hot paths
- clearer split between decision logic and delivery logic
