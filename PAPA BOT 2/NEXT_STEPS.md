# NEXT STEPS

This file tracks the practical migration status of `PAPA BOT 2` after the queue, sender, and hot-state cutover work already completed.

It is not the full architecture spec. For the long-term target, read [REBUILD_TO_1000_COMMUNITIES.md](</C:/PROJECT/GPT/PAPA BOT 2/REBUILD_TO_1000_COMMUNITIES.md>).

## Current Status

### Already Done

- inbound webhook path is thin and queue-first
- normalized inbound event envelope exists
- inbound worker exists
- outbound action queue exists
- dedicated sender cloud function exists and is wired to outbound YMQ
- message and comment sends are routed through outbound actions
- scheduler queues delayed and mailing deliveries to sender
- event worker no longer triggers `processDelayed` or `processMailing`
- durable idempotency is implemented in YDB
- YDB-backed hot-state with S3 fallback and backup is the primary runtime path for:
  - config
  - profile dashboard
  - admin auth
  - admin security
  - admin sessions
  - users
  - variables
  - app logs
  - bot version metadata

### Still Not Done

- some migrated modules still carry legacy S3-only code paths and imports that should be removed
- the hot-state layer still stores whole JSON documents for several high-write entities
- runtime state is not yet fully decomposed into narrower structured YDB entities
- repo hygiene and operator docs still need to stay aligned with the real deployed shape

## Immediate Technical Focus

The next correct focus is no longer "introduce sender" or "cut over users to hot state" because that work is already done.

The current focus is:

- finish cleanup of legacy blob-first code in modules already migrated to hot-state
- keep docs aligned with the deployed queue-first architecture
- then choose the next truly hot entity that still deserves a structured YDB table instead of whole-document mutation

## Recommended Order From Here

### Step 1

Clean migrated modules so runtime behavior has one obvious primary path:

- [src/modules/admin-profiles.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/admin-profiles.js>)
- [src/modules/admin-security.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/admin-security.js>)
- [src/modules/admin-sessions.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/admin-sessions.js>)
- [src/modules/config.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/config.js>)
- [src/modules/bot-version-store.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/bot-version-store.js>)

### Step 2

Keep operator docs truthful:

- [README.md](</C:/PROJECT/GPT/PAPA BOT 2/README.md>)
- [START_HERE.md](</C:/PROJECT/GPT/PAPA BOT 2/START_HERE.md>)
- this file
- subproject READMEs that still describe the pre-split runtime or have broken encoding

### Step 3

After cleanup, inspect the remaining hottest whole-document writes and decide whether they need structured storage now.

Inspect first:

- [src/modules/users.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/users.js>)
- [src/modules/variables.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/variables.js>)
- [src/modules/app-logs.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/app-logs.js>)
- [src/modules/row-actions.js](</C:/PROJECT/GPT/PAPA BOT 2/src/modules/row-actions.js>)

The decision criterion is simple:

- keep whole-document hot-state mutation where traffic is acceptable
- introduce narrower structured YDB entities only where contention or write amplification is still materially risky

## What Not To Do Next

- do not redesign admin UI without a separate reason
- do not reintroduce direct VK sends from decision code or scheduler loops
- do not mix unrelated migrations into one commit
- do not expand product scope while the runtime migration is still being stabilized

## Definition Of Done For The Next Slice

The next slice is successful only if:

- one concrete cleanup or storage boundary becomes simpler
- deployed behavior stays compatible
- tests cover the touched contract
- queue-driven separation stays intact
- docs match reality

## Practical Start Prompt

`Work in PAPA BOT 2. Queue-first ingress, sender worker, YMQ queues, YDB idempotency, and hot-state cutover are already in place. Continue with legacy cleanup, documentation alignment, and the next structured-storage decision only where hot-write pressure still justifies it.`
