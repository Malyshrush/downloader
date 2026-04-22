# Yandex Function Package

This directory is the deployable Yandex Cloud Functions package for the current `PAPA BOT 2` runtime.

It mirrors the queue-first production shape, not the old single-handler-only layout.

## Runtime Entry Points

- `index.js` exports the cloud entrypoints used by deployment
- `src/handler.js` handles thin webhook ingress
- `src/worker-handler.js` handles inbound queue batches
- `src/sender-handler.js` handles outbound action batches
- `src/modules/` contains business logic, queue adapters, hot-state storage, and scheduler logic
- `scripts/deploy.js` deploys the functions and infrastructure wiring

## Runtime Model

The deployed split is:

- ingress function receives VK callbacks and publishes normalized events
- worker function consumes inbound events and runs decision logic
- sender function consumes outbound actions and performs delivery work
- scheduler publishes outbound work instead of sending inline

## Storage And Infra

The package is built around:

- Yandex Message Queue for inbound and outbound transport
- YDB Document API for durable idempotency and hot-state primary storage
- Object Storage as fallback and backup for JSON hot-state objects

## Local Development

```bash
cd yandex-function
npm install
node src/local-server.js
```

## Deployment

```bash
cd yandex-function
node scripts/deploy.js
```

`event-infra.generated.json` is generated deployment metadata and should match the currently provisioned queue/function topology.
