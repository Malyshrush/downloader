# VK Callback Proxy

This service executes VK API actions that require a **User Token** and therefore should stay outside the main serverless bot runtime.

It is a helper sidecar, not the main bot process.

## Supported Actions

- `approve_request` -> `groups.approveRequest`
- `remove_user` -> `groups.removeUser`
- `delete_conversation` -> `messages.deleteConversation`

## API

### `POST /webhook`

Executes one action.

```json
{
  "secret": "shared-secret",
  "action": "approve_request",
  "groupId": "219331507",
  "userId": "787794248",
  "userToken": "vk1.a...."
}
```

### `POST /batch`

Executes a list of actions in one request.

```json
{
  "secret": "shared-secret",
  "actions": [
    {
      "action": "approve_request",
      "groupId": "219331507",
      "userId": "787794248",
      "userToken": "vk1.a...."
    },
    {
      "action": "remove_user",
      "groupId": "219331507",
      "userId": "123456789",
      "userToken": "vk1.a...."
    }
  ]
}
```

### `GET /health`

Simple health endpoint.

## Required Environment

- `CALLBACK_SECRET` must match the secret used by the caller
- `PORT` is optional for local or hosted runtimes

## Local Run

```bash
cd callback-proxy
npm install
npm start
```

## Deployment Notes

This service is typically deployed as a separate web service, for example on Render.
The main bot should call it only for operations that cannot be performed with a community token.
