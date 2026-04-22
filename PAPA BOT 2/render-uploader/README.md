# Render Uploader

This service uploads large VK attachments outside the main Yandex Cloud Functions runtime.

Use it when the main bot should not handle large multipart uploads directly.

## Purpose

- accept file uploads from admin or backend flows
- upload media to VK with the required token type
- return the final VK attachment id for later sending

## Main Endpoint

### `POST /upload`

Expected `multipart/form-data` fields:

- `file` for the binary payload
- `user_token` for VK user-token upload flows
- `community_token` when the upload path needs a community token
- `group_id` for the target community
- `target` for the delivery type such as `message` or `comment`

Example response:

```json
{
  "success": true,
  "attachment": "photo-123456_789012"
}
```

## Local Run

```bash
cd render-uploader
npm install
npm start
```

## Deployment

This folder is prepared for standalone deployment. It includes:

- `Dockerfile` for container deployment
- `fly.toml` for Fly.io-style deployment
- `DEPLOY.md` for service-specific deployment notes

## Notes

The uploader is a helper sidecar. It should stay isolated from the queue-first bot runtime and only return attachment identifiers back to the caller.
