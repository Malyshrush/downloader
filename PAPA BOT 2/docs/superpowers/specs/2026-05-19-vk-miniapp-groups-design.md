# VK Mini App Groups Design

Date: 2026-05-19
Project: PAPA BOT
Status: Draft for review

## Goal

Add a single shared VK Mini App for all PAPA BOT communities where VK users can view visual group cards and subscribe or unsubscribe from PAPA BOT groups.

The Mini App must work as a simplified Senler-like subscription surface:

- a public list of visible groups for one VK community;
- a direct group page that can open visible or hidden groups;
- per-group visual settings managed from the PAPA BOT admin panel;
- subscription and unsubscription stored in the existing PAPA BOT user group field;
- mandatory permission for community messages before a user can subscribe.

The project is still in owner/admin pre-release testing. The design should support future public scale, including the expected limit of up to 1000 VK communities per PAPA BOT instance, without forcing unnecessary data migrations before launch.

## Decisions Already Accepted

- The VK Mini App is one shared app for all PAPA BOT communities.
- The Mini App frontend is a separate small project, not embedded into `adminPanelHTML.js`.
- The frontend stack is Vite + React + VK Bridge.
- VKUI is not part of the MVP.
- Mini App links use the compact format:

```text
vk.com/appXXXX#c=229445618&g=vip
```

- `c` is the VK community id.
- `g` is the public group slug.
- `vk.com/appXXXX#c=229445618` opens the visible group list for that community.
- Hidden groups are excluded from the list but remain available by direct `c + g` link.
- The subscribe button has two admin-configurable labels:
  - text before subscription, for example `Подписаться`, `Хочу`, `Беру`;
  - text after subscription, for example `Отписаться`, `Уже участвую`, `Убрать из списка`.
- The first button state adds the group to the user data.
- The second button state removes the group from the user data.
- If the user has not allowed community messages, subscription is blocked and the Mini App shows: `Для подписки разрешите сообщения от сообщества`.
- Group icons and banners support both upload and manual image URL. Manual URL has priority when both are present.

## Recommended Approach

Build the Mini App as a separate frontend and extend PAPA BOT as the backend/API.

PAPA BOT remains the source of truth for:

- communities;
- group settings;
- users;
- current user group membership;
- image upload records if uploaded through admin panel;
- VK launch parameter signature verification.

The Mini App stays thin:

- parses `c` and `g` from the hash;
- reads VK launch parameters;
- calls PAPA BOT public Mini App API;
- renders list and detail screens;
- asks VK Bridge for permission to send messages from the community before subscribe;
- reflects subscribed/unsubscribed state from backend responses.

This is preferred over embedding the Mini App into the current admin panel because it keeps public user UI independent from the large admin bundle and leaves room for future scaling across many communities.

## Scope

In scope for MVP:

- separate Vite + React Mini App frontend;
- VK Bridge initialization;
- launch parameter signature flow between frontend and backend;
- public list screen for visible groups;
- group detail screen for visible and hidden groups by direct link;
- subscribe and unsubscribe actions;
- message permission gate before subscribe;
- admin panel group visual settings;
- icon and banner upload or manual URL;
- generated Mini App links in admin panel;
- tests for backend API contracts and admin group settings;
- update `FUNCTIONALITY.md` when implementation changes functionality.

Out of scope for MVP:

- search across communities inside the Mini App;
- showing groups from a community different from the `c` link value;
- analytics dashboards for Mini App traffic;
- paid subscriptions;
- categories/tags for groups;
- VKUI design system;
- full backend extraction from PAPA BOT into a separate service;
- automatic migration to a multi-instance PAPA BOT routing layer.

## User Experience

### Group List

When the user opens:

```text
vk.com/appXXXX#c=229445618
```

the Mini App loads only groups for community `229445618` where the group is enabled for Mini App and not hidden from list.

Each card shows:

- icon thumbnail;
- public title;
- short description if configured;
- current subscription state if available.

Clicking a card opens the group detail screen.

### Direct Group Link

When the user opens:

```text
vk.com/appXXXX#c=229445618&g=vip
```

the Mini App loads group `vip` inside community `229445618`.

The group opens even if it is hidden from the list. If the group does not exist, is disabled for Mini App, or belongs to another community, the Mini App shows a not-found state.

### Group Detail

The detail screen shows:

- banner image;
- public title;
- public description;
- one primary action button.

The button state is determined by backend user membership:

- if user is not in this PAPA BOT group, show the configured subscribe label and call subscribe;
- if user is already in this PAPA BOT group, show the configured unsubscribe label and call unsubscribe.

### Subscribe Permission Flow

On subscribe:

1. Mini App asks VK Bridge to allow messages from the current community.
2. If VK says permission was denied or cannot be granted, backend mutation is not called.
3. The UI shows `Для подписки разрешите сообщения от сообщества`.
4. If permission is granted, Mini App calls the subscribe API.
5. Backend verifies VK launch params and user identity.
6. Backend creates the PAPA BOT user if missing.
7. Backend adds the configured PAPA BOT group to the user's `ГРУППА` data.
8. The UI updates to the subscribed state.

Unsubscribe does not need the message permission gate because it removes an existing group from the user data.

## Admin Panel Design

The existing `ГРУППЫ` tab currently manages logical groups with `Группа` and `Описание`. It should gain a visual Mini App section per group.

Proposed group fields:

- `Группа`: existing logical PAPA BOT group value written to user data;
- `Описание`: existing admin/internal description;
- `MiniApp включен`: whether this group can be opened in Mini App;
- `MiniApp скрыть из списка`: visible by direct link only;
- `MiniApp slug`: public URL slug, unique inside one community;
- `MiniApp заголовок`: public title;
- `MiniApp описание`: public description;
- `MiniApp иконка URL`: manual icon URL;
- `MiniApp иконка файл`: uploaded icon file reference or URL;
- `MiniApp баннер URL`: manual banner URL;
- `MiniApp баннер файл`: uploaded banner file reference or URL;
- `MiniApp текст подписки`: button text before subscription;
- `MiniApp текст отписки`: button text after subscription.

The admin UI should display a generated link:

```text
vk.com/appXXXX#c=<vk_group_id>&g=<slug>
```

For the list page:

```text
vk.com/appXXXX#c=<vk_group_id>
```

Button groups in this UI must keep equal visual sizes, matching the repository UI rule.

## Backend API Design

The API should be public enough for Mini App reads, but user-specific state and mutations must verify VK launch parameters.

Proposed endpoints:

```text
GET /miniapp/groups?c=<vk_group_id>
```

Returns visible Mini App groups for the community. Hidden groups are excluded.

```text
GET /miniapp/groups/<slug>?c=<vk_group_id>
```

Returns one group by slug, including hidden groups, if the group is enabled for Mini App.

```text
GET /miniapp/me?c=<vk_group_id>
```

Returns current VK user state and current PAPA BOT groups. Requires valid launch params.

```text
POST /miniapp/groups/<slug>/subscribe?c=<vk_group_id>
```

Requires valid launch params and a successful message permission flow in the Mini App before the request is made. Creates the user if missing and adds the configured group.

```text
POST /miniapp/groups/<slug>/unsubscribe?c=<vk_group_id>
```

Requires valid launch params. Removes the configured group from the user.

Endpoint names may be adapted to existing handler routing style, but these contracts define the intended behavior.

## Data Model

The existing `ГРУППЫ` storage remains the source of group definitions. The existing `ПОЛЬЗОВАТЕЛИ.ГРУППА` field remains the source of user membership.

No separate subscription table is needed for MVP.

Slug rules:

- unique per community;
- lowercase latin letters, numbers, dash, underscore;
- generated from group name if admin leaves it empty;
- stable after creation unless admin explicitly edits it;
- direct links break if admin changes slug, so the UI should make that consequence clear.

Image URL resolution:

1. manual URL if present;
2. uploaded file URL if present;
3. empty placeholder/fallback in frontend.

## Security

The backend must not trust `vk_user_id` from the frontend body alone.

For user-specific endpoints, frontend sends VK launch parameters or their original signed query payload. Backend verifies `sign` using the Mini App secret/client secret configured for PAPA BOT.

Backend derives:

- `vk_user_id`;
- `vk_group_id` or requested community id compatibility;
- platform/session metadata for logs if needed.

The requested `c` value must resolve to an existing PAPA BOT community. If launch params also contain a group id, the backend should reject mismatches unless the direct-link behavior requires `c` and the verified app context still permits it. This exact VK behavior must be validated during implementation against real Mini App launch params.

The backend must not trust an arbitrary frontend boolean such as `messagesAllowed: true` as proof of message permission. For MVP, the permission gate is enforced by calling VK Bridge before the subscribe request. If VK provides a reliable backend-verifiable permission check for the community/user pair, implementation should add that server-side check before mutating user data.

Subscription must be idempotent:

- subscribing twice keeps one group value;
- unsubscribing twice leaves the user without that group and does not fail as a user-visible error.

## Error Handling

Frontend states:

- missing `c`: show `Откройте Mini App по ссылке сообщества`;
- group not found: show `Группа не найдена`;
- group disabled for Mini App: treat as not found;
- permission denied: show `Для подписки разрешите сообщения от сообщества`;
- invalid VK signature/session: show `Не удалось подтвердить пользователя VK`;
- network/backend failure: show retryable error without changing local subscription state.

Backend responses should use structured JSON:

```json
{
  "success": false,
  "error": "group_not_found",
  "message": "Группа не найдена"
}
```

## Testing

Backend tests:

- visible group list excludes hidden groups;
- direct group endpoint returns hidden group by slug;
- disabled group is not returned;
- missing/duplicate slug validation;
- subscribe verifies launch signature;
- subscribe creates missing user;
- subscribe adds existing PAPA BOT group idempotently;
- unsubscribe removes group idempotently;
- permission-denied path does not mutate user data;
- manual image URL has priority over uploaded file URL.

Admin panel tests:

- group form includes all Mini App visual fields;
- generated Mini App link uses `vk_group_id` and slug;
- action buttons in group controls remain equal-sized;
- save/load preserves new group fields.

Frontend tests or manual verification:

- list route `#c=...`;
- detail route `#c=...&g=...`;
- hidden group opens only by direct link;
- subscribe button label changes after subscribe;
- unsubscribe button label changes after unsubscribe;
- denied message permission blocks subscribe.

## Rollout

Recommended order:

1. Add backend data normalization and tests for new group fields.
2. Add admin panel fields and generated links.
3. Add public read API for group list/detail.
4. Add VK launch signature verification helper.
5. Add user state, subscribe, and unsubscribe APIs.
6. Scaffold separate Vite + React Mini App.
7. Wire VK Bridge permission flow.
8. Run local/manual Mini App route verification.
9. Update `FUNCTIONALITY.md`.

## Open Implementation Notes

- The exact VK Bridge response shape for message permission should be confirmed during implementation.
- The exact source of the Mini App secret must be added to environment configuration.
- Public file hosting for uploaded icon/banner assets should reuse the existing PAPA BOT upload mechanism if it can produce stable public URLs.
- Because the repo currently has many unrelated dirty changes, implementation work should avoid reverting or mixing those changes.
