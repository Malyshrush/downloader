# Деплой Render Uploader

## Текущее состояние

На `2026-04-30` live сервис `https://vk-uploader.onrender.com` ещё работает на старой сборке:

- `GET /` -> `404`
- `GET /healthz` -> `404`

Это означает, что код из текущей ветки в Render ещё не выкачен, даже если Yandex Cloud уже задеплоен.

## Что именно деплоить

Отдельной папки `render-uploader/` больше нет. Актуальный Render uploader теперь живёт прямо в подпроекте:

- `PAPA BOT 2/index.js`
- `PAPA BOT 2/package.json`
- `PAPA BOT 2/src/modules/render-uploader-service.js`

## Правильная конфигурация Render

Если сервис `vk-uploader` уже существует в Render dashboard, у него должны быть такие настройки:

- Repository: текущий GitHub-репозиторий проекта
- Root Directory: `PAPA BOT 2`
- Environment: `Node`
- Build Command: `npm install`
- Start Command: `npm start`

Если хочешь протестировать именно эту ветку до merge:

- Branch: `codex/ymq-ydb-idempotency`

Если Render уже смотрит только на production-ветку, тогда сначала нужен merge этой ветки в основную ветку, а потом redeploy.

## Как понять, что новый uploader реально выкатился

После deploy Render должен отвечать так:

```text
GET https://vk-uploader.onrender.com/
-> 200 {"ok":true,"service":"vk-uploader"}

GET https://vk-uploader.onrender.com/healthz
-> 200 {"ok":true,"service":"vk-uploader"}
```

Если по-прежнему `404`, значит Render всё ещё работает со старой сборкой или смотрит не в тот root directory / branch.

## Что изменилось в новой версии uploader

- больше нет `multer.memoryStorage()` для больших файлов;
- файл сначала кладётся во временный файл, затем стримится в VK;
- после обработки temp-файл удаляется;
- upload path вынесен в `src/modules/render-uploader-service.js`;
- `package-render.json` тоже обновлён и больше не указывает на несуществующий `server.js`.

## Минимальный smoke после выката Render

1. Открыть `https://vk-uploader.onrender.com/healthz`.
2. Убедиться, что ответ `200`.
3. Загрузить в админке файл больше `3.5 MB`.
4. Проверить, что ошибка больше не сводится к ложному wake-up сценарию.
5. Убедиться, что файл появился в блоке `Файлы` на вкладке `ПРОФИЛЬ` для активного сообщества.
