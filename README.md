# VK File Uploader Service

Сервис для загрузки файлов >3MB в VK для PAPA BOT.

## Деплой на Render.com

1. Создай новый Web Service на https://dashboard.render.com
2. Подключи GitHub репозиторий или загрузи файлы вручную
3. Настройки:
   - **Build Command:** `npm install`
   - **Start Command:** `npm start`
   - **Environment:** Node
   - **Plan:** Free

4. После деплоя получишь URL типа: `https://vk-file-uploader.onrender.com`

5. Обнови URL в админ-панели PAPA BOT (adminPanelHTML.js):
   ```javascript
   var RENDER_UPLOADER_URL = 'https://your-service.onrender.com/upload';
   ```

## API

### POST /upload

Загружает файл в VK.

**Параметры (multipart/form-data):**
- `file` - файл для загрузки
- `community_token` - токен сообщества VK
- `group_id` - ID группы VK
- `target` - тип загрузки (`messages` или `comments`)

**Ответ:**
```json
{
  "success": true,
  "attachment": "photo-123456_789012"
}
```

## Особенности

- Поддержка фото, видео и документов
- Автоматический выбор метода загрузки по target
- CORS включен для работы с админ-панелью
- Таймауты увеличены для больших файлов
