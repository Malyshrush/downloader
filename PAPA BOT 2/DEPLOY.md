# 🚀 Деплой VK File Uploader на Render.com

## Шаг 1: Подготовка репозитория

Папка `render-uploader` уже содержит все необходимые файлы:
- `index.js` - основной сервер
- `package.json` - зависимости
- `README.md` - документация
- `.gitignore` - игнорируемые файлы

## Шаг 2: Создание репозитория на GitHub

1. Создай новый репозиторий на GitHub: https://github.com/new
   - Название: `vk-uploader`
   - Описание: `VK file uploader service for PAPA BOT`
   - Public или Private (любой)

2. Инициализируй Git в папке render-uploader:
```bash
cd render-uploader
git init
git add .
git commit -m "Initial commit: VK file uploader service"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/vk-uploader.git
git push -u origin main
```

## Шаг 3: Деплой на Render.com

1. Зайди на https://dashboard.render.com
2. Нажми **New +** → **Web Service**
3. Подключи GitHub репозиторий `vk-uploader`
4. Настройки:
   - **Name:** `vk-uploader`
   - **Environment:** `Node`
   - **Build Command:** `npm install`
   - **Start Command:** `npm start`
   - **Plan:** `Free`

5. Нажми **Create Web Service**

## Шаг 4: Получение URL

После деплоя Render выдаст URL:
```
https://vk-uploader.onrender.com
```

## Шаг 5: Обновление кода

URL уже обновлён в `adminPanelHTML.js`:
```javascript
var RENDER_UPLOADER_URL = 'https://vk-uploader.onrender.com/upload';
```

## Шаг 6: Проверка

Проверь работу сервиса:
```bash
curl https://vk-uploader.onrender.com/upload
```

Должен вернуть ошибку 400 (это нормально, значит сервис работает).

## ⚠️ Важно

- Бесплатный план Render "засыпает" после 15 минут неактивности
- Первый запрос после "сна" может занять 30-60 секунд
- Для production рекомендуется платный план ($7/месяц)

## Альтернатива: Railway.app

Если Render не подходит, можно использовать Railway.app:
1. https://railway.app
2. New Project → Deploy from GitHub
3. Выбери репозиторий `vk-uploader`
4. Railway автоматически определит Node.js и задеплоит

---

**Дата создания:** 2026-04-15
