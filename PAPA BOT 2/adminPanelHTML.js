//=====adminPanelHTML.js=====//
const adminPanelHTML = `<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>Админ-панель &#x1F916; PAPA BOT</title>
<style>

/* ===== ТЕМЫ: CSS-переменные ===== */
:root, [data-theme="light"] {
  --body-bg: linear-gradient(180deg, #f4f7fb 0%, #edf3f8 100%);
  --container-bg: rgba(255,255,255,0.92);
  --container-shadow: 0 26px 70px rgba(15, 23, 42, 0.10);
  --text-color: #1f2937;
  --heading-color: #0f172a;
  --tab-bg: rgba(226, 232, 240, 0.72);
  --tab-border: rgba(148, 163, 184, 0.24);
  --tab-hover-bg: rgba(255,255,255,0.82);
  --tab-active-bg: linear-gradient(135deg, #2563eb 0%, #3b82f6 100%);
  --tab-active-color: #ffffff;
  --table-border: rgba(203, 213, 225, 0.86);
  --table-row-even: rgba(248, 250, 252, 0.98);
  --table-row-hover: rgba(226, 232, 240, 0.78);
  --input-bg: rgba(255,255,255,0.95);
  --input-text: #111827;
  --input-border: #cbd5e1;
  --textarea-bg: rgba(255,255,255,0.95);
  --textarea-text: #111827;
  --select-bg: rgba(255,255,255,0.95);
  --info-box-bg: linear-gradient(135deg, rgba(219, 234, 254, 0.92) 0%, rgba(239, 246, 255, 0.98) 100%);
  --info-box-border: #2563eb;
  --debug-bg: linear-gradient(135deg, rgba(255, 247, 237, 0.95) 0%, rgba(255, 251, 235, 0.98) 100%);
  --debug-border: #f59e0b;
  --status-success-bg: linear-gradient(135deg, rgba(220, 252, 231, 0.96) 0%, rgba(240, 253, 244, 0.98) 100%);
  --status-success-text: #166534;
  --status-error-bg: linear-gradient(135deg, rgba(254, 226, 226, 0.96) 0%, rgba(254, 242, 242, 0.98) 100%);
  --status-error-text: #b91c1c;
  --hint-color: #64748b;
  --tooltip-bg: #0f172a;
  --tooltip-text: #ffffff;
  --tooltip-code-bg: #1e293b;
  --modal-bg: rgba(255,255,255,0.98);
  --modal-overlay: rgba(0,0,0,0.95);
  --overlay-spinner-border: rgba(203, 213, 225, 0.8);
  --overlay-spinner-top: #2563eb;
  --overlay-text-color: #0f172a;
  --save-overlay-bg: rgba(37, 99, 235, 0.18);
  --save-text-color: #1d4ed8;
  --switch-overlay-bg: rgba(37, 99, 235, 0.12);
  --theme-selector-bg: rgba(255,255,255,0.95);
  --theme-selector-text: #0f172a;
  --theme-selector-border: rgba(148, 163, 184, 0.32);
  --surface-soft: rgba(255,255,255,0.72);
  --surface-muted: rgba(248, 250, 252, 0.92);
  --surface-strong: rgba(241, 245, 249, 0.96);
  --section-border: rgba(148, 163, 184, 0.22);
  --accent-color: #2563eb;
  --accent-strong: #1d4ed8;
  --accent-soft: rgba(37, 99, 235, 0.12);
  --focus-ring: rgba(37, 99, 235, 0.22);
  --soft-shadow: 0 14px 36px rgba(15, 23, 42, 0.08);
  --button-shadow: 0 10px 22px rgba(37, 99, 235, 0.12);
  --button-shadow-strong: 0 14px 32px rgba(37, 99, 235, 0.18);
  --theme-ring: rgba(37, 99, 235, 0.22);
  --header-gradient: linear-gradient(135deg, rgba(255,255,255,0.98) 0%, rgba(241,245,249,0.94) 100%);
  --community-label-bg: linear-gradient(135deg, rgba(220, 252, 231, 0.94) 0%, rgba(236, 253, 245, 0.98) 100%);
  --community-label-text: #166534;
  --community-label-border: rgba(34, 197, 94, 0.25);
  --bot-switcher-bg: linear-gradient(135deg, rgba(245, 243, 255, 0.96) 0%, rgba(250, 245, 255, 0.98) 100%);
  --bot-switcher-text: #6b21a8;
  --bot-switcher-border: rgba(147, 51, 234, 0.18);
  --filters-panel-bg: linear-gradient(135deg, rgba(248, 250, 252, 0.98) 0%, rgba(241, 245, 249, 0.98) 100%);
  --filters-panel-border: rgba(148, 163, 184, 0.22);
  --scroll-track: rgba(226, 232, 240, 0.55);
  --scroll-thumb: rgba(148, 163, 184, 0.65);
  --header-orb-1: rgba(59, 130, 246, 0.18);
  --header-orb-2: rgba(168, 85, 247, 0.12);
  --tab-shell-shadow: 0 20px 40px rgba(15, 23, 42, 0.08);
  --tab-border-strong: rgba(148, 163, 184, 0.24);
  --table-head-default: linear-gradient(135deg, #1d4ed8 0%, #2563eb 100%);
  --table-cell-shadow: inset 0 1px 0 rgba(255,255,255,0.38);
  --table-hover-glow: rgba(37, 99, 235, 0.08);
  --table-toolbar-bg: rgba(255,255,255,0.84);
}

[data-theme="dark"] {
  --body-bg: radial-gradient(circle at top, #162447 0%, #0f172a 45%, #020617 100%);
  --container-bg: rgba(15,23,42,0.82);
  --container-shadow: 0 28px 80px rgba(2, 6, 23, 0.55);
  --text-color: #e5eef9;
  --heading-color: #f8fbff;
  --tab-bg: rgba(30, 41, 59, 0.82);
  --tab-border: rgba(96, 165, 250, 0.16);
  --tab-hover-bg: rgba(51, 65, 85, 0.92);
  --tab-active-bg: linear-gradient(135deg, #38bdf8 0%, #2563eb 100%);
  --tab-active-color: #ffffff;
  --table-border: rgba(71, 85, 105, 0.95);
  --table-row-even: rgba(15, 23, 42, 0.78);
  --table-row-hover: rgba(30, 41, 59, 0.95);
  --input-bg: rgba(15,23,42,0.9);
  --input-text: #f8fafc;
  --input-border: rgba(71, 85, 105, 0.95);
  --textarea-bg: rgba(15,23,42,0.9);
  --textarea-text: #f8fafc;
  --select-bg: rgba(15,23,42,0.9);
  --info-box-bg: linear-gradient(135deg, rgba(12, 74, 110, 0.55) 0%, rgba(15, 23, 42, 0.92) 100%);
  --info-box-border: #38bdf8;
  --debug-bg: linear-gradient(135deg, rgba(69, 39, 0, 0.72) 0%, rgba(30, 41, 59, 0.94) 100%);
  --debug-border: #fbbf24;
  --status-success-bg: linear-gradient(135deg, rgba(20, 83, 45, 0.82) 0%, rgba(15, 23, 42, 0.94) 100%);
  --status-success-text: #86efac;
  --status-error-bg: linear-gradient(135deg, rgba(127, 29, 29, 0.82) 0%, rgba(15, 23, 42, 0.94) 100%);
  --status-error-text: #fca5a5;
  --hint-color: #9fb0c4;
  --tooltip-bg: #0f172a;
  --tooltip-text: #ffffff;
  --tooltip-code-bg: #1e293b;
  --modal-bg: rgba(15,23,42,0.98);
  --modal-overlay: rgba(0,0,0,0.95);
  --overlay-spinner-border: rgba(51, 65, 85, 0.88);
  --overlay-spinner-top: #38bdf8;
  --overlay-text-color: #eff6ff;
  --save-overlay-bg: rgba(56, 189, 248, 0.18);
  --save-text-color: #7dd3fc;
  --switch-overlay-bg: rgba(56, 189, 248, 0.12);
  --theme-selector-bg: rgba(15,23,42,0.86);
  --theme-selector-text: #e2e8f0;
  --theme-selector-border: rgba(96, 165, 250, 0.18);
  --surface-soft: rgba(15,23,42,0.58);
  --surface-muted: rgba(30,41,59,0.78);
  --surface-strong: rgba(15,23,42,0.96);
  --section-border: rgba(96, 165, 250, 0.16);
  --accent-color: #38bdf8;
  --accent-strong: #60a5fa;
  --accent-soft: rgba(56, 189, 248, 0.16);
  --focus-ring: rgba(56, 189, 248, 0.24);
  --soft-shadow: 0 18px 40px rgba(2, 6, 23, 0.35);
  --button-shadow: 0 14px 28px rgba(2, 6, 23, 0.32);
  --button-shadow-strong: 0 18px 40px rgba(2, 6, 23, 0.4);
  --theme-ring: rgba(56, 189, 248, 0.28);
  --header-gradient: linear-gradient(135deg, rgba(15,23,42,0.96) 0%, rgba(30,41,59,0.82) 100%);
  --community-label-bg: linear-gradient(135deg, rgba(20, 83, 45, 0.75) 0%, rgba(15, 23, 42, 0.94) 100%);
  --community-label-text: #bbf7d0;
  --community-label-border: rgba(74, 222, 128, 0.2);
  --bot-switcher-bg: linear-gradient(135deg, rgba(67, 56, 202, 0.38) 0%, rgba(15, 23, 42, 0.94) 100%);
  --bot-switcher-text: #ddd6fe;
  --bot-switcher-border: rgba(167, 139, 250, 0.2);
  --filters-panel-bg: linear-gradient(135deg, rgba(30, 41, 59, 0.98) 0%, rgba(15, 23, 42, 0.98) 100%);
  --filters-panel-border: rgba(96, 165, 250, 0.16);
  --scroll-track: rgba(30, 41, 59, 0.9);
  --scroll-thumb: rgba(96, 165, 250, 0.38);
  --header-orb-1: rgba(56, 189, 248, 0.18);
  --header-orb-2: rgba(96, 165, 250, 0.12);
  --tab-shell-shadow: 0 20px 44px rgba(2, 6, 23, 0.34);
  --tab-border-strong: rgba(96, 165, 250, 0.18);
  --table-head-default: linear-gradient(135deg, #1e40af 0%, #2563eb 100%);
  --table-cell-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
  --table-hover-glow: rgba(56, 189, 248, 0.08);
  --table-toolbar-bg: rgba(15,23,42,0.82);
}

/* Применение переменных к элементам */
body { background: var(--body-bg) !important; color: var(--text-color); }
.container { background: var(--container-bg) !important; box-shadow: var(--container-shadow); }
h1 { color: var(--heading-color) !important; }
.tab { background: var(--tab-bg) !important; border-color: var(--tab-border) !important; }
.tab button:hover { background: var(--tab-hover-bg) !important; }
.tab button.active { background: var(--tab-active-bg) !important; color: var(--tab-active-color) !important; }
table th, table td { border-color: var(--table-border) !important; color: var(--text-color); }
tr:nth-child(even) { background: var(--table-row-even) !important; }
tr:hover { background: var(--table-row-hover) !important; }
input, textarea, select { background: var(--input-bg) !important; color: var(--input-text) !important; border-color: var(--input-border) !important; }
textarea.editable-cell { background: var(--textarea-bg) !important; color: var(--textarea-text) !important; border-color: var(--input-border) !important; }
textarea { background: var(--textarea-bg) !important; color: var(--textarea-text) !important; }
select { background: var(--select-bg) !important; color: var(--input-text) !important; }
.info-box { background: var(--info-box-bg) !important; border-left-color: var(--info-box-border) !important; color: var(--text-color); }
.debug { background: var(--debug-bg) !important; border-left-color: var(--debug-border) !important; color: var(--text-color); }
.hint { color: var(--hint-color) !important; }
.tooltip { background: var(--tooltip-bg) !important; color: var(--tooltip-text) !important; }
.tooltip code { background: var(--tooltip-code-bg) !important; }
.status.success { background: var(--status-success-bg) !important; color: var(--status-success-text) !important; }
.status.error { background: var(--status-error-bg) !important; color: var(--status-error-text) !important; }
#authModal { background: var(--modal-overlay) !important; }
#authModal .auth-content { background: var(--modal-bg) !important; }
#switchOverlay { background: var(--switch-overlay-bg) !important; }
#switchOverlay .overlay-content { background: var(--modal-bg) !important; }
#switchOverlay .overlay-text { color: var(--overlay-text-color) !important; }
#switchOverlay .spinner { border-color: var(--overlay-spinner-border) !important; border-top-color: var(--overlay-spinner-top) !important; }
#saveOverlay { background: var(--save-overlay-bg) !important; }
#saveOverlay .save-text, #saveOverlay .save-dots { color: var(--save-text-color) !important; }
body.captcha-lock { overflow: hidden; }
body.captcha-lock .container {
  filter: blur(14px);
  opacity: 0.03;
  pointer-events: none;
  user-select: none;
  transition: filter 0.2s ease, opacity 0.2s ease;
}

/* Стиль селектора темы */
.theme-selector {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  margin-left: 16px;
  font-size: 13px;
  font-weight: 500;
  color: var(--theme-selector-text);
}
.theme-selector label {
  white-space: nowrap;
}
.theme-selector select {
  padding: 4px 8px;
  border: 1px solid var(--theme-selector-border);
  border-radius: 4px;
  background: var(--theme-selector-bg);
  color: var(--theme-selector-text);
  cursor: pointer;
  font-size: 13px;
}

/* ===== СТИЛИ ДЛЯ МОДАЛЬНОГО ОКНА АВТОРИЗАЦИИ и остальные */

/* &#x1F6E1;&#xFE0F; ПОЛНОЭКРАННОЕ УВЕДОМЛЕНИЕ СОХРАНЕНИЯ */
#saveOverlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(16, 185, 129, 0.34);
  z-index: 999998;
  display: none;
  align-items: center;
  justify-content: center;
  flex-direction: column;
  backdrop-filter: blur(10px);
}
#saveOverlay.show {
  display: flex;
}
#saveOverlay .save-text {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0;
  border-radius: 0;
  border: none;
  background: transparent;
  box-shadow: none;
  font-size: 68px;
  font-weight: 900;
  color: #166534;
  text-shadow: 0 2px 10px rgba(255,255,255,0.45);
  letter-spacing: 4px;
}
#saveOverlay .save-dots {
  font-size: 68px;
  font-weight: 900;
  color: #166534;
  margin-left: 10px;
}


* { box-sizing: border-box; }

/* &#x1F3A8; ТЕМЫ */
:root, [data-theme="light"] {
  --bg-body: var(--body-bg);
  --bg-container: var(--container-bg);
  --bg-tab: var(--tab-bg);
  --bg-table-odd: var(--table-row-odd, rgba(255,255,255,0.98));
  --bg-table-even: var(--table-row-even);
  --bg-table-hover: var(--table-row-hover);
  --bg-input: var(--input-bg);
  --bg-textarea: var(--textarea-bg);
  --bg-select: var(--select-bg);
  --text-primary: var(--text-color);
  --text-secondary: var(--hint-color);
  --text-input: var(--input-text);
  --border-color: var(--table-border);
  --border-container: var(--section-border);
  --bg-tooltip: var(--tooltip-bg);
  --text-tooltip: var(--tooltip-text);
  --bg-overlay: var(--save-overlay-bg);
  --bg-modal: var(--modal-bg);
  --bg-status-success: var(--status-success-bg);
  --text-status-success: var(--status-success-text);
  --bg-status-error: var(--status-error-bg);
  --text-status-error: var(--status-error-text);
  --bg-status-warn: var(--debug-bg);
  --text-status-warn: var(--debug-border);
  --bg-info: var(--info-box-bg);
  --bg-debug: var(--debug-bg);
}
[data-theme="dark"] {
  --bg-body: var(--body-bg);
  --bg-container: var(--container-bg);
  --bg-tab: var(--tab-bg);
  --bg-table-odd: rgba(20, 30, 48, 0.92);
  --bg-table-even: var(--table-row-even);
  --bg-table-hover: var(--table-row-hover);
  --bg-input: var(--input-bg);
  --bg-textarea: var(--textarea-bg);
  --bg-select: var(--select-bg);
  --text-primary: var(--text-color);
  --text-secondary: var(--hint-color);
  --text-input: var(--input-text);
  --border-color: var(--table-border);
  --border-container: var(--section-border);
  --bg-tooltip: var(--tooltip-bg);
  --text-tooltip: var(--tooltip-text);
  --bg-overlay: var(--save-overlay-bg);
  --bg-modal: var(--modal-bg);
  --bg-status-success: var(--status-success-bg);
  --text-status-success: var(--status-success-text);
  --bg-status-error: var(--status-error-bg);
  --text-status-error: var(--status-error-text);
  --bg-status-warn: var(--debug-bg);
  --text-status-warn: var(--debug-border);
  --bg-info: var(--info-box-bg);
  --bg-debug: var(--debug-bg);
}
[data-theme="gray"] {
  --bg-body: #424242;
  --bg-container: #616161;
  --bg-tab: #757575;
  --bg-table-odd: #616161;
  --bg-table-even: #555555;
  --bg-table-hover: #9e9e9e;
  --bg-input: #757575;
  --bg-textarea: #757575;
  --bg-select: #757575;
  --text-primary: #f5f5f5;
  --text-secondary: #e0e0e0;
  --text-input: #f5f5f5;
  --border-color: #9e9e9e;
  --border-container: rgba(255,255,255,0.15);
  --bg-tooltip: #424242;
  --text-tooltip: #f5f5f5;
  --bg-overlay: rgba(255,152,0,0.3);
  --bg-modal: #616161;
  --bg-status-success: #2e4a2e;
  --text-status-success: #81c784;
  --bg-status-error: #4a2e2e;
  --text-status-error: #e57373;
  --bg-status-warn: #4a4000;
  --text-status-warn: #ffb74d;
  --bg-info: #1a3050;
  --bg-debug: #4a4000;
}

/* Полноэкранный оверлей при переключении сообщества */
#switchOverlay {
  position: fixed; top: 0; left: 0; width: 100%; height: 100%;
  background: rgba(33, 150, 243, 0.15); backdrop-filter: blur(8px);
  z-index: 999997; display: none; align-items: center; justify-content: center;
}
#switchOverlay .overlay-content {
  background: white; padding: 40px 60px; border-radius: 16px;
  box-shadow: 0 10px 60px rgba(0,0,0,0.3); text-align: center;
}
#switchOverlay .spinner {
  width: 50px; height: 50px; border: 5px solid #e0e0e0;
  border-top: 5px solid #2196F3; border-radius: 50%;
  animation: spin 1s linear infinite; margin: 0 auto 20px;
}
@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
#switchOverlay .overlay-text {
  font-size: 18px; color: #333; font-weight: 500;
}

/* ?? СТИЛИ ДЛЯ МОДАЛЬНОГО ОКНА АВТОРИЗАЦИИ */
#authModal {
position: fixed !important;
top: 0 !important;
left: 0 !important;
width: 100% !important;
height: 100% !important;
background: rgba(0, 0, 0, 0.95) !important;
z-index: 999999 !important;
display: flex !important;
align-items: center !important;
justify-content: center !important;
margin: 0 !important;
padding: 0 !important;
}
#authModal .auth-content {
background: white !important;
padding: 40px !important;
border-radius: 12px !important;
max-width: 400px !important;
width: 90% !important;
text-align: center !important;
box-shadow: 0 10px 40px rgba(0,0,0,0.5) !important;
}
/* ?? СКРЫТЬ КОНТЕЙНЕР ПРИ АВТОРИЗАЦИИ */
body.auth-required .container {
display: none !important;
}
body.auth-required .tab {
display: none !important;
}
body.auth-required .info-box {
display: none !important;
}
body.auth-required h1 {
display: none !important;
}
body.auth-required #debugLog {
display: none !important;
}
body {
font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
margin: 0;
padding: 20px;
background: var(--bg-body);
background-attachment: fixed;
color: var(--text-primary);
font-size: 13px;
line-height: 1.45;
min-height: 100vh;
}
.container {
max-width: 100%;
margin: 0 auto;
background: var(--bg-container);
padding: 24px;
border-radius: 24px;
border: 1px solid var(--border-container);
box-shadow: var(--container-shadow);
backdrop-filter: blur(18px);
color: var(--text-primary);
}
h1 {
color: var(--text-primary);
margin: 0;
font-size: 30px;
font-weight: 800;
letter-spacing: -0.03em;
}
.page-header {
display:flex;
justify-content:space-between;
align-items:flex-start;
gap:18px;
margin-bottom:20px;
padding:20px 22px;
background: var(--header-gradient);
border: 1px solid var(--section-border);
border-radius: 22px;
box-shadow: var(--soft-shadow);
position: relative;
overflow: hidden;
}
.page-header::before,
.page-header::after {
content: '';
position: absolute;
border-radius: 999px;
filter: blur(10px);
pointer-events: none;
}
.page-header::before {
width: 220px;
height: 220px;
right: -70px;
top: -120px;
background: var(--header-orb-1);
}
.page-header::after {
width: 180px;
height: 180px;
left: -80px;
bottom: -120px;
background: var(--header-orb-2);
}
.page-header > * { position: relative; z-index: 1; }
.page-title {
flex: 1 1 auto;
min-width: 0;
max-width: none;
}
.page-title-row {
display:flex;
align-items:center;
gap:12px;
flex-wrap:nowrap;
}
.page-title-row h1 {
flex: 0 1 auto;
min-width: 0;
}
.page-title-tools {
position:relative;
z-index:96;
 margin-top: 12px;
 margin-right: 6px;
align-self: flex-end;
}
.page-eyebrow {
display:inline-flex;
align-items:center;
gap:8px;
padding:7px 12px;
border-radius:999px;
background: var(--surface-muted);
border: 1px solid var(--section-border);
box-shadow: var(--soft-shadow);
font-size: 11px;
font-weight: 800;
letter-spacing: 0.08em;
text-transform: uppercase;
color: var(--accent-strong);
margin-bottom: 12px;
}
.page-subtitle {
margin-top: 10px;
font-size: 14px;
color: var(--text-secondary);
max-width: 720px;
}
.theme-switcher {
display:flex;
align-items:center;
gap:10px;
padding:12px 14px;
border-radius:999px;
background: var(--surface-muted);
border:1px solid var(--section-border);
box-shadow: var(--soft-shadow);
flex-wrap:wrap;
backdrop-filter: blur(12px);
}
.page-title-tools .theme-switcher {
justify-content:flex-end;
}
.theme-dock-slot {
position:fixed;
 right:24px;
top:0;
z-index:110;
display:none;
width:max-content;
 pointer-events:none;
 }
.theme-dock-slot.is-visible {
display:block;
}
.theme-dock-slot .theme-switcher {
pointer-events:auto;
}
.page-header-side {
display:flex;
flex-direction:column;
align-items:flex-end;
gap:18px;
}
.version-chip-row {
display:flex;
align-items:center;
gap:8px;
flex-wrap:wrap;
justify-content:flex-end;
}
.version-chip {
display:inline-flex;
align-items:center;
padding:9px 12px;
border-radius:999px;
background: var(--surface-muted);
border:1px solid var(--section-border);
box-shadow: var(--soft-shadow);
font-size:11px;
font-weight:800;
letter-spacing:0.05em;
color: var(--text-primary);
white-space:nowrap;
}
.version-segments {
display:inline-flex;
align-items:center;
gap:2px;
flex-wrap:wrap;
}
.version-prefix {
color: var(--text-secondary);
margin-right: 4px;
}
.version-dot {
color: var(--text-secondary);
opacity: 0.9;
}
.version-segment {
font-weight:900;
}
.version-segment.seg-0 { color:#ef4444; }
.version-segment.seg-1 { color:#f97316; }
.version-segment.seg-2 { color:#eab308; }
.version-segment.seg-3 { color:#22c55e; }
.version-segment.seg-4 { color:#14b8a6; }
.version-segment.seg-5 { color:#06b6d4; }
.version-segment.seg-6 { color:#3b82f6; }
.version-segment.seg-7 { color:#6366f1; }
.version-segment.seg-8 { color:#8b5cf6; }
.version-segment.seg-9 { color:#a855f7; }
.version-segment.seg-10 { color:#ec4899; }
.version-segment.seg-11 { color:#f43f5e; }
.version-segments--large {
font-size:18px;
line-height:1.45;
}
.version-segments--large .version-prefix {
font-size:13px;
font-weight:800;
text-transform:uppercase;
letter-spacing:0.08em;
}
.version-segments--chip {
font-size:11px;
line-height:1.2;
}
.version-info-btn {
width:34px;
height:34px;
border-radius:999px;
border:1px solid var(--section-border);
background: var(--surface-muted);
color: var(--text-primary);
font-weight:900;
cursor:pointer;
box-shadow: var(--soft-shadow);
}
.capabilities-btn {
padding:10px 14px;
border-radius:999px;
border:1px solid rgba(34,197,94,0.42);
background: linear-gradient(135deg, rgba(134,239,172,0.88) 0%, rgba(74,222,128,0.94) 100%);
color:#052e16;
font-weight:800;
cursor:pointer;
box-shadow: 0 12px 24px rgba(34,197,94,0.18);
}
.version-modal-overlay {
position:fixed;
inset:0;
background:rgba(15,23,42,0.88);
display:none;
align-items:flex-start;
justify-content:center;
padding:120px 18px 18px;
z-index:9999;
}
.version-modal {
width:min(980px, 100%);
max-height:80vh;
overflow:auto;
border-radius:22px;
border:1px solid var(--section-border);
background: var(--bg-card);
box-shadow: 0 24px 60px rgba(15,23,42,0.28);
padding:18px;
}
.capabilities-grid {
display:grid;
grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
gap:12px;
}
.capabilities-card {
border:1px solid var(--section-border);
border-radius:18px;
background: var(--surface-card);
padding:14px;
}
.capabilities-card-title {
font-size:14px;
font-weight:900;
color: var(--heading-color);
margin-bottom:8px;
}
.capabilities-card-list {
display:grid;
gap:6px;
}
.capabilities-card-item {
font-size:12px;
line-height:1.5;
color: var(--text-secondary);
}
.version-modal-header {
display:flex;
align-items:flex-start;
justify-content:space-between;
gap:12px;
margin-bottom:14px;
}
.version-modal-title {
font-size:22px;
font-weight:900;
color: var(--heading-color);
}
.version-modal-subtitle {
margin-top:6px;
font-size:13px;
line-height:1.6;
color: var(--text-secondary);
}
.version-close-btn {
min-width:auto;
padding:10px 12px;
}
.version-summary-card {
border:1px solid var(--section-border);
border-radius:18px;
background: var(--surface-soft);
padding:14px;
margin-bottom:14px;
}
.version-parts-grid {
display:grid;
grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
gap:12px;
}
.version-part-card {
border:1px solid var(--section-border);
border-radius:18px;
background: var(--surface-card);
padding:14px;
}
.version-part-value {
font-size:18px;
font-weight:900;
color: var(--accent-strong);
margin-bottom:6px;
}
.version-part-title {
font-size:13px;
font-weight:800;
color: var(--heading-color);
margin-bottom:6px;
}
.version-part-text {
font-size:12px;
line-height:1.55;
color: var(--text-secondary);
}
.version-part-files {
margin-top:8px;
font-size:11px;
line-height:1.5;
color: var(--text-secondary);
}
.version-level-list {
margin-top:8px;
display:grid;
gap:4px;
}
.version-level-item {
font-size:11px;
line-height:1.45;
color: var(--text-secondary);
}
.version-current-note {
margin-top:8px;
font-size:12px;
line-height:1.6;
color: var(--text-primary);
padding:10px 12px;
border-radius:14px;
background: var(--surface-soft);
border:1px solid var(--section-border);
}
.version-history-box {
margin-top:10px;
max-height:180px;
overflow:auto;
display:grid;
gap:8px;
padding-right:4px;
}
.version-history-item {
border:1px solid var(--section-border);
border-radius:14px;
padding:10px 12px;
background: var(--surface-card);
}
.version-history-version {
font-size:12px;
font-weight:900;
margin-bottom:4px;
}
.version-history-text {
font-size:11px;
line-height:1.55;
color: var(--text-secondary);
}
.version-editor-grid {
display:grid;
grid-template-columns:260px 1fr;
gap:12px;
margin-top:14px;
}
.version-editor-panel,
.version-editor-form {
border:1px solid var(--section-border);
border-radius:18px;
background: var(--surface-card);
padding:14px;
}
.version-editor-list {
display:grid;
gap:8px;
max-height:420px;
overflow:auto;
}
.version-editor-item {
width:100%;
text-align:left;
border:1px solid var(--section-border);
border-radius:14px;
background: var(--surface-soft);
padding:10px 12px;
cursor:pointer;
}
.version-editor-item.is-active {
border-color: var(--accent-color);
box-shadow: inset 0 0 0 1px var(--accent-color);
}
.version-editor-item-title {
font-size:12px;
font-weight:800;
color: var(--heading-color);
}
.version-editor-item-meta {
margin-top:4px;
font-size:11px;
color: var(--text-secondary);
}
.version-editor-fields {
display:grid;
gap:10px;
}
.version-editor-fields label {
font-size:12px;
font-weight:800;
color: var(--text-secondary);
display:block;
margin-bottom:4px;
}
.version-editor-fields input,
.version-editor-fields textarea {
width:100%;
box-sizing:border-box;
padding:10px 12px;
border-radius:12px;
border:1px solid var(--input-border);
background: var(--input-bg);
color: var(--input-text);
font: inherit;
}
.version-editor-fields textarea {
min-height:88px;
resize:vertical;
}
.version-editor-actions {
display:flex;
gap:8px;
flex-wrap:wrap;
margin-top:12px;
}
  @media (max-width: 980px) {
    .version-editor-grid { grid-template-columns: 1fr; }
  .page-title-tools { position: static; width: 100%; margin-top: 0; margin-right: 0; align-self: stretch; }
  .theme-dock-slot { display:none !important; }
    }
.tab {
display:flex;
gap:8px;
overflow-x:auto;
justify-content:flex-start;
border: 1px solid var(--tab-border-strong);
background: var(--bg-tab);
margin-bottom: 20px;
white-space: nowrap;
padding: 10px;
border-radius: 22px;
box-shadow: var(--tab-shell-shadow), inset 0 1px 0 rgba(255,255,255,0.08);
backdrop-filter: blur(16px);
position: sticky;
	top: 0;
	z-index: 100;
}
.tab button {
background: transparent;
border: 1px solid transparent;
padding: 12px 16px;
cursor: pointer;
font-size: 13px;
font-weight: 700;
color: var(--text-primary);
border-radius: 16px;
transition: background 0.18s ease, transform 0.18s ease, color 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease;
position: relative;
text-align:left;
}
.tab button:hover {
background: var(--bg-table-hover);
transform: translateY(-1px);
border-color: var(--section-border);
}
.tab button.active {
background: var(--tab-active-bg);
color: white;
border-color: transparent;
box-shadow: var(--button-shadow-strong);
}
.tab button.tablinks--messages.active {
background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
}
.tab button.active::after {
content: '';
position: absolute;
left: 16px;
right: 16px;
bottom: -2px;
height: 3px;
border-radius: 999px;
background: rgba(255,255,255,0.85);
}


.tabcontent { display: none; }
.tabcontent.active { display: block !important; }
.tab-panel-header {
display:flex;
justify-content:space-between;
align-items:flex-start;
gap:18px;
padding:18px 20px;
margin: 0 0 14px;
border-radius: 22px;
border: 1px solid var(--section-border);
box-shadow: var(--soft-shadow);
position: relative;
overflow: hidden;
}
.tab-panel-header::after {
content:'';
position:absolute;
right:-40px;
top:-40px;
width:140px;
height:140px;
border-radius:999px;
background: rgba(255,255,255,0.08);
pointer-events:none;
}
.tab-panel-copy,
.tab-panel-side {
position: relative;
z-index: 1;
}
.tab-panel-copy { max-width: 760px; }
.tab-panel-kicker {
display:inline-flex;
align-items:center;
padding:6px 10px;
border-radius:999px;
background: rgba(255,255,255,0.14);
border: 1px solid rgba(255,255,255,0.12);
font-size:11px;
font-weight:800;
letter-spacing:0.06em;
text-transform:uppercase;
margin-bottom:10px;
}
.tab-panel-title {
margin:0;
font-size:24px;
line-height:1.15;
font-weight:800;
letter-spacing:-0.03em;
color:white;
}
.tab-panel-description {
margin:10px 0 0;
font-size:13px;
line-height:1.55;
color: rgba(255,255,255,0.88);
max-width: 760px;
}
.tab-panel-side {
display:flex;
align-items:center;
justify-content:center;
min-height:100%;
}
.tab-panel-badge {
display:inline-flex;
align-items:center;
padding:10px 14px;
border-radius:999px;
background: rgba(255,255,255,0.14);
border: 1px solid rgba(255,255,255,0.14);
font-size:12px;
font-weight:700;
color:white;
white-space:nowrap;
}
.tab-panel-header--messages { background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%); }
.tab-panel-header--comments { background: linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%); }
.tab-panel-header--users { background: linear-gradient(135deg, #0f766e 0%, #0f766e 45%, #115e59 100%); }
.tab-panel-header--variables { background: linear-gradient(135deg, #d97706 0%, #b45309 100%); }
.tab-panel-header--mailing { background: linear-gradient(135deg, #db2777 0%, #be185d 100%); }
.tab-panel-header--delayed { background: linear-gradient(135deg, #475569 0%, #334155 100%); }
.tab-panel-header--settings { background: linear-gradient(135deg, #16a34a 0%, #15803d 100%); }
.tab-panel-header--settings-danger { background: linear-gradient(135deg, #dc2626 0%, #b91c1c 100%); }


table { width: max-content; min-width: 100%; border-collapse: separate; border-spacing: 0; font-size: 12px; }
th, td { border-right: 1px solid var(--border-color); border-bottom: 1px solid var(--border-color); padding: 8px 8px; text-align: left; min-width: 80px; color: var(--text-primary); vertical-align: top; box-shadow: var(--table-cell-shadow); }
th:first-child, td:first-child { border-left: 1px solid var(--border-color); }
th { color: white; position: sticky; top: 0; z-index: 10; background: var(--table-head-default); box-shadow: inset 0 -1px 0 rgba(255,255,255,0.08), 0 6px 12px rgba(15, 23, 42, 0.12); font-size: 13px; letter-spacing: 0.02em; position: sticky; }
tr:first-child th:first-child { border-top-left-radius: 16px; }
tr:first-child th:last-child { border-top-right-radius: 16px; }
th .col-name { display: block; font-weight: 800; font-size: 13px; line-height: 1.28; padding-right: 28px; text-wrap: balance; }
th .help-icon { position: absolute; top: 8px; right: 8px; display: inline-flex; align-items: center; justify-content: center; width: 18px; height: 18px; background: rgba(255,255,255,0.22); color: white; border-radius: 50%; text-align: center; line-height: 1; font-size: 12px; font-weight: 900; cursor: help; margin-top: 0; box-shadow: inset 0 1px 0 rgba(255,255,255,0.18); }
th .help-icon:hover { background: rgba(255,255,255,0.38); }
.th-section-green { background: #C8E6C9 !important; color: #333 !important; }
.th-section-brown { background: #D7CCC8 !important; color: #333 !important; }
.th-section-orange { background: #FFE0B2 !important; color: #333 !important; }
.th-section-blue { background: #B3E5FC !important; color: #333 !important; }
.th-section-pink { background: #F8BBD0 !important; color: #333 !important; }
.th-section-red { background: #FFCDD2 !important; color: #333 !important; }
.th-yellow { background: #FFC107 !important; color: #333 !important; }
.th-green { background: #4CAF50 !important; }
.th-brown { background: #795548 !important; }
.th-orange { background: #FF9800 !important; }
.th-orange-dark { background: #c2410c !important; }
.th-pink { background: #E91E63 !important; }
.th-blue-1 { background: #2196F3 !important; }
.th-blue-2 { background: #1976D2 !important; }
.th-blue-3 { background: #0D47A1 !important; }
.th-purple-1 { background: #9C27B0 !important; }
.th-purple-2 { background: #7B1FA2 !important; }
.th-teal-1 { background: #009688 !important; }
.th-teal-2 { background: #00796B !important; }
.th-red-1 { background: #F44336 !important; }
.th-red-2 { background: #D32F2F !important; }
.th-cyan { background: #00BCD4 !important; }
.th-indigo { background: #3F51B5 !important; }
tr:nth-child(even) { background: var(--bg-table-even); }
tr:nth-child(odd) { background: var(--bg-table-odd); }
tbody tr { transition: transform 0.16s ease, background 0.16s ease, box-shadow 0.16s ease; }
tr:hover { background: var(--bg-table-hover); }
tbody tr:hover td { background: linear-gradient(0deg, var(--table-hover-glow), var(--table-hover-glow)), var(--bg-table-hover); }
input, textarea, select {
width: 100%;
padding: 8px 10px;
box-sizing: border-box;
font-size: 12px;
border: 1px solid var(--border-color);
border-radius: 10px;
background: var(--bg-input);
color: var(--text-input);
transition: border-color 0.18s ease, box-shadow 0.18s ease, background 0.18s ease;
}
input:focus, textarea:focus, select:focus {
outline: none;
border-color: var(--accent-color);
box-shadow: 0 0 0 3px var(--focus-ring);
}
textarea { resize: both; min-height: 36px; min-width: 150px; overflow: auto; }
textarea.editable-cell { resize: both; min-height: 40px; min-width: 100px; overflow: auto; font-family: inherit; line-height: 1.45; font-size: 12px; width: 100%; box-sizing: border-box; padding: 9px 10px; border: 1px solid var(--border-color); border-radius: 12px; background: var(--bg-textarea); color: var(--text-input); }
.editable-cell::placeholder { color: var(--text-secondary); }
.cell-editor-wrap {
position: relative;
min-width: 160px;
}
.cell-with-tool .editable-cell {
padding-top: 9px;
padding-right: 88px;
text-align: left;
}
.btn {
padding: 9px 14px;
border: none;
border-radius: 12px;
cursor: pointer;
margin: 3px 0;
font-size: 12px;
font-weight: 700;
letter-spacing: 0.01em;
box-shadow: var(--button-shadow);
transition: transform 0.18s ease, box-shadow 0.18s ease, filter 0.18s ease;
}
.btn:hover {
transform: translateY(-1px);
box-shadow: var(--button-shadow-strong);
filter: brightness(1.02);
}
.btn-add { background: linear-gradient(135deg, #0ea5e9 0%, #2563eb 100%); color: white; }
.btn-save { background: linear-gradient(135deg, #16a34a 0%, #22c55e 100%); color: white; font-size: 13px; padding: 10px 18px; margin: 10px 0; }
.btn-refresh { display: none !important; }
.btn-delete {
background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
color: white;
padding: 9px 16px;
font-size: 12px;
font-weight: bold;
border: none;
border-radius: 12px;
cursor: pointer;
min-width: 100px;
}
.btn-delete:hover {
background: linear-gradient(135deg, #dc2626 0%, #b91c1c 100%);
}
.btn-duplicate {
background: linear-gradient(135deg, #0ea5e9 0%, #2563eb 100%);
color: white;
padding: 9px 14px;
font-size: 11px;
font-weight: 800;
border: none;
border-radius: 12px;
cursor: pointer;
min-width: 92px;
}
.btn-duplicate:hover {
background: linear-gradient(135deg, #0284c7 0%, #1d4ed8 100%);
}
.btn-delete-all {
background: linear-gradient(135deg, #991b1b 0%, #7f1d1d 100%);
color: white;
padding: 10px 20px;
font-size: 13px;
font-weight: bold;
border: none;
border-radius: 14px;
cursor: pointer;
margin: 10px 0;
box-shadow: var(--button-shadow);
}
.btn-delete-all:hover {
background: linear-gradient(135deg, #7f1d1d 0%, #681313 100%);
}
.th-delete {
background: #FFCDD2 !important;
min-width: 120px;
text-align: left;
vertical-align: middle;
}
.status { padding: 12px 14px; margin: 10px 0; border-radius: 16px; display: none; border: 1px solid var(--section-border); box-shadow: var(--soft-shadow); }
.status.success { background: var(--bg-status-success); color: var(--text-status-success); display: block; }
.status.error { background: var(--bg-status-error); color: var(--text-status-error); display: block; }
.scroll { overflow-x: auto; max-height: 600px; overflow-y: auto; border-radius: 22px; border: 1px solid var(--section-border); background: var(--surface-soft); box-shadow: inset 0 1px 0 rgba(255,255,255,0.05), var(--soft-shadow); padding: 10px; }
.info-box { background: var(--bg-info); padding: 14px 16px; border-radius: 16px; margin-bottom: 16px; border: 1px solid var(--section-border); border-left: 5px solid var(--info-box-border); color: var(--text-primary); box-shadow: var(--soft-shadow); }
.debug { background: var(--bg-debug); padding: 12px 14px; margin-top: 20px; border-radius: 16px; border: 1px solid var(--section-border); border-left: 5px solid var(--debug-border); font-size: 11px; max-height: 200px; overflow-y: auto; white-space: pre-wrap; color: var(--text-primary); }
.tooltip { position: fixed; z-index: 10000; background: var(--bg-tooltip); color: var(--text-tooltip); padding: 10px; border-radius: 4px; font-size: 12px; max-width: 500px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); pointer-events: none; display: none; line-height: 1.5; }
.tooltip.show { display: block; }
.tooltip code { background: var(--tooltip-code-bg); padding: 2px 6px; border-radius: 6px; font-family: monospace; }
select { padding: 8px 10px; font-size: 12px; }
.hint { font-size: 11px; color: var(--text-secondary); margin-top: 4px; line-height: 1.4; }
.section-header { text-align: center; font-weight: 900; font-size: 13px; letter-spacing: 0.05em; padding: 10px 8px !important; }
.color-link-cell { min-width: 120px; }
.color-link-cell select, .color-link-cell input { width: 100%; padding: 2px; font-size: 11px; }
.color-link-cell .link-input { display: none; }
.row-actions-cell {
  min-width: 220px;
}
.row-actions-wrap {
  display:flex;
  gap:8px;
  align-items:center;
  justify-content:flex-start;
  flex-wrap:wrap;
}
.cell-with-copy .editable-cell {
  padding-bottom: 34px;
}
.cell-with-trigger-mode .editable-cell {
  padding-right: 10px;
  padding-bottom: 42px;
}
.trigger-mode-wrap {
  position: absolute;
  left: 6px;
  right: 6px;
  bottom: 6px;
  display: flex;
  gap: 6px;
  justify-content: flex-start;
  flex-wrap: nowrap;
}
.trigger-mode-btn {
  flex: 1 1 0;
  border: none;
  border-radius: 10px;
  padding: 4px 6px;
  font-size: 9px;
  font-weight: 800;
  line-height: 1.2;
  white-space: nowrap;
  color: white;
  background: linear-gradient(135deg, #94a3b8 0%, #64748b 100%);
  box-shadow: var(--button-shadow);
  cursor: pointer;
}
.trigger-mode-btn.active {
  background: linear-gradient(135deg, #16a34a 0%, #22c55e 100%);
}
.trigger-mode-btn:hover {
  transform: translateY(-1px);
  box-shadow: var(--button-shadow-strong);
}
.attach-btn,
.kb-btn-cell,
.test-btn {
  position: absolute !important;
  top: 6px !important;
  right: 6px !important;
  border: none !important;
  border-radius: 10px !important;
  padding: 4px 8px !important;
  font-size: 10px !important;
  font-weight: 700 !important;
  line-height: 1.2 !important;
  box-shadow: var(--button-shadow) !important;
}
.attach-btn { background: linear-gradient(135deg, #0ea5e9 0%, #2563eb 100%) !important; }
.kb-btn-cell { background: linear-gradient(135deg, #7c3aed 0%, #a855f7 100%) !important; }
.test-btn { background: linear-gradient(135deg, #f59e0b 0%, #f97316 100%) !important; }
.copy-btn-cell {
  position: absolute !important;
  left: 6px !important;
  bottom: 6px !important;
  border: none !important;
  border-radius: 10px !important;
  padding: 4px 8px !important;
  font-size: 10px !important;
  font-weight: 700 !important;
  line-height: 1.2 !important;
  color: white !important;
  background: linear-gradient(135deg, #10b981 0%, #059669 100%) !important;
  box-shadow: var(--button-shadow) !important;
}
.attach-btn:hover,
.kb-btn-cell:hover,
.test-btn:hover,
.copy-btn-cell:hover {
  transform: translateY(-1px);
  box-shadow: var(--button-shadow-strong) !important;
}

/* 🖼️ Модальное окно для изображений инструкций */
.image-modal { display: none; position: fixed; z-index: 10001; left: 0; top: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.85); }
.image-modal.show { display: flex; align-items: center; justify-content: center; }
.image-modal-content { max-width: 90%; max-height: 90%; position: relative; }
.image-modal-content img { max-width: 100%; max-height: 85vh; border-radius: 8px; box-shadow: 0 4px 20px rgba(0,0,0,0.5); }
.image-modal-close { position: absolute; top: -35px; right: 0; color: white; font-size: 28px; cursor: pointer; background: rgba(0,0,0,0.5); border: none; border-radius: 4px; padding: 4px 10px; }
.image-modal-close:hover { background: rgba(255,255,255,0.2); }
.instruction-img-link { color: #1976D2; cursor: pointer; text-decoration: underline; font-weight: bold; }
.instruction-img-link:hover { color: #0D47A1; }

/* Кнопки тем */
.theme-btn {
display:flex;
align-items:center;
gap:8px;
min-height:44px;
padding:0 14px;
border:1px solid var(--section-border);
border-radius:999px;
cursor:pointer;
font-size:12px;
font-weight:700;
color:var(--text-primary);
background: var(--surface-soft);
box-shadow: var(--soft-shadow);
transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease, background 0.18s ease;
}
.theme-btn .theme-icon { font-size:16px; line-height:1; }
.theme-btn .theme-name { white-space:nowrap; }
.theme-btn:hover { transform: translateY(-1px); box-shadow: var(--button-shadow); }
.theme-btn.active { box-shadow: 0 0 0 3px rgba(34, 197, 94, 0.28), var(--button-shadow-strong); transform: translateY(-1px); border-color: #22c55e; }
.theme-btn[data-theme="light"] {
background: linear-gradient(135deg, rgba(255,255,255,0.98) 0%, rgba(241,245,249,0.98) 100%);
}
.theme-btn[data-theme="dark"] {
background: linear-gradient(135deg, rgba(15,23,42,0.98) 0%, rgba(30,41,59,0.96) 100%);
color: #eff6ff;
}

/* ===== ГЛОБАЛЬНЫЙ ТЁМНЫЙ СТИЛЬ ===== */
/* Скрыть блок с JSON конфигом */
#settings-debug { display: none !important; }

/* Активное сообщество - метки */
[id^="activeCommunityLabel-"] {
    background: var(--community-label-bg) !important;
    color: var(--community-label-text) !important;
    border: 1px solid var(--community-label-border) !important;
    border-left: 5px solid #22c55e !important;
    font-weight: 700 !important;
    border-radius: 14px !important;
    padding: 10px 14px !important;
    box-shadow: var(--soft-shadow) !important;
}

/* Панели переключения ботов */
[id^="botSwitcher-"] {
    background: var(--bot-switcher-bg) !important;
    color: var(--bot-switcher-text) !important;
    border: 1px solid var(--bot-switcher-border) !important;
    border-left: 5px solid #a855f7 !important;
    border-radius: 16px !important;
    padding: 12px 14px !important;
    box-shadow: var(--soft-shadow) !important;
}

/* Панель фильтров пользователей */
#userFiltersPanel {
    background: var(--filters-panel-bg) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--filters-panel-border) !important;
    border-radius: 18px !important;
    box-shadow: var(--soft-shadow) !important;
}

.btn-info { background: linear-gradient(135deg, #0ea5e9 0%, #2563eb 100%); color: white; }
.btn-accent { background: linear-gradient(135deg, #7c3aed 0%, #a855f7 100%); color: white; }
.btn-neutral { background: linear-gradient(135deg, #64748b 0%, #475569 100%); color: white; }
.settings-surface {
  margin: 15px 0;
  padding: 16px 18px;
  border-radius: 20px;
  border: 1px solid var(--section-border);
  background: var(--surface-muted);
  box-shadow: var(--soft-shadow);
}
.settings-surface--community {
  background: var(--header-gradient);
}
.settings-hero {
  margin: 15px 0;
  padding: 22px;
  border-radius: 24px;
  background: linear-gradient(135deg, #2563eb 0%, #7c3aed 100%);
  color: white;
  box-shadow: 0 18px 40px rgba(37, 99, 235, 0.22);
}
.settings-hero-step {
  background: rgba(255,255,255,0.14);
  padding: 10px 14px;
  border-radius: 14px;
  margin: 8px 0;
  border: 1px solid rgba(255,255,255,0.12);
}
.settings-hero-footnote {
  margin-top: 14px;
  font-size: 12px;
  opacity: 0.92;
}
.settings-muted-panel {
  margin: 15px 0;
  padding: 14px;
  background: var(--surface-soft);
  border-radius: 16px;
  border: 1px solid var(--section-border);
}
.settings-helper-box {
  font-size: 12px;
  color: var(--text-primary);
  background: var(--surface-strong);
  padding: 15px;
  border-radius: 18px;
  margin: 8px 0;
  line-height: 1.65;
  border: 1px solid var(--section-border);
  box-shadow: var(--soft-shadow);
}
.settings-helper-box--blue { border-left: 5px solid #38bdf8; }
.settings-helper-box--purple { border-left: 5px solid #a855f7; }
.settings-helper-box--green { border-left: 5px solid #22c55e; }
.settings-helper-title {
  font-size: 15px;
  color: var(--text-primary);
}
.settings-helper-box code,
.settings-code {
  background: var(--surface-soft);
  padding: 2px 6px;
  border-radius: 8px;
  color: var(--accent-strong);
}
.settings-helper-link {
  color: var(--accent-color) !important;
  font-weight: 700;
}
.settings-subnote {
  font-size: 12px;
  color: var(--text-secondary);
  margin-top: 6px;
}
.settings-divider {
  margin: 22px 0;
  border: none;
  border-top: 1px solid var(--section-border);
}
.settings-link-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  text-decoration: none;
}
.community-empty-note {
  color: var(--text-secondary);
  font-size: 12px;
}
.community-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
  gap: 12px;
  margin-top: 10px;
}
.community-btn {
  display:flex;
  flex-direction:column;
  align-items:flex-start;
  gap:8px;
  background: var(--surface-soft);
  color: var(--text-primary);
  border: 1px solid var(--section-border);
  padding: 14px 16px;
  border-radius: 18px;
  cursor: pointer;
  font-size: 13px;
  box-shadow: var(--soft-shadow);
  text-align:left;
  min-height: 96px;
}
.community-btn.active {
  background: linear-gradient(135deg, #16a34a 0%, #22c55e 100%);
  color: white;
  border-color: transparent;
}
.community-btn--temp {
  background: linear-gradient(135deg, #f59e0b 0%, #f97316 100%);
  color: white;
  border-color: transparent;
}
.community-btn-title {
  font-size: 20px;
  font-weight: 800;
  line-height: 1.15;
  letter-spacing: -0.02em;
}
.community-btn-meta {
  font-size: 12px;
  font-weight: 700;
  opacity: 0.88;
}
.community-btn-state {
  display:inline-flex;
  align-items:center;
  padding:6px 10px;
  border-radius:999px;
  font-size:11px;
  font-weight:800;
  background: rgba(255,255,255,0.12);
  border: 1px solid rgba(255,255,255,0.14);
}
.community-btn:not(.active) .community-btn-state {
  background: var(--accent-soft);
  color: var(--accent-strong);
  border-color: transparent;
}
#communitySwitcher .help-icon {
  background: var(--accent-soft) !important;
  color: var(--accent-strong) !important;
}
.profile-manager {
  margin: 0 0 16px;
}
.profile-manager-header {
  display:flex;
  justify-content:space-between;
  align-items:flex-start;
  gap:16px;
  margin-bottom: 14px;
}
.profile-manager-title {
  margin: 0;
  font-size: 20px;
  font-weight: 800;
  color: var(--text-primary);
}
.profile-manager-subtitle {
  margin-top: 6px;
  color: var(--text-secondary);
  font-size: 13px;
  line-height: 1.5;
}
.profile-current-chip {
  display:inline-flex;
  align-items:center;
  padding:10px 14px;
  border-radius:999px;
  background: var(--accent-soft);
  color: var(--accent-strong);
  font-size:12px;
  font-weight:800;
}
.profile-grid {
  display:grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}
.profile-card {
  border: 1px solid var(--section-border);
  background: var(--surface-soft);
  border-radius: 18px;
  padding: 16px;
  box-shadow: var(--soft-shadow);
}
.profile-card.promo-card--status {
  border-color: #999;
}
.profile-card.current {
  border-color: #22c55e;
  box-shadow: 0 0 0 2px rgba(34, 197, 94, 0.18), var(--soft-shadow);
}
.profile-card.profile-card--active-community {
  background: linear-gradient(135deg, #16a34a 0%, #22c55e 100%);
  color: white;
  border-color: transparent;
}
.profile-card.profile-card--active-community .profile-card-name,
.profile-card.profile-card--active-community .profile-card-id,
.profile-card.profile-card--active-community .profile-card-row,
.profile-card.profile-card--active-community .profile-card-label {
  color: white;
}
.profile-card.profile-card--active-community .profile-card-badge {
  background: rgba(255,255,255,0.12);
  color: white;
  border: 1px solid rgba(255,255,255,0.14);
}
.profile-card-header {
  display:flex;
  justify-content:space-between;
  gap:10px;
  align-items:flex-start;
}
.profile-card-name {
  font-size:18px;
  font-weight:800;
  color: var(--text-primary);
  line-height:1.2;
}
.profile-card-id {
  margin-top: 5px;
  color: var(--text-secondary);
  font-size:12px;
  font-weight:700;
}
.profile-card-badge {
  display:inline-flex;
  align-items:center;
  padding:6px 10px;
  border-radius:999px;
  background: var(--accent-soft);
  color: var(--accent-strong);
  font-size:11px;
  font-weight:800;
}
.profile-card-details {
  margin-top: 12px;
  display:grid;
  gap:8px;
}
.profile-card-row {
  font-size:13px;
  color: var(--text-primary);
}
.profile-card-label {
  color: var(--text-secondary);
  font-weight:700;
}
.profile-card-actions {
  display:flex;
  flex-wrap:wrap;
  gap:8px;
  margin-top: 14px;
}
.profile-form {
  border: 1px solid var(--section-border);
  background: var(--surface-muted);
  border-radius: 18px;
  padding: 16px;
  box-shadow: var(--soft-shadow);
}
.profile-form-grid {
  display:grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
  margin-bottom: 12px;
}
.profile-form-note {
  color: var(--text-secondary);
  font-size: 12px;
  line-height: 1.5;
  margin-top: 8px;
}
.trigger-hub-toolbar {
  display:flex;
  gap:12px;
  align-items:center;
  justify-content:space-between;
  flex-wrap:wrap;
  margin-bottom:16px;
}
.trigger-hub-search {
  min-width: 280px;
  max-width: 420px;
}
.trigger-hub-filters {
  display:flex;
  gap:8px;
  flex-wrap:wrap;
}
.trigger-hub-chip {
  display:inline-flex;
  align-items:center;
  padding:8px 12px;
  border-radius:999px;
  border:1px solid var(--section-border);
  background: var(--surface-soft);
  color: var(--text-primary);
  font-size:12px;
  font-weight:800;
  cursor:pointer;
}
.trigger-hub-chip.active {
  background: linear-gradient(135deg, #16a34a 0%, #22c55e 100%);
  color: white;
  border-color: transparent;
}
.trigger-hub-grid {
  display:grid;
  grid-template-columns: repeat(auto-fit, minmax(330px, 1fr));
  gap:16px;
}
.trigger-card {
  border:1px solid var(--section-border);
  background: var(--surface-soft);
  border-radius: 22px;
  padding: 18px;
  box-shadow: var(--soft-shadow);
}
.trigger-card-header {
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:12px;
  margin-bottom:12px;
}
.trigger-card-title {
  font-size:20px;
  font-weight:800;
  line-height:1.15;
  color: var(--text-primary);
}
.trigger-card-meta {
  margin-top:6px;
  font-size:12px;
  color: var(--text-secondary);
  line-height:1.5;
}
.trigger-card-badges {
  display:flex;
  flex-wrap:wrap;
  gap:6px;
  margin-bottom:12px;
}
.trigger-badge {
  display:inline-flex;
  align-items:center;
  padding:6px 10px;
  border-radius:999px;
  font-size:11px;
  font-weight:800;
  border:1px solid transparent;
}
.trigger-badge.source-messages { background: rgba(37, 99, 235, 0.12); color: #1d4ed8; }
.trigger-badge.source-comments { background: rgba(124, 58, 237, 0.12); color: #7c3aed; }
.trigger-badge.mode-text { background: rgba(148, 163, 184, 0.18); color: var(--text-primary); }
.trigger-badge.mode-button { background: rgba(34, 197, 94, 0.16); color: #166534; }
.trigger-badge.mode-file { background: rgba(245, 158, 11, 0.16); color: #b45309; }
.trigger-card-section {
  margin-top: 10px;
  padding-top: 10px;
  border-top: 1px solid var(--section-border);
}
.trigger-card-label {
  font-size:11px;
  font-weight:900;
  letter-spacing:0.06em;
  text-transform:uppercase;
  color: var(--text-secondary);
  margin-bottom:6px;
}
.trigger-card-value {
  font-size:13px;
  line-height:1.55;
  color: var(--text-primary);
  white-space:pre-wrap;
}
.trigger-card-actions {
  display:flex;
  gap:8px;
  flex-wrap:wrap;
  margin-top:14px;
}
.structured-trigger-card .trigger-card-actions {
  margin-top:auto;
  padding-top:12px;
}
.trigger-empty {
  padding: 18px;
  border-radius: 18px;
  border:1px dashed var(--section-border);
  color: var(--text-secondary);
  background: var(--surface-soft);
}
.structured-trigger-shell {
  display:grid;
  gap:16px;
}
.structured-trigger-toolbar {
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
}
.structured-trigger-toolbar-main {
  display:flex;
  align-items:center;
  gap:10px;
  flex-wrap:nowrap;
  flex:1 1 auto;
  min-width:0;
}
.structured-trigger-toolbar-filters {
  display:flex;
  align-items:center;
  gap:8px;
  flex-wrap:nowrap;
  flex:1 1 auto;
  min-width:0;
}
.structured-trigger-filter-input,
.structured-trigger-filter-select {
  padding:10px 12px;
  border-radius:12px;
  border:1px solid var(--input-border);
  background: var(--input-bg);
  color: var(--input-text);
  font: inherit;
}
.structured-trigger-filter-input {
  min-width:0;
  flex:1 1 auto;
}
.structured-trigger-builder {
  border:1px solid var(--section-border);
  background: var(--surface-muted);
  border-radius: 22px;
  padding: 18px;
  box-shadow: var(--soft-shadow);
}
.structured-trigger-grid {
  display:grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
}
.structured-trigger-field {
  display:flex;
  flex-direction:column;
  gap:6px;
}
.structured-trigger-field--full {
  grid-column: 1 / -1;
}
.structured-trigger-field label {
  font-size:12px;
  font-weight:800;
  color: var(--text-secondary);
}
.structured-trigger-field input,
.structured-trigger-field select,
.structured-trigger-field textarea {
  width:100%;
  box-sizing:border-box;
  padding:10px 12px;
  border-radius:12px;
  border:1px solid var(--input-border);
  background: var(--input-bg);
  color: var(--input-text);
  font: inherit;
}
.structured-trigger-field textarea {
  min-height:76px;
  resize:vertical;
}
.structured-trigger-hint {
  font-size:12px;
  color: var(--text-secondary);
  line-height:1.5;
}
.structured-trigger-actions {
  display:flex;
  gap:10px;
  flex-wrap:wrap;
  margin-top:16px;
}
.structured-trigger-footnote {
  font-size:11px;
  line-height:1.5;
  color: var(--text-secondary);
}
.structured-trigger-status-row {
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
  margin-bottom:12px;
}
.structured-trigger-toggle {
  display:inline-flex;
  align-items:center;
  gap:8px;
  font-size:12px;
  font-weight:700;
  color: var(--text-primary);
}
.structured-trigger-toggle input {
  width:16px;
  height:16px;
}
.structured-trigger-card {
  min-width:0;
  padding:14px;
  border-radius:18px;
  border-width:2px;
  display:flex;
  flex-direction:column;
}
.structured-trigger-card.is-active {
  border-color: rgba(22, 163, 74, 0.95);
  background: linear-gradient(180deg, rgba(134,239,172,0.62) 0%, rgba(74,222,128,0.38) 100%);
  box-shadow: 0 14px 30px rgba(22, 163, 74, 0.22);
}
.structured-trigger-card.is-inactive {
  border-color: rgba(220, 38, 38, 0.95);
  background: linear-gradient(180deg, rgba(252,165,165,0.62) 0%, rgba(248,113,113,0.38) 100%);
  box-shadow: 0 14px 30px rgba(220, 38, 38, 0.18);
}
.structured-trigger-card-status {
  display:inline-flex;
  align-items:center;
  justify-content:center;
  padding:6px 10px;
  border-radius:999px;
  font-size:11px;
  font-weight:800;
  border:none;
  cursor:pointer;
  min-width:92px;
}
.structured-trigger-card-status.is-active {
  background:#15803d;
  color:#f0fdf4;
}
.structured-trigger-card-status.is-inactive {
  background:#b91c1c;
  color:#fef2f2;
}
.structured-trigger-cards-grid {
  display:grid;
  gap:12px;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  margin-top:12px;
}
.structured-trigger-card .trigger-card-title {
  font-size:14px;
  line-height:1.2;
  color: var(--heading-color);
  text-decoration: underline;
  text-underline-offset: 3px;
}
.structured-trigger-card .trigger-card-meta {
  margin-top:4px;
  font-size:10px;
  line-height:1.4;
  color: rgba(15, 23, 42, 0.78);
}
.structured-trigger-card .trigger-card-section {
  margin-top:8px;
  padding-top:8px;
}
.structured-trigger-card .trigger-card-label {
  font-size:10px;
  margin-bottom:4px;
  text-align:center;
  color:#1d4ed8;
  letter-spacing:0.08em;
}
.structured-trigger-card .trigger-card-value {
  font-size:12px;
  line-height:1.45;
}
.structured-trigger-detail-list {
  display:grid;
  gap:5px;
}
.structured-trigger-detail-row {
  display:grid;
  grid-template-columns: 64px 1fr;
  gap:6px;
  align-items:start;
}
.structured-trigger-detail-key {
  font-size:10px;
  font-weight:900;
  letter-spacing:0.04em;
  text-transform:uppercase;
  color: var(--text-secondary);
}
.structured-trigger-detail-text {
  font-size:11px;
  line-height:1.4;
  color: var(--text-primary);
  white-space:pre-wrap;
}
[data-theme="dark"] .structured-trigger-card .trigger-card-meta {
  color: rgba(248, 250, 252, 0.82);
}
[data-theme="dark"] .structured-trigger-card .trigger-card-label {
  color:#bfdbfe;
}
@media (max-width: 1600px) {
  .structured-trigger-cards-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
}
@media (max-width: 1280px) {
  .structured-trigger-cards-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
}
@media (max-width: 980px) {
  .structured-trigger-cards-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 640px) {
  .structured-trigger-cards-grid { grid-template-columns: 1fr; }
}
.inline-notice {
  padding: 10px 12px;
  border-radius: 14px;
  border: 1px solid var(--section-border);
  box-shadow: var(--soft-shadow);
  font-size: 12px;
  line-height: 1.5;
}
.inline-notice--info { background: var(--info-box-bg); color: var(--text-primary); }
.inline-notice--warn { background: var(--debug-bg); color: var(--text-primary); }
.inline-notice--success { background: var(--status-success-bg); color: var(--status-success-text); }
.inline-notice--error { background: var(--status-error-bg); color: var(--status-error-text); }
.inline-notice--accent { background: var(--bot-switcher-bg); color: var(--bot-switcher-text); }
.inline-text--info { color: var(--accent-color); }
.inline-text--success { color: var(--status-success-text); }
.inline-text--error { color: var(--status-error-text); }
.tab-refresh-btn {
  min-width:auto;
  padding:10px 16px;
  background: linear-gradient(135deg, #16a34a 0%, #22c55e 55%, #4ade80 100%);
  color:#f0fdf4;
  border:none;
  box-shadow: 0 12px 24px rgba(22, 163, 74, 0.22);
}
.tab-refresh-btn:hover {
  filter: brightness(1.05);
}
.tab-panel-title-row {
  display:flex;
  align-items:center;
  gap:12px;
  flex-wrap:wrap;
}
.app-log-toolbar {
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  flex-wrap:wrap;
  margin-bottom:14px;
}
.app-log-filter-row {
  display:flex;
  gap:8px;
  flex-wrap:wrap;
}
.app-log-actions {
  display:flex;
  align-items:center;
  gap:8px;
  flex-wrap:wrap;
}
.app-log-file-label {
  font-size:11px;
  color: var(--text-secondary);
}
.app-log-filter-btn {
  min-width:auto;
  padding:7px 11px;
}
.app-log-grid {
  display:grid;
  gap:8px;
}
.app-log-card {
  border:1px solid var(--section-border);
  border-radius:14px;
  padding:10px 12px;
  background: var(--surface-card);
  box-shadow: var(--soft-shadow);
}
.app-log-card-header {
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap:10px;
}
.app-log-card-title {
  font-size:13px;
  font-weight:800;
  color: var(--heading-color);
}
.app-log-card-meta {
  margin-top:2px;
  font-size:10px;
  color: var(--text-secondary);
}
.app-log-card-badge {
  padding:4px 8px;
  border-radius:999px;
  background: var(--surface-soft);
  color: var(--text-primary);
  font-size:10px;
  font-weight:800;
  white-space:nowrap;
}
.app-log-card-summary {
  margin-top:6px;
  font-size:12px;
  line-height:1.45;
  color: var(--text-primary);
}
.app-log-card-details {
  margin-top:6px;
  display:grid;
  gap:4px;
}
.app-log-card-detail {
  font-size:11px;
  line-height:1.4;
  color: var(--text-secondary);
}
.token-results {
  display: grid;
  gap: 8px;
  margin-top: 10px;
}
.token-result {
  padding: 10px 12px;
  border-radius: 14px;
  border: 1px solid var(--section-border);
  box-shadow: var(--soft-shadow);
  font-size: 12px;
}
.token-result.valid { background: var(--status-success-bg); color: var(--status-success-text); }
.token-result.invalid { background: var(--status-error-bg); color: var(--status-error-text); }
.user-group-chip {
  background: var(--surface-soft);
  color: var(--text-primary);
  border: 1px solid var(--section-border);
  padding: 5px 10px;
  margin: 2px;
  border-radius: 999px;
  cursor: pointer;
  font-size: 11px;
}
.user-group-chip.active {
  background: linear-gradient(135deg, #0ea5e9 0%, #2563eb 100%);
  color: white;
  border-color: transparent;
}
.user-filter-input {
  width: auto !important;
  flex: 0 1 220px;
  min-width: 170px;
  max-width: 240px;
}
.user-panel-divider {
  margin-top: 10px;
  padding-top: 10px;
  border-top: 1px solid var(--section-border);
}
.user-muted-text {
  color: var(--text-secondary);
  font-size: 12px;
}
.test-user-item {
  padding: 8px 12px;
  cursor: pointer;
  border-bottom: 1px solid var(--section-border);
  font-size: 13px;
  color: var(--text-primary);
}
.test-user-item:hover {
  background: var(--table-row-hover);
}
.test-user-item.active {
  background: var(--accent-soft);
  font-weight: 700;
}
.test-user-meta {
  color: var(--text-secondary);
  font-size: 11px;
}

#keyboardModal {
  backdrop-filter: blur(10px);
}
#keyboardModalContent {
  background: var(--modal-bg) !important;
  color: var(--text-primary) !important;
  border: 1px solid var(--section-border) !important;
  box-shadow: var(--container-shadow) !important;
}
#keyboardModalContent h3,
#keyboardModalContent span {
  color: var(--text-primary) !important;
}
#keyboardModalContent > div:first-child {
  border-bottom: 1px solid var(--section-border) !important;
}
#keyboardModal label {
  background: var(--surface-muted) !important;
  border: 1px solid var(--section-border) !important;
  box-shadow: none !important;
}
#kbCloseBtn {
  color: var(--text-secondary) !important;
}
#kbLimits {
  background: var(--info-box-bg) !important;
  border-left: 4px solid var(--info-box-border) !important;
  color: var(--text-primary) !important;
}
#kbGrid {
  background: var(--surface-muted) !important;
  border: 1px solid var(--section-border) !important;
}
#kbClearBtn { background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%) !important; }
#kbCancelBtn { background: linear-gradient(135deg, #64748b 0%, #475569 100%) !important; }
#kbSaveBtn { background: linear-gradient(135deg, #16a34a 0%, #22c55e 100%) !important; }
.kb-cell input[type="text"], .kb-cell select, .kb-cell .kb-link-input {
  border: 1px solid var(--border-color) !important;
  border-radius: 10px !important;
  background: var(--bg-input) !important;
  color: var(--text-input) !important;
}
.kb-placeholder {
  border: 2px dashed var(--border-color) !important;
  border-radius: 12px !important;
  color: var(--text-secondary) !important;
  background: var(--surface-soft) !important;
}
.kb-placeholder:hover {
  border-color: var(--accent-color) !important;
  color: var(--accent-color) !important;
  background: var(--accent-soft) !important;
}
.kb-btn {
  background: var(--surface-muted) !important;
  border: 1px solid var(--section-border) !important;
  border-radius: 12px !important;
}

::-webkit-scrollbar { width: 12px; height: 12px; }
::-webkit-scrollbar-track { background: var(--scroll-track); border-radius: 999px; }
::-webkit-scrollbar-thumb { background: var(--scroll-thumb); border-radius: 999px; border: 2px solid transparent; background-clip: padding-box; }

@media (max-width: 900px) {
  body { padding: 12px; }
  .container { padding: 16px; border-radius: 18px; }
  .page-header { flex-direction: column; align-items: stretch; padding: 16px; }
  .page-title-row { flex-wrap: wrap; }
  .page-title-tools { width: 100%; margin-left: 0; }
  .theme-switcher { justify-content: space-between; border-radius: 18px; }
  .theme-btn { flex: 1 1 180px; justify-content: center; }
  h1 { font-size: 24px; }
  .tab { border-radius: 16px; }
  .tab-panel-header { flex-direction: column; align-items: stretch; border-radius: 18px; }
  .tab-panel-title { font-size: 21px; }
  .tab-panel-badge { white-space: normal; }
}
</style>
</head>



<body>


<!-- ? ИСПРАВЛЕНО: tooltip перемещён НАЧАЛО body для доступности -->
<div id="tooltip" class="tooltip"></div>

<!-- 🖼️ Модальное окно для изображений инструкций -->
<div id="imageModal" class="image-modal" onclick="closeImageModal()">
    <div class="image-modal-content" onclick="event.stopPropagation()">
        <button class="image-modal-close" onclick="closeImageModal()">✕</button>
        <img id="modalImage" src="" alt="Инструкция">
    </div>
</div>

<!-- ? Добавить debugLog -->
<div id="debugLog" class="debug" style="display:none;"></div>



<!-- &#x1F6E1;&#xFE0F; ПОЛНОЭКРАННОЕ УВЕДОМЛЕНИЕ СОХРАНЕНИЯ -->
<div id="saveOverlay">
  <div class="save-text">СОХРАНЕНИЕ<span class="save-dots" id="saveDots">.....</span></div>
</div>

<!-- &#x2328;&#xFE0F; МОДАЛЬНОЕ ОКНО КОНСТРУКТОРА КЛАВИАТУРЫ -->
<div id="keyboardModal" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.6);z-index:10000;justify-content:center;align-items:center;">
  <div style="background:#fff;color:#333;max-width:720px;width:95%;max-height:90vh;overflow:auto;border-radius:12px;padding:20px;position:relative;box-shadow:0 8px 32px rgba(0,0,0,0.3);" id="keyboardModalContent">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:15px;border-bottom:2px solid #e0e0e0;padding-bottom:10px;">
      <h3 style="margin:0;color:#333;">&#x2328;&#xFE0F; Конструктор клавиатуры</h3>
      <button id="kbCloseBtn" style="background:none;border:none;font-size:28px;cursor:pointer;color:#666;line-height:1;padding:0 4px;">&times;</button>
    </div>

    <!-- Переключатель типа клавиатуры -->
    <div style="display:flex;gap:15px;margin-bottom:15px;align-items:center;">
      <span style="font-size:14px;font-weight:bold;color:#555;">Тип:</span>
      <label style="display:flex;align-items:center;gap:5px;cursor:pointer;padding:5px 10px;border-radius:6px;background:#e3f2fd;">
        <input type="radio" name="kbType" value="inline" checked>
        <span style="font-size:13px;font-weight:500;">В сообщении</span>
      </label>
      <label style="display:flex;align-items:center;gap:5px;cursor:pointer;padding:5px 10px;border-radius:6px;background:#f3e5f5;">
        <input type="radio" name="kbType" value="default">
        <span style="font-size:13px;font-weight:500;">Под сообщением</span>
      </label>
    </div>

    <!-- Информация о лимитах -->
    <div id="kbLimits" style="font-size:15px;margin-bottom:12px;padding:10px 14px;background:#f5f5f5;border-radius:8px;border-left:4px solid #2196F3;">
    </div>

    <!-- Сетка конструктора -->
    <div id="kbGrid" style="display:grid;gap:5px;margin-bottom:15px;padding:10px;background:#fafafa;border-radius:8px;border:1px solid #e0e0e0;"></div>

    <!-- Кнопки управления -->
    <div style="display:flex;gap:10px;justify-content:flex-end;padding-top:10px;border-top:1px solid #e0e0e0;">
      <button id="kbClearBtn" style="background:#f44336;color:white;border:none;padding:10px 18px;border-radius:8px;cursor:pointer;font-size:14px;font-weight:bold;">&#x1F5D1;&#xFE0F; Очистить</button>
      <button id="kbCancelBtn" style="background:#757575;color:white;border:none;padding:10px 18px;border-radius:8px;cursor:pointer;font-size:14px;font-weight:bold;">Отмена</button>
      <button id="kbSaveBtn" style="background:#4CAF50;color:white;border:none;padding:10px 18px;border-radius:8px;cursor:pointer;font-size:14px;font-weight:bold;">&#x1F4BE; Сохранить</button>
    </div>
  </div>
</div>

<style>
.kb-cell { display:flex;flex-direction:column;gap:2px; }
.kb-cell input[type="text"] { width:100%;padding:5px 6px;border:1px solid #ccc;border-radius:4px;font-size:12px;background:#fff;color:#333;box-sizing:border-box; }
.kb-cell select { width:100%;padding:4px;font-size:11px;border:1px solid #ccc;border-radius:4px;background:#fff;color:#333; }
.kb-cell .kb-link-input { width:100%;padding:5px 6px;border:1px solid #ccc;border-radius:4px;font-size:11px;background:#fff;color:#333;display:none;box-sizing:border-box; }
.kb-cell .kb-link-input.show { display:block; }
.kb-placeholder { width:100%;height:65px;border:2px dashed #bbb;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:14px;color:#999;cursor:pointer;background:#f9f9f9;transition:all 0.2s; }
.kb-placeholder:hover { border-color:#2196F3;color:#2196F3;background:#e3f2fd; }
.kb-btn { position:relative;background:#e8f5e9;border:1px solid #4CAF50;border-radius:6px;padding:4px; }
.kb-btn .kb-del { position:absolute;top:-6px;right:-6px;width:20px;height:20px;border-radius:50%;background:#f44336;color:white;border:2px solid #fff;font-size:12px;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1;box-shadow:0 1px 3px rgba(0,0,0,0.3); }
</style>


<div class="container">
<div class="page-header">
<div class="page-title">
<div class="page-eyebrow">VK Bot Control Center</div>
<div class="page-title-row"><h1>Админ-панель PAPA BOT &#x1F916;</h1><button class="capabilities-btn" type="button" onclick="openCapabilitiesModal()">&#x1F449; Что умеет</button></div>
<div class="page-subtitle">Чистый интерфейс для управления сценариями, пользователями, рассылками и настройками VK-сообществ без визуального шума.</div>
</div>
<div class="page-header-side">
<div class="version-chip-row">
<button id="botVersionChip" class="version-chip" type="button" onclick="openBotVersionModal()">version ...</button>
<button class="version-info-btn" type="button" title="Расшифровка версии" onclick="openBotVersionModal()">i</button>
</div>
<div id="headerThemeHost" class="page-title-tools"><div id="globalThemeSwitcher" class="theme-switcher"><button class="theme-btn" data-theme="light" onclick="setTheme('light')" title="Светлая" type="button"><span class="theme-icon">&#x2600;&#xFE0F;</span><span class="theme-name">Светлая</span></button><button class="theme-btn" data-theme="dark" onclick="setTheme('dark')" title="Тёмная" type="button"><span class="theme-icon">&#x1F319;</span><span class="theme-name">Тёмная</span></button></div></div>
</div>
</div>
<div id="themeDockSlot" class="theme-dock-slot"></div>
<div id="botVersionModalOverlay" class="version-modal-overlay" style="display:none;" onclick="closeBotVersionModal(event)">
<div class="version-modal" onclick="event.stopPropagation()">
<div class="version-modal-header">
<div>
<div class="version-modal-title">Обновления PAPA BOT</div>
<div id="botVersionModalSubtitle" class="version-modal-subtitle">Обновления PAPA BOT загрузятся...</div>
</div>
<button class="btn btn-neutral version-close-btn" type="button" onclick="closeBotVersionModal()">Закрыть</button>
</div>
<div id="botVersionSummary" class="version-summary-card">Загрузка...</div>
<div id="botVersionParts" class="version-parts-grid"></div>
</div>
</div>
<div id="capabilitiesModalOverlay" class="version-modal-overlay" style="display:none;" onclick="closeCapabilitiesModal(event)">
<div class="version-modal" onclick="event.stopPropagation()">
<div class="version-modal-header">
<div>
<div class="version-modal-title">Что умеет</div>
<div id="capabilitiesModalSubtitle" class="version-modal-subtitle">Список возможностей загрузится...</div>
</div>
<button class="btn btn-neutral version-close-btn" type="button" onclick="closeCapabilitiesModal()">Закрыть</button>
</div>
<div id="capabilitiesSummary" class="version-summary-card">Загрузка...</div>
<div id="capabilitiesGrid" class="capabilities-grid"></div>
</div>
</div>
<!-- ?? ВРЕМЕННО ОТКЛЮЧЕНО: <button class="btn btn-refresh" ...> -->
<div class="info-box">&#x2139;&#xFE0F; Изменения применяются после нажатия "&#x1F4BE; Сохранить", поэтому перед переключением разделов лучше фиксировать правки сразу.</div>
<div id="status" class="status"></div>
<div class="tab">
<button class="tablinks tablinks--messages active" onclick="openTab(event,'Messages')">&#x1F4AC; СООБЩЕНИЯ</button>
<button class="tablinks" onclick="openTab(event,'Comments')">&#x1F4DD; КОММЕНТАРИИ В ПОСТАХ</button>
<button class="tablinks" onclick="openTab(event,'Users')">&#x1F464; ПОЛЬЗОВАТЕЛИ</button>
<button class="tablinks" onclick="openTab(event,'Groups')">&#x1F465; ГРУППЫ</button>
<button class="tablinks" onclick="openTab(event,'Variables')">&#x1F9EE; ПЕРЕМЕННЫЕ</button>
<button class="tablinks" onclick="openTab(event,'Mailing')">&#x1F4E8; РАССЫЛКА</button>
<button class="tablinks" onclick="openTab(event,'Delayed')">&#x23F3; ОТЛОЖЕННЫЕ</button>
<button class="tablinks" onclick="openTab(event,'Triggers')">&#x1F3AF; ТРИГГЕРЫ</button>
<button class="tablinks" onclick="openTab(event,'Profile')">&#x1F4C7; ПРОФИЛЬ</button>
<button class="tablinks" onclick="openTab(event,'Settings')">&#x2699;&#xFE0F; НАСТРОЙКА</button>
<button class="tablinks" id="adminTabButton" onclick="openTab(event,'Admin')" style="display:none;">&#x1F6E1;&#xFE0F; АДМИН</button>
</div>



<!-- Блок СООБЩЕНИЯ -->
<div id="Messages" class="tabcontent">
<div class="tab-panel-header tab-panel-header--messages"><div class="tab-panel-copy"><div class="tab-panel-kicker">Сценарии диалогов</div><h2 class="tab-panel-title">Сообщения и шаги бота</h2><p class="tab-panel-description">Настраивайте триггеры, ответы, действия, переменные, клавиатуры и вложения для каждого шага. Здесь собирается основной сценарий общения с пользователем.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Триггеры • Ответы • Действия</div></div></div>
<!-- <h3>&#x1F4AC; СООБЩЕНИЯ</h3> -->
<div id="loading-Messages">Загрузка...</div>
<div id="activeCommunityLabel-Messages" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>
<!-- Панель переключения ботов -->
<div id="botSwitcher-Messages" style="background:#f3e5f5;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;">
    <div id="botButtons-Messages" style="display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin-top:6px;"></div>
    <button class="btn btn-add" onclick="showAddBotModal('Messages')" style="margin-left:8px;background:#9C27B0;font-size:11px;padding:4px 10px;">+ Добавить Бот</button>
</div>
<div class="scroll">
<table id="table-Messages"><tbody></tbody></table>
</div>
<div style="margin-top:15px;">
<div id="status-Messages" class="status" style="margin:10px 0;"></div>
<button class="btn btn-add" onclick="addStep('Messages')" style="display:block; margin-bottom:10px;">+ Добавить Шаг</button>
<button class="btn btn-save" onclick="saveData(this, 'Messages')" style="display:block;">&#x1F4BE; Сохранить</button>
</div>
</div>

<!-- Блок КОММЕНТАРИИ В ПОСТАХ -->
<div id="Comments" class="tabcontent">
<div class="tab-panel-header tab-panel-header--comments"><div class="tab-panel-copy"><div class="tab-panel-kicker">Реакции на посты</div><h2 class="tab-panel-title">Комментарии и ответы в постах</h2><p class="tab-panel-description">Управляйте реакцией бота на комментарии под постами: фильтруйте по посту, упоминаниям, группам и задавайте нужный ответ или действие.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Посты • Условия • Ответы</div></div></div>
<!-- <h3>&#x1F4DD; КОММЕНТАРИИ В ПОСТАХ</h3> -->
<div id="loading-Comments">Загрузка...</div>
<div id="activeCommunityLabel-Comments" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>
<!-- Панель переключения ботов -->
<div id="botSwitcher-Comments" style="background:#f3e5f5;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;">
    <div id="botButtons-Comments" style="display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin-top:6px;"></div>
    <button class="btn btn-add" onclick="showAddBotModal('Comments')" style="margin-left:8px;background:#9C27B0;font-size:11px;padding:4px 10px;">+ Добавить Бот</button>
</div>
<div class="scroll">
<table id="table-Comments"><tbody></tbody></table>
</div>
<div style="margin-top:15px;">
<div id="status-Comments" class="status" style="margin:10px 0;"></div>
<button class="btn btn-add" onclick="addStep('Comments')" style="display:block; margin-bottom:10px;">+ Добавить Шаг</button>
<button class="btn btn-save" onclick="saveData(this, 'Comments')" style="display:block;">&#x1F4BE; Сохранить</button>
</div>
</div>

<!-- Блок ПОЛЬЗОВАТЕЛИ -->
<div id="Users" class="tabcontent">
    <div class="tab-panel-header tab-panel-header--users"><div class="tab-panel-copy"><div class="tab-panel-kicker">Сегменты и данные</div><h2 class="tab-panel-title">Пользователи и их состояние</h2><p class="tab-panel-description">Просматривайте базу пользователей, группы, текущие шаги, персональные переменные и выполняйте массовые действия по фильтрам.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Поиск • Фильтры • Массовые действия</div></div></div>
    <!-- <h3>&#x1F465; ПОЛЬЗОВАТЕЛИ</h3> -->
    <div id="loading-Users">Загрузка...</div>
<div id="activeCommunityLabel-Users" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>
    <div class="scroll">
        <table id="table-Users"><tbody></tbody></table>
    </div>
<div style="margin-top:15px;">
<div id="status-Users" class="status" style="margin:10px 0;"></div>
<button class="btn btn-add" onclick="addRow('Users')" style="display:block; margin-bottom:10px;">+ Добавить строку</button>
<button class="btn btn-save" onclick="saveData(this, 'Users')" style="display:block;">&#x1F4BE; Сохранить</button>
</div>
</div>

<!-- Блок ГРУППЫ -->
<div id="Groups" class="tabcontent">
    <div class="tab-panel-header tab-panel-header--users"><div class="tab-panel-copy"><div class="tab-panel-kicker">Сегменты и история</div><h2 class="tab-panel-title">Группы пользователей</h2><p class="tab-panel-description">Создавайте группы, описывайте их назначение, просматривайте состав и управляйте участниками из одного раздела.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Группы • Участники • История</div></div></div>
    <div id="loading-Groups">Загрузка...</div>
    <div id="activeCommunityLabel-Groups" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>
    <div id="groupsManager" class="settings-surface profile-manager">
        <div class="community-empty-note">Раздел групп загрузится после открытия вкладки.</div>
    </div>
    <div class="scroll" style="display:none;">
        <table id="table-Groups"><tbody></tbody></table>
    </div>
    <div style="margin-top:15px;">
        <div id="status-Groups" class="status" style="margin:10px 0;"></div>
    </div>
</div>

<!-- Блок ПЕРЕМЕННЫЕ -->
<div id="Variables" class="tabcontent">
    <div class="tab-panel-header tab-panel-header--variables"><div class="tab-panel-copy"><div class="tab-panel-kicker">Данные и логика</div><h2 class="tab-panel-title">Переменные и системные значения</h2><p class="tab-panel-description">Храните глобальные значения, пользовательские переменные, переменные всех сообществ профиля и справочник встроенных переменных VK для подстановок и сценариев.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Глобальные • Пользовательские • VK</div></div></div>
    <!-- <h3>&#x1F9EE; ПЕРЕМЕННЫЕ</h3> -->
    <div id="loading-Variables">Загрузка...</div>
<div id="activeCommunityLabel-Variables" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>
    <h4 style="margin:10px 0 5px;color:var(--text-primary);">&#x1F464; Пользовательские переменные</h4>
    <div class="scroll">
        <table id="table-Variables_User"><tbody></tbody></table>
    </div>
<div style="margin-top:15px;">
<div id="status-Variables_User" class="status" style="margin:10px 0;"></div>
<button class="btn btn-add" onclick="addRow('Variables_User')" style="display:block; margin-bottom:10px;">+ Добавить пользовательскую</button>
<button class="btn btn-save" onclick="saveData(this, 'Variables_User')" style="display:block;">&#x1F4BE; Сохранить пользовательские</button>
</div>
    <h4 style="margin:15px 0 5px;color:var(--text-primary);">&#x1F310; Глобальные переменные ГП</h4>
    <div class="scroll">
        <table id="table-Variables"><tbody></tbody></table>
    </div>
<div style="margin-top:15px;">
<div id="status-Variables" class="status" style="margin:10px 0;"></div>
<button class="btn btn-add" onclick="addRow('Variables')" style="display:block; margin-bottom:10px;">+ Добавить ГП</button>
<button class="btn btn-save" onclick="saveData(this, 'Variables')" style="display:block;">&#x1F4BE; Сохранить ГП</button>
</div>
    <h4 style="margin:15px 0 5px;color:var(--text-primary);">&#x1F310; Переменные всех сообществ ПВС</h4>
    <div class="scroll">
        <table id="table-Shared_Variables"><tbody></tbody></table>
    </div>
<div style="margin-top:15px;">
<div id="status-Shared_Variables" class="status" style="margin:10px 0;"></div>
<button class="btn btn-add" onclick="addRow('Shared_Variables')" style="display:block; margin-bottom:10px;">+ Добавить ПВС</button>
<button class="btn btn-save" onclick="saveData(this, 'Shared_Variables')" style="display:block;">&#x1F4BE; Сохранить ПВС</button>
</div>
    <h4 style="margin:15px 0 5px;color:var(--text-primary);">&#x1F517; Переменные ВК</h4>
    <div class="info-box">Этот блок информационный. Здесь можно только смотреть доступные переменные ВК и их описание.</div>
    <div class="scroll">
        <table id="table-VK_Variables"><tbody></tbody></table>
    </div>
</div>

<!-- Блок РАССЫЛКА -->
<div id="Mailing" class="tabcontent">
    <div class="tab-panel-header tab-panel-header--mailing"><div class="tab-panel-copy"><div class="tab-panel-kicker">Массовые отправки</div><h2 class="tab-panel-title">Рассылки по базе и группам</h2><p class="tab-panel-description">Готовьте массовые сообщения, задавайте получателей, время отправки, вложения и отслеживайте статус выполнения каждой рассылки.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Получатели • Контент • Статус</div></div></div>
    <!-- <h3>&#x1F4E8; РАССЫЛКА</h3> -->
    <div id="loading-Mailing">Загрузка...</div>
<div id="activeCommunityLabel-Mailing" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>
    <div class="scroll">
        <table id="table-Mailing"><tbody></tbody></table>
    </div>
<div style="margin-top:15px;">
<div id="status-Mailing" class="status" style="margin:10px 0;"></div>
<button class="btn btn-add" onclick="addRow('Mailing')" style="display:block; margin-bottom:10px;">+ Добавить строку</button>
<button class="btn btn-save" onclick="saveData(this, 'Mailing')" style="display:block;">&#x1F4BE; Сохранить</button>
</div>
</div>



<!-- Блок ОТЛОЖЕННЫЕ -->
<div id="Delayed" class="tabcontent">
    <div class="tab-panel-header tab-panel-header--delayed"><div class="tab-panel-copy"><div class="tab-panel-kicker">Очередь задач</div><h2 class="tab-panel-title">Отложенные сообщения и проверки</h2><p class="tab-panel-description">Контролируйте сообщения, запланированные на потом: кому и когда они уйдут, какой у них статус и какие ошибки возникли.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">План • Статус • История</div></div></div>
    <!-- <h3>&#x23F3; ОТЛОЖЕННЫЕ</h3> -->
    <div id="loading-Delayed">Загрузка...</div>
<div id="activeCommunityLabel-Delayed" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>
    <div class="scroll">
        <table id="table-Delayed"><tbody></tbody></table>
    </div>
<div style="margin-top:15px;">
<div id="status-Delayed" class="status" style="margin:10px 0;"></div>
<button class="btn btn-add" onclick="addRow('Delayed')" style="display:block; margin-bottom:10px;">+ Добавить строку</button>
<button class="btn btn-save" onclick="saveData(this, 'Delayed')" style="display:block;">&#x1F4BE; Сохранить</button>
</div>
</div>


<!-- Блок ТРИГГЕРЫ -->
<div id="Triggers" class="tabcontent">
    <div class="tab-panel-header tab-panel-header--messages"><div class="tab-panel-copy"><div class="tab-panel-kicker">Senler-like сценарии</div><h2 class="tab-panel-title">Триггеры событий</h2><p class="tab-panel-description">Собирайте триггер как отдельную карточку: выберите тип события, точное условие, действие и сразу управляйте его активностью.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Тип события • Условие • Действие</div></div></div>
    <div id="loading-Triggers">Загрузка...</div>
    <div id="activeCommunityLabel-Triggers" style="background:#e8f5e9;padding:6px 12px;margin:8px 0;border-radius:4px;font-size:12px;display:none;"></div>
    <div class="info-box">Триггер создаётся через скрываемый конструктор. Новая карточка после сохранения создаётся выключенной, а активировать её нужно вручную переключателем на карточке.</div>
    <div class="structured-trigger-toolbar">
        <div class="structured-trigger-toolbar-main">
            <button class="btn btn-add" onclick="openNewStructuredTriggerForm()">+ Новый триггер</button>
            <div class="structured-trigger-toolbar-filters">
                <input id="structuredTriggerSearch" class="structured-trigger-filter-input" type="text" placeholder="Поиск по названию триггера" oninput="handleStructuredTriggerFilterChange()">
                <select id="structuredTriggerEventFilter" class="structured-trigger-filter-select" onchange="handleStructuredTriggerFilterChange()"></select>
            </div>
        </div>
        <div id="structuredTriggersStatus"></div>
    </div>
    <div class="structured-trigger-shell">
        <div id="structuredTriggersBuilder" class="structured-trigger-builder"></div>
        <div id="structuredTriggersCards" class="structured-trigger-cards-grid"></div>
    </div>
    <div class="scroll" style="display:none;">
        <table id="table-Triggers"><tbody></tbody></table>
    </div>
    <div style="margin-top:15px;">
        <div id="status-Triggers" class="status" style="margin:10px 0;"></div>
    </div>
</div>



<!-- Блок ПРОФИЛЬ -->
<div id="Profile" class="tabcontent">
<div class="tab-panel-header tab-panel-header--settings"><div class="tab-panel-copy"><div class="tab-panel-kicker">Профиль и лимиты</div><h2 class="tab-panel-title">Профиль, сообщества и лимиты PAPA BOT</h2><p class="tab-panel-description">Сводка по профилю: активные сообщества, статистика по каждому сообществу, дневной лимит запросов и история начислений.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Профиль • Статистика • Лимиты</div></div></div>
<div id="loading-Profile">Загрузка...</div>
<div id="profileDashboardStatus"></div>
<div id="profileDashboardContent" class="settings-surface profile-manager">
    <div class="community-empty-note">Сводка профиля загрузится после открытия вкладки.</div>
</div>
</div>

<!-- ===== ВКЛАДКА НАСТРОЙКА (мульти-сообщества) ===== -->
<div id="Admin" class="tabcontent" style="display:none;">
<div class="tab-panel-header tab-panel-header--settings"><div class="tab-panel-copy"><div class="tab-panel-kicker">Управление доступом</div><h2 class="tab-panel-title">Администрирование профилей и доступа</h2><p class="tab-panel-description">Главный админ создаёт профили, задаёт срок действия, выпускает промокоды для регистрации, обрабатывает восстановление доступа и просматривает логи входов.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Профили • Промокоды • Восстановление</div></div></div>
<div id="loading-Admin">Загрузка...</div>

<div id="legacyAdminAuditSurface" class="settings-surface profile-manager">
    <div class="profile-manager-header">
        <div>
            <h3 class="profile-manager-title">Профили администраторов</h3>
            <div class="profile-manager-subtitle">Только главный админ может создавать и редактировать профили, задавать срок их действия и открывать любой профиль для работы.</div>
        </div>
        <div id="currentProfileChipAdmin" class="profile-current-chip">Активный профиль</div>
    </div>
    <div id="adminProfilesStatus"></div>
    <div class="settings-surface" style="margin-bottom:14px;">
        <div class="profile-manager-header" style="cursor:pointer;" onclick="toggleAdminProfileFilters()">
            <div id="adminProfileFiltersToggle" style="font-size:16px;">▼ 🔍 Фильтры Профилей</div>
        </div>
        <div id="adminProfileFiltersBlock" class="profile-form">
            <div class="profile-form-grid">
                <div>
                    <label><strong>Общий поиск</strong></label>
                    <input type="text" id="adminProfileFilterSearch" placeholder="Название / логин / почта / ID / промокод" oninput="renderAdminProfiles()">
                </div>
                <div>
                    <label><strong>ID профиля</strong></label>
                    <input type="text" id="adminProfileFilterId" placeholder="Например: 2" oninput="renderAdminProfiles()">
                </div>
                <div>
                    <label><strong>Название</strong></label>
                    <input type="text" id="adminProfileFilterName" placeholder="Например: Отдел продаж" oninput="renderAdminProfiles()">
                </div>
                <div>
                    <label><strong>Логин</strong></label>
                    <input type="text" id="adminProfileFilterUsername" placeholder="Например: sales_admin" oninput="renderAdminProfiles()">
                </div>
                <div>
                    <label><strong>Почта</strong></label>
                    <input type="text" id="adminProfileFilterEmail" placeholder="Например: admin@example.com" oninput="renderAdminProfiles()">
                </div>
                <div>
                    <label><strong>Промокод</strong></label>
                    <input type="text" id="adminProfileFilterPromo" placeholder="Например: PAPA-2026-001" oninput="renderAdminProfiles()">
                </div>
                <div>
                    <label><strong>Роль</strong></label>
                    <select id="adminProfileFilterRole" onchange="renderAdminProfiles()">
                        <option value="">Все роли</option>
                        <option value="main_admin">Главный админ</option>
                        <option value="admin">Профиль</option>
                    </select>
                </div>
                <div>
                    <label><strong>Статус</strong></label>
                    <select id="adminProfileFilterActive" onchange="renderAdminProfiles()">
                        <option value="">Все статусы</option>
                        <option value="active">Активные</option>
                        <option value="inactive">Отключённые</option>
                    </select>
                </div>
                <div>
                    <label><strong>Срок действия</strong></label>
                    <select id="adminProfileFilterExpiry" onchange="renderAdminProfiles()">
                        <option value="">Все варианты</option>
                        <option value="active">Не истёк</option>
                        <option value="expired">Истёк</option>
                        <option value="infinite">Бессрочный</option>
                    </select>
                </div>
                <div>
                    <label><strong>Осталось минут от</strong></label>
                    <input type="number" id="adminProfileFilterDurationMin" min="0" placeholder="Например: 60" oninput="renderAdminProfiles()">
                </div>
                <div>
                    <label><strong>Осталось минут до</strong></label>
                    <input type="number" id="adminProfileFilterDurationMax" min="0" placeholder="Например: 1440" oninput="renderAdminProfiles()">
                </div>
                <div>
                    <label><strong>Лимит от</strong></label>
                    <input type="number" id="adminProfileFilterLimitMin" min="0" placeholder="Например: 1000" oninput="renderAdminProfiles()">
                </div>
                <div>
                    <label><strong>Лимит до</strong></label>
                    <input type="number" id="adminProfileFilterLimitMax" min="0" placeholder="Например: 50000" oninput="renderAdminProfiles()">
                </div>
            </div>
            <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px;">
                <button class="btn btn-neutral" type="button" onclick="resetAdminProfileFilters()">Сбросить фильтры</button>
            </div>
        </div>
    </div>
    <div style="margin-bottom:14px;">
        <button id="toggleCreateProfileBtn" class="btn btn-accent" type="button" onclick="toggleCreateProfileForm()">+ Создать Профиль</button>
    </div>
    <div id="adminProfileForm" class="profile-form" style="display:none;">
        <div class="profile-form-title" style="font-size:18px;font-weight:700;margin-bottom:12px;">Создание профиля</div>
        <input type="hidden" id="profileFormId">
        <div class="profile-form-grid">
            <div>
                <label><strong>Название профиля</strong></label>
                <input type="text" id="profileFormName" placeholder="Например: Отдел продаж">
            </div>
            <div>
                <label><strong>Логин</strong></label>
                <input type="text" id="profileFormUsername" placeholder="Например: sales_admin">
            </div>
            <div>
                <label><strong>Пароль</strong></label>
                <input type="text" id="profileFormPassword" placeholder="Новый пароль или временный пароль">
            </div>
            <div>
                <label><strong>Email для восстановления</strong></label>
                <input type="email" id="profileFormEmail" placeholder="Например: admin@example.com">
            </div>
            <div>
                <label><strong>Срок действия в минутах</strong></label>
                <input type="number" id="profileFormDuration" min="1" placeholder="Пусто = бесконечно">
            </div>
            <div>
                <label><strong>Лимит запросов в сутки</strong></label>
                <input type="number" id="profileFormRequestsLimit" min="1" placeholder="Например: 1000">
            </div>
        </div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;">
            <button class="btn btn-save" type="button" onclick="saveAdminProfile()">💾 Сохранить профиль</button>
            <button class="btn btn-neutral" type="button" onclick="closeAdminProfileForm()">Отмена</button>
        </div>
        <div class="profile-form-note">Если поле срока действия пустое, профиль действует бесконечно. При сохранении существующего профиля новый срок считается от текущего момента.</div>
    </div>
    <div id="adminProfilesList" class="profile-grid"></div>
</div>

<div class="settings-surface profile-manager">
    <div class="profile-manager-header">
        <div>
            <h3 class="profile-manager-title">Промокоды для регистрации</h3>
            <div class="profile-manager-subtitle">Пользователь сможет создать аккаунт только после ввода действующего промокода. После трёх неверных попыток ввод блокируется на 24 часа.</div>
        </div>
    </div>
    <div id="promoCodesStatus"></div>
    <div style="margin-bottom:14px;">
        <button id="togglePromoFormBtn" class="btn btn-accent" type="button" onclick="togglePromoForm()">+ Добавить Промокод</button>
    </div>
    <div id="promoFormBlock" class="profile-form" style="display:none;">
        <input type="hidden" id="promoFormId">
        <div class="profile-form-grid">
            <div>
                <label><strong>Промокод</strong></label>
                <div style="display:flex;gap:6px;align-items:center;">
                    <input type="text" id="promoFormCode" placeholder="Например: PAPA-2026-001" style="flex:1;">
                    <button class="btn btn-accent" type="button" onclick="generatePromoCode()" title="Сгенерировать промокод" style="padding:6px 10px;font-size:14px;">❇️</button>
                </div>
            </div>
            <div>
                <label><strong>Описание</strong></label>
                <input type="text" id="promoFormLabel" placeholder="Например: Доступ для тестера">
            </div>
            <div>
                <label><strong>Срок профиля в минутах</strong></label>
                <input type="number" id="promoFormDuration" min="1" placeholder="Пусто = бесконечно">
            </div>
            <div>
                <label><strong>Лимит запросов в сутки</strong></label>
                <input type="number" id="promoFormRequestsLimit" min="1" placeholder="Например: 1000">
            </div>
            <div>
                <label><strong>Количество активаций</strong></label>
                <input type="number" id="promoFormMaxUses" min="1" value="1">
            </div>
        </div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;">
            <button class="btn btn-save" type="button" onclick="savePromoCode()">🎟️ Сохранить промокод</button>
            <button class="btn btn-neutral" type="button" onclick="closePromoForm()">Отмена</button>
        </div>
    </div>
    <div class="settings-surface" style="margin-bottom:14px;">
        <div class="profile-manager-header" style="cursor:pointer;" onclick="togglePromoFilters()">
            <div id="promoFiltersToggle" style="font-size:16px;">▶ 🔍 Фильтры Промокодов</div>
        </div>
        <div id="promoFiltersBlock" class="profile-form" style="display:none;">
            <div class="profile-form-grid">
                <div>
                    <label><strong>Общий поиск</strong></label>
                    <input type="text" id="promoFilterSearch" placeholder="Код / описание" oninput="renderPromoCodes()">
                </div>
                <div>
                    <label><strong>Статус</strong></label>
                    <select id="promoFilterStatus" onchange="renderPromoCodes()">
                        <option value="">Все</option>
                        <option value="available">Доступен</option>
                        <option value="used">Использован</option>
                    </select>
                </div>
                <div>
                    <label><strong>Активаций от</strong></label>
                    <input type="number" id="promoFilterUsesMin" min="0" placeholder="Например: 0" oninput="renderPromoCodes()">
                </div>
                <div>
                    <label><strong>Активаций до</strong></label>
                    <input type="number" id="promoFilterUsesMax" min="0" placeholder="Например: 10" oninput="renderPromoCodes()">
                </div>
            </div>
            <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px;">
                <button class="btn btn-neutral" type="button" onclick="resetPromoFilters()">Сбросить фильтры</button>
            </div>
        </div>
    </div>
    <div id="promoCodesList" class="profile-grid" style="margin-top:14px;"></div>
</div>

<div class="settings-surface profile-manager">
    <div class="profile-manager-header">
        <div>
            <h3 class="profile-manager-title">Увеличение лимитов</h3>
            <div class="profile-manager-subtitle">Запросы профилей на увеличение суточного лимита PAPA BOT. Блок виден постоянно и доступен только главному админу.</div>
        </div>
    </div>
    <div id="adminLimitRequestsPanel" class="app-log-grid">
        <div class="community-empty-note">Запросы на увеличение лимита загрузятся после открытия вкладки.</div>
    </div>
</div>

<div class="settings-surface profile-manager">
    <div class="profile-manager-header">
        <div>
            <h3 class="profile-manager-title">Восстановление и логи</h3>
            <div class="profile-manager-subtitle">Здесь главный админ видит запросы на восстановление доступа и журнал успешных и неуспешных входов.</div>
        </div>
    </div>
    <div id="recoveryRequestsStatus"></div>
    <div id="recoveryRequestsList" class="profile-grid"></div>
    <div id="loginLogsList" class="debug" style="display:block;max-height:320px;">Логи загрузятся после открытия вкладки.</div>
</div>

<div class="settings-surface profile-manager">
    <div class="profile-manager-header">
        <div>
            <h3 class="profile-manager-title">Журнал работы бота</h3>
            <div class="profile-manager-subtitle">Понятные события по вкладкам: что пришло, какой шаг сработал, что отправилось и какие действия выполнились.</div>
        </div>
    </div>
    <div class="app-log-toolbar">
        <div id="appLogFilterRow" class="app-log-filter-row"></div>
        <div class="app-log-actions">
            <label class="structured-trigger-toggle"><input id="appLogsEnabledToggle" type="checkbox" onchange="toggleAppLogsEnabled(this.checked)">Логирование включено</label>
            <button class="btn btn-neutral tab-refresh-btn" type="button" onclick="loadAppLogs(true)">↻ Обновить журнал</button>
            <button class="btn btn-neutral tab-refresh-btn" type="button" onclick="clearAppLogsFromAdmin()">Очистить логи</button>
            <button class="btn btn-delete tab-refresh-btn" type="button" onclick="deleteAppLogsFileFromAdmin()">Удалить файл логов</button>
        </div>
    </div>
    <div id="appLogsFileLabel" class="app-log-file-label"></div>
    <div id="appLogsStatus"></div>
    <div id="appLogsList" class="app-log-grid">
        <div class="community-empty-note">Журнал загрузится после открытия вкладки.</div>
    </div>
</div>

<div class="settings-surface profile-manager">
    <div class="profile-manager-header">
        <div>
            <h3 class="profile-manager-title">Редактор версии PAPA BOT</h3>
            <div class="profile-manager-subtitle">Здесь можно редактировать историю блоков версии по факту выполненных изменений и сохранять её без ручного открытия файла.</div>
        </div>
    </div>
    <div id="botVersionEditorStatus"></div>
    <div class="version-editor-grid">
        <div class="version-editor-panel">
            <div class="profile-manager-subtitle" style="margin-bottom:10px;">Выбери блок версии</div>
            <div id="versionEditorPartsList" class="version-editor-list"></div>
        </div>
        <div class="version-editor-form">
            <div class="version-editor-fields">
                <div>
                    <label>Блок</label>
                    <input id="versionEditorLabel" type="text" readonly>
                </div>
                <div>
                    <label>Текущий номер блока</label>
                    <input id="versionEditorValue" type="text" placeholder="Например: 0002">
                </div>
                <div>
                    <label>Что сейчас реально работает в блоке</label>
                    <textarea id="versionEditorCurrentSummary" placeholder="Опиши текущее фактическое состояние блока"></textarea>
                </div>
                <div>
                    <label>Текст блока</label>
                    <textarea id="versionEditorDescription" placeholder="Коротко: что покрывает этот блок"></textarea>
                </div>
                <div>
                    <label>Новая или текущая запись истории: номер</label>
                    <input id="versionEditorHistoryVersion" type="text" placeholder="Например: 0002">
                </div>
                <div>
                    <label>Новая или текущая запись истории: описание</label>
                    <textarea id="versionEditorHistorySummary" placeholder="Что сделано, что работает, что исправлено в этой версии блока"></textarea>
                </div>
            </div>
            <div class="version-editor-actions">
                <button class="btn btn-save" type="button" onclick="saveVersionEditorChanges()">Сохранить версию</button>
                <button class="btn btn-neutral" type="button" onclick="appendVersionHistoryEntry()">Добавить запись в историю</button>
                <button class="btn btn-neutral" type="button" onclick="reloadVersionEditor()">Перезагрузить из базы</button>
            </div>
        </div>
    </div>
</div>
</div>

<div id="Settings" class="tabcontent">
<div class="tab-panel-header tab-panel-header--settings-danger"><div class="tab-panel-copy"><div class="tab-panel-kicker">Подключение и инфраструктура</div><h2 class="tab-panel-title">Настройка сообществ и VK API</h2><p class="tab-panel-description">Подключайте новые сообщества, проверяйте токены, настраивайте Callback API и храните служебные данные для работы бота в каждом сообществе отдельно.</p></div><div class="tab-panel-side"><div class="tab-panel-badge">Сообщества • Токены • Callback API</div></div></div>
<div id="loading-Settings">Загрузка...</div>
    <!-- <div class="info-box"> -->
        <!-- <strong>?? Управление сообществами</strong><br> -->
        <!-- • Каждое сообщество имеет независимые настройки.<br> -->
        <!-- • Переключайтесь между сообществами через кнопки сверху.<br> -->
        <!-- • Все данные сохраняются в bot_config.json в Object Storage. -->
    <!-- </div> -->

<!-- ===== Панель переключения сообществ с tooltip ===== -->
<div id="communitySwitcher" class="settings-surface settings-surface--community">
    <strong style="font-size: 15px;">&#x1F4CB; Активные сообщества:</strong>
    <span class="help-icon" style="display:inline-block;width:18px;height:18px;background:rgba(255,255,255,0.2);color:white;border-radius:50%;text-align:center;line-height:18px;font-size:12px;font-weight:bold;cursor:help;margin-left:5px;vertical-align:middle;" title="• Каждое сообщество имеет независимые настройки.&#10;• Переключайтесь между сообществами через кнопки сверху.&#10;• Все данные сохраняются в bot_config.json в Object Storage.">?</span>

    <div id="communityButtons" class="community-grid">
        <!-- Кнопки сообществ будут добавлены динамически -->
    </div>
    <button id="addCommunityBtn" class="btn btn-info" onclick="addNewCommunity()" style="margin-left: 10px;">
        + Добавить Сообщество
    </button>
</div>

<!-- ===== 📋 ИНСТРУКЦИЯ ПО НАСТРОЙКЕ ===== -->
<div class="settings-hero">
    <strong style="font-size: 16px;">🚀 Быстрый старт — 5 шагов для запуска бота</strong>
    <div style="margin-top: 12px; line-height: 1.8; font-size: 13px;">
        <button class="settings-hero-step" type="button" onclick="runQuickStartStep('community')" style="width:100%;text-align:left;cursor:pointer;background:rgba(255,255,255,0.6);border:1px solid var(--section-border);border-radius:12px;padding:10px 12px;"><strong>1️⃣ Добавьте сообщество</strong> — нажмите «+ Добавить Сообщество» или выберите существующее</button>
        <button class="settings-hero-step" type="button" onclick="runQuickStartStep('vkGroupId')" style="width:100%;text-align:left;cursor:pointer;background:rgba(255,255,255,0.6);border:1px solid var(--section-border);border-radius:12px;padding:10px 12px;"><strong>2️⃣ Введите VK Group ID</strong> — числовой ID вашего сообщества (без минуса, например: 219331507)</button>
        <button class="settings-hero-step" type="button" onclick="runQuickStartStep('vkTokens')" style="width:100%;text-align:left;cursor:pointer;background:rgba(255,255,255,0.6);border:1px solid var(--section-border);border-radius:12px;padding:10px 12px;"><strong>3️⃣ Введите VK Token</strong> — токен сообщества (выберите Все права при создании)</button>
        <button class="settings-hero-step" type="button" onclick="runQuickStartStep('userToken')" style="width:100%;text-align:left;cursor:pointer;background:rgba(255,255,255,0.6);border:1px solid var(--section-border);border-radius:12px;padding:10px 12px;"><strong>4️⃣ Введите User Token</strong> — нужен для отправки вложений (фото, документы, видео). Получите через кнопку «ПОЛУЧИТЬ ТОКЕН» внизу страницы</button>
        <button class="settings-hero-step" type="button" onclick="runQuickStartStep('callback')" style="width:100%;text-align:left;cursor:pointer;background:rgba(255,255,255,0.6);border:1px solid var(--section-border);border-radius:12px;padding:10px 12px;"><strong>5️⃣ Нажмите «Автонастройка сервера ВК»</strong> — бот сам подключится к вашему сообществу</button>
    </div>
    <div class="settings-hero-footnote">
        ✅ После этого перейдите на вкладку <strong>«СООБЩЕНИЯ»</strong> и настройте триггеры и ответы бота.
    </div>
</div>
<!-- ===== КОНЕЦ ИНСТРУКЦИИ ===== -->

    <!-- Форма настроек текущего сообщества -->
    <div id="communitySettingsForm">

        <label><strong>&#x1F3F7;&#xFE0F; Название сообщества (для отображения):</strong></label>
        <input type="text" id="communityName" placeholder="Например: Магазин одежды" style="margin: 10px 0; padding: 8px; width: 100%;">

        <!-- &#x1F512; Скрытые поля: Confirmation Code и Secret Key -->
        <div class="settings-muted-panel">
            <button class="btn btn-neutral" onclick="toggleSecretFields()" style="width:100%;">
                &#x1F512; Показать/скрыть служебные поля
            </button>
            <div id="secretFields" style="display:none; margin-top:10px;">
                <label><strong>&#x1F511; Код подтверждения от VK (Confirmation Code):</strong></label>
                <input type="text" id="confirmationCode" placeholder="Автоматически при автонастройке" style="margin: 5px 0; padding: 8px; width: 100%;">

                <label><strong>&#x1F5DD;&#xFE0F; Secret Key:</strong></label>
                <input type="text" id="secretKey" placeholder="Автоматически при автонастройке" style="margin: 5px 0 10px; padding: 8px; width: 100%;">
            </div>
        </div>

        <label><strong>&#x1F5DD;&#xFE0F; VK Tokens (массив, каждый с новой строки, макс. 7):</strong></label>
        <div class="settings-helper-box settings-helper-box--blue">
            &#x2753; <strong>Зачем:</strong> Токены(ключи) сообщества для отправки сообщений. Бот перебирает их по очереди при ошибках.<br>
            &#x1F4D6; <strong>Как получить:</strong><br>
            1. Откройте ваше Сообщество<br>
            2. Нажмите на &#x2192; Управление &#x2192; Дополнительно &#x2192; Работа с API<br>
            3. Нажмите на кнопку «Создать ключ»<br>
            4. Выберите <strong>Все</strong> параметры(разрешения) и нажмите «Создать»<br>
            5. Подтвердите это действие с помощью приложения ВК в телефоне<br>
            6. Появится строка Ключа — Скопируйте ее полностью<br>
            7. Вставьте этот Токен(ключ) ниже на строку<br>
            8. Максимум можно вставить 7 ключей, каждый ключ с новой строки<br>
            <a href="#" class="instruction-img-link settings-helper-link" onclick="return showInstructionImage('как создать VK TOKEN - 1 этап.png');">&#x1F5BC;&#xFE0F; 1 этап</a> |
            <a href="#" class="instruction-img-link settings-helper-link" onclick="return showInstructionImage('права VK TOKEN - 2 этап.png');">&#x1F5BC;&#xFE0F; 2 этап</a> |
            <a href="#" class="instruction-img-link settings-helper-link" onclick="return showInstructionImage('Подверждение создания VK TOKEN - 3 этап.png');">&#x1F5BC;&#xFE0F; 3 этап</a> |
            <a href="#" class="instruction-img-link settings-helper-link" onclick="return showInstructionImage('Копирование VK TOKEN - 4 этап.png');">&#x1F5BC;&#xFE0F; 4 этап</a>
        </div>
        <textarea id="vkTokens" placeholder="токен_1&#10;токен_2&#10;токен_3" style="margin: 10px 0; padding: 8px; width: 100%; min-height: 100px; font-family: monospace;"></textarea>
        <div class="settings-subnote">
            &#x1F504; Бот будет перебирать токены по порядку при ошибках.
        </div>
        <button class="btn btn-info" onclick="checkTokens()" style="margin-top: 10px;">
            &#x1F50D; Проверить все токены
        </button>
        <div id="tokensStatus" style="margin-top: 10px;"></div>

        <label><strong>&#x1F194; VK Group ID (числовой, без минуса):</strong></label>
        <div class="settings-helper-box settings-helper-box--blue">
            &#x2753; <strong>Зачем:</strong> ID вашего сообщества ВКонтакте. Нужен для отправки сообщений от имени сообщества.<br>
            &#x1F4D6; <strong>Где найти:</strong><br>
            1. Откройте ваше сообщество ВКонтакте<br>
            2. Посмотрите в адресную строку: <code class="settings-code">vk.com/public<strong>123456789</strong></code><br>
            3. Или: Управление → Настройки → ID группы<br>
            <em class="settings-subnote" style="display:block;">Вводите БЕЗ минуса! Например: 219331507</em>
            <a href="#" class="instruction-img-link settings-helper-link" onclick="return showInstructionImage('где взять ID сообщества.png');">&#x1F5BC;&#xFE0F; Показать картинку-инструкцию</a>
        </div>
        <input type="number" id="vkGroupId" placeholder="Например: 229445618" style="margin: 10px 0; padding: 8px; width: 100%;">

        <div class="settings-helper-box settings-helper-box--purple">
            <strong class="settings-helper-title">&#x1F6E0;&#xFE0F; Автонастройка сервера ВК</strong>
            <div style="font-size: 12px; margin: 10px 0; line-height: 1.6;">
                &#x2753; <strong>Зачем:</strong> Автоматически подключает бота к сообщениям сообщества через Callback API.<br>
                &#x1F4D6; <strong>Что делает:</strong><br>
                1. Создаёт сервер Callback API в вашем сообществе<br>
                2. Подтверждает сервер (вводит Confirmation Code)<br>
                3. Генерирует Secret Key для проверки запросов<br>
                4. Настраивает типы событий (сообщения, комментарии)<br><br>
                &#x1F4DD; <strong>Перед нажатием убедитесь что:</strong><br>
                &nbsp;&nbsp;✅ Поле «VK Tokens» заполнено токеном сообщества<br>
                &nbsp;&nbsp;✅ Поле «VK Group ID» содержит правильный ID<br>
                &nbsp;&nbsp;✅ У вас есть доступ к управлению сообществом
            </div>
        </div>
        <button id="autoSetupCallbackBtn" class="btn btn-accent" onclick="autoSetupCallback()">
            &#x1F6E0;&#xFE0F; Автонастройка сервера ВК
        </button>
        <div id="confirmation-status" style="margin-top: 10px;"></div>

        <hr class="settings-divider">

        <!-- Блок User Token -->
        <div class="settings-helper-box settings-helper-box--green">
            <strong class="settings-helper-title">&#x1F511; User Token (для вложений)</strong>
            <div style="font-size: 13px; margin: 10px 0; line-height: 1.6;">
                &#x2753; <strong>Зачем:</strong> Для отправки файлов, фото, видео в сообщения и комментарии, а также одобрения заявок на вступление в сообщество пользователями / удаление пользователей с сообщества и их данных(переписки)...<br>
                &#x1F4D6; <strong>Как получить:</strong><br>
                1. Нажмите «ПОЛУЧИТЬ ТОКЕН» &#x2192; откроется malyshrush.github.io/vk-token-generator<br>
                2. Нажмите «Выбрать все» &#x2192; "Сгенерировать ссылку" &#x2192; "Открыть ссылку"<br>
                3. Авторизуйтесь нажав на " Продолжить как ...."<br>
                4. Скопируйте ВСЮ ссылку из адресной строки<br>
                5. Вставьте ссылку в поле ниже &#x2014; бот сам извлечёт токен<br>
                <a href="#" class="instruction-img-link settings-helper-link" onclick="return showInstructionImage('USER TOKEN - 1 этап.png');">&#x1F5BC;&#xFE0F; 1 этап</a> |
                <a href="#" class="instruction-img-link settings-helper-link" onclick="return showInstructionImage('USER TOKEN - 2 этап.png');">&#x1F5BC;&#xFE0F; 2 этап</a> |
                <a href="#" class="instruction-img-link settings-helper-link" onclick="return showInstructionImage('USER TOKEN - 3 этап.png');">&#x1F5BC;&#xFE0F; 3 этап</a> |
                <a href="#" class="instruction-img-link settings-helper-link" onclick="return showInstructionImage('USER TOKEN - 4 этап.png');">&#x1F5BC;&#xFE0F; 4 этап</a>
            </div>
            <div style="text-align: left; margin: 20px 0;">
                <a id="getUserTokenBtn" href="https://malyshrush.github.io/vk-token-generator/" target="_blank" class="btn btn-save settings-link-btn" style="font-size: 16px; padding: 10px 20px;">
                    &#x1F511; ПОЛУЧИТЬ ТОКЕН
                </a>
            </div>
            <label><strong>&#x1F517; Вставьте ссылку или чистый токен:</strong></label>
            <input type="text" id="userToken" placeholder="Загрузка..." style="margin: 10px 0; padding: 8px; width: 100%;">
        </div>

        <div class="debug" id="settings-debug">Загрузка настроек...</div>
    </div>

    <!-- Кнопки сохранения -->
    <button class="btn btn-save" onclick="saveCommunitySettings()" style="margin: 15px 0; font-size: 14px; padding: 10px 20px;">
        &#x1F4BE; Сохранить настройки сообщества
    </button>
    <button class="btn btn-delete" onclick="deleteCurrentCommunity()" style="margin: 15px 0;">
        &#x1F5D1;&#xFE0F; Удалить текущее сообщество
    </button>

    <div id="save-status" class="status"></div>
</div>









<script>


// &#x1F6E0;&#xFE0F; ГЛОБАЛЬНАЯ ФУНКЦИЯ DEBUG
window.debug = function(msg) {
    console.log('[Admin]', msg);
    var el = document.getElementById('debugLog');
    if (el) el.innerHTML += '<br>[' + new Date().toLocaleTimeString() + '] ' + msg;
};




// &#x1F6E1;&#xFE0F; СИСТЕМА АВТОРИЗАЦИИ (БЕЗ BACKTICKS)
var AUTH_CONFIG = {
maxAttempts: 3,
lockoutTime: 30 * 60 * 1000,
recoveryEmail: 'admin@example.com'
};





// ИСПОЛЬЗУЕМ localStorage ВМЕСТО sessionStorage
function getAdminSessionToken() {
try {
return String(localStorage.getItem('adminSessionToken') || '').trim();
} catch (e) {
return '';
}
}

function checkAuthSession() {
try {
var sessionAuth = localStorage.getItem('adminAuthenticated');
var sessionToken = getAdminSessionToken();
if (sessionAuth === 'true' && sessionToken) {
return true;
}
} catch(e) {}
return false;
}




function getAttemptInfo() {
var attempts = parseInt(localStorage.getItem('loginAttempts') || '0');
var lockoutUntil = parseInt(localStorage.getItem('lockoutUntil') || '0');
return { attempts: attempts, lockoutUntil: lockoutUntil };
}




function incrementAttempt() {
var info = getAttemptInfo();
localStorage.setItem('loginAttempts', (info.attempts + 1).toString());
if (info.attempts + 1 >= AUTH_CONFIG.maxAttempts) {
var lockoutUntil = Date.now() + AUTH_CONFIG.lockoutTime;
localStorage.setItem('lockoutUntil', lockoutUntil.toString());
}
}




function resetAttempts() {
localStorage.setItem('loginAttempts', '0');
localStorage.setItem('lockoutUntil', '0');
}




function clearAuthSession() {
localStorage.removeItem('adminAuthenticated');
localStorage.removeItem('adminToken');
localStorage.removeItem('adminSessionToken');
localStorage.removeItem('adminProfileId');
localStorage.removeItem('adminProfileName');
localStorage.removeItem('adminPrincipalProfileId');
localStorage.removeItem('adminPrincipalProfileName');
localStorage.removeItem('adminRole');
localStorage.removeItem('adminIsMainAdmin');
localStorage.removeItem('vkBotLastCommunity');
localStorage.removeItem('vkBotLastTab');
localStorage.removeItem('vkBot_activeBot');
}

function forceLogoutToLogin(message) {
clearAuthSession();
var existingRegisterModal = document.getElementById('registerModal');
if (existingRegisterModal) existingRegisterModal.remove();
var existingReactivationModal = document.getElementById('reactivateModal');
if (existingReactivationModal) existingReactivationModal.remove();
var existingAuthModal = document.getElementById('authModal');
if (existingAuthModal) existingAuthModal.remove();
document.body.classList.add('auth-required');
document.body.insertAdjacentHTML('beforeend', showLoginModal());
var statusEl = document.getElementById('loginStatus');
if (statusEl && message) {
statusEl.innerText = message;
statusEl.style.color = '#f44336';
}
}

function getCurrentProfileId() {
try {
return localStorage.getItem('adminProfileId') || '1';
} catch (e) {
return '1';
}
}

function getPrincipalProfileId() {
try {
return localStorage.getItem('adminPrincipalProfileId') || getCurrentProfileId();
} catch (e) {
return getCurrentProfileId();
}
}

function isMainAdminSession() {
try {
if (localStorage.getItem('adminIsMainAdmin') === 'true') return true;
if (localStorage.getItem('adminAuthenticated') === 'true' && getPrincipalProfileId() === '1') return true;
return false;
} catch (e) {
return false;
}
}

function getPromoClientId() {
try {
var current = localStorage.getItem('promoClientId');
if (current) return current;
var generated = 'client_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
localStorage.setItem('promoClientId', generated);
return generated;
} catch (e) {
return 'client_fallback';
}
}

window.completeAuthSession = function(data) {
var sessionToken = String((data && data.sessionToken) || '').trim();
localStorage.setItem('adminAuthenticated', 'true');
localStorage.removeItem('adminToken');
if (sessionToken) {
localStorage.setItem('adminSessionToken', sessionToken);
} else {
localStorage.removeItem('adminSessionToken');
}
localStorage.setItem('adminProfileId', data.profileId || '1');
localStorage.setItem('adminProfileName', data.profileName || ('Профиль ' + (data.profileId || '1')));
localStorage.setItem('adminPrincipalProfileId', data.principalProfileId || data.profileId || '1');
localStorage.setItem('adminPrincipalProfileName', data.profileName || ('Профиль ' + (data.profileId || '1')));
localStorage.setItem('adminRole', data.role || 'admin');
localStorage.setItem('adminIsMainAdmin', data.isMainAdmin ? 'true' : 'false');
resetAttempts();
};

window.authUiState = window.authUiState || {
loginCaptchaRequired: false,
sessionCaptchaRequired: false
};

function getSessionCaptchaOverlayMarkup() {
return '<div id="sessionCaptchaOverlay" class="session-captcha-overlay" style="display:none;position:fixed;inset:0;background:var(--modal-overlay);backdrop-filter:blur(14px);z-index:1000003;align-items:center;justify-content:center;padding:24px;">' +
'<div class="session-captcha-modal" style="width:min(480px,100%);background:var(--modal-bg);border:1px solid var(--section-border);border-radius:24px;padding:28px;box-shadow:0 32px 90px rgba(15,23,42,0.42);">' +
'<h2 style="margin:0 0 10px 0;color:var(--text-primary);">Подтверждение безопасности</h2>' +
'<p style="margin:0 0 16px 0;color:var(--text-secondary);">Обнаружено подозрительное изменение сети или устройства. Пройди каптчу, чтобы продолжить работу.</p>' +
'<div id="sessionCaptchaImage" style="margin-bottom:14px;min-height:96px;display:flex;align-items:center;justify-content:center;padding:14px;border:1px dashed var(--border-color);border-radius:18px;background:var(--surface-soft);color:var(--text-secondary);font-size:14px;">Загружаем каптчу...</div>' +
'<label for="sessionCaptchaAnswer" style="display:block;margin:0 0 8px 0;color:var(--text-primary);font-size:13px;font-weight:700;">Введите символы с картинки</label>' +
'<input id="sessionCaptchaAnswer" type="text" autocomplete="off" autocapitalize="characters" spellcheck="false" placeholder="Например: AB12CD" style="width:100%;padding:12px;margin-bottom:12px;border:1px solid var(--border-color);border-radius:12px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<div class="profile-card-actions" style="display:flex;gap:10px;flex-wrap:wrap;">' +
'<button class="btn btn-save" type="button" onclick="submitSessionCaptcha()">Подтвердить</button>' +
'<button id="sessionCaptchaRefreshButton" class="btn btn-neutral" type="button" onclick="refreshSessionCaptcha(true)">Обновить каптчу</button>' +
'</div>' +
'<div id="sessionCaptchaStatus" style="margin-top:14px;font-size:13px;"></div>' +
'</div>' +
'</div>';
}

function ensureSessionCaptchaOverlay() {
if (document.getElementById('sessionCaptchaOverlay')) return;
document.body.insertAdjacentHTML('beforeend', getSessionCaptchaOverlayMarkup());
}

function setSessionCaptchaLock(visible) {
ensureSessionCaptchaOverlay();
document.body.classList.toggle('captcha-lock', !!visible);
var overlay = document.getElementById('sessionCaptchaOverlay');
if (overlay) overlay.style.display = visible ? 'flex' : 'none';
if (!visible) {
var statusEl = document.getElementById('sessionCaptchaStatus');
if (statusEl) statusEl.innerHTML = '';
var inputEl = document.getElementById('sessionCaptchaAnswer');
if (inputEl) inputEl.value = '';
}
}

function toggleLoginCaptcha(visible) {
window.authUiState.loginCaptchaRequired = !!visible;
var section = document.getElementById('loginCaptchaSection');
if (section) section.style.display = visible ? 'block' : 'none';
if (!visible) {
var answerEl = document.getElementById('loginCaptchaAnswer');
if (answerEl) answerEl.value = '';
}
}

function performForcedLogout(message) {
window.authUiState.sessionCaptchaRequired = false;
window.authUiState.loginCaptchaRequired = false;
setSessionCaptchaLock(false);
forceLogoutToLogin(message || 'Сессия завершена. Войдите снова.');
toggleLoginCaptcha(false);
}

function buildAdminRequestHeaders(headers) {
var nextHeaders = Object.assign({}, headers || {});
var sessionToken = getAdminSessionToken();
if (sessionToken && !nextHeaders['X-Admin-Session'] && !nextHeaders['x-admin-session']) {
nextHeaders['X-Admin-Session'] = sessionToken;
}
return nextHeaders;
}

function isAdminRequestUrl(url) {
if (!url || typeof url !== 'string') return false;
if (/^https?:\\/\\//i.test(url) && url.indexOf(window.location.origin) !== 0) return false;
return true;
}

function isSessionCaptchaChallengeResponse(url, data) {
return !!(
typeof url === 'string' &&
url.indexOf('getCaptcha=1') !== -1 &&
url.indexOf('mode=session') !== -1 &&
data &&
typeof data.captchaSvg === 'string'
);
}

window.sessionCaptchaRefreshInFlight = null;
window.sessionCaptchaLastRefreshAt = 0;

function setSessionCaptchaRefreshButtonState(pending) {
var button = document.getElementById('sessionCaptchaRefreshButton');
if (!button) return;
button.disabled = !!pending;
button.style.opacity = pending ? '0.65' : '1';
button.style.cursor = pending ? 'wait' : 'pointer';
button.textContent = pending ? 'Обновляем...' : 'Обновить каптчу';
}

async function fetchAdminJson(url, options) {
var response = await fetch(url, {
credentials: 'include',
...(options || {}),
headers: buildAdminRequestHeaders({
'Content-Type': 'application/json',
...((options && options.headers) ? options.headers : {})
})
});
var text = await response.text();
var data = {};
try {
data = text ? JSON.parse(text) : {};
} catch (e) {
throw new Error(text || 'Некорректный ответ сервера');
}
if (data && data.captchaRequired && !isSessionCaptchaChallengeResponse(url, data)) {
window.authUiState.sessionCaptchaRequired = true;
setSessionCaptchaLock(true);
await refreshSessionCaptcha();
throw new Error('CAPTCHA_REQUIRED');
}
if (data && data.sessionInvalid) {
performForcedLogout(data.error || 'Сессия недействительна');
throw new Error('SESSION_INVALID');
}
return data;
}

async function refreshLoginCaptcha() {
var baseUrl = window.location.href.split('?')[0];
var data = await fetchAdminJson(baseUrl + '?getCaptcha=1&mode=login', { method: 'GET' });
var box = document.getElementById('loginCaptchaBox');
if (box) box.innerHTML = data.captchaSvg || '';
toggleLoginCaptcha(true);
}

async function refreshSessionCaptcha(force) {
if (window.sessionCaptchaRefreshInFlight) {
return window.sessionCaptchaRefreshInFlight;
}
var baseUrl = window.location.href.split('?')[0];
var box = document.getElementById('sessionCaptchaImage');
var statusEl = document.getElementById('sessionCaptchaStatus');
var answerEl = document.getElementById('sessionCaptchaAnswer');
var previousMarkup = box ? box.innerHTML : '';
var hasPreviousMarkup = !!(previousMarkup && previousMarkup.indexOf('Загружаем каптчу') === -1);
if (!force && hasPreviousMarkup && Date.now() - Number(window.sessionCaptchaLastRefreshAt || 0) < 10000) {
return { success: true, reused: true };
}
if (statusEl) statusEl.innerHTML = makeInlineText('info', 'Обновляем каптчу...');
if (box && !hasPreviousMarkup) {
box.innerHTML = '<div style="color:var(--text-secondary);font-size:14px;">Загружаем каптчу...</div>';
}
setSessionCaptchaRefreshButtonState(true);
window.sessionCaptchaRefreshInFlight = (async function() {
var data = await fetchAdminJson(baseUrl + '?getCaptcha=1&mode=session', { method: 'GET' });
if (data && data.errorCode === 'captcha_rate_limited') {
var cooldownSeconds = Math.max(1, Math.ceil((Number(data.cooldownMs) || 0) / 1000));
if (box && previousMarkup) box.innerHTML = previousMarkup;
if (statusEl) statusEl.innerHTML = makeInlineText('error', 'Подожди ' + cooldownSeconds + ' сек. и обнови каптчу снова.');
return data;
}
if (box) {
box.innerHTML = data.captchaSvg || previousMarkup || '<div style="color:var(--text-secondary);font-size:14px;">Не удалось загрузить картинку капчи. Нажмите "Обновить каптчу".</div>';
}
window.sessionCaptchaLastRefreshAt = Date.now();
if (statusEl) statusEl.innerHTML = '';
if (answerEl) {
answerEl.value = '';
answerEl.focus();
}
return data;
})().catch(function(e) {
if (box && previousMarkup) box.innerHTML = previousMarkup;
if (statusEl) statusEl.innerHTML = makeInlineText('error', e.message || 'Не удалось обновить каптчу.');
throw e;
}).finally(function() {
setSessionCaptchaRefreshButtonState(false);
window.sessionCaptchaRefreshInFlight = null;
});
return window.sessionCaptchaRefreshInFlight;
}

function getActiveAdminTabName() {
var activeTab = document.querySelector('.tablinks.active');
if (!activeTab) return 'Messages';
var onclickAttr = activeTab.getAttribute('onclick') || '';
var match = onclickAttr.match(/'(\w+)'/);
return (match && match[1]) ? match[1] : 'Messages';
}

function restoreCurrentCommunityIdFromStorage() {
if (window.currentCommunityId) return window.currentCommunityId;
try {
var communityId = localStorage.getItem('vkBotLastCommunity') || '';
if (communityId) window.currentCommunityId = communityId;
return communityId;
} catch(e) {
return window.currentCommunityId || '';
}
}

function reloadActiveTabAfterSessionCaptcha() {
var tabName = getActiveAdminTabName();
restoreCurrentCommunityIdFromStorage();
setTimeout(function() {
if (typeof window.refreshTabContent === 'function') {
window.refreshTabContent(tabName);
} else if (typeof loadData === 'function') {
loadData(tabName);
}
}, 0);
}

async function submitSessionCaptcha() {
var baseUrl = window.location.href.split('?')[0];
var answerEl = document.getElementById('sessionCaptchaAnswer');
var statusEl = document.getElementById('sessionCaptchaStatus');
var answer = answerEl ? answerEl.value.trim() : '';
if (!answer) {
if (statusEl) statusEl.innerHTML = makeInlineText('error', 'Введите ответ каптчи.');
return;
}
try {
var data = await fetchAdminJson(baseUrl + '?verifyCaptcha=1', {
method: 'POST',
body: JSON.stringify({ mode: 'session', answer: answer })
});
if (data && data.success) {
if (data.sessionToken) {
localStorage.setItem('adminSessionToken', data.sessionToken);
}
window.authUiState.sessionCaptchaRequired = false;
setSessionCaptchaLock(false);
reloadActiveTabAfterSessionCaptcha();
}
} catch (e) {
if (e && e.message === 'CAPTCHA_REQUIRED' || e && e.message === 'SESSION_INVALID') return;
if (statusEl) statusEl.innerHTML = makeInlineText('error', e.message || 'Не удалось подтвердить каптчу.');
}
}

function appendProfileIdToUrl(url) {
if (!url || typeof url !== 'string') return url;
if (/^https?:\\/\\//i.test(url) && url.indexOf(window.location.origin) !== 0) return url;
if (url.indexOf('verifyAuth') !== -1 || url.indexOf('loginAdmin') !== -1 || url.indexOf('requestRecovery') !== -1 || url.indexOf('getCaptcha') !== -1 || url.indexOf('verifyCaptcha') !== -1 || url.indexOf('logoutAdmin') !== -1) return url;
if (/[?&]profileId=/.test(url)) return url;
var profileId = getCurrentProfileId();
if (!profileId) return url;
var principalProfileId = getPrincipalProfileId();
var separator = url.indexOf('?') === -1 ? '?' : '&';
url += separator + 'profileId=' + encodeURIComponent(profileId);
if (!/[?&]principalProfileId=/.test(url) && principalProfileId) {
url += '&principalProfileId=' + encodeURIComponent(principalProfileId);
}
return url;
}

if (window.fetch && !window.__adminProfileFetchWrapped) {
var originalFetch = window.fetch.bind(window);
window.fetch = function(input, init) {
var nextInit = init ? Object.assign({}, init) : {};
if (!nextInit.credentials) nextInit.credentials = 'include';
if (typeof input === 'string') {
if (isAdminRequestUrl(input)) {
nextInit.headers = buildAdminRequestHeaders(nextInit.headers);
}
return originalFetch(appendProfileIdToUrl(input), nextInit).then(async function(response) {
try {
if (response && (response.status === 401 || response.status === 403)) {
var cloned = response.clone();
var data = await cloned.json().catch(function() { return null; });
if (data && data.captchaRequired) {
window.authUiState.sessionCaptchaRequired = true;
setSessionCaptchaLock(true);
setTimeout(function() {
refreshSessionCaptcha();
}, 0);
}
if (data && data.sessionInvalid) {
setTimeout(function() {
performForcedLogout(data.error || 'Сессия завершена. Войдите снова.');
}, 0);
}
}
} catch (e) {}
return response;
});
}
return originalFetch(input, nextInit);
};
window.__adminProfileFetchWrapped = true;
}

function applyAdminAccessUI() {
var adminTabButton = document.getElementById('adminTabButton');
if (adminTabButton) {
adminTabButton.style.display = isMainAdminSession() ? '' : 'none';
}
var adminTab = document.getElementById('Admin');
if (adminTab && !isMainAdminSession()) {
adminTab.style.display = 'none';
adminTab.classList.remove('active');
}
}




// &#x1F6E1;&#xFE0F; ПОКАЗАТЬ ОКНО АВТОРИЗАЦИИ
function makeInlineNotice(type, message) {
var safeType = type || 'info';
return '<div class="inline-notice inline-notice--' + safeType + '">' + message + '</div>';
}

function makeInlineText(type, message) {
var safeType = type || 'info';
return '<span class="inline-text--' + safeType + '">' + message + '</span>';
}

function showLoginModal() {
var info = getAttemptInfo();
var now = Date.now();
document.body.classList.add('auth-required');
if (info.lockoutUntil > now) {
var minutesLeft = Math.ceil((info.lockoutUntil - now) / 60000);
return '<div id="authModal">' +
'<div class="auth-content">' +
'<h2 style="color:#f44336;margin-bottom:20px;">&#x1F6E1;&#xFE0F; Доступ заблокирован</h2>' +
'<p style="color:var(--text-secondary);margin-bottom:20px;">Превышено количество попыток входа.</p>' +
'<p style="color:#f44336;font-weight:bold;margin-bottom:30px;">Повторите через <span style="font-size:18px;">' + minutesLeft + ' мин.</span></p>' +
'<hr style="margin:20px 0;border:none;border-top:1px solid var(--section-border);">' +
'<h3 style="margin-bottom:15px;">&#x1F511; Восстановление доступа</h3>' +
'<input type="text" id="recoveryUsername" placeholder="Ваш логин" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<input type="email" id="recoveryEmail" placeholder="Ваша почта" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<button onclick="requestRecovery()" style="width:100%;padding:12px;background:#2196F3;color:white;border:none;border-radius:6px;cursor:pointer;font-size:14px;font-weight:bold;">?? Запросить восстановление</button>' +
'<div id="recoveryStatus" style="margin-top:15px;"></div>' +
'</div>' +
'</div>';
}



var remainingAttempts = AUTH_CONFIG.maxAttempts - info.attempts;
var attemptColor = remainingAttempts <= 1 ? '#f44336' : '#4CAF50';
return '<div id="authModal">' +
'<div class="auth-content">' +
'<div style="font-size:24px;font-weight:900;color:var(--text-primary);margin-bottom:8px;">PAPA BOT &#x1F916;</div>' +
'<h2 style="color:var(--text-primary);margin-bottom:10px;">&#x1F6E1;&#xFE0F; Авторизация</h2>' +
'<p style="color:var(--text-secondary);margin-bottom:20px;font-size:14px;">Вход в Админ-панель</p>' +
'<p style="color:var(--text-secondary);margin-bottom:20px;font-size:13px;">Осталось попыток: <strong style="color:' + attemptColor + ';font-size:16px;">' + remainingAttempts + '</strong></p>' +
'<input type="text" id="loginUsername" placeholder="Логин" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<input type="password" id="loginPassword" placeholder="Пароль" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<div id="loginCaptchaSection" style="display:none;margin-top:10px;">' +
'<div id="loginCaptchaBox" style="margin-bottom:12px;"></div>' +
'<input type="text" id="loginCaptchaAnswer" placeholder="Введите символы с картинки" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<button type="button" onclick="refreshLoginCaptcha()" style="width:100%;padding:11px;background:#64748b;color:white;border:none;border-radius:6px;cursor:pointer;font-size:13px;font-weight:bold;margin-bottom:6px;">Обновить каптчу</button>' +
'</div>' +
'<button onclick="performLogin()" style="width:100%;padding:14px;background:#4CAF50;color:white;border:none;border-radius:6px;cursor:pointer;font-size:16px;font-weight:bold;margin-top:10px;">&#x1F680; Войти</button>' +
'<button onclick="openRegisterModal()" style="width:100%;padding:12px;background:#2563eb;color:white;border:none;border-radius:6px;cursor:pointer;font-size:14px;font-weight:bold;margin-top:10px;">&#x2728; Создать аккаунт</button>' +
'<div id="loginStatus" style="margin-top:15px;color:#f44336;font-size:13px;"></div>' +
'<hr style="margin:25px 0;border:none;border-top:1px solid var(--section-border);">' +
'<p style="font-size:12px;color:var(--text-secondary);">&#x2139;&#xFE0F; Сессия сохраняется в браузере</p>' +
'</div>' +
'</div>';
}

function showRegisterModal() {
return '<div id="registerModal" style="position:fixed;top:0;left:0;width:100%;height:100%;background:var(--modal-overlay);z-index:1000001;display:flex;align-items:center;justify-content:center;">' +
'<div class="auth-content" style="max-width:520px;">' +
'<h2 style="color:var(--text-primary);margin-bottom:10px;">&#x2728; Создание аккаунта</h2>' +
'<p style="color:var(--text-secondary);margin-bottom:16px;font-size:14px;">Сначала введите промокод. Если он существует, появится форма регистрации нового аккаунта.</p>' +
'<div id="registerStatus" style="margin-bottom:12px;"></div>' +
'<div id="promoStep">' +
'<input type="text" id="registerPromoCode" placeholder="Промокод" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<button onclick="verifyPromoCode()" style="width:100%;padding:12px;background:#7c3aed;color:white;border:none;border-radius:6px;cursor:pointer;font-size:14px;font-weight:bold;">&#x1F3AB; Проверить промокод</button>' +
'</div>' +
'<div id="registerForm" style="display:none;">' +
'<input type="text" id="registerName" placeholder="Название профиля" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<input type="text" id="registerUsername" placeholder="Логин" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<input type="password" id="registerPassword" placeholder="Пароль" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<input type="email" id="registerEmail" placeholder="Email для восстановления" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<div id="registerPromoInfo" class="hint" style="margin-bottom:10px;"></div>' +
'<button onclick="submitRegisterAccount()" style="width:100%;padding:12px;background:#16a34a;color:white;border:none;border-radius:6px;cursor:pointer;font-size:14px;font-weight:bold;">&#x1F680; Создать аккаунт</button>' +
'</div>' +
'<button onclick="closeRegisterModal()" style="width:100%;padding:11px;background:#64748b;color:white;border:none;border-radius:6px;cursor:pointer;font-size:14px;font-weight:bold;margin-top:12px;">Закрыть</button>' +
'</div>' +
'</div>';
}

function showReactivationModal(profileName) {
var safeProfileName = String(profileName || 'аккаунт')
.replace(/&/g, '&amp;')
.replace(/</g, '&lt;')
.replace(/>/g, '&gt;')
.replace(/"/g, '&quot;')
.replace(/'/g, '&#39;');
return '<div id="reactivateModal" style="position:fixed;top:0;left:0;width:100%;height:100%;background:var(--modal-overlay);z-index:1000002;display:flex;align-items:center;justify-content:center;">' +
'<div class="auth-content" style="max-width:520px;">' +
'<h2 style="color:var(--text-primary);margin-bottom:10px;">&#x23F0; Срок действия профиля истёк</h2>' +
'<p style="color:var(--text-secondary);margin-bottom:16px;font-size:14px;">Профиль <strong>' + safeProfileName + '</strong> можно повторно активировать промокодом. После правильного промокода вход выполнится сразу автоматически.</p>' +
'<div id="reactivateStatus" style="margin-bottom:12px;"></div>' +
'<input type="text" id="reactivatePromoCode" placeholder="Промокод для повторной активации" style="width:100%;padding:12px;margin:10px 0;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
'<button onclick="submitReactivationPromo()" style="width:100%;padding:12px;background:#7c3aed;color:white;border:none;border-radius:6px;cursor:pointer;font-size:14px;font-weight:bold;">&#x1F504; Активировать и войти</button>' +
'<button onclick="closeReactivationModal()" style="width:100%;padding:11px;background:#64748b;color:white;border:none;border-radius:6px;cursor:pointer;font-size:14px;font-weight:bold;margin-top:12px;">Назад ко входу</button>' +
'</div>' +
'</div>';
}



window.saveBotSettings = async function() {
// &#x1F6E1;&#xFE0F; ПОКАЗЫВАЕМ ПОЛНОЭКРАННОЕ УВЕДОМЛЕНИЕ
showSaveOverlay();

// &#x1F6E0;&#xFE0F; ИСПРАВЛЕНИЕ: Безопасное получение значений с проверкой на null
const getValue = (id) => {
    const el = document.getElementById(id);
    return el ? el.value.trim() : '';
};

// &#x2728; НОВОЕ: Получаем токены из textarea (каждый с новой строки)
const vkTokensText = getValue('vkTokens');
const vkTokensArray = vkTokensText.split('\\n').map(t => t.trim()).filter(t => t);
const firstToken = vkTokensArray[0] || '';

const settings = {
        vk_tokens: vkTokensArray,  // ?? МАССИВ ТОКЕНОВ
        vk_token: firstToken,       // ?? Первый токен для совместимости
        confirmation_token: getValue('confirmationCode'),
        secret_key: getValue('secretKey'),
        vk_group_id: getValue('vkGroupId') ? parseInt(getValue('vkGroupId'), 10) : null,
        user_token: getValue('userToken')
    };

    const statusDiv = document.getElementById('save-status');
    if (!statusDiv) return;
    statusDiv.innerHTML = makeInlineNotice('warn', '&#x1F4BE; Сохранение...');

    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?saveBotSettings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(settings)
        });
        const data = await res.json();
        if (data.success) {
            statusDiv.innerHTML = makeInlineNotice('success', '✅ Все настройки сохранены! Нажмите F5 или Ctrl+F5');
            setTimeout(() => statusDiv.innerHTML = '', 3000);
        } 
        else {
            statusDiv.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + (data.error || 'неизвестная'));
             //statusEl.innerText = '❌ Ошибка: ' + (data.error  || 'неизвестная');
        }
    } catch (e) {
        statusDiv.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + e.message);
        //statusEl.innerText = '❌ Ошибка: ' + (data.error  || 'неизвестная');
    }
};




window.performLogin = async function() {
var username = document.getElementById('loginUsername').value.trim();
var password = document.getElementById('loginPassword').value.trim();
var captchaAnswerEl = document.getElementById('loginCaptchaAnswer');
var statusEl = document.getElementById('loginStatus');
if (!username || !password) {
statusEl.innerText = '? Введите логин и пароль';
return;
}
try {
var baseUrl = window.location.href.split('?')[0];
debug('?&#x1F916; Auth request to: ' + baseUrl);
var data = await fetchAdminJson(baseUrl + '?loginAdmin=1', {
method: 'POST',
body: JSON.stringify({
username: username,
password: password,
captchaAnswer: window.authUiState.loginCaptchaRequired && captchaAnswerEl ? captchaAnswerEl.value.trim() : ''
})
});
if (data.success) {
debug('&#x1F916; Auth successful for ' + username);
window.completeAuthSession(data);
toggleLoginCaptcha(false);
setSessionCaptchaLock(false);
var modal = document.getElementById('authModal');
if (modal) modal.remove();
statusEl.innerText = '? Вход выполнен!';
setTimeout(function() { location.reload(); }, 500);
} else if (data.expired && data.canReactivate) {
        window.pendingExpiredLogin = {
username: username,
password: password,
profileId: data.profileId || '',
profileName: data.profileName || username
};
statusEl.innerText = '? Срок действия профиля истёк. Нужен промокод для повторной активации.';
var existingReactivationModal = document.getElementById('reactivateModal');
if (existingReactivationModal) existingReactivationModal.remove();
var authModal = document.getElementById('authModal');
if (authModal) authModal.style.display = 'none';
document.body.insertAdjacentHTML('beforeend', showReactivationModal(window.pendingExpiredLogin.profileName));
} else {
debug('&#x1F916; Auth failed: ' + (data.error || 'Unknown error'));
if (data.loginCaptchaRequired) {
await refreshLoginCaptcha();
}
if (typeof data.remainingAttempts === 'number') {
var attemptsUsed = Math.max(0, AUTH_CONFIG.maxAttempts - data.remainingAttempts);
localStorage.setItem('loginAttempts', attemptsUsed.toString());
if (data.lockUntil) {
localStorage.setItem('lockoutUntil', data.lockUntil.toString());
}
} else {
incrementAttempt();
}
statusEl.innerText = '? ' + (data.error || 'Неверный логин или пароль');
if (data.locked && data.lockUntil) {
localStorage.setItem('lockoutUntil', data.lockUntil.toString());
}
var info = getAttemptInfo();
var remainingAttempts = AUTH_CONFIG.maxAttempts - info.attempts;
var attemptColor = remainingAttempts <= 1 ? '#f44336' : '#4CAF50';
var attemptsEl = document.querySelector('#authModal p[style*="font-size:13px"] strong');
if (attemptsEl) {
attemptsEl.innerText = remainingAttempts;
attemptsEl.style.color = attemptColor;
}
if (remainingAttempts === 1) {
var warningEl = document.getElementById('loginStatus');
if (warningEl && !warningEl.innerHTML.includes('ВНИМАНИЕ')) {
warningEl.innerHTML += '<br><strong style="color:#f44336;">?? ВНИМАНИЕ: При следующей ошибке доступ будет заблокирован на 30 минут!</strong>';
}
}
if (info.attempts >= AUTH_CONFIG.maxAttempts) {
setTimeout(function() { location.reload(); }, 2000);
}
}
} catch (e) {
if (e && (e.message === 'CAPTCHA_REQUIRED' || e.message === 'SESSION_INVALID')) {
return;
}
debug('&#x1F916; Auth error: ' + e.message);
statusEl.innerText = '❌ Ошибка: ' + e.message;
}
};
// &#x1F504; ФУНКЦИЯ ОБНОВЛЕНИЯ ДАННЫХ С ВИЗУАЛЬНЫМ ЭФФЕКТОМ
window.refreshAllData = async function() {
const activeTab = document.querySelector('.tablinks.active');
if (!activeTab) return;
const tabName = activeTab.getAttribute('onclick').match(/'(\\w+)'/)[1];
const loadingEl = document.getElementById('loading-' + tabName);
const btn = document.querySelector('.btn-add[onclick="refreshAllData()"]');
if (btn) {
var originalText = btn.innerText;
btn.innerText = '? Обновление...';
btn.disabled = true;
btn.style.background = '#FF9800';
}
if (loadingEl) {
loadingEl.style.display = 'block';
loadingEl.innerText = '&#x1F504; Обновление данных...';
}
try {
await loadData(tabName);
showStatus('? Данные обновлены!', 'success');
debug('?? Data refreshed for ' + tabName);
var table = document.getElementById('table-' + tabName);
if (table) {
table.style.transition = 'background 0.3s';
table.style.background = '#E8F5E9';
setTimeout(function() {
table.style.background = '';
}, 500);
}
} catch (e) {
showStatus('❌ Ошибка обновления: ' + e.message, 'error');
debug('? Refresh error: ' + e.message);
} finally {
if (loadingEl) {
loadingEl.style.display = 'none';
}
if (btn) {
btn.innerText = '?? Обновить данные';
btn.disabled = false;
btn.style.background = '#2196F3';
}
}
};







// &#x1F511; ЗАПРОС ВОССТАНОВЛЕНИЯ
window.requestRecovery = async function() {
var username = document.getElementById('recoveryUsername') ? document.getElementById('recoveryUsername').value.trim() : '';
var email = document.getElementById('recoveryEmail').value.trim();
var statusEl = document.getElementById('recoveryStatus');
if (!email && !username) {
statusEl.innerText = '? Введите логин или email';
statusEl.style.color = '#f44336';
return;
}
try {
var baseUrl = window.location.href.split('?')[0];
var res = await fetch(baseUrl + '?requestRecovery', {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({ email: email, username: username })
});
var data = await res.json();
if (data.success) {
statusEl.innerText = '? Запрос на восстановление отправлен главному админу';
statusEl.style.color = '#4CAF50';
} else {
statusEl.innerText = '? ' + (data.error || 'Ошибка отправки');
statusEl.style.color = '#f44336';
}
} catch (e) {
statusEl.innerText = '❌ Ошибка: ' + e.message;
statusEl.style.color = '#f44336';
}
};

window.openRegisterModal = function() {
if (document.getElementById('registerModal')) return;
var authModal = document.getElementById('authModal');
if (authModal) authModal.style.display = 'none';
document.body.insertAdjacentHTML('beforeend', showRegisterModal());
};

window.closeRegisterModal = function() {
var modal = document.getElementById('registerModal');
if (modal) modal.remove();
var authModal = document.getElementById('authModal');
if (authModal) authModal.style.display = 'flex';
};

window.openReactivationModal = function(profileName) {
if (document.getElementById('reactivateModal')) return;
var authModal = document.getElementById('authModal');
if (authModal) authModal.style.display = 'none';
document.body.insertAdjacentHTML('beforeend', showReactivationModal(profileName));
};

window.closeReactivationModal = function() {
var modal = document.getElementById('reactivateModal');
if (modal) modal.remove();
var authModal = document.getElementById('authModal');
if (authModal) authModal.style.display = 'flex';
};

window.submitReactivationPromo = async function() {
var statusEl = document.getElementById('reactivateStatus');
var code = document.getElementById('reactivatePromoCode').value.trim();
var pending = window.pendingExpiredLogin || null;
if (!pending || !pending.username || !pending.password) {
statusEl.innerHTML = makeInlineNotice('error', 'Нет данных истекшего входа. Попробуйте войти снова.');
return;
}
if (!code) {
statusEl.innerHTML = makeInlineNotice('error', 'Введите промокод для повторной активации.');
return;
}
try {
var baseUrl = window.location.href.split('?')[0];
var res = await fetch(baseUrl + '?reactivateExpiredProfile', {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({
username: pending.username,
password: pending.password,
code: code,
clientId: 'reactivate_' + pending.username
})
});
var data = await res.json();
if (!data.success) {
statusEl.innerHTML = makeInlineNotice('error', data.error || 'Не удалось активировать профиль');
return;
}
 window.completeAuthSession(data);
statusEl.innerHTML = makeInlineNotice('success', '✅ Профиль активирован. Выполняем вход...');
window.pendingExpiredLogin = null;
setTimeout(function() {
closeReactivationModal();
var authModal = document.getElementById('authModal');
if (authModal) authModal.remove();
location.reload();
}, 500);
} catch (e) {
statusEl.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + e.message);
}
};

window.verifyPromoCode = async function() {
var code = document.getElementById('registerPromoCode').value.trim();
var statusEl = document.getElementById('registerStatus');
if (!code) {
statusEl.innerHTML = makeInlineNotice('error', 'Введите промокод');
return;
}
try {
var baseUrl = window.location.href.split('?')[0];
var res = await fetch(baseUrl + '?verifyPromoCode', {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({ code: code, clientId: getPromoClientId() })
});
var data = await res.json();
if (!data.success) {
statusEl.innerHTML = makeInlineNotice('error', data.error || 'Промокод недоступен');
return;
}
window.verifiedPromoCode = data.promo.code;
document.getElementById('registerForm').style.display = 'block';
document.getElementById('promoStep').style.display = 'none';
document.getElementById('registerPromoInfo').textContent = data.promo.durationMinutes ? ('Срок профиля: ' + data.promo.durationMinutes + ' мин.') : 'Срок профиля: бессрочно';
statusEl.innerHTML = makeInlineNotice('success', '✅ Промокод подтверждён. Теперь заполните данные аккаунта.');
} catch (e) {
statusEl.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + e.message);
}
};

window.submitRegisterAccount = async function() {
var statusEl = document.getElementById('registerStatus');
var payload = {
code: window.verifiedPromoCode || document.getElementById('registerPromoCode').value.trim(),
name: document.getElementById('registerName').value.trim(),
username: document.getElementById('registerUsername').value.trim(),
password: document.getElementById('registerPassword').value.trim(),
recoveryEmail: document.getElementById('registerEmail').value.trim(),
clientId: getPromoClientId()
};
if (!payload.code || !payload.name || !payload.username || !payload.password) {
statusEl.innerHTML = makeInlineNotice('error', 'Заполните название профиля, логин, пароль и подтверждённый промокод.');
return;
}
try {
var baseUrl = window.location.href.split('?')[0];
var res = await fetch(baseUrl + '?registerAccount', {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify(payload)
});
var data = await res.json();
if (!data.success) {
statusEl.innerHTML = makeInlineNotice('error', data.error || 'Не удалось создать аккаунт');
return;
}
statusEl.innerHTML = makeInlineNotice('success', '✅ Аккаунт создан. Теперь войдите под новым логином и паролем.');
window.verifiedPromoCode = null;
setTimeout(function() {
closeRegisterModal();
}, 1200);
} catch (e) {
statusEl.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + e.message);
}
};
// &#x1F6E1;&#xFE0F; ПРОВЕРКА ПРИ ЗАГРУЗКЕ СТРАНИЦЫ
window.checkAuthOnLoad = async function() {
try {
if (!checkAuthSession()) {
document.body.classList.add('auth-required');
document.body.insertAdjacentHTML('beforeend', showLoginModal());
return false;
}

var baseUrl = window.location.href.split('?')[0];
var data = await fetchAdminJson(baseUrl + '?validateSession', { method: 'GET' });
if (!data || !data.success) {
forceLogoutToLogin(data && data.error ? data.error : 'Сессия больше не действует. Войдите снова.');
return false;
}
} catch(e) {
if (e && (e.message === 'CAPTCHA_REQUIRED' || e.message === 'SESSION_INVALID')) {
return false;
}
console.error('checkAuthOnLoad error:', e);
forceLogoutToLogin('Не удалось проверить сессию. Войдите снова.');
return false;
}
return true;
};






// Глобальная функция debug (используется везде, до IIFE)
window.debug = function(msg) {
    console.log('[Admin]', msg);
    try {
        const el = document.getElementById('debugLog');
        if (el) el.innerHTML += '<br>[' + new Date().toLocaleTimeString() + '] ' + msg;
    } catch(e) {}
};

// &#x1F680; ТЕПЕРЬ IIFE С ОСНОВНЫМ КОДОМ
(function() {

    const debug = window.debug;

    let dataStore = {};


const sheetMap = {
'Messages': 'СООБЩЕНИЯ',
'Comments': 'КОММЕНТАРИИ В ПОСТАХ',
'Users': 'ПОЛЬЗОВАТЕЛИ',
'Groups': 'ГРУППЫ',
'Variables': 'ПЕРЕМЕННЫЕ',
'Variables_User': 'ПЕРЕМЕННЫЕ',
'VK_Variables': 'ПЕРЕМЕННЫЕ',
'Shared_Variables': 'ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ',
'Mailing': 'РАССЫЛКА',
'Delayed': 'ОТЛОЖЕННЫЕ',
'Triggers': 'ТРИГГЕРЫ'
};
const buttonColors = ['th-blue-1','th-blue-2','th-blue-3','th-purple-1','th-purple-2','th-teal-1','th-teal-2','th-red-1','th-red-2','th-cyan'];
const fallbackButtonColors = ['th-indigo','th-purple-1','th-purple-2','th-teal-1','th-teal-2','th-red-1','th-red-2','th-cyan','th-blue-1','th-blue-2'];



function generateMailingButtonColumns(startIdx, endIdx) {
    var cols = [];
    var colors = ['th-blue-1','th-blue-2','th-blue-3','th-purple-1','th-purple-2','th-teal-1','th-teal-2','th-red-1','th-red-2','th-cyan'];
    for (var i = startIdx; i <= endIdx; i++) {
        var colorIdx = (i - 1) % colors.length;
        cols.push({
            name: 'Кнопка-' + i,
            class: colors[colorIdx],
            hint: 'Текст кнопки ' + i,
            section: 'КНОПКИ В РАССЫЛКЕ'
        });
        cols.push({
            name: 'Цвет/Ссылка-' + i,
            class: colors[colorIdx],
            type: 'select',
            options: ['красный','зелёный','синий','белый','ССЫЛКА...'],
            hint: 'Цвет или ссылка для кнопки ' + i,
            section: 'КНОПКИ В РАССЫЛКЕ'
        });
    }
    return cols;
};



const messagesColumns = [
    { name: 'Бот', class: 'th-green', hint: 'Название бота, к которому относится эта строка. Один бот может содержать много шагов. Если в таблице несколько ботов, сначала выберите нужного бота сверху, затем редактируйте его шаги.', section: 'ОСНОВНОЕ' },
    { name: 'Шаг', class: 'th-green', hint: 'Точка сценария внутри выбранного бота. Пользователь попадает на шаг, а затем бот проверяет триггер, отправляет ответ и может перевести пользователя дальше.', section: 'ОСНОВНОЕ' },
    { name: 'Триггер', class: 'th-red-1', hint: 'Что должен написать пользователь, чтобы строка сработала. Здесь можно указать слово, фразу, несколько вариантов через новую строку или оставить пусто для специальных файловых сценариев.', section: 'ОСНОВНОЕ' },
    { name: 'Ответ', class: 'th-blue-1', hint: 'Текст, который бот отправит пользователю при срабатывании строки. Можно использовать переносы строк и переменные.', section: 'ОСНОВНОЕ' },
    { name: 'Вложения к ответу', class: 'th-blue-1', hint: 'Вложения, которые будут отправлены вместе с ответом. Указывайте attachment ID через запятую или с новой строки: фото, документы, видео, аудио и другие вложения VK.', section: 'ОСНОВНОЕ' },
    { name: 'Точно/Не точно', class: 'th-brown', type: 'select', options: ['ТОЧНО', 'НЕ ТОЧНО'], hint: 'ТОЧНО: сообщение должно полностью совпасть с триггером. НЕ ТОЧНО: достаточно, чтобы триггер содержался внутри сообщения.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
    { name: 'Регистр', class: 'th-brown', type: 'select', options: ['важно', 'не важно'], hint: 'Определяет, учитывать ли заглавные и строчные буквы. Если выбрано «не важно», Привет и привет будут считаться одинаковыми.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
    { name: 'Ответить если в Группе', class: 'th-brown', hint: 'Ограничение по группам пользователя. Если поле заполнено, строка сработает только для пользователей, которые состоят хотя бы в одной из указанных групп.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
    { name: 'Ответил на Шаг', class: 'th-brown', hint: 'Проверка текущего шага пользователя. Используйте это поле, если строка должна работать только после конкретного предыдущего шага.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
    { name: 'Пользовательская', class: 'th-brown', hint: 'Дополнительная проверка по пользовательским переменным. Здесь указываются имена ПП, которые должны существовать у пользователя для срабатывания строки.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
    { name: 'Глобальная', class: 'th-brown', hint: 'Дополнительная проверка по глобальным переменным текущего сообщества. Используйте, когда логика зависит от общих значений, например флага акции или остатка.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
    { name: 'Переменная ПВС', class: 'th-brown', hint: 'Проверка по ПВС пользователя. ПВС привязана к конкретному пользователю, но видна во всех сообществах текущего профиля.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
    { name: 'Задержка отправки на Шаг', class: 'th-orange-dark', hint: 'Отложенная отправка следующего шага. Можно указать время в минутах, часах или коротко: 10мин, 1час, 2дня. Пока задержка не пройдёт, следующий шаг не отправится.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
    { name: 'Отправить на Шаг', class: 'th-orange-dark', hint: 'На какой шаг перевести пользователя после выполнения этой строки. Если указана ещё и задержка, перевод произойдёт не сразу, а по расписанию.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
    { name: 'ДОБАВИТЬ ГРУППУ', class: 'th-orange', hint: 'Какие группы нужно выдать пользователю после срабатывания строки. Можно указывать несколько групп через запятую или новую строку.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
    { name: 'УДАЛИТЬ ГРУППУ', class: 'th-orange', hint: 'Из каких групп нужно удалить пользователя после срабатывания строки. Полезно для смены статуса, сегмента или завершения цепочки.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
    { name: 'Действия с ПП', class: 'th-orange', hint: 'Пользовательские переменные привязаны только к пользователю.<br>С ними можно проводить различные действия:<br>Сложение "+", Вычитание "-", Умножение "*", Деление "/", Объединение "&".<br>Примеры:<br>pp1 = 10<br>pp2 = 20<br>pp3 = pp1 + pp2 (тоже самое что "10 + 20" → 30)<br>pp4 = pp1 & pp2 (тоже самое что "10&20" → "1020")<br>pp5 = "Привет Мир!" (текстовое значение)<br>pp6 = pp3 * 2 (→ 60)<br>pp7 = pp6 - pp1 (→ 50)<br>В ячейке можно записывать несколько действий.<br>Каждое новое действие — с новой строки.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
    { name: 'Действия с ГП', class: 'th-orange', hint: 'Глобальные переменные общие для всех пользователей.<br>С ними можно проводить различные действия:<br>Сложение "+", Вычитание "-", Умножение "*", Деление "/", Объединение "&".<br>Примеры:<br>gp1 = 1000<br>gp2 = 500<br>gp3 = gp1 + gp2 (тоже самое что "1000 + 500" → 1500)<br>gp4 = gp1 & gp2 (тоже самое что "1000&500" → "1000500")<br>gp5 = "Добро пожаловать!" (текстовое значение)<br>gp6 = gp3 / 10 (→ 150)<br>gp7 = gp6 - gp2 (→ -350)<br>В ячейке можно записывать несколько действий.<br>Каждое новое действие — с новой строки.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
    { name: 'Действия с ПВС', class: 'th-orange', hint: 'Действия с ПВС пользователя. ПВС привязана к пользователю, но доступна во всех сообществах текущего профиля. В тексте переменные можно подставлять в формате #name#.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' }
];





// &#x1F6E1;&#xFE0F; ФУНКЦИЯ ПОКАЗА ПОЛНОЭКРАННОГО УВЕДОМЛЕНИЯ "СОХРАНЕНИЕ..."
window.showSaveOverlay = function() {
  const overlay = document.getElementById('saveOverlay');
  const dotsEl = document.getElementById('saveDots');
  if (!overlay || !dotsEl) return;

  // Добавляем подсказку если её ещё нет
  let hintEl = document.getElementById('saveHint');
  if (!hintEl) {
    hintEl = document.createElement('div');
    hintEl.id = 'saveHint';
    hintEl.style.cssText = 'position:absolute;top:calc(50% + 118px);font-size:28px;font-weight:900;color:#ef4444;text-align:center;max-width:88%;line-height:1.45;text-shadow:1px 1px 2px rgba(0,0,0,0.12);';
    hintEl.textContent = 'Если данные не обновились, то обновите страницу F5 или Ctrl + F5';
    overlay.appendChild(hintEl);
  }

  overlay.classList.add('show');
  
  // Анимация точек (4 секунды, по 1 секунде на цикл)
  let cycleCount = 0;
  const maxCycles = 4;
  
  const animateDots = () => {
    if (cycleCount >= maxCycles) {
      overlay.classList.remove('show');
      return;
    }
    
    // Исчезновение и появление точек (1 секунда)
    let dotCount = 0;
    const dotInterval = setInterval(() => {
      dotsEl.textContent = '.'.repeat(5 - dotCount);
      dotCount++;
      
      if (dotCount > 5) {
        dotCount = 0;
        cycleCount++;
        clearInterval(dotInterval);
        if (cycleCount < maxCycles) {
          setTimeout(animateDots, 100);
        } else {
          overlay.classList.remove('show');
        }
      }
    }, 200);
  };
  
  // Запуск анимации через 1 секунду после появления
  setTimeout(animateDots, 1000);
  
  // Максимум 5 секунд
  setTimeout(() => {
    overlay.classList.remove('show');
  }, 5000);
};





// ✅ ПОЛНОЭКРАННОЕ УВЕДОМЛЕНИЕ ДЛЯ АВТОНАСТРОЙКИ (процесс)
window.showAutoSetupOverlay = function() {
  const existing = document.getElementById('autoSetupOverlay');
  if (existing) existing.remove();

  const overlay = document.createElement('div');
  overlay.id = 'autoSetupOverlay';
  overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(156,39,176,0.3);z-index:999998;display:flex;align-items:center;justify-content:center;flex-direction:column;';

  const text = document.createElement('div');
  text.style.cssText = 'font-size:48px;font-weight:bold;color:#4a148c;text-shadow:2px 2px 4px rgba(0,0,0,0.2);letter-spacing:3px;';
  text.textContent = 'АВТОНАСТРОЙКА';

  const dots = document.createElement('div');
  dots.id = 'autoDots';
  dots.style.cssText = 'font-size:48px;font-weight:bold;color:#4a148c;margin-left:10px;';
  dots.textContent = '.....';

  overlay.appendChild(text);
  overlay.appendChild(dots);
  document.body.appendChild(overlay);

  // Анимация точек
  let cycleCount = 0;
  const maxCycles = 4;

  const animateDots = function() {
    // ✅ НЕ закрываем оверлей - он закроется только когда завершится запрос
    let dotCount = 0;
    const dotInterval = setInterval(function() {
      dots.textContent = '.'.repeat(5 - dotCount);
      dotCount++;
      if (dotCount > 5) {
        dotCount = 0;
      }
    }, 200);
  };

  setTimeout(animateDots, 1000);
  // ✅ НЕ закрываем автоматически - закроется когда завершится запрос
};

// ✅ ПОЛНОЭКРАННОЕ УВЕДОМЛЕНИЕ ОБ УСПЕХЕ (5 секунд)
window.showSuccessOverlay = function(message) {
  const existing = document.getElementById('successOverlay');
  if (existing) existing.remove();

  const overlay = document.createElement('div');
  overlay.id = 'successOverlay';
  overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:var(--save-overlay-bg);z-index:999999;display:flex;align-items:center;justify-content:center;flex-direction:column;padding:20px;box-sizing:border-box;backdrop-filter:blur(10px);';

  const content = document.createElement('div');
  content.style.cssText = 'background:var(--modal-bg);color:var(--text-primary);padding:40px;border-radius:20px;border:1px solid var(--section-border);box-shadow:var(--container-shadow);max-width:600px;width:90%;text-align:center;';
  content.innerHTML = message;

  overlay.appendChild(content);
  document.body.appendChild(overlay);

  // ✅ Скрываем через 5 секунд
  setTimeout(function() {
    if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
  }, 5000);
};






function syncDataFromTable(tab) {
    const table = document.getElementById('table-' + tab);
    if (!table) return;

    const rows = table.querySelectorAll('tbody tr');
    rows.forEach((tr) => {
        // ВАЖНО: используем data-idx для правильного маппинга DOM → dataStore
        const firstInput = tr.querySelector('input[data-idx], textarea[data-idx]');
        const rowIdx = firstInput ? parseInt(firstInput.getAttribute('data-idx'), 10) : -1;
        if (rowIdx < 0 || !dataStore[tab] || !dataStore[tab][rowIdx]) return;

        const cells = tr.querySelectorAll('td');
        cells.forEach((cell) => {
            // Обрабатываем текстовые поля и textarea
            const input = cell.querySelector('input.editable-cell, textarea.editable-cell');
            if (input) {
                const name = input.getAttribute('data-name');
                const value = input.value;
                if (name) {
                    dataStore[tab][rowIdx][name] = value;
                }
            }

            // Обрабатываем выпадающие списки (включая цветные с ссылками)
            const select = cell.querySelector('select.color-select');
            if (select) {
                const name = select.getAttribute('data-name');
                const color = select.value;
                const linkInput = cell.querySelector('.link-input');
                const linkValue = linkInput ? linkInput.value : undefined;
                if (name) {
                    if (linkValue !== undefined) {
                        dataStore[tab][rowIdx][name] = color + '||' + linkValue;
                    } else {
                        dataStore[tab][rowIdx][name] = color;
                    }
                }
            }
        });
    });
}






function generateAnswerButtonColumns(startIdx, endIdx) {
const cols = [];
for (let i = startIdx; i <= endIdx; i++) {
const colorIdx = (i - 1) % buttonColors.length;
cols.push({ name: 'Кнопка Ответа-' + i, class: buttonColors[colorIdx], hint: 'Текст кнопки ' + i, section: 'КНОПКИ В ОТВЕТЕ' });
cols.push({ name: 'Цвет/Ссылка Ответа-' + i, class: buttonColors[colorIdx], type: 'select', options: ['красный','зелёный','синий','белый','ССЫЛКА...'], hint: 'Цвет или ссылка', section: 'КНОПКИ В ОТВЕТЕ' });
}
return cols;
}
function generateFallbackButtonColumns(startIdx, endIdx) {
const cols = [];
for (let i = startIdx; i <= endIdx; i++) {
const colorIdx = (i - 1) % fallbackButtonColors.length;
cols.push({ name: 'Кнопка ЗО-' + i, class: fallbackButtonColors[colorIdx], hint: 'Текст кнопки ЗО ' + i, section: 'КНОПКИ В ЗАГОТОВЛЕННОМ ОТВЕТЕ' });
cols.push({ name: 'Цвет/Ссылка ЗО-' + i, class: fallbackButtonColors[colorIdx], type: 'select', options: ['красный','зелёный','синий','белый','ССЫЛКА...'], hint: 'Цвет или ссылка для ЗО', section: 'КНОПКИ В ЗАГОТОВЛЕННОМ ОТВЕТЕ' });
}
return cols;
}






const columns = {
'Messages': [...messagesColumns],



'Comments': [
{ name: 'Бот', class: 'th-green', hint: 'Название бота, которому принадлежит эта строка комментариев. Используйте, когда в комментариях работает несколько разных сценариев.', section: 'ОСНОВНОЕ' },
{ name: 'Шаг', class: 'th-green', hint: 'Шаг сценария внутри выбранного бота. Помогает строить цепочки ответов и переводить пользователя между состояниями.', section: 'ОСНОВНОЕ' },
{ name: 'Триггер', class: 'th-red-1', hint: 'Текст комментария, на который бот должен отреагировать. Можно использовать слово, фразу или несколько вариантов.', section: 'ОСНОВНОЕ' },
{ name: 'Ответ', class: 'th-blue-1', hint: 'Сообщение, которое бот отправит в ответ на комментарий. Можно вставлять переменные и переносы строк.', section: 'ОСНОВНОЕ' },
{ name: 'Вложения к ответу', class: 'th-blue-1', hint: 'Вложения, которые будут приложены к ответу под постом: фото, документы, видео и другие attachment ID.', section: 'ОСНОВНОЕ' },
{ name: 'Пост', class: 'th-brown', hint: 'Ограничение по посту. Можно указать один пост, несколько постов через новую строку или значение «ВСЕ», если правило должно работать под любым постом.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
{ name: 'Отметили', class: 'th-brown', type: 'select', options: ['', 'ДА', 'НЕТ'], hint: 'Проверка на упоминание сообщества в комментарии. Помогает разделять обычные комментарии и комментарии с обращением к сообществу.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
{ name: 'Точно/Не точно', class: 'th-brown', type: 'select', options: ['ТОЧНО', 'НЕ ТОЧНО'], section: 'УСЛОВИЯ ПРОВЕРКИ' },
{ name: 'Регистр', class: 'th-brown', type: 'select', options: ['важно', 'не важно'], section: 'УСЛОВИЯ ПРОВЕРКИ' },
{ name: 'Ответить если в Группе', class: 'th-brown', hint: 'Ограничение по группам пользователя. Если поле заполнено, правило сработает только для участников указанных групп.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
{ name: 'Ответил на Шаг', class: 'th-brown', hint: 'Проверка текущего шага пользователя. Нужна, если комментарий должен обрабатываться только в конкретном состоянии сценария.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
{ name: 'Пользовательская', class: 'th-brown', hint: 'Проверка по пользовательским переменным автора комментария. Используйте, когда логика зависит от персональных данных пользователя.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
{ name: 'Глобальная', class: 'th-brown', hint: 'Проверка по глобальным переменным сообщества. Подходит для общих флагов и состояний, которые действуют для всех пользователей.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
{ name: 'Переменная ПВС', class: 'th-brown', hint: 'Проверка по ПВС пользователя. Эта переменная привязана к пользователю и доступна во всех сообществах его профиля.', section: 'УСЛОВИЯ ПРОВЕРКИ' },
{ name: 'Задержка отправки на Шаг', class: 'th-orange-dark', hint: 'Если после ответа нужно перевести пользователя дальше не сразу, а через время, задайте здесь задержку.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
{ name: 'Отправить на Шаг', class: 'th-orange-dark', hint: 'Следующий шаг, на который нужно перевести пользователя после обработки комментария.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
{ name: 'ДОБАВИТЬ ГРУППУ', class: 'th-orange', hint: 'Какие группы выдать пользователю после обработки комментария.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
{ name: 'УДАЛИТЬ ГРУППУ', class: 'th-orange', hint: 'Из каких групп убрать пользователя после обработки комментария.', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
{ name: 'Действия с ПП', class: 'th-orange', hint: 'ПП: pp1 = 100 | pp2 = 20 + 30 | name = "Текст + Равно"', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
{ name: 'Действия с ГП', class: 'th-orange', hint: 'ГП: gp1 = 1000 | sum = gp1 + 50 | msg = "Добро пожаловать!"', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
{ name: 'Действия с ПВС', class: 'th-orange', hint: 'ПВС: pvs1 = 100 | promo = "Скидка"', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' },
{ name: 'Заготовленный ответ', class: 'th-pink', hint: 'Fallback ответ', section: 'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ' }
],

'Triggers': [
{ name: 'Название', class: 'th-green', hint: 'Название триггера' },
{ name: 'Тип события', class: 'th-red-1', hint: 'Верхний уровень: Сообщения / Записи на стене / Пользователи' },
{ name: 'Раздел события', class: 'th-red-1', hint: 'Второй уровень выбранного события' },
{ name: 'Событие', class: 'th-red-1', hint: 'Конкретное отслеживаемое событие' },
{ name: 'Код события', class: 'th-red-1', hint: 'Служебный код события' },
{ name: 'Условие', class: 'th-brown', hint: 'Основное условие срабатывания' },
{ name: 'Код условия', class: 'th-brown', hint: 'Служебный код условия' },
{ name: 'Значение', class: 'th-brown', hint: 'Текст, число, regex или ссылки на записи' },
{ name: 'Доп. условие', class: 'th-brown', hint: 'Дополнительное условие для комментариев на стене' },
{ name: 'Код доп. условия', class: 'th-brown', hint: 'Служебный код дополнительного условия' },
{ name: 'Доп. значение', class: 'th-brown', hint: 'Текст для дополнительного условия' },
{ name: 'Действие', class: 'th-orange', hint: 'Какое действие выполнить после срабатывания' },
{ name: 'Код действия', class: 'th-orange', hint: 'Служебный код действия' },
{ name: 'Группа', class: 'th-orange', hint: 'Группа для добавления или исключения' },
{ name: 'Бот', class: 'th-green', hint: 'Бот для действия' },
{ name: 'Шаг', class: 'th-green', hint: 'Шаг выбранного бота' },
{ name: 'Название переменной', class: 'th-orange', hint: 'Название ПП, ГП или ПВС для действия триггера' },
{ name: 'Значение переменной', class: 'th-orange', hint: 'Значение переменной для действия триггера' },
{ name: 'Активен', class: 'th-pink', hint: 'ДА = триггер отслеживается, НЕТ = карточка отключена' },
{ name: 'Не применять остальные правила', class: 'th-pink', type: 'select', options: ['', 'ДА'], hint: 'Остановить дальнейшую обработку триггеров' }
],





'Users': [
{ name: 'ID', class: 'th-yellow', hint: 'VK ID пользователя. По нему бот понимает, кому принадлежит запись, и использует его для отправки сообщений и поиска данных.', section: 'ОСНОВНОЕ' },
{ name: 'ИМЯ', class: 'th-green', hint: 'Имя пользователя для удобного просмотра базы. Это отображаемое значение, чтобы быстрее находить человека без ручной проверки по ID.', section: 'ОСНОВНОЕ' },
{ name: 'ГРУППА', class: 'th-orange', hint: 'Список групп, в которых сейчас состоит пользователь. Можно указывать несколько групп через запятую или новую строку.', section: 'ОСНОВНОЕ' },
{ name: 'Пользовательская', class: 'th-orange', hint: 'Имена пользовательских переменных, которые есть у этого пользователя. Используется как краткая сводка по доступным ПП.', section: 'ПЕРЕМЕННЫЕ' },
{ name: 'Значения ПП', class: 'th-orange', hint: 'Значения пользовательских переменных. Обычно хранятся парами вида name=value, чтобы было понятно текущее состояние пользователя.', section: 'ПЕРЕМЕННЫЕ' },
{ name: 'Переменная ПВС', class: 'th-orange', hint: 'Имя ПВС пользователя. Это межсообщественная переменная, которая привязана к конкретному пользователю и доступна во всех сообществах профиля.', section: 'ПЕРЕМЕННЫЕ' },
{ name: 'Значение ПВС', class: 'th-orange', hint: 'Значение ПВС пользователя. Оно общее для этого пользователя во всех сообществах текущего профиля.', section: 'ПЕРЕМЕННЫЕ' },
{ name: 'Текущий Бот', class: 'th-green', hint: 'Какой бот сейчас считается активным для пользователя. Полезно, когда в одном сообществе работает несколько независимых сценариев.', section: 'СЦЕНАРИЙ' },
{ name: 'Текущий Шаг', class: 'th-green', hint: 'На каком шаге пользователь находится сейчас. Именно от этого шага зависит дальнейшая логика ответов.', section: 'СЦЕНАРИЙ' },
{ name: 'Отправленные Шаги', class: 'th-blue-1', hint: 'Служебная история шагов, которые уже были отправлены пользователю. Помогает не дублировать шаги и понимать, что уже выполнялось.', section: 'СИСТЕМНОЕ' }
],

'Groups': [
{ name: 'Группа', class: 'th-green', hint: 'Название группы. Это логическая метка для сегментации пользователей, правил и рассылок.' },
{ name: 'Описание', class: 'th-blue-1', hint: 'Понятное описание группы: что это за группа, зачем она нужна и кто в неё должен попадать.' }
],



'Variables_User': [
{ name: 'Пользовательская', class: 'th-orange', hint: 'Имя пользовательской переменной. Это шаблон ПП, который затем может использоваться у разных пользователей с разными значениями.' }
],

'Variables': [
{ name: 'Глобальная', class: 'th-orange', hint: 'Имя глобальной переменной текущего сообщества. Эти значения общие для всех пользователей именно этого сообщества.' },
{ name: 'Значение ГП', class: 'th-orange', hint: 'Текущее значение глобальной переменной. Его можно читать в сообщениях, комментариях, триггерах и действиях.' }
],

'Shared_Variables': [
{ name: 'Переменная ПВС', class: 'th-orange', hint: 'Имя ПВС пользователей профиля. Здесь показываются названия межсообщественных пользовательских переменных, встречающихся в текущем профиле.' },
{ name: 'Значение ПВС', class: 'th-orange', hint: 'Актуальные значения этой ПВС по профилю. Сохраняется в structured catalog и используется как справочник.' }
],

'VK_Variables': [
{ name: 'Переменная ВК', class: 'th-green', hint: 'Системная переменная ВК' },
{ name: 'Описание', class: 'th-green', hint: 'Описание переменной ВК' }
],


'Mailing': (function() {
    var base = [
        { name: '№', class: 'th-yellow', hint: 'Порядковый номер рассылки в таблице. Нужен для удобной навигации и поиска конкретной отправки.', section: 'ОСНОВНОЕ' },
        { name: 'ID Получателей', class: 'th-orange', hint: 'Список конкретных VK ID получателей. Если поле заполнено, рассылка уйдёт именно этим пользователям. Можно указывать через запятую или с новой строки.', section: 'ОСНОВНОЕ' },
        { name: 'ГРУППА Получателей', class: 'th-orange', hint: 'Список групп, участникам которых должна уйти рассылка. Удобно для сегментированных рассылок по базе.', section: 'ОСНОВНОЕ' },
        { name: 'Сообщение Рассылки', class: 'th-green', hint: 'Текст, который будет отправлен получателям рассылки. Можно использовать переменные и переносы строк.', section: 'ОСНОВНОЕ' },
        { name: 'Вложение к рассылке', class: 'th-blue-1', hint: 'Вложения, которые будут приложены к рассылке. Указывайте attachment ID через запятую или с новой строки.', section: 'ОСНОВНОЕ' }
    ];
    var rest = [
        { name: 'Дата и время отправки (по мск.)', class: 'th-brown', hint: 'Когда рассылка должна стартовать по московскому времени. Формат: YYYY-MM-DD HH:MM:SS.', section: 'УПРАВЛЕНИЕ' },
        { name: 'Статус', class: 'th-brown', type: 'select', options: ['Ожидает', 'Отправлено', 'Ошибка'], hint: 'Текущее состояние рассылки: ещё ожидает, уже отправлена или завершилась ошибкой.', section: 'УПРАВЛЕНИЕ' },
        { name: 'Фактическое время отправки (по мск.)', class: 'th-brown', hint: 'Фактическое время, когда рассылка реально была обработана системой. Обычно заполняется автоматически.', section: 'УПРАВЛЕНИЕ' },
        { name: 'Ошибка', class: 'th-red-1', hint: 'Текст последней ошибки, если отправка не удалась. Это поле помогает понять причину сбоя.', section: 'УПРАВЛЕНИЕ' }
    ];
    return base.concat(rest);
})(),



'Delayed': [
{ name: '№', class: 'th-yellow', hint: 'Порядковый номер отложенной задачи. Помогает быстро найти нужную запись в очереди.', section: 'ОСНОВНОЕ' },
{ name: 'Шаг', class: 'th-green', hint: 'Какой шаг будет отправлен или обработан по расписанию.', section: 'ОСНОВНОЕ' },
{ name: 'ID Пользователя', class: 'th-orange', hint: 'Кому предназначена отложенная отправка. Это VK ID конкретного пользователя.', section: 'ОСНОВНОЕ' },
{ name: 'Группа', class: 'th-orange', hint: 'Служебная группа или сообщество, к которому относится задача. Помогает понять контекст отложенного действия.', section: 'ОСНОВНОЕ' },
{ name: 'Тип', class: 'th-brown', type: 'select', options: ['message', 'comment'], hint: 'Тип отложенной задачи: сообщение в личку или действие, связанное с комментариями.', section: 'ОСНОВНОЕ' },
{ name: 'Дата и время отправки (по мск.)', class: 'th-brown', hint: 'Запланированное время выполнения по московскому времени. Пока это время не наступило, задача будет в статусе ожидания.', section: 'УПРАВЛЕНИЕ' },
{ name: 'Статус', class: 'th-brown', type: 'select', options: ['Ожидает', 'Отправлено', 'Ошибка'], hint: 'Состояние задачи: ожидает, уже выполнена или завершилась ошибкой.', section: 'УПРАВЛЕНИЕ' },
{ name: 'Факт. время отправки (по мск.)', class: 'th-brown', hint: 'Фактический момент выполнения задачи по московскому времени. Заполняется после успешной или неуспешной обработки.', section: 'УПРАВЛЕНИЕ' },
{ name: 'Ошибка', class: 'th-red-1', hint: 'Если задача не выполнилась, здесь будет причина ошибки. Это главный ориентир для диагностики.', section: 'УПРАВЛЕНИЕ' }
]
};






const tooltip = document.getElementById('tooltip');
if (tooltip) {
    document.addEventListener('mouseover', function(e) {
        if (e.target.classList.contains('help-icon')) {
            const text = e.target.getAttribute('data-hint');
            if (text) {
                tooltip.innerHTML = text.replace(/\\n/g, '<br>');
                tooltip.classList.add('show');
            }
        }
    });
    document.addEventListener('mouseout', function(e) {
        if (e.target.classList.contains('help-icon')) {
            tooltip.classList.remove('show');
        }
    });
    document.addEventListener('mousemove', function(e) {
        if (tooltip.classList.contains('show')) {
            tooltip.style.left = (e.clientX + 10) + 'px';
            tooltip.style.top = (e.clientY + 10) + 'px';
        }
    });
  }




async function loadData(tab) {
const sheet = sheetMap[tab];
const loadingEl = document.getElementById('loading-'+tab);
if (loadingEl) loadingEl.style.display = 'block';
const internalCommunityId = window.currentCommunityId || '';
debug('📥 loadData: tab=' + tab + ', internalCommunityId=' + internalCommunityId + ', sheet=' + sheet);
try {
const baseUrl = window.location.href.split('?')[0];

// ✅ Получаем vk_group_id из настроек для загрузки правильных данных
const settingsRes = await fetch(baseUrl + '?getBotSettings');
const settingsData = await settingsRes.json();
const communityConfig = settingsData.communities?.[internalCommunityId] || {};
const vkGroupId = communityConfig.vk_group_id || internalCommunityId;

debug('📥 Using vk_group_id=' + vkGroupId + ' for data loading');

const url = baseUrl + '?sheet=' + encodeURIComponent(sheet) +
           (vkGroupId ? '&communityId=' + encodeURIComponent(vkGroupId) : '') +
           '&t=' + Date.now();

// Таймаут 15 секунд
const controller = new AbortController();
const timeoutId = setTimeout(() => controller.abort(), 15000);

const res = await fetch(url, { signal: controller.signal });
clearTimeout(timeoutId);

if (!res.ok) {
throw new Error('HTTP ' + res.status + ': ' + res.statusText);
}
const text = await res.text();
let data;
try {
data = JSON.parse(text);
} catch (e) {
throw new Error('Invalid JSON response');
}
if (Array.isArray(data)) {
data = data.map(row => {
const normalizedRow = {};
for (const [key, value] of Object.entries(row)) {
normalizedRow[key.trim()] = value;
}
return normalizedRow;
});
}
dataStore[tab] = data || [];

if (tab === 'Variables') {
    const sharedUrl = baseUrl + '?sheet=' + encodeURIComponent('ПЕРЕМЕННЫЕ ВСЕХ СООБЩЕСТВ') + '&t=' + Date.now();
    const sharedRes = await fetch(sharedUrl);
    const sharedText = await sharedRes.text();
    let sharedData = [];
    try {
        sharedData = JSON.parse(sharedText);
    } catch (e) {
        sharedData = [];
    }
    dataStore['Shared_Variables'] = Array.isArray(sharedData) ? sharedData.map(function(row) {
        return {
            'Переменная ПВС': row['Переменная ПВС'] || '',
            'Значение ПВС': row['Значение ПВС'] || ''
        };
    }) : [];
}

if (tab === 'Users') {
    const sharedUrl = baseUrl + '?sheet=' + encodeURIComponent('ПВС ПОЛЬЗОВАТЕЛЕЙ ПРОФИЛЯ') + '&t=' + Date.now();
    const sharedRes = await fetch(sharedUrl);
    const sharedText = await sharedRes.text();
    let sharedData = [];
    try {
        sharedData = JSON.parse(sharedText);
    } catch (e) {
        sharedData = [];
    }
    const sharedByUser = {};
    (Array.isArray(sharedData) ? sharedData : []).forEach(function(row) {
        const userId = String(row['ID'] || '').trim();
        const varName = String(row['Переменная ПВС'] || '').trim();
        const varValue = String(row['Значение ПВС'] || '').trim();
        if (!userId || !varName) return;
        if (!sharedByUser[userId]) {
            sharedByUser[userId] = { names: [], values: [] };
        }
        sharedByUser[userId].names.push(varName);
        sharedByUser[userId].values.push(varValue);
    });
    dataStore['Users'] = (dataStore['Users'] || []).map(function(row) {
        const userId = String(row['ID'] || '').trim();
        const sharedInfo = sharedByUser[userId] || { names: [], values: [] };
        row['Переменная ПВС'] = sharedInfo.names.join('\\n');
        row['Значение ПВС'] = sharedInfo.values.join('\\n');
        return row;
    });
    data = dataStore['Users'];
}

// &#x1F916; Извлекаем список ботов из загруженных данных для Messages/Comments
if (tab === 'Messages' || tab === 'Comments') {
    const botNames = [];
    const seen = {};
    (data || []).forEach(row => {
        const botName = row['Бот'];
        if (botName && !seen[botName]) {
            seen[botName] = true;
            botNames.push(botName);
        }
    });
    // Если сервер вернул пустые данные — очищаем ботов полностью!
    if (botNames.length === 0 && data && data.length === 0) {
        const botsKey = getBotsKey(tab);
        delete window.botsList[botsKey];
        delete window.activeBot[tab];
        saveBotsListToStorage();
        saveActiveBotToStorage();
        console.log('[Admin] 🤖 loadData: server returned empty data, cleared bots for ' + botsKey);
        renderBotButtons(tab);
    } else {
        // ОБЪЕДИНЯЕМ ботов из localStorage и с сервера чтобы не потерять ни одного!
        const existingBots = getBotsForTab(tab);
        const allBots = [...new Set([...existingBots, ...botNames])];
        setBotsForTab(tab, allBots);
        console.log('[Admin] 🤖 loadData: existingBots=' + JSON.stringify(existingBots) + ', fromServer=' + JSON.stringify(botNames) + ', merged=' + JSON.stringify(allBots));
        // Устанавливаем активного бота ТОЛЬКО если ещё не установлен
        // ИЛИ если текущий activeBot больше не существует в списке
        if (allBots.length > 0) {
            const currentActive = window.activeBot[tab];
            if (!currentActive || !allBots.includes(currentActive)) {
                setActiveBot(tab, allBots[0]);
                debug('🤖 loadData: установлен первый бот: ' + allBots[0]);
            } else {
                debug('🤖 loadData: сохранён активный бот: ' + currentActive);
            }
        }
        renderBotButtons(tab);
    }
}

renderTable(tab, data);
if (tab === 'Triggers') {
    renderStructuredTriggersTab();
}

// ✅ Рендерим панель фильтров для Users
if (tab === 'Users') {
    setTimeout(function() { renderUserFilters(); }, 200);
}

// ✅ Обновляем метки с правильным VK_GROUP_ID из настроек (используем уже полученные данные)
const communityName = communityConfig.group_name || internalCommunityId;
debug('🏷️ loadData: обновляем метки: name=' + communityName + ', vkGroupId=' + vkGroupId);
updateCommunityLabels(internalCommunityId, communityName, vkGroupId);
debug('\uD83E\uDD16 Loaded ' + (data?.length || 0) + ' rows');
// Авто-заполнение VK переменных при первом запуске
if (tab === 'Variables' && data.length === 0) {
dataStore['Variables_User'] = [];
dataStore['Variables'] = [];
renderTable('Variables_User', []);
renderTable('Variables', []);
dataStore['VK_Variables'] = [
{ 'Переменная ВК': '%username%', 'Описание': 'Имя пользователя' },
{ 'Переменная ВК': '%fullname%', 'Описание': 'Имя и фамилия пользователя' },
{ 'Переменная ВК': '%userid%', 'Описание': 'ID пользователя ВКонтакте' },
{ 'Переменная ВК': '%group%', 'Описание': 'Ссылка на сообщество' },
{ 'Переменная ВК': '%vk_date%', 'Описание': 'Текущая дата' },
{ 'Переменная ВК': '%vk_time%', 'Описание': 'Текущее время' },
{ 'Переменная ВК': '%unsubscribe%', 'Описание': 'Ссылка на отписку' },
{ 'Переменная ВК': '%city%', 'Описание': 'Город пользователя' },
{ 'Переменная ВК': '%country%', 'Описание': 'Страна пользователя' },
{ 'Переменная ВК': '%gender%', 'Описание': 'Пол пользователя' },
{ 'Переменная ВК': '%ref%', 'Описание': 'UTM метка перехода' },
{ 'Переменная ВК': '%utm_source%', 'Описание': 'UTM source' },
{ 'Переменная ВК': '%utm_medium%', 'Описание': 'UTM medium' },
{ 'Переменная ВК': '%utm_campaign%', 'Описание': 'UTM campaign' }
];
renderTable('VK_Variables', dataStore['VK_Variables']);
renderTable('Shared_Variables', dataStore['Shared_Variables'] || []);
}

// Разделяем данные Variables и VK_Variables при загрузке
if (tab === 'Variables' && data.length > 0) {
const userVars = [];
const mainVars = [];
const vkVars = [];
data.forEach(row => {
if (row['Переменные ВК'] || row['Переменная ВК']) {
vkVars.push({
'Переменная ВК': row['Переменные ВК'] || row['Переменная ВК'] || '',
'Описание': row['Значение/Описание ПВК'] || row['Описание'] || ''
});
} else {
    if (row['Пользовательская']) {
        userVars.push({
            'Пользовательская': row['Пользовательская'] || ''
        });
    }
    if (row['Глобальная'] || row['Значение ГП']) {
        mainVars.push({
        'Глобальная': row['Глобальная'] || '',
        'Значение ГП': row['Значение ГП'] || ''
        });
    }
}
});
dataStore['Variables_User'] = userVars;
dataStore['Variables'] = mainVars;
dataStore['VK_Variables'] = vkVars;
renderTable('Variables_User', userVars);
renderTable('Variables', mainVars);
renderTable('VK_Variables', vkVars);
renderTable('Shared_Variables', dataStore['Shared_Variables'] || []);
// Обновляем метки но НЕ рендерим таблицу Variables снова
const communityName2 = communityConfig.group_name || internalCommunityId;
updateCommunityLabels(internalCommunityId, communityName2, vkGroupId);
}
} catch (e) {
debug('? Error: ' + e.message);
showStatus('Ошибка загрузки: ' + e.message, 'error');
dataStore[tab] = [];
renderTable(tab, []);
if (tab === 'Triggers') {
    renderStructuredTriggersTab();
}
} finally {
if (loadingEl) loadingEl.style.display = 'none';
}
}







function escapeHtml(str) {
if (typeof str !== 'string') return '';
return str
.replace(/&/g, '&amp;')
.replace(/</g, '&lt;')
.replace(/>/g, '&gt;')
.replace(/"/g, '&quot;')
.replace(/'/g, '&#39;');
}



const STRUCTURED_TRIGGER_CATALOG = {
    messages: {
        label: 'Сообщения',
        sections: {
            incoming: {
                label: 'Входящее сообщение',
                events: {
                    incoming_message: { label: 'Входящее сообщение', mode: 'message' }
                }
            },
            outgoing: {
                label: 'Исходящее сообщение',
                events: {
                    outgoing_message: { label: 'Исходящее сообщение', mode: 'message' }
                }
            },
            buttons: {
                label: 'Нажал на кнопку сообщения',
                events: {
                    message_button_click: { label: 'Нажал на кнопку сообщения', mode: 'button' }
                }
            }
        }
    },
    wall: {
        label: 'Записи на стене',
        sections: {
            repost: {
                label: 'Репост записи',
                events: {
                    wall_repost: { label: 'Репост записи', mode: 'post' }
                }
            },
            like: {
                label: 'Лайк записи',
                events: {
                    wall_like: { label: 'Лайк записи', mode: 'post' }
                }
            },
            comments: {
                label: 'Комментарии на стене',
                events: {
                    wall_comment_add: { label: 'Добавление комментария к записи', mode: 'wall_comment' },
                    wall_comment_delete: { label: 'Удаление комментария к записи', mode: 'wall_comment' }
                }
            }
        }
    },
    users: {
        label: 'Пользователи',
        sections: {
            join: {
                label: 'Вступление в сообщество',
                events: {
                    user_group_join: { label: 'Вступление в сообщество', mode: 'none' }
                }
            },
            request: {
                label: 'Заявка на вступление в сообщество',
                events: {
                    user_group_request: { label: 'Заявка на вступление в сообщество', mode: 'join_request' }
                }
            },
            leave: {
                label: 'Выход из сообщества',
                events: {
                    user_group_leave: { label: 'Выход из сообщества', mode: 'none' }
                }
            }
        }
    }
};

const STRUCTURED_TRIGGER_MESSAGE_CONDITIONS = [
    { value: 'any_message', label: 'Любое сообщение' },
    { value: 'text_equals', label: 'Сообщение равно', needsValue: true },
    { value: 'text_not_equals', label: 'Сообщение не равно', needsValue: true },
    { value: 'text_contains', label: 'Сообщение содержит', needsValue: true },
    { value: 'text_not_contains', label: 'Сообщение не содержит', needsValue: true },
    { value: 'text_contains_user_var', label: 'Сообщение содержит Пользовательскую ПП', needsParam: true, paramLabel: 'Имя Пользовательской ПП', paramPlaceholder: 'Например: promo_text' },
    { value: 'text_contains_global_var', label: 'Сообщение содержит Глобальную ГП', needsParam: true, paramLabel: 'Имя Глобальной ГП', paramPlaceholder: 'Например: banner_text' },
    { value: 'text_contains_shared_var', label: 'Сообщение содержит Переменную ПВС', needsParam: true, paramLabel: 'Имя Переменной ПВС', paramPlaceholder: 'Например: profile_offer' },
    { value: 'text_regex', label: 'Сообщение соответствует регулярному выражению', needsValue: true },
    { value: 'phone_ru', label: 'Сообщение - телефон российского формата' },
    { value: 'email', label: 'Сообщение - E-mail' },
    { value: 'number', label: 'Сообщение - Число' },
    { value: 'number_less_than', label: 'Сообщение меньше числа', needsValue: true },
    { value: 'number_greater_than', label: 'Сообщение больше числа', needsValue: true },
    { value: 'message_has_photo', label: 'Сообщение содержит фотографию' },
    { value: 'message_has_video', label: 'Сообщение содержит видеозапись' },
    { value: 'message_has_audio', label: 'Сообщение содержит аудиозапись' },
    { value: 'message_has_document', label: 'Сообщение содержит документ' },
    { value: 'message_has_voice', label: 'Сообщение содержит голосовое сообщение' },
    { value: 'message_has_product', label: 'Сообщение содержит товар/услугу' }
];

const STRUCTURED_TRIGGER_BUTTON_CONDITIONS = [
    { value: 'any_button', label: 'Любая' },
    { value: 'button_label_equals', label: 'Указать название кнопки', needsValue: true }
];

const STRUCTURED_TRIGGER_POST_CONDITIONS = [
    { value: 'any_post', label: 'Любая' },
    { value: 'post_links_match', label: 'Указать ссылки на записи', needsValue: true, placeholder: 'Одна ссылка на строку' }
];

const STRUCTURED_TRIGGER_COMMENT_TEXT_CONDITIONS = [
    { value: 'any_comment_text', label: 'Любой' },
    { value: 'comment_text_contains', label: 'Текст содержит', needsValue: true }
];

const STRUCTURED_TRIGGER_JOIN_REQUEST_CONDITIONS = [
    { value: 'any_request_condition', label: 'Без дополнительного условия' },
    { value: 'shared_var_equals', label: 'Переменная ПВС равна', needsParam: true, needsValue: true, paramLabel: 'Имя Переменной ПВС', paramPlaceholder: 'Например: invite_status' },
    { value: 'shared_var_not_equals', label: 'Переменная ПВС не равна', needsParam: true, needsValue: true, paramLabel: 'Имя Переменной ПВС', paramPlaceholder: 'Например: invite_status' }
];

const STRUCTURED_TRIGGER_ACTIONS = [
    { value: 'add_group', label: 'Добавить в группу' },
    { value: 'remove_group', label: 'Исключить из группы' },
    { value: 'add_to_bot', label: 'Добавить в бота' },
    { value: 'send_bot_answer', label: 'Отправить ответ с бота' },
    { value: 'remove_from_bot', label: 'Исключить из бота' },
    { value: 'approve_group_request', label: 'Одобрить заявку в Сообщество' },
    { value: 'remove_from_community', label: 'Удалить пользователя из сообщества' },
    { value: 'delete_user_data', label: 'Удалить данные пользователя' },
    { value: 'delete_user_conversation', label: 'Удалить переписку с пользователем' },
    { value: 'delete_user_data_and_conversation', label: 'Удалить данные пользователя и переписку' },
    { value: 'user_var_add', label: 'ПП: Добавить' },
    { value: 'user_var_update', label: 'ПП: Изменить' },
    { value: 'user_var_delete', label: 'ПП: Удалить' },
    { value: 'global_var_add', label: 'ГП: Добавить' },
    { value: 'global_var_update', label: 'ГП: Изменить' },
    { value: 'global_var_delete', label: 'ГП: Удалить' },
    { value: 'shared_var_add', label: 'ПВС: Добавить' },
    { value: 'shared_var_update', label: 'ПВС: Изменить' },
    { value: 'shared_var_delete', label: 'ПВС: Удалить' }
];

window.structuredTriggerEditIndex = -1;
window.structuredTriggerBuilderVisible = false;
window.structuredTriggerDraftState = null;

function getDefaultStructuredTriggerAction() {
    return {
        action: 'add_group',
        actionGroup: '',
        actionBot: '',
        actionStep: '',
        actionCommunityId: '',
        actionVarName: '',
        actionVarValue: ''
    };
}

function normalizeStructuredTriggerActions(rawActions, rawState) {
    var actions = Array.isArray(rawActions) ? rawActions : [];
    if (!actions.length) {
        actions = [Object.assign({}, getDefaultStructuredTriggerAction(), {
            action: rawState?.action || 'add_group',
            actionGroup: rawState?.actionGroup || '',
            actionBot: rawState?.actionBot || '',
            actionStep: rawState?.actionStep || '',
            actionCommunityId: rawState?.actionCommunityId || '',
            actionVarName: rawState?.actionVarName || '',
            actionVarValue: rawState?.actionVarValue || ''
        })];
    }

    return actions.map(function(item) {
        var normalized = Object.assign({}, getDefaultStructuredTriggerAction(), item || {});
        if (!STRUCTURED_TRIGGER_ACTIONS.some(function(option) { return option.value === normalized.action; })) {
            normalized.action = 'add_group';
        }
        normalized.actionGroup = String(normalized.actionGroup || '');
        normalized.actionBot = String(normalized.actionBot || '');
        normalized.actionStep = String(normalized.actionStep || '');
        normalized.actionCommunityId = String(normalized.actionCommunityId || '');
        normalized.actionVarName = String(normalized.actionVarName || '');
        normalized.actionVarValue = String(normalized.actionVarValue || '');
        return normalized;
    });
}

function shouldStructuredTriggerConditionShowParam(condition) {
    return !!(condition && condition.needsParam);
}

function shouldStructuredTriggerActionShowGroup(actionCode) {
    return actionCode === 'add_group' || actionCode === 'remove_group';
}

function shouldStructuredTriggerActionShowBot(actionCode) {
    return actionCode === 'add_to_bot' || actionCode === 'send_bot_answer' || actionCode === 'remove_from_bot';
}

function shouldStructuredTriggerActionShowStep(actionCode) {
    return actionCode === 'add_to_bot' || actionCode === 'send_bot_answer';
}

function shouldStructuredTriggerActionShowCommunity(actionCode) {
    return actionCode === 'approve_group_request' || actionCode === 'remove_from_community';
}

function shouldStructuredTriggerActionShowVarName(actionCode) {
    return /_var_/.test(actionCode);
}

function shouldStructuredTriggerActionShowVarValue(actionCode) {
    return /_var_(add|update)$/.test(actionCode);
}

function getStructuredTriggerOptions(list) {
    return Array.isArray(list) ? list : [];
}

function getStructuredTriggerCategories() {
    return Object.keys(STRUCTURED_TRIGGER_CATALOG).map(function(key) {
        return { value: key, label: STRUCTURED_TRIGGER_CATALOG[key].label };
    });
}

function getStructuredTriggerSections(category) {
    var config = STRUCTURED_TRIGGER_CATALOG[category] || null;
    if (!config) return [];
    return Object.keys(config.sections).map(function(key) {
        return { value: key, label: config.sections[key].label };
    });
}

function getStructuredTriggerEvents(category, section) {
    var sectionConfig = STRUCTURED_TRIGGER_CATALOG[category]?.sections?.[section] || null;
    if (!sectionConfig) return [];
    return Object.keys(sectionConfig.events).map(function(key) {
        return { value: key, label: sectionConfig.events[key].label, mode: sectionConfig.events[key].mode };
    });
}

function findStructuredTriggerLocation(eventCode) {
    var found = null;
    Object.keys(STRUCTURED_TRIGGER_CATALOG).forEach(function(categoryKey) {
        var category = STRUCTURED_TRIGGER_CATALOG[categoryKey];
        Object.keys(category.sections).forEach(function(sectionKey) {
            var section = category.sections[sectionKey];
            Object.keys(section.events).forEach(function(code) {
                if (code === eventCode) {
                    found = {
                        category: categoryKey,
                        section: sectionKey,
                        event: code,
                        categoryLabel: category.label,
                        sectionLabel: section.label,
                        eventLabel: section.events[code].label,
                        mode: section.events[code].mode
                    };
                }
            });
        });
    });
    return found;
}

function getStructuredTriggerDefinition(category, section, eventCode) {
    return findStructuredTriggerLocation(eventCode) || {
        category: category,
        section: section,
        event: eventCode,
        categoryLabel: '',
        sectionLabel: '',
        eventLabel: '',
        mode: 'message'
    };
}

function getStructuredTriggerConditionOptions(mode) {
    if (mode === 'button') return STRUCTURED_TRIGGER_BUTTON_CONDITIONS;
    if (mode === 'post') return STRUCTURED_TRIGGER_POST_CONDITIONS;
    if (mode === 'wall_comment') return STRUCTURED_TRIGGER_POST_CONDITIONS;
    if (mode === 'join_request') return STRUCTURED_TRIGGER_JOIN_REQUEST_CONDITIONS;
    if (mode === 'none') return [];
    return STRUCTURED_TRIGGER_MESSAGE_CONDITIONS;
}

function getStructuredTriggerDefaultState() {
    return {
        title: '',
        category: 'messages',
        section: 'incoming',
        event: 'incoming_message',
        condition: 'any_message',
        conditionParam: '',
        value: '',
        extraCondition: 'any_comment_text',
        extraValue: '',
        actions: [getDefaultStructuredTriggerAction()],
        active: false,
        stopFurther: false
    };
}

function normalizeStructuredTriggerState(rawState) {
    var state = Object.assign({}, getStructuredTriggerDefaultState(), rawState || {});
    var categories = getStructuredTriggerCategories();
    if (!categories.some(function(item) { return item.value === state.category; })) {
        state.category = categories[0] ? categories[0].value : 'messages';
    }

    var sections = getStructuredTriggerSections(state.category);
    if (!sections.some(function(item) { return item.value === state.section; })) {
        state.section = sections[0] ? sections[0].value : '';
    }

    var events = getStructuredTriggerEvents(state.category, state.section);
    if (!events.some(function(item) { return item.value === state.event; })) {
        state.event = events[0] ? events[0].value : '';
    }

    var definition = getStructuredTriggerDefinition(state.category, state.section, state.event);
    var conditionOptions = getStructuredTriggerConditionOptions(definition.mode);
    if (conditionOptions.length > 0 && !conditionOptions.some(function(item) { return item.value === state.condition; })) {
        state.condition = conditionOptions[0].value;
    }
    if (conditionOptions.length === 0) {
        state.condition = '';
        state.conditionParam = '';
        state.value = '';
    }

    if (definition.mode !== 'wall_comment') {
        state.extraCondition = '';
        state.extraValue = '';
    } else if (!STRUCTURED_TRIGGER_COMMENT_TEXT_CONDITIONS.some(function(item) { return item.value === state.extraCondition; })) {
        state.extraCondition = 'any_comment_text';
    }

    state.title = String(state.title || '');
    state.conditionParam = String(state.conditionParam || '');
    state.value = String(state.value || '');
    state.extraValue = String(state.extraValue || '');
    state.actions = normalizeStructuredTriggerActions(state.actions, state);
    state.action = state.actions[0].action;
    state.actionGroup = state.actions[0].actionGroup;
    state.actionBot = state.actions[0].actionBot;
    state.actionStep = state.actions[0].actionStep;
    state.actionCommunityId = state.actions[0].actionCommunityId;
    state.actionVarName = state.actions[0].actionVarName;
    state.actionVarValue = state.actions[0].actionVarValue;
    state.active = !!state.active;
    state.stopFurther = !!state.stopFurther;
    return state;
}

function getStructuredTriggerFormState() {
    var state = Object.assign({}, getStructuredTriggerDefaultState(), window.structuredTriggerDraftState || {});
    var mappings = {
        title: 'structuredTriggerTitle',
        category: 'structuredTriggerCategory',
        section: 'structuredTriggerSection',
        event: 'structuredTriggerEvent',
        condition: 'structuredTriggerCondition',
        conditionParam: 'structuredTriggerConditionParam',
        value: 'structuredTriggerValue',
        extraCondition: 'structuredTriggerExtraCondition',
        extraValue: 'structuredTriggerExtraValue',
        action: 'structuredTriggerAction',
        actionGroup: 'structuredTriggerActionGroup',
        actionBot: 'structuredTriggerActionBot',
        actionStep: 'structuredTriggerActionStep',
        actionVarName: 'structuredTriggerActionVarName',
        actionVarValue: 'structuredTriggerActionVarValue'
    };

    Object.keys(mappings).forEach(function(key) {
        var el = document.getElementById(mappings[key]);
        if (el) state[key] = el.value;
    });

    var activeEl = document.getElementById('structuredTriggerActive');
    if (activeEl) state.active = !!activeEl.checked;
    var stopEl = document.getElementById('structuredTriggerStop');
    if (stopEl) state.stopFurther = !!stopEl.checked;

    var actionRows = Array.from(document.querySelectorAll('.structured-trigger-action-row')).map(function(rowEl) {
        var idx = rowEl.getAttribute('data-action-index');
        return {
            action: document.getElementById('structuredTriggerAction_' + idx)?.value || 'add_group',
            actionGroup: document.getElementById('structuredTriggerActionGroup_' + idx)?.value || '',
            actionBot: document.getElementById('structuredTriggerActionBot_' + idx)?.value || '',
            actionStep: document.getElementById('structuredTriggerActionStep_' + idx)?.value || '',
            actionCommunityId: document.getElementById('structuredTriggerActionCommunity_' + idx)?.value || '',
            actionVarName: document.getElementById('structuredTriggerActionVarName_' + idx)?.value || '',
            actionVarValue: document.getElementById('structuredTriggerActionVarValue_' + idx)?.value || ''
        };
    });
    if (actionRows.length) state.actions = actionRows;
    return normalizeStructuredTriggerState(state);
}

function mapLegacyStructuredTriggerRow(row) {
    var eventCode = String(row['Код события'] || row['Тип события'] || '').trim();
    var locationMap = {
        message_new: 'incoming_message',
        message_reply: 'outgoing_message',
        wall_reply_new: 'wall_comment_add',
        wall_reply_delete: 'wall_comment_delete',
        wall_repost: 'wall_repost',
        like_add: 'wall_like',
        group_join: 'user_group_join',
        group_leave: 'user_group_leave'
    };
    var conditionMap = {
        all: 'any_message',
        equals: 'text_equals',
        contains: 'text_contains',
        regex: 'text_regex',
        photo: 'message_has_photo',
        video: 'message_has_video',
        doc: 'message_has_document',
        attach: 'message_has_document'
    };
    var resolvedEvent = locationMap[eventCode] || eventCode || 'incoming_message';
    var location = findStructuredTriggerLocation(resolvedEvent) || findStructuredTriggerLocation('incoming_message');
    var rawAction = '';
    if (row['Действие']) rawAction = row['Действие'];
    else if (row['ДОБАВИТЬ ГРУППУ']) rawAction = 'add_group';
    else if (row['УДАЛИТЬ ГРУППУ']) rawAction = 'remove_group';
    else if (row['Бот'] && row['Шаг']) rawAction = 'add_to_bot';

    return normalizeStructuredTriggerState({
        title: row['Название'] || '',
        category: location.category,
        section: location.section,
        event: location.event,
        condition: conditionMap[String(row['Проверка'] || '').trim()] || 'any_message',
        conditionParam: row['Параметр условия'] || row['Имя переменной условия'] || '',
        value: row['Значение'] || '',
        extraCondition: row['Код доп. условия'] || 'any_comment_text',
        extraValue: row['Доп. значение'] || '',
        action: String(rawAction || '').trim() || 'add_group',
        actionGroup: row['Группа'] || row['ДОБАВИТЬ ГРУППУ'] || row['УДАЛИТЬ ГРУППУ'] || '',
        actionBot: row['Бот'] || '',
        actionStep: row['Шаг'] || '',
        actionCommunityId: row['ID сообщества действия'] || row['ID сообщества'] || '',
        actionVarName: row['Название переменной'] || '',
        actionVarValue: row['Значение переменной'] || '',
        active: String(row['Активен'] || 'ДА').trim().toUpperCase() !== 'НЕТ',
        stopFurther: String(row['Не применять остальные правила'] || '').trim().toUpperCase() === 'ДА'
    });
}

function getStructuredTriggerStateFromRow(row) {
    if (!row) return getStructuredTriggerDefaultState();
    if (!row['Код события']) return mapLegacyStructuredTriggerRow(row);
    var location = findStructuredTriggerLocation(String(row['Код события']).trim()) || findStructuredTriggerLocation('incoming_message');
    return normalizeStructuredTriggerState({
        title: row['Название'] || '',
        category: location.category,
        section: location.section,
        event: location.event,
        condition: row['Код условия'] || 'any_message',
        conditionParam: row['Параметр условия'] || row['Имя переменной условия'] || '',
        value: row['Значение'] || '',
        extraCondition: row['Код доп. условия'] || '',
        extraValue: row['Доп. значение'] || '',
        actions: (function() {
            try {
                var parsed = JSON.parse(String(row['Действия JSON'] || '').trim() || '[]');
                if (Array.isArray(parsed) && parsed.length) return parsed;
            } catch (e) {}
            return [{
                action: row['Код действия'] || row['Действие'] || 'add_group',
                actionGroup: row['Группа'] || '',
                actionBot: row['Бот'] || '',
                actionStep: row['Шаг'] || '',
                actionCommunityId: row['ID сообщества действия'] || row['ID сообщества'] || '',
                actionVarName: row['Название переменной'] || '',
                actionVarValue: row['Значение переменной'] || ''
            }];
        })(),
        active: String(row['Активен'] || 'ДА').trim().toUpperCase() !== 'НЕТ',
        stopFurther: String(row['Не применять остальные правила'] || '').trim().toUpperCase() === 'ДА'
    });
}

function getStructuredTriggerConditionLabel(mode, value) {
    var all = getStructuredTriggerConditionOptions(mode).concat(STRUCTURED_TRIGGER_COMMENT_TEXT_CONDITIONS);
    var item = all.find(function(option) { return option.value === value; });
    return item ? item.label : '';
}

function getStructuredTriggerActionLabel(value) {
    var item = STRUCTURED_TRIGGER_ACTIONS.find(function(option) { return option.value === value; });
    return item ? item.label : '';
}

function buildStructuredTriggerOptionTags(options, selectedValue, placeholder) {
    var html = '';
    if (placeholder) {
        html += '<option value="">' + escapeHtml(placeholder) + '</option>';
    }
    options.forEach(function(option) {
        var selected = option.value === selectedValue ? ' selected' : '';
        html += '<option value="' + escapeHtml(option.value) + '"' + selected + '>' + escapeHtml(option.label) + '</option>';
    });
    return html;
}

function collectStructuredTriggerBots(currentValue) {
    var names = [];
    ['Messages', 'Comments'].forEach(function(tab) {
        (dataStore[tab] || []).forEach(function(row) {
            var botName = String(row['Бот'] || '').trim();
            if (botName && !names.includes(botName)) names.push(botName);
        });
        (getBotsForTab(tab) || []).forEach(function(botName) {
            if (botName && !names.includes(botName)) names.push(botName);
        });
    });
    var current = String(currentValue || '').trim();
    if (current && !names.includes(current)) names.push(current);
    return names.map(function(name) { return { value: name, label: name }; });
}

function collectStructuredTriggerGroups(currentValue) {
    var names = [];

    collectAllGroups().forEach(function(name) {
        if (name && !names.includes(name)) names.push(name);
    });

    (dataStore['Triggers'] || []).forEach(function(row) {
        var groupName = String(row['Группа'] || row['ДОБАВИТЬ ГРУППУ'] || row['УДАЛИТЬ ГРУППУ'] || '').trim();
        if (groupName && !names.includes(groupName)) names.push(groupName);
    });

    var current = String(currentValue || '').trim();
    if (current && !names.includes(current)) names.push(current);

    return names.sort().map(function(name) {
        return { value: name, label: name };
    });
}

function collectStructuredTriggerSteps(botName, currentValue, includeCurrentValue) {
    var steps = [];
    ['Messages', 'Comments'].forEach(function(tab) {
        (dataStore[tab] || []).forEach(function(row) {
            var rowBot = String(row['Бот'] || '').trim();
            var step = String(row['Шаг'] || '').trim();
            if (rowBot === botName && step && !steps.includes(step)) steps.push(step);
        });
    });
    var current = String(currentValue || '').trim();
    if (includeCurrentValue && current && !steps.includes(current)) steps.push(current);
    return steps.map(function(step) { return { value: step, label: step }; });
}

function buildStructuredTriggerDetailList(items, emptyText) {
    var rows = (items || []).filter(function(item) {
        return item && String(item.value || '').trim();
    });

    if (!rows.length) {
        return '<div class="structured-trigger-detail-text">' + escapeHtml(emptyText || 'Не указано') + '</div>';
    }

    return '<div class="structured-trigger-detail-list">' + rows.map(function(item) {
        return '<div class="structured-trigger-detail-row">' +
            '<div class="structured-trigger-detail-key">' + escapeHtml(item.label || '') + '</div>' +
            '<div class="structured-trigger-detail-text">' + escapeHtml(String(item.value || '')) + '</div>' +
        '</div>';
    }).join('') + '</div>';
}

function buildStructuredTriggerRowFromState(state) {
    var normalized = normalizeStructuredTriggerState(state);
    var definition = getStructuredTriggerDefinition(normalized.category, normalized.section, normalized.event);
    var conditionLabel = getStructuredTriggerConditionLabel(definition.mode, normalized.condition);
    var extraConditionLabel = getStructuredTriggerConditionLabel(definition.mode, normalized.extraCondition);
    var primaryAction = normalized.actions[0] || getDefaultStructuredTriggerAction();
    var actionLabel = getStructuredTriggerActionLabel(primaryAction.action);
    var title = String(normalized.title || '').trim();
    if (!title) {
        title = definition.eventLabel + ' -> ' + (actionLabel || 'Действие');
    }

    return {
        'Название': title,
        'Тип события': definition.categoryLabel,
        'Раздел события': definition.sectionLabel,
        'Событие': definition.eventLabel,
        'Код события': normalized.event,
        'Условие': conditionLabel,
        'Код условия': normalized.condition,
        'Параметр условия': normalized.conditionParam,
        'Значение': normalized.value,
        'Доп. условие': extraConditionLabel,
        'Код доп. условия': normalized.extraCondition,
        'Доп. значение': normalized.extraValue,
        'Действие': actionLabel,
        'Код действия': primaryAction.action,
        'Группа': primaryAction.actionGroup,
        'Бот': primaryAction.actionBot,
        'Шаг': primaryAction.actionStep,
        'ID сообщества действия': primaryAction.actionCommunityId,
        'Название переменной': primaryAction.actionVarName,
        'Значение переменной': primaryAction.actionVarValue,
        'Действия JSON': JSON.stringify(normalized.actions),
        'Активен': normalized.active ? 'ДА' : 'НЕТ',
        'Не применять остальные правила': normalized.stopFurther ? 'ДА' : '',
        'Ответ': '',
        'Вложения к ответу': '',
        'Ответить если в Группе': '',
        'ДОБАВИТЬ ГРУППУ': primaryAction.action === 'add_group' ? primaryAction.actionGroup : '',
        'УДАЛИТЬ ГРУППУ': primaryAction.action === 'remove_group' ? primaryAction.actionGroup : '',
        'Отправить на Шаг': '',
        'Действия с ПП': '',
        'Действия с ГП': '',
        'Действия с ПВС': ''
    };
}

function clearStructuredTriggerStatusTimers() {
    if (window.structuredTriggerStatusTimer) {
        clearTimeout(window.structuredTriggerStatusTimer);
        window.structuredTriggerStatusTimer = null;
    }
}

function updateStructuredTriggerStatus(message, type, autoHide) {
    var statusEl = document.getElementById('structuredTriggersStatus');
    if (statusEl) {
        statusEl.innerHTML = message ? makeInlineNotice(type || 'info', message) : '';
    }

    var legacyStatusEl = document.getElementById('status-Triggers');
    if (legacyStatusEl) {
        if (message) {
            legacyStatusEl.className = 'status ' + (type || 'info');
            legacyStatusEl.style.display = 'block';
            legacyStatusEl.innerHTML = message;
        } else {
            legacyStatusEl.innerHTML = '';
            legacyStatusEl.style.display = 'none';
        }
    }

    clearStructuredTriggerStatusTimers();
    if (autoHide) {
        window.structuredTriggerStatusTimer = setTimeout(function() {
            if (statusEl) statusEl.innerHTML = '';
            if (legacyStatusEl) {
                legacyStatusEl.innerHTML = '';
                legacyStatusEl.style.display = 'none';
            }
            window.structuredTriggerStatusTimer = null;
        }, 5000);
    }
}

function renderStructuredTriggerBuilder(customState) {
    var builder = document.getElementById('structuredTriggersBuilder');
    if (!builder) return;

    if (!window.structuredTriggerBuilderVisible) {
        builder.style.display = 'none';
        builder.innerHTML = '';
        return;
    }

    builder.style.display = 'block';

    var currentState = normalizeStructuredTriggerState(customState || getStructuredTriggerFormState());
    window.structuredTriggerDraftState = currentState;

    var definition = getStructuredTriggerDefinition(currentState.category, currentState.section, currentState.event);
    var categories = getStructuredTriggerCategories();
    var sections = getStructuredTriggerSections(currentState.category);
    var events = getStructuredTriggerEvents(currentState.category, currentState.section);
    var conditionOptions = getStructuredTriggerConditionOptions(definition.mode);
    var currentCondition = conditionOptions.find(function(option) { return option.value === currentState.condition; }) || null;
    var showConditionValue = !!(currentCondition && currentCondition.needsValue);
    var showExtra = definition.mode === 'wall_comment';
    var extraCondition = STRUCTURED_TRIGGER_COMMENT_TEXT_CONDITIONS.find(function(option) { return option.value === currentState.extraCondition; }) || STRUCTURED_TRIGGER_COMMENT_TEXT_CONDITIONS[0];
    var showExtraValue = !!(showExtra && extraCondition && extraCondition.needsValue);
    var groupOptions = collectStructuredTriggerGroups(currentState.actionGroup);
    var botOptions = collectStructuredTriggerBots(currentState.actionBot);
    var stepOptions = collectStructuredTriggerSteps(currentState.actionBot, currentState.actionStep, true);
    var modeHint = '';

    if (definition.mode === 'message') modeHint = 'Для сообщений доступны точные, текстовые, числовые, файловые проверки и проверки по значениям ПП, ГП и ПВС.';
    if (definition.mode === 'button') modeHint = 'Кнопки отслеживаются по payload и названию кнопки в сообщении.';
    if (definition.mode === 'post') modeHint = 'Ссылки на записи можно указывать по одной ссылке с новой строки.';
    if (definition.mode === 'wall_comment') modeHint = 'Сначала фильтр по записи, затем при необходимости по тексту комментария.';
    if (definition.mode === 'none') modeHint = 'Для событий вступления и выхода дополнительные условия не нужны.';
    if (definition.mode === 'join_request') modeHint = 'Для заявки на вступление можно проверить значение Переменной ПВС и после этого одобрить заявку или выполнить другие действия.';

    var actionsHtml = currentState.actions.map(function(actionItem, idx) {
        var localAction = Object.assign({}, getDefaultStructuredTriggerAction(), actionItem || {});
        var localShowGroup = shouldStructuredTriggerActionShowGroup(localAction.action);
        var localShowBot = shouldStructuredTriggerActionShowBot(localAction.action);
        var localShowStep = shouldStructuredTriggerActionShowStep(localAction.action);
        var localShowCommunity = shouldStructuredTriggerActionShowCommunity(localAction.action);
        var localShowVarName = shouldStructuredTriggerActionShowVarName(localAction.action);
        var localShowVarValue = shouldStructuredTriggerActionShowVarValue(localAction.action);
        var localStepOptions = collectStructuredTriggerSteps(localAction.actionBot, localAction.actionStep, true);

        return '<div class="structured-trigger-field structured-trigger-field--full structured-trigger-action-row" data-action-index="' + idx + '" style="border:1px solid var(--section-border);border-radius:16px;padding:12px;background:var(--surface-soft);">' +
            '<div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:10px;"><label style="margin:0;">Действие #' + (idx + 1) + '</label>' + (currentState.actions.length > 1 ? '<button class="btn btn-delete" type="button" onclick="removeStructuredTriggerAction(' + idx + ')">Удалить действие</button>' : '') + '</div>' +
            '<div class="structured-trigger-grid">' +
                '<div class="structured-trigger-field"><label>Тип действия</label><select id="structuredTriggerAction_' + idx + '" onchange="handleStructuredTriggerFormChange()">' + buildStructuredTriggerOptionTags(STRUCTURED_TRIGGER_ACTIONS, localAction.action) + '</select></div>' +
                (localShowGroup ? '<div class="structured-trigger-field"><label>Группа</label><input id="structuredTriggerActionGroup_' + idx + '" type="text" list="structuredTriggerGroupsList" value="' + escapeHtml(localAction.actionGroup) + '" placeholder="Выбери или впиши новую группу" oninput="handleStructuredTriggerInputChange()"></div>' : '') +
                (localShowBot ? '<div class="structured-trigger-field"><label>Бот</label><select id="structuredTriggerActionBot_' + idx + '" onchange="handleStructuredTriggerFormChange()">' + buildStructuredTriggerOptionTags(botOptions, localAction.actionBot, 'Выбери бота') + '</select></div>' : '') +
                (localShowStep ? '<div class="structured-trigger-field"><label>Шаг</label><select id="structuredTriggerActionStep_' + idx + '" onchange="handleStructuredTriggerFormChange()">' + buildStructuredTriggerOptionTags(localStepOptions, localAction.actionStep, 'Выбери шаг') + '</select></div>' : '') +
                (localShowCommunity ? '<div class="structured-trigger-field"><label>ID сообщества</label><input id="structuredTriggerActionCommunity_' + idx + '" type="text" value="' + escapeHtml(localAction.actionCommunityId) + '" placeholder="Например: 229445618" oninput="handleStructuredTriggerInputChange()"></div>' : '') +
                (localShowVarName ? '<div class="structured-trigger-field"><label>Название переменной</label><input id="structuredTriggerActionVarName_' + idx + '" type="text" value="' + escapeHtml(localAction.actionVarName) + '" placeholder="Например: pvs_banner" oninput="handleStructuredTriggerInputChange()"></div>' : '') +
                (localShowVarValue ? '<div class="structured-trigger-field structured-trigger-field--full"><label>Значение переменной</label><textarea id="structuredTriggerActionVarValue_' + idx + '" placeholder="Введите значение переменной" oninput="handleStructuredTriggerInputChange()">' + escapeHtml(localAction.actionVarValue) + '</textarea></div>' : '') +
            '</div>' +
        '</div>';
    }).join('');

    var showConditionParam = shouldStructuredTriggerConditionShowParam(currentCondition);
    var conditionParamLabel = (currentCondition && currentCondition.paramLabel) || 'Параметр условия';
    var conditionParamPlaceholder = (currentCondition && currentCondition.paramPlaceholder) || 'Введите параметр';

    builder.innerHTML = '<div class="structured-trigger-status-row">' +
        '<div>' +
            '<div class="tab-panel-kicker">Конструктор триггера</div>' +
            '<div class="structured-trigger-hint">' + escapeHtml(modeHint) + '</div>' +
        '</div>' +
        '<label class="structured-trigger-toggle"><input id="structuredTriggerActive" type="checkbox" ' + (currentState.active ? 'checked' : '') + ' onchange="handleStructuredTriggerCheckboxChange()">Сразу активировать триггер</label>' +
    '</div>' +
    '<div class="structured-trigger-grid">' +
        '<div class="structured-trigger-field structured-trigger-field--full"><label>Название</label><input id="structuredTriggerTitle" type="text" value="' + escapeHtml(currentState.title) + '" placeholder="Например: Вступление в VIP бота" oninput="handleStructuredTriggerInputChange()"></div>' +
        '<div class="structured-trigger-field"><label>Тип события</label><select id="structuredTriggerCategory" onchange="handleStructuredTriggerFormChange()">' + buildStructuredTriggerOptionTags(categories, currentState.category) + '</select></div>' +
        '<div class="structured-trigger-field"><label>Раздел</label><select id="structuredTriggerSection" onchange="handleStructuredTriggerFormChange()">' + buildStructuredTriggerOptionTags(sections, currentState.section) + '</select></div>' +
        '<div class="structured-trigger-field"><label>Событие</label><select id="structuredTriggerEvent" onchange="handleStructuredTriggerFormChange()">' + buildStructuredTriggerOptionTags(events, currentState.event) + '</select></div>' +
        (conditionOptions.length ? '<div class="structured-trigger-field"><label>Условие</label><select id="structuredTriggerCondition" onchange="handleStructuredTriggerFormChange()">' + buildStructuredTriggerOptionTags(conditionOptions, currentState.condition) + '</select></div>' : '') +
        (showConditionParam ? '<div class="structured-trigger-field"><label>' + escapeHtml(conditionParamLabel) + '</label><input id="structuredTriggerConditionParam" type="text" value="' + escapeHtml(currentState.conditionParam) + '" placeholder="' + escapeHtml(conditionParamPlaceholder) + '" oninput="handleStructuredTriggerInputChange()"></div>' : '') +
        (showConditionValue ? '<div class="structured-trigger-field structured-trigger-field--full"><label>Значение условия</label><textarea id="structuredTriggerValue" placeholder="' + escapeHtml((currentCondition && currentCondition.placeholder) || 'Введите значение') + '" oninput="handleStructuredTriggerInputChange()">' + escapeHtml(currentState.value) + '</textarea></div>' : '') +
        (showExtra ? '<div class="structured-trigger-field"><label>Текст комментария</label><select id="structuredTriggerExtraCondition" onchange="handleStructuredTriggerFormChange()">' + buildStructuredTriggerOptionTags(STRUCTURED_TRIGGER_COMMENT_TEXT_CONDITIONS, currentState.extraCondition) + '</select></div>' : '') +
        (showExtraValue ? '<div class="structured-trigger-field structured-trigger-field--full"><label>Значение дополнительного условия</label><input id="structuredTriggerExtraValue" type="text" value="' + escapeHtml(currentState.extraValue) + '" placeholder="Впишите искомый текст" oninput="handleStructuredTriggerInputChange()"></div>' : '') +
        '<div class="structured-trigger-field structured-trigger-field--full"><label>Список действий</label><datalist id="structuredTriggerGroupsList">' + buildStructuredTriggerOptionTags(groupOptions, '', '') + '</datalist>' + actionsHtml + '<button class="btn btn-add" type="button" onclick="addStructuredTriggerAction()">+ Добавить действие</button></div>' +
        '<div class="structured-trigger-field structured-trigger-field--full"><label class="structured-trigger-toggle"><input id="structuredTriggerStop" type="checkbox" ' + (currentState.stopFurther ? 'checked' : '') + ' onchange="handleStructuredTriggerCheckboxChange()">Не применять остальные правила</label><div class="structured-trigger-footnote">Если включить, после срабатывания этого триггера остальные карточки больше не будут проверяться на это же событие.</div></div>' +
    '</div>' +
    '<div class="structured-trigger-actions">' +
        '<button class="btn btn-save" type="button" onclick="saveStructuredTriggerForm()">' + (window.structuredTriggerEditIndex >= 0 ? 'Сохранить изменения' : 'Сохранить триггер') + '</button>' +
        '<button class="btn btn-neutral" type="button" onclick="hideStructuredTriggerBuilder()">Закрыть конструктор</button>' +
    '</div>';
}

window.handleStructuredTriggerFormChange = function() {
    var previousState = normalizeStructuredTriggerState(window.structuredTriggerDraftState || getStructuredTriggerDefaultState());
    var nextState = getStructuredTriggerFormState();
    var definition = getStructuredTriggerDefinition(nextState.category, nextState.section, nextState.event);
    var conditionOptions = getStructuredTriggerConditionOptions(definition.mode);
    var currentCondition = conditionOptions.find(function(option) { return option.value === nextState.condition; }) || null;

    if (!shouldStructuredTriggerConditionShowParam(currentCondition)) {
        nextState.conditionParam = '';
    }
    if (!(currentCondition && currentCondition.needsValue)) {
        nextState.value = '';
    }

    nextState.actions = nextState.actions.map(function(actionItem, idx) {
        var previousAction = previousState.actions[idx] || getDefaultStructuredTriggerAction();
        var nextAction = Object.assign({}, actionItem);

        if (!shouldStructuredTriggerActionShowStep(nextAction.action)) {
            nextAction.actionStep = '';
        }
        if (!shouldStructuredTriggerActionShowCommunity(nextAction.action)) {
            nextAction.actionCommunityId = '';
        }
        if (!shouldStructuredTriggerActionShowVarName(nextAction.action)) {
            nextAction.actionVarName = '';
            nextAction.actionVarValue = '';
        }
        if (!shouldStructuredTriggerActionShowVarValue(nextAction.action)) {
            nextAction.actionVarValue = '';
        }
        if (previousAction.actionBot !== nextAction.actionBot) {
            var validSteps = collectStructuredTriggerSteps(nextAction.actionBot, '', false).map(function(item) {
                return item.value;
            });
            if (!validSteps.includes(nextAction.actionStep)) {
                nextAction.actionStep = '';
            }
        }
        return nextAction;
    });

    window.structuredTriggerDraftState = normalizeStructuredTriggerState(nextState);
    renderStructuredTriggerBuilder(window.structuredTriggerDraftState);
};

window.handleStructuredTriggerInputChange = function() {
    window.structuredTriggerDraftState = getStructuredTriggerFormState();
};

window.addStructuredTriggerAction = function() {
    var state = getStructuredTriggerFormState();
    state.actions.push(getDefaultStructuredTriggerAction());
    window.structuredTriggerDraftState = normalizeStructuredTriggerState(state);
    renderStructuredTriggerBuilder(window.structuredTriggerDraftState);
};

window.removeStructuredTriggerAction = function(idx) {
    var state = getStructuredTriggerFormState();
    if (state.actions.length <= 1) return;
    state.actions.splice(idx, 1);
    window.structuredTriggerDraftState = normalizeStructuredTriggerState(state);
    renderStructuredTriggerBuilder(window.structuredTriggerDraftState);
};

window.handleStructuredTriggerCheckboxChange = function() {
    window.structuredTriggerDraftState = getStructuredTriggerFormState();
};

window.openNewStructuredTriggerForm = function() {
    window.structuredTriggerEditIndex = -1;
    window.structuredTriggerBuilderVisible = true;
    window.structuredTriggerDraftState = getStructuredTriggerDefaultState();
    renderStructuredTriggersTab();
};

window.hideStructuredTriggerBuilder = function() {
    window.structuredTriggerEditIndex = -1;
    window.structuredTriggerBuilderVisible = false;
    window.structuredTriggerDraftState = getStructuredTriggerDefaultState();
    renderStructuredTriggersTab();
};

window.resetStructuredTriggerForm = function() {
    window.openNewStructuredTriggerForm();
};

function renderStructuredTriggerCards() {
    var listEl = document.getElementById('structuredTriggersCards');
    if (!listEl) return;

    var rows = dataStore['Triggers'] || [];
    if (!rows.length) {
        listEl.innerHTML = '<div class="trigger-empty">Пока нет ни одного событийного триггера. Собери его в форме выше и сохрани.</div>';
        return;
    }

    var filteredRows = rows.map(function(row, idx) {
        return { row: row, idx: idx };
    }).filter(function(item) {
        var state = getStructuredTriggerStateFromRow(item.row);
        var definition = getStructuredTriggerDefinition(state.category, state.section, state.event);
        var title = String(item.row['Название'] || definition.eventLabel || '').toLowerCase();
        var query = String(window.structuredTriggerFilterState.query || '').trim().toLowerCase();
        var eventCode = String(window.structuredTriggerFilterState.eventCode || 'ALL').trim();
        if (query && !title.includes(query)) return false;
        if (eventCode !== 'ALL' && state.event !== eventCode) return false;
        return true;
    });

    if (!filteredRows.length) {
        listEl.innerHTML = '<div class="trigger-empty">По текущему фильтру триггеры не найдены.</div>';
        return;
    }

    listEl.innerHTML = filteredRows.map(function(item) {
        var row = item.row;
        var idx = item.idx;
        var state = getStructuredTriggerStateFromRow(row);
        var definition = getStructuredTriggerDefinition(state.category, state.section, state.event);
        var conditionLabel = getStructuredTriggerConditionLabel(definition.mode, state.condition) || 'Без условия';
        var extraLabel = definition.mode === 'wall_comment' ? getStructuredTriggerConditionLabel(definition.mode, state.extraCondition) : '';
        var actionsList = Array.isArray(state.actions) ? state.actions : [];
        var conditionHtml = buildStructuredTriggerDetailList([
            { label: 'Тип', value: conditionLabel },
            { label: 'Параметр', value: state.conditionParam },
            { label: 'Значение', value: state.value },
            { label: 'Доп. условие', value: extraLabel },
            { label: 'Доп. значение', value: state.extraValue }
        ], 'Условие не задано');
        var actionHtml = actionsList.length ? actionsList.map(function(actionItem) {
            return buildStructuredTriggerDetailList([
                { label: 'Действие', value: getStructuredTriggerActionLabel(actionItem.action) || 'Без действия' },
                { label: 'Группа', value: actionItem.actionGroup },
                { label: 'Бот', value: actionItem.actionBot },
                { label: 'Шаг', value: actionItem.actionStep },
                { label: 'ID сообщества', value: actionItem.actionCommunityId },
                { label: 'Перем.', value: actionItem.actionVarName },
                { label: 'Значение', value: actionItem.actionVarValue }
            ], 'Действие не задано');
        }).join('<div style="height:8px"></div>') : 'Действие не задано';
        var behaviorHtml = buildStructuredTriggerDetailList([
            { label: 'Режим', value: state.stopFurther ? 'После срабатывания остановить остальные правила' : 'После срабатывания продолжать проверку остальных правил' }
        ], '');
        return '<div class="trigger-card structured-trigger-card ' + (state.active ? 'is-active' : 'is-inactive') + '">' +
            '<div class="trigger-card-header">' +
                '<div>' +
                    '<div class="trigger-card-title">' + escapeHtml(String(row['Название'] || definition.eventLabel || ('Триггер ' + (idx + 1)))) + '</div>' +
                    '<div class="trigger-card-meta">' + escapeHtml(definition.categoryLabel) + ' • ' + escapeHtml(definition.sectionLabel) + ' • ' + escapeHtml(definition.eventLabel) + '</div>' +
                '</div>' +
                '<button class="structured-trigger-card-status ' + (state.active ? 'is-active' : 'is-inactive') + '" type="button" onclick="toggleStructuredTriggerActive(' + idx + ', ' + (!state.active ? 'true' : 'false') + ')">' + (state.active ? 'Активен' : 'Не активен') + '</button>' +
            '</div>' +
            '<div class="trigger-card-section"><div class="trigger-card-label">Условие</div><div class="trigger-card-value">' + conditionHtml + '</div></div>' +
            '<div class="trigger-card-section"><div class="trigger-card-label">Действие</div><div class="trigger-card-value">' + actionHtml + '</div></div>' +
            '<div class="trigger-card-section"><div class="trigger-card-label">Поведение</div><div class="trigger-card-value">' + behaviorHtml + '</div></div>' +
            '<div class="trigger-card-actions">' +
                '<button class="btn btn-info" type="button" onclick="editStructuredTrigger(' + idx + ')">Редактировать</button>' +
                '<button class="btn btn-delete" type="button" onclick="deleteStructuredTrigger(' + idx + ')">Удалить</button>' +
            '</div>' +
        '</div>';
    }).join('');
}

async function persistStructuredTriggers(message) {
    updateStructuredTriggerStatus('Сохраняю триггеры...', 'warn', false);
    var success = await saveDataDirectly('Triggers');
    updateStructuredTriggerStatus(
        success
            ? '✅ ' + escapeHtml(message || 'Триггеры сохранены.')
            : '❌ Не удалось сохранить триггеры в хранилище. Локально карточки уже обновлены.',
        success ? 'success' : 'error',
        true
    );
    return success;
}

window.saveStructuredTriggerForm = async function() {
    var state = getStructuredTriggerFormState();
    var statusEl = document.getElementById('structuredTriggersStatus');
    var currentDefinition = getStructuredTriggerDefinition(state.category, state.section, state.event);
    var currentConditionOptions = getStructuredTriggerConditionOptions(currentDefinition.mode);
    var currentCondition = currentConditionOptions.find(function(option) { return option.value === state.condition; }) || null;

    if (shouldStructuredTriggerConditionShowParam(currentCondition) && !state.conditionParam.trim()) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Укажи параметр для выбранного условия триггера.');
        return;
    }
    if (currentCondition && currentCondition.needsValue && !state.value.trim()) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Укажи значение для выбранного условия триггера.');
        return;
    }

    for (var i = 0; i < state.actions.length; i++) {
        var action = state.actions[i];
        var actionLabel = getStructuredTriggerActionLabel(action.action);
        if ((action.action === 'add_group' || action.action === 'remove_group') && !action.actionGroup.trim()) {
            if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Укажи группу для действия «' + escapeHtml(actionLabel) + '».');
            return;
        }
        if ((action.action === 'add_to_bot' || action.action === 'remove_from_bot') && !action.actionBot.trim()) {
            if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Выбери бота для действия «' + escapeHtml(actionLabel) + '».');
            return;
        }
        if (action.action === 'add_to_bot' && !action.actionStep.trim()) {
            if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Выбери шаг для добавления в бота.');
            return;
        }
        if (shouldStructuredTriggerActionShowCommunity(action.action) && !action.actionCommunityId.trim()) {
            if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Укажи ID сообщества для действия «' + escapeHtml(actionLabel) + '».');
            return;
        }
        if (/_var_/.test(action.action) && !action.actionVarName.trim()) {
            if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Укажи название переменной для выбранного действия.');
            return;
        }
        if (/_var_(add|update)$/.test(action.action) && !action.actionVarValue.trim()) {
            if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Укажи значение переменной для выбранного действия.');
            return;
        }
    }

    var row = buildStructuredTriggerRowFromState(state);
    if (!dataStore['Triggers']) dataStore['Triggers'] = [];

    if (window.structuredTriggerEditIndex >= 0 && dataStore['Triggers'][window.structuredTriggerEditIndex]) {
        dataStore['Triggers'][window.structuredTriggerEditIndex] = row;
    } else {
        dataStore['Triggers'].push(row);
    }

    renderStructuredTriggerCards();
    await persistStructuredTriggers(window.structuredTriggerEditIndex >= 0 ? 'Триггер обновлён.' : 'Триггер сохранён и добавлен в карточки.');
    window.structuredTriggerEditIndex = -1;
    window.structuredTriggerBuilderVisible = false;
    window.structuredTriggerDraftState = getStructuredTriggerDefaultState();
    renderStructuredTriggersTab();
};

window.editStructuredTrigger = function(idx) {
    if (!dataStore['Triggers'] || !dataStore['Triggers'][idx]) return;
    window.structuredTriggerEditIndex = idx;
    window.structuredTriggerBuilderVisible = true;
    window.structuredTriggerDraftState = getStructuredTriggerStateFromRow(dataStore['Triggers'][idx]);
    renderStructuredTriggersTab();
    updateStructuredTriggerStatus('Редактирование карточки #' + (idx + 1) + '.', 'info', true);
};

window.deleteStructuredTrigger = async function(idx) {
    if (!dataStore['Triggers'] || !dataStore['Triggers'][idx]) return;
    if (!confirm('Удалить этот триггер?')) return;
    dataStore['Triggers'].splice(idx, 1);
    if (window.structuredTriggerEditIndex === idx) {
        window.structuredTriggerEditIndex = -1;
        window.structuredTriggerBuilderVisible = false;
        window.structuredTriggerDraftState = getStructuredTriggerDefaultState();
    }
    renderStructuredTriggerCards();
    await persistStructuredTriggers('Триггер удалён.');
    renderStructuredTriggersTab();
};

window.toggleStructuredTriggerActive = async function(idx, checked) {
    if (!dataStore['Triggers'] || !dataStore['Triggers'][idx]) return;
    var row = dataStore['Triggers'][idx];
    var isActive = String(row['Активен'] || 'ДА').trim().toUpperCase() !== 'НЕТ';
    var nextActive = typeof checked === 'boolean' ? checked : !isActive;
    row['Активен'] = nextActive ? 'ДА' : 'НЕТ';
    renderStructuredTriggerCards();
    await persistStructuredTriggers(nextActive ? 'Триггер включён.' : 'Триггер выключен.');
};

window.renderStructuredTriggersTab = function() {
    if (!dataStore['Triggers']) dataStore['Triggers'] = [];
    renderStructuredTriggerBuilder(window.structuredTriggerDraftState || getStructuredTriggerDefaultState());
    renderStructuredTriggerFilters();
    renderStructuredTriggerCards();
};



function getLineCount(str) {
    if (typeof str !== 'string' || !str) return 1;
    let count = 1;
    for (let i = 0; i < str.length; i++) {
        const code = str.charCodeAt(i);
        if (code === 10) { // \n
            count++;
        } else if (code === 13) { // \r
            if (i + 1 >= str.length || str.charCodeAt(i + 1) !== 10) {
                count++;
            }
        }
    }
    return count;
}

function getCellPlaceholder(tab, name, hint) {
    const placeholders = {
        'Бот': 'Например: Основной бот',
        'Шаг': 'Например: START',
        'Триггер': 'Например: привет',
        'Ответ': 'Например: Здравствуйте! Чем могу помочь?',
        'Заготовленный ответ': 'Например: Я на связи, напишите подробнее',
        'Вложения к ответу': 'Например: photo123_456, doc123_456',
        'Вложения': 'Например: photo123_456, doc123_456',
        'Вложение к рассылке': 'Например: photo123_456, doc123_456',
        'Пост': 'Например: ВСЕ или 123_456',
        'Ответить если в Группе': 'Например: VIP, клиенты',
        'Ответил на Шаг': 'Например: MENU',
        'Пользовательская': tab === 'Variables_User' ? 'Например: balance' : 'Например: age, city',
        'Глобальная': 'Например: discount',
        'Переменная ПВС': 'Например: invite_status',
        'Значение ГП': 'Например: 1500 или Добро пожаловать',
        'Значение ПВС': 'Например: approved',
        'Задержка отправки на Шаг': 'Например: 10мин или 1час',
        'ДОБАВИТЬ ГРУППУ': 'Например: VIP',
        'УДАЛИТЬ ГРУППУ': 'Например: archive',
        'Отправить на Шаг': 'Например: MENU',
        'Действия с ПП': 'Например: balance = balance + 100',
        'Действия с ГП': 'Например: stock = stock - 1',
        'Действия с ПВС': 'Например: invite_status = "approved"',
        'ID': 'Например: 123456789',
        'ИМЯ': 'Например: Иван',
        'ГРУППА': 'Например: vip, premium',
        'Значения ПП': 'Например: balance=100\\ncity=Москва',
        'Переменная ПВС': 'Например: promo_banner',
        'Значение ПВС': 'Например: Весенняя акция',
        'Текущий Бот': 'Например: Основной бот',
        'Текущий Шаг': 'Например: START',
        'Отправленные Шаги': 'Например: bot:step',
        'Переменная ВК': 'Например: %vk_user%',
        'Описание': 'Например: Имя пользователя из VK',
        '№': 'Например: 1',
        'ID Получателей': 'Например: 12345, 67890',
        'ГРУППА Получателей': 'Например: VIP',
        'Сообщение Рассылки': 'Например: Напоминаем о вашем заказе',
        'Дата и время отправки (по мск.)': 'Например: 2026-04-12 18:30:00',
        'Фактическое время отправки (по мск.)': 'Заполнится автоматически после отправки',
        'Ошибка': 'Заполнится только если возникнет ошибка',
        'ID Пользователя': 'Например: 123456789',
        'Группа': 'Например: 229445618',
        'Факт. время отправки (по мск.)': 'Заполнится автоматически после отправки'
    };

    if (placeholders[name]) return placeholders[name];
    if (hint) {
        const brRegex = new RegExp('<br\\s*/?>', 'gi');
        const plainHint = String(hint).replace(brRegex, ' ').replace(/<[^>]*>/g, '').trim();
        if (plainHint) return plainHint;
    }
    return 'Введите значение';
}

function normalizeTriggerMode(mode) {
    var normalized = String(mode || 'TEXT').trim().toUpperCase();
    return ['TEXT', 'BUTTON', 'FILE'].includes(normalized) ? normalized : 'TEXT';
}



function renderTable(tab, data) {
  const table = document.getElementById('table-'+tab);
  const cols = columns[tab] || [];
  const duplicableTabs = ['Messages', 'Comments', 'Users', 'Variables', 'Mailing', 'Delayed'];
  const canDuplicateRows = duplicableTabs.includes(tab);
  const isReadOnly = tab === 'VK_Variables';

  // &#x1F916; ФИЛЬТРАЦИЯ ПО АКТИВНОМУ БОТУ для Messages и Comments
  let displayData = data;
  let displayToOriginalIndex = null; // Маппинг индексов displayData → dataStore
  if (tab === 'Messages' || tab === 'Comments') {
    const activeBot = getActiveBot(tab);
    if (activeBot) {
      // Показываем только строки где колонка 'Бот' совпадает с активным
      // СОХРАНЯЕМ оригинальные индексы для правильного удаления!
      displayToOriginalIndex = [];
      displayData = [];
      for (var i = 0; i < data.length; i++) {
        if (data[i]['Бот'] === activeBot) {
          displayToOriginalIndex.push(i);
          displayData.push(data[i]);
        }
      }
      console.log('[Admin] 🤖 Filtered rows for bot "' + activeBot + '": ' + displayData.length + ' of ' + data.length);
    }
    // Обновляем текст в empty state
    if (!displayData || !displayData.length) {
      const botInfo = activeBot ? ' для бота "' + activeBot + '"' : '';
      table.innerHTML = '<tr><td colspan="'+(cols.length+1)+'">Нет данных' + botInfo + '. Нажмите "+ Добавить Шаг"</td></tr>';
      return;
    }
  } else {
    // Для остальных вкладок старая проверка
    if (!data || !data.length) {
      table.innerHTML = '<tr><td colspan="'+(cols.length+1)+'">Нет данных. Нажмите "+ Добавить строку"</td></tr>';
      return;
    }
  }

  // Группировка колонок по секциям
  const sections = {};
  cols.forEach((col, idx) => {
    const section = (typeof col === 'object' && col.section) ? col.section : 'ОСНОВНОЕ';
    if (!sections[section]) sections[section] = [];
    sections[section].push({ col, idx });
  });

  const sectionColors = {
    'ОСНОВНОЕ': '#C8E6C9',
    'УСЛОВИЯ ПРОВЕРКИ': '#D7CCC8',
    'ВЫПОЛНЯЕМЫЕ ДЕЙСТВИЯ': '#FFE0B2',
    'КНОПКИ В ОТВЕТЕ': '#B3E5FC',
    'КНОПКИ В ЗАГОТОВЛЕННОМ ОТВЕТЕ': '#F8BBD0',
    'УДАЛЕНИЕ': '#FFCDD2'
  };

  let html = '<thead><tr>';
  // Заголовки секций (объединённые ячейки)
  for (const [sectionName, sectionCols] of Object.entries(sections)) {
    const bgColor = sectionColors[sectionName] || '#E0E0E0';
    html += '<th colspan="'+sectionCols.length+'" class="section-header" style="background:'+bgColor+'">'+sectionName+'</th>';
  }
  if (!isReadOnly) {
    html += '<th class="section-header" style="background:#FFCDD2">&#x1F5D1;&#xFE0F;</th>';
  }
  html += '</tr><tr>';

  // Заголовки колонок (отдельные ячейки)
  cols.forEach(col => {
    const name = typeof col === 'string' ? col : col.name;
    const colClass = typeof col === 'object' && col.class ? col.class : 'th-green';
    const hint = typeof col === 'object' && col.hint ? col.hint : '';
    html += '<th class="'+colClass+'">';
    html += '<span class="col-name">'+escapeHtml(name)+'</span>';
    if (hint) {
      html += '<span class="help-icon" data-hint="'+escapeHtml(hint)+'">?</span>';
    }
    html += '</th>';
  });
  if (!isReadOnly) {
    html += '<th style="background:#FFCDD2"><button class="btn btn-delete-all" data-tab="' + escapeHtml(tab) + '" onclick="deleteAllRows(this)">УДАЛИТЬ ВСЕ</button></th>';
  }
  html += '</tr></thead><tbody>';

  // Тело таблицы
  displayData.forEach((row, displayIdx) => {
    // ВАЖНО: используем оригинальный индекс для правильной привязки к dataStore
    const originalIdx = displayToOriginalIndex ? displayToOriginalIndex[displayIdx] : displayIdx;
    html += '<tr>';
    cols.forEach(col => {
      const name = typeof col === 'string' ? col : col.name;
      const colType = typeof col === 'object' && col.type ? col.type : 'text';
      const options = typeof col === 'object' && col.options ? col.options : null;
      const hint = typeof col === 'object' && col.hint ? col.hint : '';
      const val = row[name] || '';
      const placeholder = escapeHtml(getCellPlaceholder(tab, name, hint));

      if (isReadOnly) {
        html += '<td><div class="trigger-card-value">' + escapeHtml(String(val || '')) + '</div></td>';
      } else if (colType === 'select' && options) {
        // Обработка выпадающего списка, в том числе для цвета/ссылки
        const isLinkField = name.includes('Цвет/Ссылка');
        const [currentColor, currentLink] = (val || '').split('||');
        const normalizedCurrentColor = (currentColor || '').trim().toLowerCase().replace(/ё/g, 'е');
        html += '<td class="color-link-cell">';
        html += '<select class="color-select" data-idx="'+originalIdx+'" data-name="'+escapeHtml(name)+'" data-tab="'+escapeHtml(tab)+'" title="'+placeholder+'">';
        options.forEach(opt => {
          const normalizedOpt = opt.trim().toLowerCase().replace(/ё/g, 'е');
          const selected = normalizedCurrentColor === normalizedOpt ? 'selected' : '';
          html += '<option value="'+escapeHtml(opt)+'" '+selected+'>'+escapeHtml(opt)+'</option>';
        });
        html += '</select>';
        if (isLinkField) {
          const linkStyle = currentColor === 'ССЫЛКА...' ? 'display:block;' : 'display:none;';
          html += '<input type="url" class="link-input" placeholder="https://example.com" value="'+escapeHtml(currentLink || '')+'" style="'+linkStyle+'margin-top:3px;">';
        }
        html += '</td>';
      } else {
        // ✅ ВСЕ поля - textarea (расширяемые мышкой)
        const lineCount = getLineCount(val);
        const rows = Math.max(lineCount, 2);

        // Колонки для которых нужна кнопка "Test"
        const testColumns = ['Ответ', 'Шаг'];
        const isTestColumn = testColumns.includes(name);

        // Колонки для которых нужна кнопка "Кнопки" (конструктор клавиатуры)
        const keyboardColumns = ['Ответ', 'Сообщение Рассылки'];
        const isKeyboardColumn = keyboardColumns.includes(name);
        const tabForKeyboard = (name === 'Ответ' && tab === 'Messages') || (name === 'Сообщение Рассылки' && tab === 'Mailing');

        // &#x1F6E0;&#xFE0F; Кнопка "+" ТОЛЬКО для "Вложения к ответу"
        if (name === 'Вложения к ответу' || name === 'Вложения' || name === 'Вложение к рассылке') {
          // Кнопка "+" для поля Вложения
          html += '<td class="cell-editor-wrap cell-with-tool">';
          html += '<textarea class="editable-cell" placeholder="'+placeholder+'" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'" data-name="'+escapeHtml(name)+'" rows="'+rows+'">'+escapeHtml(val)+'</textarea>';
          html += '<button class="attach-btn" data-tab="'+tab+'" data-idx="'+originalIdx+'" data-col="'+name+'">+</button>';
          html += '</td>';
        } else if (isKeyboardColumn && tabForKeyboard) {
          // &#x2328;&#xFE0F; Кнопка "Кнопки" для конструктора клавиатуры
          console.log('[Admin] 🎹 renderTable kb-btn: tab=' + tab + ', displayIdx=' + displayIdx + ', originalIdx=' + originalIdx + ', bot=' + (row['Бот'] || 'unknown') + ', hasKb=' + (row._keyboard ? 'yes' : 'no'));
          const hasKb = row._keyboard ? 'background:#4CAF50;' : 'background:#9E9E9E;';
          html += '<td class="cell-editor-wrap cell-with-tool">';
          html += '<textarea class="editable-cell" placeholder="'+placeholder+'" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'" data-name="'+escapeHtml(name)+'" rows="'+rows+'">'+escapeHtml(val)+'</textarea>';
          html += '<button class="kb-btn-cell" data-tab="'+tab+'" data-idx="'+originalIdx+'" style="'+hasKb+' color:white;">&#x2328;&#xFE0F; Кнопки</button>';
          html += '</td>';
        } else if (isTestColumn && (tab === 'Messages' || tab === 'Comments')) {
          // &#x1F9EA; Кнопка "Test" для нужного поля
          html += '<td class="cell-editor-wrap cell-with-tool">';
          html += '<textarea class="editable-cell" placeholder="'+placeholder+'" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'" data-name="'+escapeHtml(name)+'" rows="'+rows+'">'+escapeHtml(val)+'</textarea>';
          html += '<button class="test-btn" data-tab="'+tab+'" data-idx="'+originalIdx+'" data-col="'+name+'">Test</button>';
          html += '</td>';
        } else if (name === 'Триггер' && (tab === 'Messages' || tab === 'Comments')) {
          const triggerMode = normalizeTriggerMode(row._triggerMode);
          html += '<td class="cell-editor-wrap cell-with-tool cell-with-trigger-mode">';
          html += '<textarea class="editable-cell" placeholder="'+placeholder+'" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'" data-name="'+escapeHtml(name)+'" rows="'+rows+'">'+escapeHtml(val)+'</textarea>';
          html += '<div class="trigger-mode-wrap">';
          html += '<button class="trigger-mode-btn' + (triggerMode === 'TEXT' ? ' active' : '') + '" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'" data-mode="TEXT">ТЕКСТ</button>';
          html += '<button class="trigger-mode-btn' + (triggerMode === 'BUTTON' ? ' active' : '') + '" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'" data-mode="BUTTON">КНОПКА</button>';
          html += '<button class="trigger-mode-btn' + (triggerMode === 'FILE' ? ' active' : '') + '" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'" data-mode="FILE">ФАЙЛ</button>';
          html += '</div>';
          html += '</td>';
        } else if ((name === 'Бот' && canDuplicateRows) || (name === '№' && (tab === 'Mailing' || tab === 'Delayed')) || (name === 'ID' && tab === 'Users')) {
          html += '<td class="cell-editor-wrap cell-with-tool cell-with-copy">';
          html += '<textarea class="editable-cell" placeholder="'+placeholder+'" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'" data-name="'+escapeHtml(name)+'" rows="'+rows+'">'+escapeHtml(val)+'</textarea>';
          html += '<button class="copy-btn-cell" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'">Copy</button>';
          html += '</td>';
        } else {
          // ✅ Обычное поле textarea (расширяемое)
          html += '<td><textarea class="editable-cell" placeholder="'+placeholder+'" data-tab="'+escapeHtml(tab)+'" data-idx="'+originalIdx+'" data-name="'+escapeHtml(name)+'" rows="'+rows+'">'+escapeHtml(val)+'</textarea></td>';
        }
      }
    });
    // Кнопки действий по строке — используем ОРИГИНАЛЬНЫЙ индекс
    if (!isReadOnly) {
      html += '<td class="th-delete row-actions-cell"><div class="row-actions-wrap">';
      html += '<button class="btn btn-delete" data-tab="' + escapeHtml(tab) + '" data-idx="' + originalIdx + '">УДАЛИТЬ</button>';
      html += '</div></td>';
    }
    html += '</tr>';
  });
  html += '</tbody>';
  table.innerHTML = html;
  attachTableHandlers(tab);

  // &#x1F6E0;&#xFE0F; Навешиваем обработчики на кнопки "+" (для Вложения к ответу)
document.querySelectorAll('.attach-btn').forEach(btn => {
btn.addEventListener('click', (e) => {
  const tab = btn.getAttribute('data-tab');
  const idx = parseInt(btn.getAttribute('data-idx'));
  const col = btn.getAttribute('data-col');
  console.log('Attach button clicked:', {tab, idx, col});
  showAttachmentDialog(tab, idx, col);
});
});

// &#x1F9EA; Навешиваем обработчики на кнопки "Test"
document.querySelectorAll('.test-btn').forEach(btn => {
btn.addEventListener('click', (e) => {
  const tab = btn.getAttribute('data-tab');
  const idx = parseInt(btn.getAttribute('data-idx'));
  const col = btn.getAttribute('data-col');
  console.log('Test button clicked:', {tab, idx, col});
  showTestDialog(tab, idx, col);
});
});
}



// Файл: adminPanelHTML.js (внутри анонимной функции)
// Функция получения настроек бота с проверкой ответа
async function getBotSettings() {
  const baseUrl = window.location.href.split('?')[0];
  const url = baseUrl + '?getBotSettings';
  console.log('&#x1F916; Fetching bot settings from:', url);
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error('HTTP ' + res.status + ': ' + text.substring(0, 200));
  }
  const data = await res.json();
  console.log('? Bot settings loaded:', data);
  return data;
}






function showAttachmentDialog(tab, idx, col) {


        console.log('showAttachmentDialog called', {tab, idx, col});
        var existing = document.getElementById('attachmentModal');
        if (existing) existing.remove();

        var modal = document.createElement('div');
        modal.id = 'attachmentModal';
        modal.style.cssText = 'position:fixed; top:50%; left:50%; transform:translate(-50%,-50%); background:var(--modal-bg); color:var(--text-primary); padding:20px; border-radius:16px; border:1px solid var(--section-border); z-index:10000; min-width:320px; box-shadow:var(--container-shadow);';

  var title = document.createElement('h4');
  title.style.margin = '0 0 15px 0';
  title.textContent = 'Добавить вложение к ' + col;
  modal.appendChild(title);

  var fileLabel = document.createElement('label');
  fileLabel.textContent = 'Выберите файл:';
  var fileInput = document.createElement('input');
  fileInput.type = 'file';
  fileInput.id = 'attachFile';
  fileInput.accept = '*/*';
  fileInput.style.cssText = 'width:100%; margin-top:5px;';
  var fileDiv = document.createElement('div');
  fileDiv.style.marginBottom = '15px';
  fileDiv.appendChild(fileLabel);
  fileDiv.appendChild(fileInput);
  modal.appendChild(fileDiv);

  var buttonsDiv = document.createElement('div');
  buttonsDiv.style.cssText = 'display:flex; justify-content:flex-end; gap:10px;';
  var saveBtn = document.createElement('button');
  saveBtn.id = 'attachSave';
  saveBtn.textContent = 'Сохранить';
  saveBtn.style.cssText = 'background:#4CAF50; color:white; border:none; padding:8px 16px; border-radius:6px; cursor:pointer;';
  var cancelBtn = document.createElement('button');
  cancelBtn.id = 'attachCancel';
  cancelBtn.textContent = 'Отмена';
  cancelBtn.style.cssText = 'background:#f44336; color:white; border:none; padding:8px 16px; border-radius:6px; cursor:pointer;';
  buttonsDiv.appendChild(saveBtn);
  buttonsDiv.appendChild(cancelBtn);
  modal.appendChild(buttonsDiv);

  var statusDiv = document.createElement('div');
  statusDiv.id = 'attachStatus';
  statusDiv.style.cssText = 'margin-top:10px; font-size:12px; color:#999;';
  modal.appendChild(statusDiv);

  document.body.appendChild(modal);

saveBtn.onclick = function() {
  if (fileInput.files.length === 0) {
    statusDiv.innerText = '? Выберите файл';
    return;
  }

  var file = fileInput.files[0];
  var statusEl = statusDiv;
  statusEl.innerText = '🔄 Загрузка файла...';
  
  // ✅ Объявляем uploadAnimInterval заранее чтобы catch мог его видеть
  var uploadAnimInterval = null;

  // Проверяем размер файла
  var fileSizeMB = file.size / (1024 * 1024);
  var useRenderService = fileSizeMB > 3; // Файлы больше 3MB загружаем через Render
  var communityId = window.currentCommunityId || '';
  if (!communityId) {
    communityId = restoreCurrentCommunityIdFromStorage();
  }
  var groupId = '';
  var target = (tab === 'Messages') ? 'message' : 'comment';
  var baseUrl = window.location.href.split('?')[0];

  getBotSettings().then(async function(settings) {
    // ✅ Берём VK Token (Community Token), User Token и vk_group_id из конфига активного сообщества
    var communityToken = '';
    var userToken = '';

    const communityConfig = settings.communities?.[communityId] || {};
    
    // Community Token — первый из массива vk_tokens
    communityToken = communityConfig.vk_tokens?.[0] || communityConfig.vk_token || '';
    userToken = communityConfig.user_token || '';
    groupId = communityConfig.vk_group_id || '';

    if (!communityId) throw new Error('Сообщество не выбрано. Обновите страницу и выберите сообщество.');
    if (!communityToken) throw new Error('VK Token (Community Token) не настроен. Проверьте НАСТРОЙКА сообщества.');
    if (!userToken) throw new Error('User Token не настроен. Проверьте НАСТРОЙКА сообщества.');
    if (!groupId) throw new Error('VK Group ID не настроен');

    statusEl.innerText = '🌕 Загрузка в VK через сервис ' + (useRenderService ? 'Render' : 'PAPA BOT');
    var uploadPhase = 0;
    var uploadPhases = ['🌑', '🌘', '🌗', '🌖', '🌕', '🌔', '🌓', '🌒'];
    uploadAnimInterval = setInterval(function() {
      uploadPhase = (uploadPhase + 1) % uploadPhases.length;
      statusEl.innerText = uploadPhases[uploadPhase] + ' Загрузка в VK через сервис ' + (useRenderService ? 'Render' : 'PAPA BOT');
    }, 500);

    var reader = new FileReader();
    return new Promise(function(resolve, reject) {
      reader.onload = function() {
        var result = String(reader.result || '');
        var base64 = result.includes(',') ? result.split(',')[1] : result;

        if (useRenderService) {
          // Загружаем через Render для больших файлов
          var RENDER_SERVICE_URL = 'https://vk-uploader.onrender.com';
          var RENDER_UPLOADER_URL = 'https://vk-uploader.onrender.com/upload';
          var uploadId = 'upload_' + Date.now() + '_' + Math.random().toString(36).slice(2, 12);
          
          var formData = new FormData();
          var byteString = atob(base64);
          var ab = new ArrayBuffer(byteString.length);
          var ia = new Uint8Array(ab);
          for (var i = 0; i < byteString.length; i++) ia[i] = byteString.charCodeAt(i);
          var blob = new Blob([ab], { type: file.type || 'application/octet-stream' });
          formData.append('file', blob, file.name);
          formData.append('user_token', userToken);
          formData.append('community_token', communityToken);
          formData.append('group_id', groupId);
          formData.append('target', target);
          formData.append('upload_id', uploadId);

          var RENDER_INITIAL_UPLOAD_TIMEOUT_MS = 20000;
          var RENDER_RETRY_UPLOAD_TIMEOUT_MS = 120000;
          var RENDER_FINAL_RETRY_DELAY_MS = 10000;

          var uploadViaRender = function(timeoutMs) {
            return fetch(RENDER_UPLOADER_URL, {
              method: 'POST',
              body: formData,
              signal: AbortSignal.timeout(timeoutMs)
            }).then(function(response) {
              if (!response.ok) {
                return response.text().then(function(text) {
                  throw new Error('Render service error: ' + response.status + ' - ' + text.substring(0, 200));
                });
              }
              return response;
            });
          };

          var responseFromUploadResult = function(result) {
            return new Response(JSON.stringify(result), {
              status: 200,
              headers: { 'Content-Type': 'application/json' }
            });
          };

          var recoverRenderResult = function(timeoutMs) {
            var startedAt = Date.now();
            var backendRecoveryUrl = baseUrl;

            return new Promise(function(resolve, reject) {
              var poll = function() {
                fetch(backendRecoveryUrl, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({
                    action: 'recover_render_upload',
                    upload_id: uploadId
                  }),
                  signal: AbortSignal.timeout(5000)
                }).then(function(response) {
                  if (response.status === 202 || response.status === 404) {
                    if (Date.now() - startedAt >= timeoutMs) {
                      throw new Error('Render result is not ready');
                    }
                    setTimeout(poll, 2000);
                    return null;
                  }

                  if (!response.ok) {
                    return response.text().then(function(text) {
                      throw new Error('Render result error: ' + response.status + ' - ' + text.substring(0, 200));
                    });
                  }

                  return response.json().then(function(result) {
                    if (!result || !result.success) {
                      throw new Error((result && result.error) || 'Render result failed');
                    }
                    resolve(responseFromUploadResult(result));
                    return null;
                  });
                }).catch(function(error) {
                  if (Date.now() - startedAt >= timeoutMs) {
                    reject(error);
                    return;
                  }
                  setTimeout(poll, 2000);
                });
              };

              poll();
            });
          };

          var isRenderWakeCandidate = function(error) {
            var message = String((error && error.message) || '').toLowerCase();
            return !!(
              (error && error.name === 'AbortError') ||
              message.includes('fetch') ||
              message.includes('502') ||
              message.includes('503') ||
              message.includes('504') ||
              message.includes('timeout') ||
              message.includes('timed out') ||
              message.includes('network')
            );
          };

          uploadViaRender(RENDER_INITIAL_UPLOAD_TIMEOUT_MS).then(function(response) {
            clearInterval(uploadAnimInterval);
            resolve(response);
          }).catch(function(error) {
            if (!isRenderWakeCandidate(error)) {
              clearInterval(uploadAnimInterval);
              reject(new Error('Render недоступен: ' + error.message));
              return;
            }

            statusEl.innerText = '⏳ Render долго отвечает, проверяем результат и повторяем при необходимости...';
            recoverRenderResult(15000).then(function(response) {
              clearInterval(uploadAnimInterval);
              resolve(response);
            }).catch(function() {
            uploadViaRender(RENDER_RETRY_UPLOAD_TIMEOUT_MS).then(function(response) {
              clearInterval(uploadAnimInterval);
              resolve(response);
            }).catch(function(retryError) {
              if (!isRenderWakeCandidate(retryError)) {
                clearInterval(uploadAnimInterval);
                reject(new Error('Render недоступен: ' + retryError.message));
                return;
              }

              statusEl.innerText = '⌛ Render всё ещё обрабатывает файл, восстанавливаем результат или делаем последнюю попытку...';
              recoverRenderResult(30000).then(function(response) {
                clearInterval(uploadAnimInterval);
                resolve(response);
              }).catch(function() {
              setTimeout(function() {
                uploadViaRender(RENDER_RETRY_UPLOAD_TIMEOUT_MS).then(function(response) {
                  clearInterval(uploadAnimInterval);
                  resolve(response);
                }).catch(function(finalError) {
                  recoverRenderResult(60000).then(function(response) {
                    clearInterval(uploadAnimInterval);
                    resolve(response);
                  }).catch(function(recoveryError) {
                    clearInterval(uploadAnimInterval);
                    reject(new Error('Render загрузил файл, но браузер не получил ответ: ' + finalError.message + '. Recovery: ' + recoveryError.message));
                  });
                });
              }, RENDER_FINAL_RETRY_DELAY_MS);
              });
            });
            });
          });
        } else {
          // Загружаем через PAPA BOT backend для маленьких файлов
          fetch(baseUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
              action: 'upload_attachment',
              fileBase64: base64,
              fileName: file.name,
              fileType: file.type || 'application/octet-stream',
              fileSize: file.size || 0,
              target: target,
              groupId: groupId,
              communityId: communityId
            })
          }).then(function(response) {
            clearInterval(uploadAnimInterval);
            resolve(response);
          }).catch(function(error) {
            clearInterval(uploadAnimInterval);
            reject(error);
          });
        }
      };
      reader.onerror = function() {
        clearInterval(uploadAnimInterval);
        reject(new Error('Не удалось прочитать файл'));
      };
      reader.readAsDataURL(file);
    });
  })
  .then(function(response) { 
    if (!response.ok) {
      return response.text().then(function(text) {
        throw new Error('HTTP ' + response.status + ': ' + text.substring(0, 200));
      });
    }
    return response.json(); 
  })
  .then(function(result) {
    if (result.success) {
      var attachment = result.attachment;
      return persistUploadedCommunityFileRecord({
        fileName: file.name,
        fileType: file.type || 'application/octet-stream',
        fileSize: file.size || 0,
        attachment: attachment,
        communityId: communityId,
        groupId: groupId
      }).catch(function(error) {
        console.warn('[Admin] Failed to persist uploaded file record:', error);
      }).then(function() {
      // &#x1F6E0;&#xFE0F; ИСПРАВЛЕНИЕ: используем имя поля из параметра col
      var attachmentsField = col;
      if (!dataStore[tab][idx]) dataStore[tab][idx] = {};
      var currentAttachments = dataStore[tab][idx][attachmentsField] || '';
      var attachmentsList = currentAttachments ? currentAttachments.split(',').map(function(a) { return a.trim(); }).filter(function(a) { return a; }) : [];
      if (attachmentsList.indexOf(attachment) === -1) {
        attachmentsList.push(attachment);
        dataStore[tab][idx][attachmentsField] = attachmentsList.join(',');
        console.log('?? Attachment saved:', attachment);
        console.log('[Admin] 📎 Attachment stored in dataStore:', {
          tab: tab,
          rowIndex: idx,
          field: attachmentsField,
          value: dataStore[tab][idx][attachmentsField]
        });
      }
      renderTable(tab, dataStore[tab]);
      statusEl.innerText = '? Вложение добавлено: ' + attachment;
      setTimeout(function() { modal.remove(); }, 1500);
      });
    } else {
      throw new Error(result.error || 'Ошибка загрузки');
    }
  })
        .catch(function(err) {
            clearInterval(uploadAnimInterval);
            console.error('Upload error:', err);
            statusEl.innerText = '❌ Ошибка: ' + err.message;
        });
    }; // ? Закрывает saveBtn.onclick

    // ? cancelBtn.onclick (на том же уровне что и saveBtn.onclick)
    cancelBtn.onclick = function() {
        modal.remove();
    };

} // ? Закрывает showAttachmentDialog

// &#x1F9EA; Модальное окно тестовой отправки
function showTestDialog(tab, idx, col) {
    const existing = document.getElementById('testModal');
    if (existing) existing.remove();

    const row = dataStore[tab]?.[idx] || {};

    // Определяем тип теста и текст кнопки в зависимости от колонки
    let modalTitle = '🧪 Тест';
    let actionButtonText = 'Отправить';

    if (col === 'Ответ') {
        modalTitle = '🧪 Тест: Отправить ответ';
        actionButtonText = 'Отправить ответ';
    } else if (col === 'Шаг') {
        modalTitle = '🧪 Тест: Отправить на Шаг';
        actionButtonText = 'Отправить на Шаг';
    } else if (col === 'Задержка отправки на Шаг') {
        modalTitle = '🧪 Тест: Задержка отправки';
        actionButtonText = 'Применить и отправить';
    } else if (col === 'ДОБАВИТЬ ГРУППУ') {
        modalTitle = '🧪 Тест: Добавить в группу';
        actionButtonText = 'Добавить и отправить';
    } else if (col === 'УДАЛИТЬ ГРУППУ') {
        modalTitle = '🧪 Тест: Удалить из группы';
        actionButtonText = 'Удалить и отправить';
    } else if (col === 'Отправить на Шаг') {
        modalTitle = '🧪 Тест: Отправить на Шаг';
        actionButtonText = 'Отправить на Шаг';
    } else if (col === 'Действия с ПП/ГП/ПВК') {
        modalTitle = '🧪 Тест: Действия с переменными';
        actionButtonText = 'Применить и отправить';
    }

    const modal = document.createElement('div');
    modal.id = 'testModal';
    modal.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:var(--modal-bg);color:var(--text-primary);padding:25px;border-radius:18px;border:1px solid var(--section-border);z-index:10002;min-width:350px;max-width:500px;box-shadow:var(--container-shadow);';
    modal.innerHTML = '<h4 style="margin:0 0 15px 0;">' + modalTitle + '</h4>' +
        '<div style="margin-bottom:15px;">' +
        '<label style="display:block;margin-bottom:5px;font-size:13px;font-weight:bold;">👤 Выберите пользователя:</label>' +
        '<input type="text" id="testUserSearch" placeholder="Начните вводить имя или ID..." style="width:100%;padding:8px;border:1px solid var(--border-color);border-radius:10px;font-size:13px;background:var(--bg-input);color:var(--text-input);">' +
        '<div id="testUserList" style="max-height:200px;overflow-y:auto;margin-top:8px;border:1px solid var(--section-border);border-radius:12px;background:var(--surface-muted);"></div>' +
        '<input type="hidden" id="testUserId" value="">' +
        '</div>' +
        '<div id="testStatus" style="margin-bottom:10px;font-size:12px;min-height:20px;"></div>' +
        '<div style="display:flex;gap:10px;justify-content:flex-end;">' +
        '<button id="testSendBtn" style="background:#FF9800;color:white;border:none;padding:10px 20px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:bold;">' + actionButtonText + '</button>' +
        '<button id="testCancelBtn" style="background:#f44336;color:white;border:none;padding:10px 20px;border-radius:6px;cursor:pointer;">Отмена</button>' +
        '</div>';

    document.body.appendChild(modal);

    document.getElementById('testCancelBtn').onclick = function() { modal.remove(); };

    // Загрузка списка пользователей
    loadUsersForTest(tab);

    // Поиск пользователей
    document.getElementById('testUserSearch').addEventListener('input', function() {
        filterUserList(this.value.trim(), tab);
    });

    // Отправка
    document.getElementById('testSendBtn').onclick = function() {
        const userId = document.getElementById('testUserId').value.trim();
        if (!userId) {
            document.getElementById('testStatus').innerHTML = makeInlineText('error', '⚠️ Выберите пользователя');
            return;
        }
        executeTestSend(tab, idx, col, userId);
    };
}

// Загрузить пользователей для теста
async function loadUsersForTest(tab) {
    const userListEl = document.getElementById('testUserList');
    const statusEl = document.getElementById('testStatus');

    try {
        const baseUrl = window.location.href.split('?')[0];
        const sheet = sheetMap['Users'] || 'ПОЛЬЗОВАТЕЛИ';
        const settingsRes = await fetch(baseUrl + '?getBotSettings');
        const settingsData = await settingsRes.json();
        const communityConfig = settingsData.communities?.[window.currentCommunityId] || {};
        const vkGroupId = communityConfig.vk_group_id || window.currentCommunityId || '';

        const url = baseUrl + '?sheet=' + encodeURIComponent(sheet) +
                   (vkGroupId ? '&communityId=' + encodeURIComponent(vkGroupId) : '') +
                   '&t=' + Date.now();

        const res = await fetch(url);
        const users = await res.json();

        if (!Array.isArray(users) || users.length === 0) {
            userListEl.innerHTML = makeInlineNotice('info', 'Нет пользователей');
            return;
        }

        // Сохраняем в глобальную переменную для фильтрации
        window._testUsers = users;
        renderUserList(users);
    } catch (e) {
        statusEl.innerHTML = makeInlineText('error', '❌ Ошибка загрузки пользователей: ' + e.message);
        userListEl.innerHTML = makeInlineNotice('error', 'Ошибка загрузки');
    }
}

// Отрендерить список пользователей
function renderUserList(users) {
    const userListEl = document.getElementById('testUserList');
    if (!userListEl) return;

    if (!users || users.length === 0) {
        userListEl.innerHTML = makeInlineNotice('info', 'Ничего не найдено');
        return;
    }

        let html = '';
        users.slice(0, 50).forEach(user => {
            const id = user['ID'] || user['id'] || '';
            const name = user['ИМЯ'] || user['Имя'] || user['name'] || 'Пользователь ' + id;
        html += '<div class="test-user-item" data-user-id="' + id.replace(/"/g, '&quot;') + '" data-user-name="' + name.replace(/"/g, '&quot;') + '" ' +
            '>' +
            '<strong>' + escapeHtml(name) + '</strong> <span class="test-user-meta">(ID: ' + escapeHtml(id) + ')</span>' +
            '</div>';
    });
    userListEl.innerHTML = html;

    // Event delegation для кликов
    userListEl.querySelectorAll('.test-user-item').forEach(el => {
        el.addEventListener('click', function() {
            selectTestUser(this.dataset.userId, this.dataset.userName);
        });
    });
}

// Фильтровать список пользователей
function filterUserList(query, tab) {
    const users = window._testUsers || [];
    if (!query) {
        renderUserList(users);
        return;
    }
    const q = query.toLowerCase();
    const filtered = users.filter(user => {
        const id = String(user['ID'] || user['id'] || '');
        const name = String(user['ИМЯ'] || user['Имя'] || user['name'] || '');
        return id.includes(q) || name.toLowerCase().includes(q);
    });
    renderUserList(filtered);
}

// Выбрать пользователя для теста
window.selectTestUser = function(userId, userName) {
    document.getElementById('testUserId').value = userId;
    document.getElementById('testUserSearch').value = userName;
    // Подсветить выбранный
    document.querySelectorAll('.test-user-item').forEach(el => {
        el.classList.toggle('active', el.dataset.userId === userId);
    });
};

// Выполнить тестовую отправку
async function executeTestSend(tab, idx, col, userId) {
    const statusEl = document.getElementById('testStatus');
    const sendBtn = document.getElementById('testSendBtn');

    statusEl.innerHTML = makeInlineText('info', '🔄 Отправка...');
    sendBtn.disabled = true;
    sendBtn.style.opacity = '0.6';

    try {
        const row = dataStore[tab]?.[idx] || {};
        const baseUrl = window.location.href.split('?')[0];

        // Получаем текст ответа и вложения
        let text = '';
        let attachments = '';
        let keyboard = null;

        if (col === 'Ответ') {
            text = row['Ответ'] || 'Тестовое сообщение';
            attachments = row['Вложения к ответу'] || row['Вложения'] || '';

            // Собираем клавиатуру из кнопок ответа
            keyboard = buildKeyboardFromRow(row);
        } else if (col === 'Шаг') {
            // Для шага — находим строку с ЭТИМ шагом и берём её ответ + выполняем все действия
            const stepName = row['Шаг'] || '';
            // Ищем строку где колонка "Шаг" совпадает с именем этого шага и тот же бот
            const targetRow = dataStore[tab]?.find(r => r['Шаг'] === stepName && r['Бот'] === row['Бот']);

            if (targetRow) {
                // Отправляем ответ из найденной строки
                text = targetRow['Ответ'] || 'Тестовый ответ для шага "' + stepName + '"';
                attachments = targetRow['Вложения к ответу'] || targetRow['Вложения'] || '';
                keyboard = buildKeyboardFromRow(targetRow);

                // Собираем информацию о действиях, которые должны выполниться
                const actions = [];

                // 1. Задержка отправки
                if (targetRow['Задержка отправки на Шаг']) {
                    actions.push('⏱️ <strong>Задержка отправки:</strong> ' + escapeHtml(targetRow['Задержка отправки на Шаг']));
                }

                // 2. Добавить группу
                if (targetRow['ДОБАВИТЬ ГРУППУ']) {
                    actions.push('➕ <strong>Добавить в группу:</strong> ' + escapeHtml(targetRow['ДОБАВИТЬ ГРУППУ']));
                }

                // 3. Удалить из группы
                if (targetRow['УДАЛИТЬ ГРУППУ']) {
                    actions.push('➖ <strong>Удалить из группы:</strong> ' + escapeHtml(targetRow['УДАЛИТЬ ГРУППУ']));
                }

                // 4. Отправить на Шаг (переход)
                if (targetRow['Отправить на Шаг']) {
                    actions.push('🔄 <strong>Перевести на шаг:</strong> ' + escapeHtml(targetRow['Отправить на Шаг']));
                }

                // 5. Действия с переменными
                if (targetRow['Действия с ПП/ГП/ПВК']) {
                    actions.push('📊 <strong>Переменные:</strong> ' + escapeHtml(targetRow['Действия с ПП/ГП/ПВК']));
                }

                // Передаём actions на сервер чтобы они реально выполнились
                window._testStepActions = actions.length > 0 ? {
                    delay: targetRow['Задержка отправки на Шаг'] || '',
                    addGroup: targetRow['ДОБАВИТЬ ГРУППУ'] || '',
                    removeGroup: targetRow['УДАЛИТЬ ГРУППУ'] || '',
                    sendToStep: targetRow['Отправить на Шаг'] || '',
                    ppActions: targetRow['Действия с ПП'] || '',
                    gpActions: targetRow['Действия с ГП'] || '',
                    variableActions: targetRow['Действия с ПП/ГП/ПВК'] || '',
                    bot: targetRow['Бот'] || '',
                    step: targetRow['Шаг'] || ''
                } : null;
            } else {
                // Строка с этим шагом не найдена — просто отправляем текст
                text = '⚠️ Шаг "' + stepName + '" не найден в боте "' + (row['Бот'] || '') + '". Отправляю этот шаг как текст.';
                window._testStepActions = null;
            }
        }

        const stepActions = col === 'Шаг' ? (window._testStepActions || null) : null;

        // Получаем vk_group_id из настроек для сервера
        var vkGroupId = null;
        try {
            var settingsEl = document.getElementById('vkGroupId');
            if (settingsEl && settingsEl.value) {
                vkGroupId = settingsEl.value.trim();
            }
        } catch(e) {}

        const res = await fetch(baseUrl + '?testSend', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                userId: userId,
                text: text,
                attachments: attachments,
                keyboard: keyboard,
                communityId: window.currentCommunityId,
                vkGroupId: vkGroupId,
                stepActions: stepActions
            })
        });

        const result = await res.json();

        if (result.success) {
            let msg = makeInlineText('success', '✅ Сообщение отправлено! (message_id: ' + (result.messageId || 'ok') + ')');

            // Показать результаты выполненных действий
            if (result.actionResults && result.actionResults.length > 0) {
                var actionsHtml = '<div class="inline-notice inline-notice--accent" style="margin-top:12px;">' +
                    '<strong>🔧 Действия, которые должны выполниться:</strong><br><br>' +
                    result.actionResults.join('<br>') + '</div>';
                msg += actionsHtml;
            }

            statusEl.innerHTML = msg;
            window._testStepActions = null;
        } else {
            statusEl.innerHTML = makeInlineText('error', '❌ Ошибка: ' + escapeHtml(result.error || 'неизвестная') + (result.errorCode ? ' (код: ' + result.errorCode + ')' : ''));
        }
    } catch (e) {
        statusEl.innerHTML = makeInlineText('error', '❌ Ошибка: ' + escapeHtml(e.message));
    }

    sendBtn.disabled = false;
    sendBtn.style.opacity = '1';
}

// Собрать клавиатуру из строки данных
function buildKeyboardFromRow(row) {
    // ✅ Новый формат: _keyboard поле (JSON)
    if (row._keyboard) {
        try {
            const kb = typeof row._keyboard === 'string' ? JSON.parse(row._keyboard) : row._keyboard;
            return JSON.stringify(kb);
        } catch(e) {
            // fallback к старому формату
        }
    }

    // ✅ Старый формат: Кнопка Ответа-N, Цвет/Ссылка Ответа-N (обратная совместимость)
    const buttons = [];
    // Ищем кнопки ответа: Кнопка Ответа-1, Цвет/Ссылка Ответа-1, и т.д.
    for (let i = 1; i <= 10; i++) {
        const btnText = row['Кнопка Ответа-' + i];
        const colorLink = row['Цвет/Ссылка Ответа-' + i];
        if (btnText) {
            let color = 'primary';
            let linkUrl = null;
            if (colorLink) {
                const parts = colorLink.split('||');
                const c = parts[0].trim().toLowerCase();
                if (c === 'красный') color = 'negative';
                else if (c === 'зелёный') color = 'positive';
                else if (c === 'синий') color = 'primary';
                else if (c === 'белый') color = 'secondary';
                else if (c === 'ссылка...' && parts[1]) {
                    color = 'positive';
                    linkUrl = parts[1].trim();
                }
            }

            let btn;
            if (linkUrl) {
                // Кнопка-ссылка
                btn = {
                    action: {
                        type: 'open_link',
                        label: btnText,
                        link: linkUrl
                    }
                };
            } else {
                // Обычная текстовая кнопка
                btn = {
                    action: {
                        type: 'text',
                        label: btnText,
                        payload: { btn: i }
                    }
                };
                // Цвет только для text кнопок
                if (color) btn.action.color = color;
            }
            buttons.push([btn]);
        }
    }
    if (buttons.length === 0) return null;
    return JSON.stringify({ one_time: false, inline: true, buttons: buttons });
}

// ===== КОНСТРУКТОР КЛАВИАТУРЫ =====
// kbCtx.grid = 2D массив [row][col] — каждая ячейка содержит кнопку или null
var kbCtx = { tab: null, rowIdx: null, type: 'inline', grid: [], maxCols: 6, maxRows: 5, maxButtons: 10 };
var KB_COLORS = [
    {v:'primary',l:'Синий'},{v:'secondary',l:'Белый'},{v:'positive',l:'Зелёный'},{v:'negative',l:'Красный'}
];

function initKbGrid() {
    kbCtx.grid = [];
    for (var r = 0; r < kbCtx.maxRows; r++) {
        var row = [];
        for (var c = 0; c < kbCtx.maxCols; c++) {
            row.push(null);
        }
        kbCtx.grid.push(row);
    }
}

function countKbButtons() {
    var count = 0;
    for (var r = 0; r < kbCtx.grid.length; r++) {
        for (var c = 0; c < kbCtx.grid[r].length; c++) {
            if (kbCtx.grid[r][c]) count++;
        }
    }
    return count;
}

function openKeyboardModal(tab, rowIdx) {
    kbCtx.tab = tab;
    kbCtx.rowIdx = rowIdx;
    var row = dataStore[tab] ? dataStore[tab][rowIdx] : null;
    if (!row) { alert('Строка не найдена. Сохраните данные.'); return; }

    console.log('[Admin] 🎹 Opening keyboard modal, rowIdx:', rowIdx, '_keyboard:', row._keyboard ? 'present' : 'missing');
    console.log('[Admin] 🎹 Row keys:', Object.keys(row));

    if (row._keyboard) {
        try {
            console.log('[Admin] 🎹 _keyboard type:', typeof row._keyboard, 'length:', typeof row._keyboard === 'string' ? row._keyboard.length : 'N/A');
            if (typeof row._keyboard === 'string') {
                console.log('[Admin] 🎹 _keyboard raw (first 300):', row._keyboard.substring(0, 300));
            }
            var kb = typeof row._keyboard === 'string' ? JSON.parse(row._keyboard) : row._keyboard;
            console.log('[Admin] 🎹 Parsed kb:', JSON.stringify(kb).substring(0, 500));
            console.log('[Admin] 🎹 kb.inline:', kb.inline, 'kb.buttons rows:', kb.buttons ? kb.buttons.length : 0);
            if (kb.buttons) {
                kb.buttons.forEach(function(rb, i) {
                    console.log('[Admin] 🎹   Row', i, ':', rb ? rb.length : 0, 'buttons —', JSON.stringify(rb).substring(0, 200));
                });
            }
            kbCtx.type = kb.inline !== undefined ? (kb.inline ? 'inline' : 'default') : 'inline';
            // Загружаем ряды в grid
            initKbGrid();
            if (kb.buttons && Array.isArray(kb.buttons)) {
                for (var r = 0; r < kb.buttons.length && r < kbCtx.maxRows; r++) {
                    var rowBtns = kb.buttons[r];
                    if (Array.isArray(rowBtns)) {
                        for (var c = 0; c < rowBtns.length && c < kbCtx.maxCols; c++) {
                            var btn = rowBtns[c];
                            // Для open_link кнопок: если нет label, берём из link или hostname
                            if (btn && btn.action && btn.action.type === 'open_link' && !btn.action.label) {
                                try {
                                    var urlObj = new URL(btn.action.link);
                                    btn.action.label = urlObj.hostname.substring(0, 40);
                                } catch(e) {
                                    btn.action.label = 'Открыть';
                                }
                            }
                            kbCtx.grid[r][c] = btn;
                        }
                    }
                }
            }
            // Лог grid после загрузки
            var loadedBtnCount = 0;
            for (var r2 = 0; r2 < kbCtx.grid.length; r2++) {
                for (var c2 = 0; c2 < kbCtx.grid[r2].length; c2++) {
                    if (kbCtx.grid[r2][c2]) {
                        loadedBtnCount++;
                        var b = kbCtx.grid[r2][c2];
                        console.log('[Admin] 🎹   Grid[' + r2 + '][' + c2 + ']: type=' + (b.action ? b.action.type : '?') + ' label=' + (b.action ? b.action.label : 'N/A'));
                    }
                }
            }
            console.log('[Admin] 🎹 Loaded', loadedBtnCount, 'buttons into grid, type:', kbCtx.type);
        } catch(e) {
            console.error('[Admin] 🎹 Parse error:', e.message, e.stack);
            console.error('[Admin] 🎹 _keyboard was:', typeof row._keyboard === 'string' ? row._keyboard.substring(0, 200) : typeof row._keyboard);
            kbCtx.type = 'inline';
            initKbGrid();
        }
    } else {
        console.log('[Admin] 🎹 No _keyboard, creating empty grid');
        kbCtx.type = 'inline';
        initKbGrid();
    }

    document.querySelectorAll('input[name="kbType"]').forEach(function(r) { r.checked = r.value === kbCtx.type; });
    updateKbLimits();

    var grid = document.getElementById('kbGrid');
    if (!grid) { console.error('[Admin] kbGrid not found!'); return; }
    renderKbGrid();
    console.log('[Admin] 🎹 renderKbGrid done, grid children:', grid.children.length);

    var modal = document.getElementById('keyboardModal');
    if (!modal) { console.error('[Admin] keyboardModal not found!'); return; }
    // Принудительный reflow для гарантированной CSS перерисовки
    modal.style.display = 'none';
    void modal.offsetHeight;
    modal.style.display = 'flex';
    void modal.offsetHeight;

    setTimeout(function() {
        var btnEls = grid.querySelectorAll('.kb-btn');
        var visibleBtns = 0;
        btnEls.forEach(function(el) {
            if (el.offsetParent !== null) visibleBtns++;
        });
        console.log('[Admin] 🎹 Final visibility - kb-btn:', btnEls.length, 'visible:', visibleBtns);
        if (visibleBtns === 0 && btnEls.length > 0) {
            console.warn('[Admin] 🎹 Buttons not visible after 50ms! Forcing re-render...');
            modal.style.display = 'none';
            void modal.offsetHeight;
            modal.style.display = 'flex';
            void modal.offsetHeight;
            setTimeout(function() {
                var btns2 = grid.querySelectorAll('.kb-btn');
                var vis2 = 0;
                btns2.forEach(function(el) { if (el.offsetParent !== null) vis2++; });
                console.log('[Admin] 🎹 Re-render visibility - kb-btn:', btns2.length, 'visible:', vis2);
            }, 50);
        }
    }, 50);

    // Bind buttons
    document.getElementById('kbCloseBtn').onclick = closeKeyboardModal;
    document.getElementById('kbCancelBtn').onclick = closeKeyboardModal;
    document.getElementById('kbClearBtn').onclick = clearKeyboard;
    document.getElementById('kbSaveBtn').onclick = saveKeyboard;

    // Bind radio type change
    document.querySelectorAll('input[name="kbType"]').forEach(function(r) {
        r.onchange = onKbTypeChange;
    });
}

function closeKeyboardModal() {
    document.getElementById('keyboardModal').style.display = 'none';
}

function updateKbLimits() {
    var el = document.getElementById('kbLimits');
    var oldMaxCols = kbCtx.maxCols;
    if (kbCtx.type === 'inline') {
        kbCtx.maxCols = 6; kbCtx.maxRows = 5; kbCtx.maxButtons = 10;
        el.innerHTML = 'Сетка: 5&times;6 | До <span style="font-size:22px;font-weight:bold;color:#d32f2f;">10</span> кнопок | Текст: до 40 символов';
        el.style.borderLeftColor = '#2196F3';
    } else {
        kbCtx.maxCols = 10; kbCtx.maxRows = 5; kbCtx.maxButtons = 40;
        el.innerHTML = 'Сетка: 5&times;10 | До <span style="font-size:22px;font-weight:bold;color:#d32f2f;">40</span> кнопок | Текст: до 40 символов';
        el.style.borderLeftColor = '#9C27B0';
    }
    // Перестраиваем grid при изменении размеров, СОХРАНЯЯ данные
    if (kbCtx.grid && kbCtx.grid.length > 0 && kbCtx.grid[0].length !== kbCtx.maxCols) {
        var oldGrid = kbCtx.grid;
        kbCtx.grid = [];
        for (var r = 0; r < kbCtx.maxRows; r++) {
            var newRow = [];
            for (var c = 0; c < kbCtx.maxCols; c++) {
                newRow.push((oldGrid[r] && oldGrid[r][c]) || null);
            }
            kbCtx.grid.push(newRow);
        }
        console.log('[Admin] 🎹 Resized grid from', oldMaxCols, 'to', kbCtx.maxCols, 'cols');
    }
}

function onKbTypeChange() {
    kbCtx.type = document.querySelector('input[name="kbType"]:checked').value;
    // Rebuild grid to new dimensions
    var oldGrid = kbCtx.grid || [];
    kbCtx.grid = [];
    for (var r = 0; r < kbCtx.maxRows; r++) {
        var newRow = [];
        for (var c = 0; c < kbCtx.maxCols; c++) {
            newRow.push((oldGrid[r] && oldGrid[r][c]) || null);
        }
        kbCtx.grid.push(newRow);
    }
    updateKbLimits();
    renderKbGrid();
}

function renderKbGrid() {
    var grid = document.getElementById('kbGrid');
    grid.style.gridTemplateColumns = 'repeat(' + kbCtx.maxCols + ', 1fr)';
    grid.innerHTML = '';

    if (!kbCtx.grid) initKbGrid();

    var totalCells = kbCtx.maxCols * kbCtx.maxRows;
    for (var i = 0; i < totalCells; i++) {
        var rowIdx = Math.floor(i / kbCtx.maxCols);
        var colIdx = i % kbCtx.maxCols;
        var existingBtn = kbCtx.grid[rowIdx] ? kbCtx.grid[rowIdx][colIdx] : null;

        var cell = document.createElement('div');
        cell.className = 'kb-cell';
        cell.setAttribute('data-row', rowIdx);
        cell.setAttribute('data-col', colIdx);

        if (existingBtn && existingBtn.action) {
            var kbBtnDiv = document.createElement('div');
            kbBtnDiv.className = 'kb-btn';

            // Delete button
            var delBtn = document.createElement('button');
            delBtn.className = 'kb-del';
            delBtn.textContent = '\u00D7';
            delBtn.addEventListener('click', function(e) {
                e.stopPropagation();
                var cellEl = e.target.closest('.kb-cell');
                var r = parseInt(cellEl.getAttribute('data-row'), 10);
                var c = parseInt(cellEl.getAttribute('data-col'), 10);
                kbCtx.grid[r][c] = null;
                renderKbGrid();
            });
            kbBtnDiv.appendChild(delBtn);

            // Text input
            var txtInput = document.createElement('input');
            txtInput.type = 'text';
            txtInput.value = existingBtn.action.label || '';
            txtInput.placeholder = 'Текст';
            txtInput.maxLength = 40;
            txtInput.style.fontWeight = 'bold';
            txtInput.addEventListener('input', function(e) {
                var cellEl = e.target.closest('.kb-cell');
                var r = parseInt(cellEl.getAttribute('data-row'), 10);
                var c = parseInt(cellEl.getAttribute('data-col'), 10);
                var btn = kbCtx.grid[r][c];
                if (btn && btn.action) btn.action.label = e.target.value.substring(0, 40);
            });
            kbBtnDiv.appendChild(txtInput);

            var isLink = existingBtn.action.link ? true : false;
            // Для не-inline клавиатур VK НЕ поддерживает open_link — скрываем "Ссылка"
            var isInline = kbCtx.type === 'inline';

            // Type selector
            var typeSelect = document.createElement('select');
            var optColor = document.createElement('option');
            optColor.value = 'color';
            optColor.textContent = 'Цвет';
            if (!isLink) optColor.selected = true;
            typeSelect.appendChild(optColor);
            if (isInline) {
                var optLink = document.createElement('option');
                optLink.value = 'link';
                optLink.textContent = 'Ссылка';
                if (isLink) optLink.selected = true;
                typeSelect.appendChild(optLink);
            }
            typeSelect.addEventListener('change', function(e) {
                var cellEl = e.target.closest('.kb-cell');
                var r = parseInt(cellEl.getAttribute('data-row'), 10);
                var c = parseInt(cellEl.getAttribute('data-col'), 10);
                var btn = kbCtx.grid[r][c];
                if (!btn || !btn.action) return;
                var linkInput = cellEl.querySelector('.kb-link-input');
                var colorSelect = cellEl.querySelector('.kb-color');
                if (e.target.value === 'link') {
                    btn.action.type = 'open_link';
                    btn.action.link = btn.action.link || '';
                    delete btn.action.payload;
                    if (linkInput) linkInput.classList.add('show');
                    if (colorSelect) colorSelect.style.display = 'none';
                } else {
                    btn.action.type = 'text';
                    btn.action.payload = {};
                    if (!btn.color) btn.color = 'primary';
                    if (linkInput) linkInput.classList.remove('show');
                    if (colorSelect) colorSelect.style.display = '';
                }
            });
            kbBtnDiv.appendChild(typeSelect);

            // Link input
            var linkInput = document.createElement('input');
            linkInput.type = 'text';
            linkInput.className = 'kb-link-input' + (isLink ? ' show' : '');
            linkInput.value = existingBtn.action.link || '';
            linkInput.placeholder = 'https://...';
            linkInput.addEventListener('input', function(e) {
                var cellEl = e.target.closest('.kb-cell');
                var r = parseInt(cellEl.getAttribute('data-row'), 10);
                var c = parseInt(cellEl.getAttribute('data-col'), 10);
                var btn = kbCtx.grid[r][c];
                if (btn && btn.action) btn.action.link = e.target.value;
            });
            kbBtnDiv.appendChild(linkInput);

            // Color selector
            var colorSelect = document.createElement('select');
            colorSelect.className = 'kb-color';
            if (isLink) colorSelect.style.display = 'none';
            for (var ci = 0; ci < KB_COLORS.length; ci++) {
                var opt = document.createElement('option');
                opt.value = KB_COLORS[ci].v;
                opt.textContent = KB_COLORS[ci].l;
                if (existingBtn.color === KB_COLORS[ci].v) opt.selected = true;
                colorSelect.appendChild(opt);
            }
            colorSelect.addEventListener('change', function(e) {
                var cellEl = e.target.closest('.kb-cell');
                var r = parseInt(cellEl.getAttribute('data-row'), 10);
                var c = parseInt(cellEl.getAttribute('data-col'), 10);
                var btn = kbCtx.grid[r][c];
                if (btn) btn.color = e.target.value;
            });
            kbBtnDiv.appendChild(colorSelect);

            cell.appendChild(kbBtnDiv);
        } else {
            // Placeholder for adding
            var btnCount = countKbButtons();
            if (btnCount < kbCtx.maxButtons) {
                var ph = document.createElement('div');
                ph.className = 'kb-placeholder';
                ph.textContent = '+ Кнопка';
                ph.addEventListener('click', function(e) {
                    var cellEl = e.target.closest('.kb-cell');
                    var r = parseInt(cellEl.getAttribute('data-row'), 10);
                    var c = parseInt(cellEl.getAttribute('data-col'), 10);
                    kbCtx.grid[r][c] = { action: { type: 'text', label: '', payload: {} }, color: 'primary' };
                    renderKbGrid();
                });
                cell.appendChild(ph);
            } else {
                var empty = document.createElement('div');
                empty.style.width = '100%';
                empty.style.height = '65px';
                cell.appendChild(empty);
            }
        }
        grid.appendChild(cell);
    }
}

function clearKeyboard() {
    if (confirm('Очистить всю клавиатуру?')) {
        initKbGrid();
        renderKbGrid();
    }
}

function saveKeyboard() {
    var errors = [];
    var validButtons = [];
    for (var r = 0; r < kbCtx.grid.length; r++) {
        var rowBtns = [];
        for (var c = 0; c < kbCtx.grid[r].length; c++) {
            var b = kbCtx.grid[r][c];
            if (b && b.action && b.action.label && b.action.label.trim()) {
                var label = b.action.label.trim();
                if (label.length > 40) {
                    errors.push('Ряд ' + (r + 1) + ', кнопка "' + label.substring(0, 20) + '..." — текст больше 40 символов (' + label.length + ')');
                    continue;
                }
                var btnObj;
                if (b.action.type === 'open_link') {
                    if (!b.action.link || !b.action.link.trim()) {
                        errors.push('Ряд ' + (r + 1) + ', кнопка "' + label + '" — укажите ссылку');
                        continue;
                    }
                    // Для inline open_link: label ОБЯЗАТЕЛЕН для VK API
                    btnObj = { action: { type: 'open_link', label: label, link: b.action.link.trim() } };
                } else {
                    btnObj = { action: { type: 'text', label: label, payload: b.action.payload || {} }, color: b.color || 'primary' };
                }
                rowBtns.push(btnObj);
            }
        }
        if (rowBtns.length > 0) {
            validButtons.push(rowBtns);
        }
    }

    if (errors.length > 0) {
        var NL = String.fromCharCode(10);
        var errorMsg = 'Ошибки валидации:' + NL + NL + errors.join(NL) + NL + NL + 'Исправьте и сохраните снова.';
        alert(errorMsg);
        return;
    }

    // Разрешаем сохранять пустую клавиатуру (удаление всех кнопок)
    // Проверка лимитов
    var totalBtns = 0;
    for (var i = 0; i < validButtons.length; i++) totalBtns += validButtons[i].length;

    if (kbCtx.type === 'inline' && totalBtns > 10) {
        alert('Для клавиатуры "В сообщении" максимум 10 кнопок (сейчас ' + totalBtns + ')');
        return;
    }
    if (kbCtx.type === 'default' && totalBtns > 40) {
        alert('Для клавиатуры "Под сообщением" максимум 40 кнопок (сейчас ' + totalBtns + ')');
        return;
    }
    if (validButtons.length > 10) {
        alert('Максимум 10 рядов в клавиатуре');
        return;
    }

    var kb = {
        one_time: false,
        inline: kbCtx.type === 'inline',
        buttons: validButtons
    };

    var kbStr = JSON.stringify(kb);
    console.log('[Admin] 💾 saveKeyboard:', kbStr.substring(0, 500));

    // НЕ используем syncTableData — она может перезаписать данные ДРУГИХ ботов!
    // Вместо этого сохраняем только _keyboard напрямую в нужную строку
    var row = dataStore[kbCtx.tab] ? dataStore[kbCtx.tab][kbCtx.rowIdx] : null;
    if (row) {
        row._keyboard = kbStr;
        console.log('[Admin] 💾 _keyboard saved to dataStore[' + kbCtx.rowIdx + '], length:', kbStr.length);
    } else {
        console.error('[Admin] ❌ saveKeyboard: row ' + kbCtx.rowIdx + ' not found in dataStore!');
    }

    closeKeyboardModal();
    renderTable(kbCtx.tab, dataStore[kbCtx.tab]);
}











function attachTableHandlers(tab) {
const table = document.getElementById('table-'+tab);
if (!table) return;
table.querySelectorAll('.color-select').forEach(select => {
select.removeEventListener('change', handleColorSelectChange);
select.addEventListener('change', handleColorSelectChange);
});
table.querySelectorAll('.editable-cell').forEach(input => {
input.removeEventListener('change', handleCellChange);
input.addEventListener('change', handleCellChange);
});
table.querySelectorAll('.btn-delete').forEach(btn => {
btn.removeEventListener('click', handleDeleteRow);
btn.addEventListener('click', handleDeleteRow);
});
table.querySelectorAll('.btn-duplicate').forEach(btn => {
btn.removeEventListener('click', handleDuplicateRow);
btn.addEventListener('click', handleDuplicateRow);
});
table.querySelectorAll('.copy-btn-cell').forEach(btn => {
btn.removeEventListener('click', handleDuplicateRow);
btn.addEventListener('click', handleDuplicateRow);
});
table.querySelectorAll('.trigger-mode-btn').forEach(btn => {
btn.removeEventListener('click', handleTriggerModeClick);
btn.addEventListener('click', handleTriggerModeClick);
});
// Конструктор клавиатуры — делегирование событий
table.querySelectorAll('.kb-btn-cell').forEach(btn => {
    const rowIdx = parseInt(btn.getAttribute('data-idx'), 10);
    btn.addEventListener('click', function(e) {
        console.log('[Admin] 🎹 kb-btn-cell clicked: data-idx=' + rowIdx + ', bot=' + (dataStore[tab] && dataStore[tab][rowIdx] ? dataStore[tab][rowIdx]['Бот'] : 'N/A'));
    });
    btn.removeEventListener('click', handleKbBtnClick);
    btn.addEventListener('click', handleKbBtnClick);
});
} // ← Закрываем attachTableHandlers

function handleKbBtnClick(e) {
    const btn = e.target;
    const tab = btn.getAttribute('data-tab');
    const idx = parseInt(btn.getAttribute('data-idx'), 10);
    console.log('[Admin] 🎹 handleKbBtnClick: tab=' + tab + ', data-idx=' + idx);
    openKeyboardModal(tab, idx);
}




function handleColorSelectChange(e) {
const select = e.target;
const tab = select.getAttribute('data-tab');
const idx = parseInt(select.getAttribute('data-idx'), 10);
const name = select.getAttribute('data-name');
const color = select.value;
const cell = select.closest('td');
const linkInput = cell ? cell.querySelector('.link-input') : null;
const linkValue = linkInput ? linkInput.value : undefined;
updateColorLink(tab, idx, name, color, linkValue);
}




function handleCellChange(e) {
const input = e.target;
const tab = input.getAttribute('data-tab');
const idx = parseInt(input.getAttribute('data-idx'), 10);
const name = input.getAttribute('data-name');
updateCell(tab, idx, name, input.value);
}




function handleDeleteRow(e) {
const btn = e.target;
const tab = btn.getAttribute('data-tab');
const idx = parseInt(btn.getAttribute('data-idx'), 10);
if (confirm('Удалить строку?')) {
dataStore[tab].splice(idx, 1);
renderTable(tab, dataStore[tab]);
debug('Deleted row '+idx+' from '+tab);
}
}

function rerenderAfterRowClone(tab) {
if (tab === 'Users' && typeof applyUserFilters === 'function') {
applyUserFilters();
return;
}
renderTable(tab, dataStore[tab]);
}

function handleDuplicateRow(e) {
const btn = e.target;
const tab = btn.getAttribute('data-tab');
const idx = parseInt(btn.getAttribute('data-idx'), 10);
if (!dataStore[tab] || !dataStore[tab][idx]) return;

const duplicatedRow = JSON.parse(JSON.stringify(dataStore[tab][idx]));
dataStore[tab].splice(idx + 1, 0, duplicatedRow);
rerenderAfterRowClone(tab);
debug('Duplicated row ' + idx + ' in ' + tab);
}

function handleTriggerModeClick(e) {
const btn = e.target;
const tab = btn.getAttribute('data-tab');
const idx = parseInt(btn.getAttribute('data-idx'), 10);
const mode = normalizeTriggerMode(btn.getAttribute('data-mode'));
if (!dataStore[tab] || !dataStore[tab][idx]) return;
dataStore[tab][idx]._triggerMode = mode;
renderTable(tab, dataStore[tab]);
debug('Trigger mode changed to ' + mode + ' for row ' + idx + ' in ' + tab);
}











window.checkTokens = async function() {
    const tokensText = document.getElementById('vkTokens').value.trim();
    const tokens = tokensText.split('\\n').filter(t => t.trim());
    
    const statusDiv = document.getElementById('tokensStatus');
    statusDiv.innerHTML = makeInlineNotice('info', '🔍 Проверка токенов...');
    
    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?checkVkTokens', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tokens: tokens })
        });
        const data = await res.json();
        
        if (data.results) {
            let html = '<div class="token-results">';
            data.results.forEach(function(r, i) {
                const icon = r.valid ? '✅' : '❌';
                const cls = r.valid ? 'valid' : 'invalid';
                const statusText = r.valid ? 'Валиден' : 'Ошибка: ' + r.error;
                html += '<div class="token-result ' + cls + '">' + icon + ' Токен ' + (i + 1) + ': ' + statusText + '</div>';
            });
            html += '</div>';
            statusDiv.innerHTML = html;
        }
    } catch (e) {
        statusDiv.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + e.message);
    }
};





window.updateCell = function(tab, idx, key, val) {
if (!dataStore[tab][idx]) dataStore[tab][idx] = {};
dataStore[tab][idx][key] = val;
};




window.updateColorLink = function(tab, idx, key, color, linkValue) {
console.log('?? updateColorLink CALL:', {tab, idx, key, color, linkValue});
debug('?? updateColorLink ВЫЗВАНА: tab='+tab+', idx='+idx+', key='+key+', color='+color+', linkValue='+linkValue);
if (!dataStore[tab]) {
console.error('? dataStore['+tab+'] НЕ СУЩЕСТВУЕТ!');
debug('? ОШИБКА: dataStore['+tab+'] не существует!');
dataStore[tab] = [];
}
if (!dataStore[tab][idx]) {
console.error('? dataStore['+tab+']['+idx+'] НЕ СУЩЕСТВУЕТ! Создаю...');
debug('?? Создаю новый объект для dataStore['+tab+']['+idx+']');
dataStore[tab][idx] = {};
}
if (linkValue !== undefined && linkValue !== null && linkValue !== '') {
dataStore[tab][idx][key] = color + '||' + linkValue;
console.log('? Saved (with link):', key, '=', dataStore[tab][idx][key]);
debug('? Сохранено (с ссылкой): dataStore['+tab+']['+idx+']['+key+'] = "'+dataStore[tab][idx][key]+'"');
} else {
dataStore[tab][idx][key] = color;
console.log('? Saved (no link):', key, '=', dataStore[tab][idx][key]);
debug('? Сохранено (без ссылки): dataStore['+tab+']['+idx+']['+key+'] = "'+dataStore[tab][idx][key]+'"');
}
console.log('?? VERIFY:', key, '=', dataStore[tab][idx][key]);
debug('?? ПРОВЕРКА: dataStore['+tab+']['+idx+']['+key+'] = "'+dataStore[tab][idx][key]+'"');
};




document.addEventListener('change', function(e) {
if (e.target.classList.contains('color-select')) {
const cell = e.target.closest('td');
const linkInput = cell?.querySelector('.link-input');
if (linkInput) {
linkInput.style.display = e.target.value === 'ССЫЛКА...' ? 'block' : 'none';
if (e.target.value !== 'ССЫЛКА...') {
linkInput.value = '';
}
}
}
});




window.deleteRow = function(tab, idx) {
if (confirm('Удалить строку?')) {
dataStore[tab].splice(idx, 1);
renderTable(tab, dataStore[tab]);
debug('Deleted row '+idx+' from '+tab);
}
};




window.deleteAllRows = async function(btn) {
    const tab = btn.getAttribute('data-tab');
    if (!confirm('WARNING: Delete ALL rows from "' + tab + '"?')) return;

    dataStore[tab] = [];
    renderTable(tab, dataStore[tab]);

    // ОЧИЩАЕМ список ботов для этой вкладки в localStorage
    if (tab === 'Messages' || tab === 'Comments') {
        const botsKey = getBotsKey(tab);
        delete window.botsList[botsKey];
        delete window.activeBot[tab];
        saveBotsListToStorage();
        saveActiveBotToStorage();
        console.log('[Admin] 🤖 deleteAllRows: cleared bots for ' + botsKey);
    }

    // АВТОМАТИЧЕСКИ сохраняем пустые данные на сервер и ЖДЁМ завершения
    const saved = await saveDataDirectly(tab);
    if (!saved) {
        console.error('[Admin] ❌ deleteAllRows: save failed! Data may not be deleted on server.');
        showStatus('Ошибка сохранения! Данные могут не удалиться на сервере.', 'error');
    }

    debug('Deleted ALL rows from ' + tab);
    showStatus('Все строки удалены и сохранены!', 'error');
};




// Синхронизирует текущие данные из DOM таблицы обратно в dataStore
// Вызывается перед renderTable чтобы не потерять несохранённые изменения
function syncTableData(tab) {
    var table = document.getElementById('table-' + tab);
    if (!table || !dataStore[tab]) return;

    // ВАЖНО: при фильтрации по боту строки в DOM соответствуют НЕ всем строкам dataStore
    // Используем data-idx атрибуты для правильного маппинга
    var rows = table.querySelectorAll('tbody tr');
    var syncedCount = 0;
    rows.forEach(function(tr) {
        // Пробуем получить оригинальный индекс из data-idx атрибута первого input
        var firstInput = tr.querySelector('input[data-idx], textarea[data-idx]');
        var rowIdx = firstInput ? parseInt(firstInput.getAttribute('data-idx'), 10) : -1;

        // Если нет data-idx, пропускаем
        if (rowIdx < 0 || !dataStore[tab][rowIdx]) return;

        var inputs = tr.querySelectorAll('input.editable-cell, textarea.editable-cell');
        inputs.forEach(function(input) {
            var name = input.getAttribute('data-name');
            if (name) {
                dataStore[tab][rowIdx][name] = input.value;
                syncedCount++;
            }
        });

        var selects = tr.querySelectorAll('select.color-select');
        selects.forEach(function(select) {
            var name = select.getAttribute('data-name');
            if (name) {
                var cell = select.closest('td');
                var linkInput = cell ? cell.querySelector('.link-input') : null;
                var linkValue = linkInput ? linkInput.value : undefined;
                if (linkValue !== undefined) {
                    dataStore[tab][rowIdx][name] = select.value + '||' + linkValue;
                } else {
                    dataStore[tab][rowIdx][name] = select.value;
                }
                syncedCount++;
            }
        });
    });
    if (syncedCount > 0) {
        console.log('[Admin] 🔄 syncTableData: synced ' + syncedCount + ' fields from ' + rows.length + ' DOM rows for ' + tab);
    }
}

// ===== СИСТЕМА БОТОВ =====
// Хранит состояние активного бота для каждой вкладки
// Формат: { 'Messages': 'Бот1', 'Comments': 'Бот1' }
// ПЕРСИСТЕНТНОСТЬ: сохраняется в localStorage чтобы не теряться при перезагрузке
function loadBotsFromStorage() {
    try {
        var saved = localStorage.getItem('vkBot_activeBot');
        if (saved) window.activeBot = JSON.parse(saved);
    } catch(e) { console.error('Failed to load activeBot:', e); }
    try {
        var saved = localStorage.getItem('vkBot_botsList');
        if (saved) window.botsList = JSON.parse(saved);
    } catch(e) { console.error('Failed to load botsList:', e); }
}

function saveActiveBotToStorage() {
    try { localStorage.setItem('vkBot_activeBot', JSON.stringify(window.activeBot)); }
    catch(e) { console.error('Failed to save activeBot:', e); }
}

function saveBotsListToStorage() {
    try { localStorage.setItem('vkBot_botsList', JSON.stringify(window.botsList)); }
    catch(e) { console.error('Failed to save botsList:', e); }
}

window.activeBot = window.activeBot || {};
window.botsList = window.botsList || {};
loadBotsFromStorage();

// Получает ключ для botsList на основе текущего сообщества и вкладки
function getBotsKey(tab) {
    return (window.currentCommunityId || 'default') + '_' + tab;
}

// Получает список ботов для текущей вкладки
function getBotsForTab(tab) {
    const key = getBotsKey(tab);
    return window.botsList[key] || [];
}

// Устанавливает список ботов для текущей вкладки
function setBotsForTab(tab, bots) {
    const key = getBotsKey(tab);
    window.botsList[key] = bots;
    saveBotsListToStorage();
}

// Рендерит кнопки ботов для вкладки
function renderBotButtons(tab) {
    const switcherEl = document.getElementById('botSwitcher-' + tab);
    const buttonsEl = document.getElementById('botButtons-' + tab);
    if (!switcherEl || !buttonsEl) return;

    const bots = getBotsForTab(tab);
    const activeBot = getActiveBot(tab);

    if (bots.length === 0) {
        switcherEl.style.display = 'none';
        return;
    }

    switcherEl.style.display = 'block';
    buttonsEl.innerHTML = '';

    bots.forEach(botName => {
        const btn = document.createElement('button');
        const isActive = botName === activeBot;
        btn.className = 'btn';
        btn.style.background = isActive ? '#2E7D32' : '#e0e0e0';
        btn.style.color = isActive ? 'white' : '#333';
        btn.style.border = isActive ? '2px solid #1B5E20' : '1px solid #ccc';
        btn.style.padding = '4px 10px';
        btn.style.borderRadius = '6px';
        btn.style.cursor = 'pointer';
        btn.style.fontSize = '11px';
        btn.textContent = botName + (isActive ? ' ✓' : '');
        btn.onclick = function() { switchBot(botName, tab); };

        // Добавляем иконку редактирования при наведении
        btn.ondblclick = function() { renameBot(botName, tab); };
        btn.title = 'Нажмите чтобы переключиться. Двойной клик чтобы переименовать.';

        buttonsEl.appendChild(btn);
    });
}

// Переключить активного бота
window.switchBot = function(botName, tab) {
    console.log('[Admin] 🤖 switchBot called: bot=' + botName + ', tab=' + tab + ', communityId=' + (window.currentCommunityId || 'default'));
    // НЕ сохраняем при переключении — сохраняем только по кнопке "Сохранить"
    setActiveBot(tab, botName);
    renderBotButtons(tab);
    // Перерендерить таблицу с данными для этого бота
    renderTable(tab, dataStore[tab] || []);
    debug('🤖 Переключен на бота: ' + botName + ' (вкладка ' + tab + ')');
}

// Установить активный бот для вкладки
function setActiveBot(tab, botName) {
    const botsKey = getBotsKey(tab);
    console.log('[Admin] 🤖 setActiveBot: tab=' + tab + ', bot=' + botName + ', botsKey=' + botsKey + ', communityId=' + (window.currentCommunityId || 'default'));
    window.activeBot[tab] = botName;
    saveActiveBotToStorage();
}

// Получить активный бот для вкладки (по умолчанию первый в списке)
function getActiveBot(tab) {
    const bots = getBotsForTab(tab);
    const botsKey = getBotsKey(tab);
    const currentActive = window.activeBot[tab];
    console.log('[Admin] 🤖 getActiveBot: tab=' + tab + ', botsKey=' + botsKey + ', botsList=' + JSON.stringify(bots) + ', currentActive=' + currentActive);
    if (bots.length === 0) return '';
    if (currentActive && bots.includes(currentActive)) return currentActive;
    console.log('[Admin] 🤖 getActiveBot: returning first bot: ' + (bots[0] || 'none'));
    return bots[0];
}

// Показать модальное окно для добавления бота
window.showAddBotModal = function(tab) {
    const existing = document.getElementById('addBotModal');
    if (existing) existing.remove();

    const modal = document.createElement('div');
    modal.id = 'addBotModal';
    modal.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:var(--modal-bg);color:var(--text-primary);padding:25px;border-radius:18px;border:1px solid var(--section-border);z-index:10001;min-width:300px;box-shadow:var(--container-shadow);';
    modal.innerHTML = '<h4 style="margin:0 0 15px 0;">🤖 Добавить нового бота</h4>' +
        '<input type="text" id="newBotName" placeholder="Название бота" style="width:100%;padding:10px;margin-bottom:15px;border:1px solid var(--border-color);border-radius:10px;font-size:14px;background:var(--bg-input);color:var(--text-input);">' +
        '<div style="display:flex;gap:10px;justify-content:flex-end;">' +
        '<button id="addBotSave" style="background:#9C27B0;color:white;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;">Добавить</button>' +
        '<button id="addBotCancel" style="background:#f44336;color:white;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;">Отмена</button>' +
        '</div>';

    document.body.appendChild(modal);

    const input = document.getElementById('newBotName');
    input.focus();

    document.getElementById('addBotSave').onclick = function() {
        const name = input.value.trim();
        if (!name) {
            alert('Введите название бота');
            return;
        }
        addBot(name, tab);
        modal.remove();
    };

    document.getElementById('addBotCancel').onclick = function() {
        modal.remove();
    };

    input.onkeydown = function(e) {
        if (e.key === 'Enter') {
            document.getElementById('addBotSave').click();
        }
        if (e.key === 'Escape') {
            modal.remove();
        }
    };
}

// Добавить нового бота
window.addBot = function(botName, tab) {
    const bots = getBotsForTab(tab);

    // Проверка на дубликат
    if (bots.includes(botName)) {
        alert('Бот с таким именем уже существует');
        return;
    }

    bots.push(botName);
    setBotsForTab(tab, bots);
    setActiveBot(tab, botName);

    // Добавляем 1 шаг для нового бота
    addStepForBot(tab, botName);

    renderBotButtons(tab);
    debug('🤖 Добавлен бот: ' + botName + ' (вкладка ' + tab + ')');
}

// Переименовать бота (двойной клик по кнопке)
window.renameBot = function(oldName, tab) {
    const newName = prompt('Введите новое название для бота "' + oldName + '":', oldName);
    if (!newName || newName.trim() === '' || newName === oldName) return;

    const trimmedName = newName.trim();
    const bots = getBotsForTab(tab);

    // Проверка на дубликат
    if (bots.includes(trimmedName) && trimmedName !== oldName) {
        alert('Бот с таким именем уже существует');
        return;
    }

    // Обновить имя бота в списке
    const idx = bots.indexOf(oldName);
    if (idx !== -1) {
        bots[idx] = trimmedName;
        setBotsForTab(tab, bots);
    }

    // Обновить имя в активной вкладке
    if (window.activeBot[tab] === oldName) {
        setActiveBot(tab, trimmedName);
    }

    // ОБНОВИТЬ ИМЯ ВО ВСЕХ СТРОКАХ ГДЕ ОНО ИСПОЛЬЗОВАЛОСЬ
    if (dataStore[tab]) {
        dataStore[tab].forEach(row => {
            if (row['Бот'] === oldName) {
                row['Бот'] = trimmedName;
            }
        });
        renderTable(tab, dataStore[tab]);
    }

    renderBotButtons(tab);
    debug('🤖 Бот переименован: ' + oldName + ' → ' + trimmedName);
}

// Добавить шаг для конкретного бота (внутренняя функция)
function addStepForBot(tab, botName) {
    if (!dataStore[tab]) dataStore[tab] = [];
    const cols = columns[tab] || [];
    const template = {};
    cols.forEach(col => {
        const name = typeof col === 'object' ? col.name : col;
        if (name === 'Тип кнопок ответа' || name === 'Тип кнопок ЗО') {
            template[name] = 'ДА';
        } else {
            template[name] = '';
        }
    });
    // Автозаполняем колонку "Бот" именем бота
    if (template.hasOwnProperty('Бот') || cols.some(c => (typeof c === 'object' ? c.name : c) === 'Бот')) {
        template['Бот'] = botName;
    }
    dataStore[tab].push(template);
    renderTable(tab, dataStore[tab]);
    debug('📝 Добавлен шаг для бота ' + botName + ' (вкладка ' + tab + ')');
}

// ===== ФУНКЦИЯ addStep (публичная, вызывается из HTML) =====
window.addStep = function(tab) {
    const activeBot = getActiveBot(tab);
    if (!activeBot) {
        // Если нет ботов — сначала добавим бота
        showAddBotModal(tab);
        return;
    }
    addStepForBot(tab, activeBot);
};

// ===== СТАРАЯ ФУНКЦИЯ addRow (оставлена для совместимости с другими вкладками) =====
window.addRow = function(tab) {
if (tab === 'Triggers') {
resetStructuredTriggerForm();
return;
}
if (tab === 'VK_Variables') {
return;
}
if (!dataStore[tab]) dataStore[tab] = [];
const cols = columns[tab] || [];
const template = {};
cols.forEach(col => {
const name = typeof col === 'string' ? col : col.name;
if (name === 'Тип кнопок ответа' || name === 'Тип кнопок ЗО') {
template[name] = 'ДА';
} else {
template[name] = '';
}
});
if (template['№']) template['№'] = (dataStore[tab].length + 1).toString();
dataStore[tab].push(template);
renderTable(tab, dataStore[tab]);
debug('Added row to '+tab);
};




window.saveData = async function(btn, tab) {
showSaveOverlay();

syncDataFromTable(tab);

// Для Variables/Variables_User/VK_Variables - объединяем перед сохранением
let saveData = dataStore[tab];
if (tab === 'Variables' || tab === 'Variables_User' || tab === 'VK_Variables') {
    const userVars = dataStore['Variables_User'] || [];
    const mainVars = dataStore['Variables'] || [];
    const vkVars = dataStore['VK_Variables'] || [];

    const userNames = [];
    for (const v of userVars) {
        const name = (v['Пользовательская'] || '').trim().toLowerCase();
        if (name) {
            if (userNames.includes(name)) {
                alert('⚠️ Пользовательская переменная "' + v['Пользовательская'] + '" уже существует! Удалите дубликат.');
                btn.disabled = false;
                return;
            }
            userNames.push(name);
        }
    }

    // ✅ ПРОВЕРКА дубликатов ГЛОБАЛЬНЫХ переменных
    const gpNames = [];
    for (const v of mainVars) {
        const name = (v['Глобальная'] || '').trim().toLowerCase();
        if (name) {
            if (gpNames.includes(name)) {
                alert('⚠️ Глобальная переменная "' + v['Глобальная'] + '" уже существует! Удалите дубликат.');
                btn.disabled = false;
                return;
            }
            gpNames.push(name);
        }
    }

    // ✅ ПРОВЕРКА дубликатов VK переменных
    const vkNames = [];
    for (const v of vkVars) {
        const name = (v['Переменная ВК'] || '').trim().toLowerCase();
        if (name) {
            if (vkNames.includes(name)) {
                alert('⚠️ Переменная ВК "' + v['Переменная ВК'] + '" уже существует! Удалите дубликат.');
                btn.disabled = false;
                return;
            }
            vkNames.push(name);
        }
    }

    saveData = userVars.map(v => ({
        'Пользовательская': v['Пользовательская'] || '',
        'Глобальная': '',
        'Значение ГП': '',
        'Переменные ВК': '',
        'Значение/Описание ПВК': ''
    }));
    mainVars.forEach(v => {
        saveData.push({
        'Пользовательская': '',
        'Глобальная': v['Глобальная'] || '',
        'Значение ГП': v['Значение ГП'] || '',
        'Переменные ВК': '',
        'Значение/Описание ПВК': ''
    });
    });
    vkVars.forEach(vk => {
        saveData.push({
            'Пользовательская': '',
            'Глобальная': '',
            'Значение ГП': '',
            'Переменные ВК': vk['Переменная ВК'] || '',
            'Значение/Описание ПВК': vk['Описание'] || ''
        });
    });
}

const orig = btn.textContent;
btn.textContent = '\uD83D\uDCBE \u0421\u043E\u0445\u0440\u0430\u043D\u0435\u043D\u0438\u0435...';
btn.disabled = true;
try {
const sheet = sheetMap[tab];
const baseUrl = window.location.href.split('?')[0];

// ✅ Получаем vk_group_id из настроек для сохранения в правильный файл
const settingsRes = await fetch(baseUrl + '?getBotSettings');
const settingsData = await settingsRes.json();
const communityConfig = settingsData.communities?.[window.currentCommunityId] || {};
const vkGroupId = communityConfig.vk_group_id || window.currentCommunityId || '';

debug('💾 Saving ' + sheet + ' with vk_group_id: ' + vkGroupId);

const targetCommunityId = tab === 'Shared_Variables' ? '' : vkGroupId;
const url = baseUrl + '?save=' + encodeURIComponent(sheet) +
           (targetCommunityId ? '&communityId=' + encodeURIComponent(targetCommunityId) : '');
const res = await fetch(url, {
method: 'POST',
headers: {'Content-Type':'application/json'},
body: JSON.stringify(saveData)
});
const text = await res.text();
let result;
try { result = JSON.parse(text); } catch (e) { throw new Error('Invalid JSON response'); }
if (!result.success) throw new Error(result.error || '\u041E\u0448\u0438\u0431\u043A\u0430 \u0441\u043E\u0445\u0440\u0430\u043D\u0435\u043D\u0438\u044F');
debug('✅ Save successful');
} catch (e) {
debug('Save error: ' + e.message);
}
btn.textContent = orig;
btn.disabled = false;
};

// Сохраняет данные напрямую (без кнопки) — используется в deleteAllRows
window.saveDataDirectly = async function(tab) {
    try {
        const sheet = sheetMap[tab];
        if (!sheet) {
            console.error('[Admin] ❌ saveDataDirectly: unknown tab=' + tab);
            return false;
        }
        const baseUrl = window.location.href.split('?')[0];

        // Получаем vk_group_id
        const settingsRes = await fetch(baseUrl + '?getBotSettings');
        const settingsData = await settingsRes.json();
        const communityConfig = settingsData.communities?.[window.currentCommunityId] || {};
        const vkGroupId = communityConfig.vk_group_id || window.currentCommunityId || '';

        let dataToSave = dataStore[tab] || [];
        if (tab === 'Variables' || tab === 'Variables_User' || tab === 'VK_Variables') {
            const userVars = dataStore['Variables_User'] || [];
            const mainVars = dataStore['Variables'] || [];
            const vkVars = dataStore['VK_Variables'] || [];
            dataToSave = userVars.map(function(v) {
                return {
                    'Пользовательская': v['Пользовательская'] || '',
                    'Глобальная': '',
                    'Значение ГП': '',
                    'Переменные ВК': '',
                    'Значение/Описание ПВК': ''
                };
            });
            mainVars.forEach(function(v) {
                dataToSave.push({
                    'Пользовательская': '',
                    'Глобальная': v['Глобальная'] || '',
                    'Значение ГП': v['Значение ГП'] || '',
                    'Переменные ВК': '',
                    'Значение/Описание ПВК': ''
                });
            });
            vkVars.forEach(function(v) {
                dataToSave.push({
                    'Пользовательская': '',
                    'Глобальная': '',
                    'Значение ГП': '',
                    'Переменные ВК': v['Переменная ВК'] || '',
                    'Значение/Описание ПВК': v['Описание'] || ''
                });
            });
        }
        console.log('[Admin] 💾 saveDataDirectly: saving ' + sheet + ' with ' + dataToSave.length + ' rows');

        const targetCommunityId = tab === 'Shared_Variables' ? '' : vkGroupId;
        const url = baseUrl + '?save=' + encodeURIComponent(sheet) +
                   (targetCommunityId ? '&communityId=' + encodeURIComponent(targetCommunityId) : '');
        const res = await fetch(url, {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify(dataToSave)
        });
        if (!res.ok) {
            console.error('[Admin] ❌ saveDataDirectly failed: ' + res.status + ' ' + res.statusText);
            return false;
        }
        const result = await res.json();
        if (!result.success) {
            console.error('[Admin] ❌ saveDataDirectly: server returned success=false');
            return false;
        }
        console.log('[Admin] ✅ saveDataDirectly: saved ' + sheet);
        return true;
    } catch (e) {
        console.error('[Admin] ❌ saveDataDirectly error: ' + e.message);
        return false;
    }
};

function showStatus(msg, type) {
const el = document.getElementById('status');
el.innerText = msg;
el.className = 'status ' + type;
setTimeout(() => el.style.display = 'none', 5000);
}

// 🖼️ Функции для показа изображений инструкций
window.showInstructionImage = function(imageName) {
    // Картинки хранятся в публичном бакете 1212121212 в папке file_admin_panel
    const baseUrl = 'https://storage.yandexcloud.net/1212121212/file_admin_panel/';
    const modal = document.getElementById('imageModal');
    const img = document.getElementById('modalImage');
    // НЕ кодируем URL — Yandex Storage принимает кириллицу напрямую
    img.src = baseUrl + imageName;
    img.onerror = function() {
        img.src = '';
        img.alt = 'Не удалось загрузить изображение. Убедитесь что файл "' + imageName + '" загружен в публичный бакет 1212121212/file_admin_panel/';
    };
    modal.classList.add('show');
    return false;
};

window.closeImageModal = function() {
    document.getElementById('imageModal').classList.remove('show');
};

// Закрытие по Escape
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') closeImageModal();
});



window.openTab = function(evt, name) {
    const loadingEl = document.getElementById('loading-' + name);
    document.querySelectorAll('.tabcontent').forEach(el => { el.style.display = 'none'; el.classList.remove('active'); });
    document.querySelectorAll('.tablinks').forEach(el => el.classList.remove('active'));
    document.getElementById(name).style.display = 'block'; document.getElementById(name).classList.add('active');
    // Если evt есть, устанавливаем активный класс на кнопке
    if (evt && evt.currentTarget) {
        evt.currentTarget.classList.add('active');
    } else {
        // Иначе ищем кнопку по имени вкладки
        const btn = Array.from(document.querySelectorAll('.tablinks')).find(b => b.getAttribute('onclick')?.includes(",'" + name + "')"));
        if (btn) btn.classList.add('active');
    }
    if (loadingEl) loadingEl.style.display = 'block';
    if (name === 'Settings') {
        loadSettings();
    } else if (name === 'Admin') {
        if (!isMainAdminSession()) {
            if (loadingEl) loadingEl.style.display = 'none';
            showStatus('Доступ к вкладке АДМИН есть только у главного администратора', 'error');
            return;
        }
        loadAdminProfiles();
    } else if (name === 'Profile') {
        loadProfileDashboard();
    } else if (name === 'Groups') {
        loadGroupsTab();
    } else if (name === 'Variables') {
        if (!dataStore['Variables'] || dataStore['Variables'].length === 0) {
            loadData('Variables');
        } else {
            setTimeout(function() {
                renderTable('Variables_User', dataStore['Variables_User'] || []);
                renderTable('Variables', dataStore['Variables']);
                renderTable('Shared_Variables', dataStore['Shared_Variables'] || []);
                renderTable('VK_Variables', dataStore['VK_Variables'] || []);
                if (loadingEl) loadingEl.style.display = 'none';
            }, 30);
        }
    } else {
        // ✅ НЕ вызываем updateCommunityLabels здесь - это сделает loadData
        if (!dataStore[name] || dataStore[name].length === 0) {
            loadData(name);
        } else {
            setTimeout(function() {
                renderTable(name, dataStore[name]);
                if (name === 'Triggers') {
                    renderStructuredTriggersTab();
                }
                if (name === 'Messages' || name === 'Comments') {
                    renderBotButtons(name);
                }
                if (loadingEl) loadingEl.style.display = 'none';
            }, 30);
        }
    }
};



window.saveConfirmationCode = async function() {
const code = document.getElementById('confirmationCode').value.trim();
const status = document.getElementById('confirmation-status');
if (!code) {
status.innerHTML = '<div class="status error">? Введите код подтверждения!</div>';
return;
}
try {
const baseUrl = window.location.href.split('?')[0];
const url = baseUrl + '?saveConfirmation=' + encodeURIComponent(code);
const res = await fetch(url, {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({ confirmation_code: code })
});
const text = await res.text();
let result;
try {
result = JSON.parse(text);
} catch (e) {
throw new Error('Invalid JSON: ' + text.substring(0, 200));
}
if (result.success) {
status.innerHTML = '<div class="status success">? Код подтверждения сохранён!</div>';
debug('Confirmation code saved: ' + code);
} else {
status.innerHTML = '<div class="status error">❌ Ошибка: ' + result.error + '</div>';
}
} catch (e) {
status.innerHTML = '<div class="status error">❌ Ошибка: ' + e.message + '</div>';
}
};








window.saveSecretKey = async function() {
const key = document.getElementById('secretKey').value.trim();
const status = document.getElementById('confirmation-status');
if (!key) {
status.innerHTML = '<div class="status error">? Введите Secret Key!</div>';
return;
}
try {
const baseUrl = window.location.href.split('?')[0];
const url = baseUrl + '?saveSecretKey=' + encodeURIComponent(key);
const res = await fetch(url, {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({ secret_key: key })
});
const text = await res.text();
let result;
try {
result = JSON.parse(text);
} catch (e) {
throw new Error('Invalid JSON: ' + text.substring(0, 200));
}
if (result.success) {
status.innerHTML = '<div class="status success">? Secret Key сохранён!</div>';
debug('Secret Key saved');
} else {
status.innerHTML = '<div class="status error">❌ Ошибка: ' + result.error + '</div>';
}
} catch (e) {
status.innerHTML = '<div class="status error">❌ Ошибка: ' + e.message + '</div>';
}
};




window.loadSettings = async function() {
  const settingsDebug = document.getElementById('settings-debug');
  const tokenStatusEl = document.getElementById('token-status');
  const loadingEl = document.getElementById('loading-Settings');
  try {
    if (loadingEl) loadingEl.style.display = 'block';
    const baseUrl = window.location.href.split('?')[0];
    const res = await fetch(baseUrl + '?getBotSettings');
    const data = await res.json();
    // Заполняем поля
    document.getElementById('confirmationCode').value = data.confirmation_token || '';
    document.getElementById('secretKey').value = data.secret_key || '';
    
    // &#x2728; НОВОЕ: Загрузка массива токенов
    const vkTokensTextarea = document.getElementById('vkTokens');
    if (data.vk_tokens && Array.isArray(data.vk_tokens)) {
        // Если есть массив - выводим каждый токен с новой строки
        vkTokensTextarea.value = data.vk_tokens.join('\\n');
    } else if (data.vk_token) {
        // Если старого формата - выводим один токен
        vkTokensTextarea.value = data.vk_token;
    } else {
        vkTokensTextarea.value = '';
    }
    
    document.getElementById('vkGroupId').value = data.vk_group_id || '';
    document.getElementById('userToken').value = data.user_token || '';
    // Выводим отладочную информацию
    settingsDebug.innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
    if (tokenStatusEl) {
      tokenStatusEl.innerHTML = makeInlineNotice('success', '✅ Настройки загружены из Object Storage') +
        '<div><strong>VK Token:</strong> ' + (data.vk_token ? '***' + data.vk_token.slice(-4) : 'не настроен') + '</div>' +
        '<div><strong>Confirmation:</strong> ' + (data.confirmation_token ? '***' + data.confirmation_token.slice(-4) : 'не настроен') + '</div>' +
        '<div><strong>Secret Key:</strong> ' + (data.secret_key ? '***' + data.secret_key.slice(-4) : 'не настроен') + '</div>' +
        '<div><strong>Group ID:</strong> ' + (data.vk_group_id || 'не настроен') + '</div>';
    }
  } catch (e) {
    settingsDebug.innerHTML = '❌ Ошибка загрузки: ' + e.message;
    if (tokenStatusEl) tokenStatusEl.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + e.message);
  } finally {
    if (loadingEl) loadingEl.style.display = 'none';
  }
};






window.refreshConfirmationToken = async function() {
const status = document.getElementById('confirmation-status');
const debugEl = document.getElementById('settings-debug');
status.innerHTML = makeInlineNotice('warn', '🔄 Запрос к VK API...');
try {
const baseUrl = window.location.href.split('?')[0];
const url = baseUrl + '?getSettings';
const res = await fetch(url);
const data = await res.json();
if (!data.vk_token_set) {
status.innerHTML = '<div class="status error">? VK_TOKEN не настроен в переменных окружения!</div>';
return;
}
const updateUrl = baseUrl + '?refreshConfirmationToken';
const updateRes = await fetch(updateUrl, { method: 'POST' });
const updateData = await updateRes.json();
if (updateData.success) {
status.innerHTML = '<div class="status success">&#x1F511; Токен обновлён из VK API: ' + (updateData.code || '***') + '</div>';
document.getElementById('confirmationCode').value = updateData.code || '';
debugEl.innerHTML += '<br>[' + new Date().toLocaleTimeString() + '] Token refreshed from VK API<br>';
} else {
status.innerHTML = '<div class="status error">❌ Ошибка: ' + (updateData.error || 'Неизвестная ошибка') + '</div>';
}
} catch (e) {
status.innerHTML = '<div class="status error">❌ Ошибка: ' + e.message + '</div>';
}
};





// ===== OVERLAY ЗАГРУЗКИ =====
window.showLoadOverlay = function() {
    let overlay = document.getElementById('loadOverlay');
    if (!overlay) {
        overlay = document.createElement('div');
        overlay.id = 'loadOverlay';
        overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(33,150,243,0.95);z-index:999999;display:flex;align-items:center;justify-content:center;flex-direction:column;transition:opacity 0.3s;';
        overlay.innerHTML = '<div style="font-size:72px;font-weight:900;color:white;text-shadow:2px 2px 4px rgba(0,0,0,0.3);letter-spacing:4px;" id="loadText">ЗАГРУЗКА</div>' +
            '<div style="margin-top:24px;font-size:30px;color:white;font-weight:800;" id="loadDots">.....</div>' +
            '<div style="margin-top:16px;font-size:28px;color:#dbeafe;font-weight:700;" id="loadStatus">Инициализация...</div>';
        document.body.appendChild(overlay);
    }
    overlay.classList.add('show');
    overlay.style.opacity = '1';
    overlay.style.display = 'flex';

    // Анимация точек
    let dotCount = 0;
    const loadDots = document.getElementById('loadDots');
    if (loadDots && !window._loadDotsInterval) {
        window._loadDotsInterval = setInterval(function() {
            dotCount = (dotCount + 1) % 6;
            loadDots.textContent = '.'.repeat(dotCount + 1);
        }, 200);
    }
};

window.updateLoadStatus = function(msg) {
    const el = document.getElementById('loadStatus');
    if (el) el.textContent = msg;
};

window.hideLoadOverlay = function() {
    const overlay = document.getElementById('loadOverlay');
    if (overlay) {
        overlay.style.opacity = '0';
        setTimeout(function() {
            overlay.style.display = 'none';
            if (window._loadDotsInterval) {
                clearInterval(window._loadDotsInterval);
                window._loadDotsInterval = null;
            }
        }, 300);
    }
};

window.onload = async function() {
    // Показываем overlay загрузки
    showLoadOverlay();

    try {
        if (!await checkAuthOnLoad()) {
            hideLoadOverlay();
            return;
        }
        applyAdminAccessUI();
        injectTabRefreshButtons();
        await loadBotVersion();
    } catch(e) {
        console.error('Auth check error:', e);
        hideLoadOverlay();
    }

    try {
        const state = restoreAppState();
        debug('🚀 onload: восстанавливаем state tabName=' + state.tabName + ', communityId=' + state.communityId);
        updateLoadStatus('Открываем настройки...');

        // ✅ ВСЕГДА начинаем с вкладки НАСТРОЙКА
        debug('📋 Открываем вкладку НАСТРОЙКА...');
        const settingsTabBtn = Array.from(document.querySelectorAll('.tablinks')).find(b =>
            b.getAttribute('onclick')?.includes(",'Settings')")
        );
        if (settingsTabBtn) {
            openTab({ currentTarget: settingsTabBtn }, 'Settings');
        }

        // ✅ Загружаем настройки сообществ и рендерим кнопки
        if (isMainAdminSession()) {
            await loadAdminProfiles();
        }
        debug('🏘️ Загружаем renderCommunityButtons...');
        updateLoadStatus('Загружаем настройки...');
        await renderCommunityButtons();

        // ✅ Теперь загружаем данные для ВСЕХ вкладок последовательно
        const allTabs = ['Messages', 'Comments', 'Users', 'Variables', 'Mailing', 'Delayed', 'Triggers'];
        const tabEmojis = {
            'Messages': '💬 СООБЩЕНИЯ',
            'Comments': '📝 КОММЕНТАРИИ В ПОСТАХ',
            'Users': '👥 ПОЛЬЗОВАТЕЛИ',
            'Variables': '🧮 ПЕРЕМЕННЫЕ',
            'Mailing': '📨 РАССЫЛКА',
            'Delayed': '⏳ ОТЛОЖЕННЫЕ',
            'Triggers': '🎯 ТРИГГЕРЫ',
            'Settings': '⚙️ НАСТРОЙКА',
            'Admin': '🛡️ АДМИН'
        };
        for (let i = 0; i < allTabs.length; i++) {
            const tabName = allTabs[i];
            const displayTabName = tabEmojis[tabName] || tabName;
            updateLoadStatus('Загружаем ' + displayTabName + ' (' + (i + 1) + '/' + allTabs.length + ')...');
            debug('📥 Загружаем ' + tabName + ' для сообщества ' + window.currentCommunityId);
            await loadData(tabName);
        }

        // ✅ Если была сохранена другая вкладка - переключаемся на неё
        if (state.tabName && state.tabName !== 'Settings') {
            debug('🔄 Переключаемся на последнюю вкладку: ' + state.tabName);
            const displayTabName = tabEmojis[state.tabName] || state.tabName;
            updateLoadStatus('Открываем ' + displayTabName + '...');
            const tabBtn = Array.from(document.querySelectorAll('.tablinks')).find(b =>
                b.getAttribute('onclick')?.includes(",'" + state.tabName + "')")
            );
            if (tabBtn) {
                openTab({ currentTarget: tabBtn }, state.tabName);
            }
        }

        debug('✅ Загрузка завершена!');
        updateLoadStatus('Готово!');
        // Небольшая задержка чтобы пользователь увидел "Готово!"
        setTimeout(function() {
            hideLoadOverlay();
        }, 500);
    } catch (e) {
        console.error('Page load error:', e);
        updateLoadStatus('Ошибка загрузки: ' + e.message);
        setTimeout(function() {
            hideLoadOverlay();
        }, 2000);
    }
};





// &#x1F512; Показать/скрыть служебные поля (Confirmation Code и Secret Key)
window.toggleSecretFields = function() {
    const el = document.getElementById('secretFields');
    if (el) {
        el.style.display = el.style.display === 'none' ? 'block' : 'none';
    }
};

window.autoSetupCallback = async function() {
    // ✅ Показываем "АВТОНАСТРОЙКА....." - будет висеть до завершения запроса
    showAutoSetupOverlay();

    try {
        const baseUrl = window.location.href.split('?')[0];

        // ✅ Получаем VK Group ID из поля ввода - это надёжный идентификатор сообщества
        const vkGroupId = document.getElementById('vkGroupId').value ? parseInt(document.getElementById('vkGroupId').value, 10) : null;
        
        // ✅ Используем vk_group_id как community_id для консистентности
        const communityId = vkGroupId ? vkGroupId.toString() : window.currentCommunityId;
        
        debug('🛠️ Автонастройка: community_id=' + communityId + ', vk_group_id=' + vkGroupId + ', internal=' + window.currentCommunityId);

        // ✅ Готовим данные для отправки
        const setupData = {
            community_id: communityId,
            vk_token: document.getElementById('vkTokens').value.trim().split('\\n')[0] || '',
            vk_group_id: vkGroupId,
            secret_key: document.getElementById('secretKey').value.trim() || ''
        };

        const res = await fetch(baseUrl + '?setupCallback', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(setupData)
        });
        const data = await res.json();

        if (data.success) {
            // ✅ Обновляем поля в интерфейсе
            if (data.confirmation_code) {
                const el = document.getElementById('confirmationCode');
                if (el) el.value = data.confirmation_code;
            }
            if (data.secret_key) {
                const el = document.getElementById('secretKey');
                if (el) el.value = data.secret_key;
            }
            // ✅ Обновляем список кнопок сообществ
            if (typeof renderCommunityButtons === 'function') {
                await renderCommunityButtons();
            }

            // ✅ Закрываем оверлей "АВТОНАСТРОЙКА" если ещё висит
            const autoOverlay = document.getElementById('autoSetupOverlay');
            if (autoOverlay && autoOverlay.parentNode) autoOverlay.parentNode.removeChild(autoOverlay);

            // ✅ Показываем полноэкранное уведомление об успехе на 5 секунд
            const successMsg = '<div style="font-size:36px;font-weight:bold;color:var(--status-success-text);margin-bottom:20px;">✅ АВТОНАСТРОЙКА УСПЕШНА!</div>' +
                '<div style="font-size:18px;color:var(--text-primary);line-height:1.8;">' +
                '<strong>VK Group ID:</strong> ' + vkGroupId + '<br>' +
                '<strong>Сервер:</strong> ' + (data.server_name || 'PAPA_BOT') + ' (ID: ' + data.server_id + ')<br>' +
                '<strong>Confirmation code:</strong> ' + data.confirmation_code + '<br>' +
                '<strong>Secret key:</strong> ' + data.secret_key +
                '</div>';

            showSuccessOverlay(successMsg);
        } else {
            // ✅ Закрываем оверлей "АВТОНАСТРОЙКА" если ещё висит
            const autoOverlay = document.getElementById('autoSetupOverlay');
            if (autoOverlay && autoOverlay.parentNode) autoOverlay.parentNode.removeChild(autoOverlay);

            showSuccessOverlay('<div style="font-size:36px;font-weight:bold;color:var(--status-error-text);margin-bottom:20px;">❌ ОШИБКА АВТОНАСТРОЙКИ</div><div style="font-size:18px;color:var(--text-primary);">' + (data.error || 'неизвестная') + '</div>');
        }
    } catch (e) {
        // ✅ Закрываем оверлей "АВТОНАСТРОЙКА" если ещё висит
        const autoOverlay = document.getElementById('autoSetupOverlay');
        if (autoOverlay && autoOverlay.parentNode) autoOverlay.parentNode.removeChild(autoOverlay);

        showSuccessOverlay('<div style="font-size:36px;font-weight:bold;color:var(--status-error-text);margin-bottom:20px;">❌ ОШИБКА АВТОНАСТРОЙКИ</div><div style="font-size:18px;color:var(--text-primary);">' + e.message + '</div>');
    }
};





// ===== ФУНКЦИИ УПРАВЛЕНИЯ СООБЩЕСТВАМИ =====

window.currentCommunityId = null;
window.adminProfiles = [];
window.adminDashboard = { promoCodes: [], recoveryRequests: [], loginLogs: [] };
window.triggersHubFilter = 'all';

function getTriggerHubSourceLabel(sourceTab) {
    return sourceTab === 'Comments' ? 'Комментарии' : 'Сообщения';
}

function getTriggerHubModeLabel(mode) {
    const normalized = normalizeTriggerMode(mode);
    if (normalized === 'BUTTON') return 'Кнопка';
    if (normalized === 'FILE') return 'Файл';
    return 'Текст';
}

function getTriggerHubCards() {
    const cards = [];
    ['Messages', 'Comments'].forEach(function(sourceTab) {
        const rows = dataStore[sourceTab] || [];
        rows.forEach(function(row, idx) {
            const triggerMode = normalizeTriggerMode(row._triggerMode);
            const triggerValue = String(row['Триггер'] || '').trim();
            const hasTrigger = !!triggerValue || triggerMode === 'FILE';
            if (!hasTrigger) return;

            const conditions = [];
            const actions = [];
            if (row['Ответить если в Группе']) conditions.push('Группа: ' + row['Ответить если в Группе']);
            if (row['Ответил на Шаг']) conditions.push('После шага: ' + row['Ответил на Шаг']);
            if (row['Пользовательская']) conditions.push('ПП: ' + row['Пользовательская']);
            if (row['Глобальная']) conditions.push('ГП: ' + row['Глобальная']);
            if (row['Пост']) conditions.push('Пост: ' + row['Пост']);
            if (row['Отметили']) conditions.push('Отметка: ' + row['Отметили']);

            if (row['Задержка отправки на Шаг']) actions.push('Задержка: ' + row['Задержка отправки на Шаг']);
            if (row['ДОБАВИТЬ ГРУППУ']) actions.push('Добавить группу: ' + row['ДОБАВИТЬ ГРУППУ']);
            if (row['УДАЛИТЬ ГРУППУ']) actions.push('Удалить группу: ' + row['УДАЛИТЬ ГРУППУ']);
            if (row['Отправить на Шаг']) actions.push('На шаг: ' + row['Отправить на Шаг']);
            if (row['Действия с ПП']) actions.push('ПП: ' + row['Действия с ПП']);
            if (row['Действия с ГП']) actions.push('ГП: ' + row['Действия с ГП']);

            cards.push({
                sourceTab: sourceTab,
                rowIdx: idx,
                triggerMode: triggerMode,
                sourceLabel: getTriggerHubSourceLabel(sourceTab),
                bot: String(row['Бот'] || '').trim(),
                step: String(row['Шаг'] || '').trim(),
                trigger: triggerValue || (triggerMode === 'FILE' ? 'Любое вложение' : ''),
                answer: String(row['Ответ'] || row['Заготовленный ответ'] || '').trim(),
                conditions: conditions,
                actions: actions,
                raw: row
            });
        });
    });
    return cards;
}

window.setTriggersHubFilter = function(filter) {
    window.triggersHubFilter = filter || 'all';
    document.querySelectorAll('.trigger-hub-chip').forEach(function(chip) {
        chip.classList.toggle('active', chip.getAttribute('data-filter') === window.triggersHubFilter);
    });
    renderTriggersHub();
};

window.openTabByName = function(name) {
    const btn = Array.from(document.querySelectorAll('.tablinks')).find(function(b) {
        return b.getAttribute('onclick')?.includes("'" + name + "'");
    });
    openTab(btn ? { currentTarget: btn } : null, name);
};

window.openTriggerSource = function(sourceTab, rowIdx, botName) {
    if (botName && (sourceTab === 'Messages' || sourceTab === 'Comments')) {
        setActiveBot(sourceTab, botName);
    }
    openTabByName(sourceTab);
    setTimeout(function() {
        renderTable(sourceTab, dataStore[sourceTab] || []);
        const table = document.getElementById('table-' + sourceTab);
        const rowEl = table ? table.querySelector('[data-idx="' + rowIdx + '"]') : null;
        if (rowEl) {
            const tr = rowEl.closest('tr');
            if (tr) {
                tr.scrollIntoView({ behavior: 'smooth', block: 'center' });
                tr.style.transition = 'box-shadow 0.2s ease, background 0.2s ease';
                tr.style.boxShadow = '0 0 0 3px rgba(34, 197, 94, 0.28)';
                tr.style.background = 'rgba(34, 197, 94, 0.08)';
                setTimeout(function() {
                    tr.style.boxShadow = '';
                    tr.style.background = '';
                }, 1800);
            }
        }
    }, 120);
};

window.duplicateTriggerSource = function(sourceTab, rowIdx) {
    if (!dataStore[sourceTab] || !dataStore[sourceTab][rowIdx]) return;
    const duplicatedRow = JSON.parse(JSON.stringify(dataStore[sourceTab][rowIdx]));
    dataStore[sourceTab].splice(rowIdx + 1, 0, duplicatedRow);
    renderTriggersHub();
    if (sourceTab === 'Messages' || sourceTab === 'Comments') {
        renderTable(sourceTab, dataStore[sourceTab]);
    }
    debug('Duplicated trigger row ' + rowIdx + ' from ' + sourceTab + ' in Triggers hub');
};

window.renderTriggersHub = function() {
    const listEl = document.getElementById('triggersHubList');
    const statsEl = document.getElementById('triggersHubStats');
    const searchEl = document.getElementById('triggerHubSearch');
    if (!listEl) return;

    const query = String(searchEl?.value || '').trim().toLowerCase();
    const currentFilter = window.triggersHubFilter || 'all';
    const allCards = getTriggerHubCards();
    const filtered = allCards.filter(function(card) {
        if (currentFilter === 'messages' && card.sourceTab !== 'Messages') return false;
        if (currentFilter === 'comments' && card.sourceTab !== 'Comments') return false;
        if (currentFilter === 'button' && card.triggerMode !== 'BUTTON') return false;
        if (currentFilter === 'file' && card.triggerMode !== 'FILE') return false;
        if (!query) return true;
        const haystack = [card.sourceLabel, card.bot, card.step, card.trigger, card.answer, card.conditions.join(' '), card.actions.join(' ')].join(' ').toLowerCase();
        return haystack.includes(query);
    });

    if (statsEl) {
        statsEl.textContent = 'Показано триггеров: ' + filtered.length + ' из ' + allCards.length;
    }

    if (!filtered.length) {
        listEl.innerHTML = '<div class="trigger-empty">Триггеры не найдены. Попробуй снять фильтр или создать новый шаг в сообщениях/комментариях.</div>';
        return;
    }

    listEl.innerHTML = filtered.map(function(card) {
        const sourceCls = card.sourceTab === 'Comments' ? 'source-comments' : 'source-messages';
        const modeCls = card.triggerMode === 'BUTTON' ? 'mode-button' : (card.triggerMode === 'FILE' ? 'mode-file' : 'mode-text');
        return '<div class="trigger-card">' +
            '<div class="trigger-card-header">' +
                '<div>' +
                    '<div class="trigger-card-title">' + escapeHtml(card.trigger || 'Без триггера') + '</div>' +
                    '<div class="trigger-card-meta">Бот: ' + escapeHtml(card.bot || 'не задан') + ' • Шаг: ' + escapeHtml(card.step || 'не задан') + '</div>' +
                '</div>' +
            '</div>' +
            '<div class="trigger-card-badges">' +
                '<span class="trigger-badge ' + sourceCls + '">' + escapeHtml(card.sourceLabel) + '</span>' +
                '<span class="trigger-badge ' + modeCls + '">' + escapeHtml(getTriggerHubModeLabel(card.triggerMode)) + '</span>' +
            '</div>' +
            '<div class="trigger-card-section"><div class="trigger-card-label">Ответ</div><div class="trigger-card-value">' + escapeHtml(card.answer || 'Ответ не задан') + '</div></div>' +
            '<div class="trigger-card-section"><div class="trigger-card-label">Условия</div><div class="trigger-card-value">' + escapeHtml(card.conditions.length ? card.conditions.join('\\n') : 'Дополнительных условий нет') + '</div></div>' +
            '<div class="trigger-card-section"><div class="trigger-card-label">Действия</div><div class="trigger-card-value">' + escapeHtml(card.actions.length ? card.actions.join('\\n') : 'Дополнительных действий нет') + '</div></div>' +
            '<div class="trigger-card-actions">' +
                '<button class="btn btn-info" type="button" onclick="openTriggerSource(&quot;' + escapeHtml(card.sourceTab) + '&quot;,' + card.rowIdx + ',&quot;' + escapeHtml(card.bot || '') + '&quot;)">Открыть источник</button>' +
                '<button class="btn btn-add" type="button" onclick="duplicateTriggerSource(&quot;' + escapeHtml(card.sourceTab) + '&quot;,' + card.rowIdx + ')">Copy</button>' +
            '</div>' +
        '</div>';
    }).join('');
};

window.toggleAdminProfileFilters = function() {
    var block = document.getElementById('adminProfileFiltersBlock');
    var toggle = document.getElementById('adminProfileFiltersToggle');
    if (!block || !toggle) return;
    var isHidden = block.style.display === 'none';
    block.style.display = isHidden ? '' : 'none';
    toggle.textContent = isHidden ? '▼ 🔍 Фильтры Профилей' : '▶ 🔍 Фильтры Профилей';
};

window.toggleCreateProfileForm = function() {
    var form = document.getElementById('adminProfileForm');
    var btn = document.getElementById('toggleCreateProfileBtn');
    if (!form) return;
    var isHidden = form.style.display === 'none';
    if (isHidden) {
        resetAdminProfileForm();
        var title = form.querySelector('.profile-form-title');
        if (title) title.textContent = 'Создание профиля';
        var statusEl = document.getElementById('adminProfilesStatus');
        if (statusEl) statusEl.innerHTML = '';
        form.style.display = '';
        if (btn) btn.textContent = '✕ Закрыть';
        form.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    } else {
        closeAdminProfileForm();
    }
};

window.ensureCreateProfileFormVisible = function() {
    var form = document.getElementById('adminProfileForm');
    var btn = document.getElementById('toggleCreateProfileBtn');
    if (form && form.style.display === 'none') {
        form.style.display = '';
        if (btn) btn.textContent = '✕ Закрыть';
    }
};

window.editAdminProfile = function(profileId) {
    if (!isMainAdminSession()) return;
    var profile = (window.adminProfiles || []).find(function(item) { return item.id === profileId; });
    if (!profile) return;
    document.getElementById('profileFormId').value = profile.id || '';
    document.getElementById('profileFormName').value = profile.name || '';
    document.getElementById('profileFormUsername').value = profile.username || '';
    document.getElementById('profileFormPassword').value = '';
    document.getElementById('profileFormEmail').value = profile.recoveryEmail || '';
    document.getElementById('profileFormDuration').value = profile.remainingMinutes || '';
    document.getElementById('profileFormRequestsLimit').value = profile.requestsLimit || '';
    var form = document.getElementById('adminProfileForm');
    if (form) {
        form.style.display = '';
        var title = form.querySelector('.profile-form-title');
        if (title) title.textContent = 'Редактирование профиля';
    }
    var btn = document.getElementById('toggleCreateProfileBtn');
    if (btn) btn.textContent = '✕ Закрыть';
    var statusEl = document.getElementById('adminProfilesStatus');
    if (statusEl) {
        var lifetimeLabel = profile.expiresAt
            ? ('Сейчас доступ действует до <strong>' + escapeHtml(new Date(profile.expiresAt).toLocaleString('ru-RU')) + '</strong>.')
            : 'Сейчас профиль бессрочный.';
        statusEl.innerHTML = makeInlineNotice('info', 'Редактирование профиля <strong>' + escapeHtml(profile.name || ('Профиль ' + profile.id)) + '</strong>. Для смены пароля введите новый пароль. ' + lifetimeLabel);
    }
    form?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
};

window.closeAdminProfileForm = function() {
    var form = document.getElementById('adminProfileForm');
    var btn = document.getElementById('toggleCreateProfileBtn');
    if (form) form.style.display = 'none';
    if (btn) btn.textContent = '+ Создать Профиль';
    resetAdminProfileForm();
    var statusEl = document.getElementById('adminProfilesStatus');
    if (statusEl) statusEl.innerHTML = '';
};

window.resetAdminProfileForm = function() {
    var fields = ['profileFormId', 'profileFormName', 'profileFormUsername', 'profileFormPassword', 'profileFormEmail', 'profileFormDuration', 'profileFormRequestsLimit'];
    fields.forEach(function(id) {
        var el = document.getElementById(id);
        if (el) el.value = '';
    });
};

window.resetAdminProfileFilters = function() {
    const defaults = {
        adminProfileFilterSearch: '',
        adminProfileFilterId: '',
        adminProfileFilterName: '',
        adminProfileFilterUsername: '',
        adminProfileFilterEmail: '',
        adminProfileFilterPromo: '',
        adminProfileFilterRole: '',
        adminProfileFilterActive: '',
        adminProfileFilterExpiry: '',
        adminProfileFilterDurationMin: '',
        adminProfileFilterDurationMax: '',
        adminProfileFilterLimitMin: '',
        adminProfileFilterLimitMax: ''
    };
    Object.keys(defaults).forEach(function(id) {
        const el = document.getElementById(id);
        if (el) el.value = defaults[id];
    });
    renderAdminProfiles();
};

window.togglePromoForm = function() {
    var form = document.getElementById('promoFormBlock');
    var btn = document.getElementById('togglePromoFormBtn');
    if (!form) return;
    var isHidden = form.style.display === 'none';
    if (isHidden) {
        resetPromoForm();
        form.style.display = '';
        if (btn) btn.textContent = '✕ Закрыть';
        form.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    } else {
        closePromoForm();
    }
};

window.closePromoForm = function() {
    var form = document.getElementById('promoFormBlock');
    var btn = document.getElementById('togglePromoFormBtn');
    if (form) form.style.display = 'none';
    if (btn) btn.textContent = '+ Добавить Промокод';
    resetPromoForm();
};

window.togglePromoFilters = function() {
    var block = document.getElementById('promoFiltersBlock');
    var toggle = document.getElementById('promoFiltersToggle');
    if (!block || !toggle) return;
    var isHidden = block.style.display === 'none';
    block.style.display = isHidden ? '' : 'none';
    toggle.textContent = isHidden ? '▼ 🔍 Фильтры Промокодов' : '▶ 🔍 Фильтры Промокодов';
};

window.resetPromoFilters = function() {
    var defaults = {
        promoFilterSearch: '',
        promoFilterStatus: '',
        promoFilterUsesMin: '',
        promoFilterUsesMax: ''
    };
    Object.keys(defaults).forEach(function(id) {
        var el = document.getElementById(id);
        if (el) el.value = defaults[id];
    });
    renderPromoCodes();
};

window.generatePromoCode = function() {
    var chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/*-$%@!_';
    var parts = [];
    for (var p = 0; p < 5; p++) {
        var part = '';
        for (var i = 0; i < 5; i++) {
            part += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        parts.push(part);
    }
    var el = document.getElementById('promoFormCode');
    if (el) {
        el.value = parts.join('-');
        el.focus();
    }
};

window.resetPromoForm = function() {
    const defaults = {
        promoFormId: '',
        promoFormCode: '',
        promoFormLabel: '',
        promoFormDuration: '',
        promoFormRequestsLimit: '',
        promoFormMaxUses: '1'
    };
    Object.keys(defaults).forEach(function(id) {
        const el = document.getElementById(id);
        if (el) el.value = defaults[id];
    });
};

window.switchAdminWorkingProfile = function(profileId) {
    const profile = (window.adminProfiles || []).find(function(item) { return item.id === profileId; });
    if (!profile) return;
    localStorage.setItem('adminProfileId', profile.id);
    localStorage.setItem('adminProfileName', profile.name || ('Профиль ' + profile.id));
    location.reload();
};

window.renderAdminProfiles = function() {
    const listEl = document.getElementById('adminProfilesList');
    const chips = ['currentProfileChip', 'currentProfileChipAdmin'];
    if (!listEl) return;

    const allProfiles = window.adminProfiles || [];
    const currentProfileId = getCurrentProfileId();
    const currentProfile = allProfiles.find(function(item) { return item.id === currentProfileId; });
    chips.forEach(function(id) {
        const chip = document.getElementById(id);
        if (chip) chip.textContent = currentProfile ? ('Рабочий профиль: ' + currentProfile.name) : ('Рабочий профиль: ' + currentProfileId);
    });

    const search = String(document.getElementById('adminProfileFilterSearch')?.value || '').trim().toLowerCase();
    const idFilter = String(document.getElementById('adminProfileFilterId')?.value || '').trim().toLowerCase();
    const nameFilter = String(document.getElementById('adminProfileFilterName')?.value || '').trim().toLowerCase();
    const usernameFilter = String(document.getElementById('adminProfileFilterUsername')?.value || '').trim().toLowerCase();
    const emailFilter = String(document.getElementById('adminProfileFilterEmail')?.value || '').trim().toLowerCase();
    const promoFilter = String(document.getElementById('adminProfileFilterPromo')?.value || '').trim().toLowerCase();
    const roleFilter = String(document.getElementById('adminProfileFilterRole')?.value || '').trim();
    const activeFilter = String(document.getElementById('adminProfileFilterActive')?.value || '').trim();
    const expiryFilter = String(document.getElementById('adminProfileFilterExpiry')?.value || '').trim();
    const durationMinRaw = String(document.getElementById('adminProfileFilterDurationMin')?.value || '').trim();
    const durationMaxRaw = String(document.getElementById('adminProfileFilterDurationMax')?.value || '').trim();
    const limitMinRaw = String(document.getElementById('adminProfileFilterLimitMin')?.value || '').trim();
    const limitMaxRaw = String(document.getElementById('adminProfileFilterLimitMax')?.value || '').trim();
    const durationMin = durationMinRaw ? Number(durationMinRaw) : null;
    const durationMax = durationMaxRaw ? Number(durationMaxRaw) : null;
    const limitMin = limitMinRaw ? Number(limitMinRaw) : null;
    const limitMax = limitMaxRaw ? Number(limitMaxRaw) : null;
    const profiles = allProfiles.filter(function(profile) {
        const haystack = [
            profile.id,
            profile.name,
            profile.username,
            profile.recoveryEmail,
            profile.role,
            profile.promoCodeUsed,
            profile.lastLoginAt,
            profile.createdAt,
            profile.expiresAt,
            profile.requestsLimit
        ].join(' ').toLowerCase();
        if (search && haystack.indexOf(search) === -1) return false;
        if (idFilter && String(profile.id || '').toLowerCase().indexOf(idFilter) === -1) return false;
        if (nameFilter && String(profile.name || '').toLowerCase().indexOf(nameFilter) === -1) return false;
        if (usernameFilter && String(profile.username || '').toLowerCase().indexOf(usernameFilter) === -1) return false;
        if (emailFilter && String(profile.recoveryEmail || '').toLowerCase().indexOf(emailFilter) === -1) return false;
        if (promoFilter && String(profile.promoCodeUsed || '').toLowerCase().indexOf(promoFilter) === -1) return false;
        if (roleFilter && profile.role !== roleFilter) return false;
        if (activeFilter === 'active' && profile.active === false) return false;
        if (activeFilter === 'inactive' && profile.active !== false) return false;
        if (expiryFilter === 'expired' && !profile.isExpired) return false;
        if (expiryFilter === 'active' && profile.isExpired) return false;
        if (expiryFilter === 'infinite' && profile.expiresAt) return false;
        var remainingMinutes = Number.isFinite(Number(profile.remainingMinutes)) ? Number(profile.remainingMinutes) : null;
        if (durationMin !== null && (remainingMinutes === null || remainingMinutes < durationMin)) return false;
        if (durationMax !== null && (remainingMinutes === null || remainingMinutes > durationMax)) return false;
        var requestsLimit = profile.requestsLimit ? Number(profile.requestsLimit) : null;
        if (limitMin !== null && (!requestsLimit || requestsLimit < limitMin)) return false;
        if (limitMax !== null && requestsLimit !== null && requestsLimit > limitMax) return false;
        if (limitMax !== null && requestsLimit === null) return false;
        return true;
    });

    if (profiles.length === 0) {
        listEl.innerHTML = '<div class="community-empty-note">Профили по текущим фильтрам не найдены.</div>';
        return;
    }

    listEl.innerHTML = profiles.map(function(profile) {
        var roleLabel = profile.role === 'main_admin' ? 'Главный админ' : 'Обычный профиль';
        var expiresLabel = profile.expiresAt ? new Date(profile.expiresAt).toLocaleString('ru-RU') : 'Бессрочно';
        var remainingMinutesLabel = profile.remainingMinutes === null || profile.remainingMinutes === undefined
            ? (profile.expiresAt ? 'Истёк' : 'Бессрочно')
            : String(profile.remainingMinutes);
        var openButton = '<button class="btn btn-accent" type="button" onclick="switchAdminWorkingProfile(&quot;' + escapeHtml(profile.id) + '&quot;)" style="min-width:auto;">Открыть профиль</button>';
        var requestsLimitLabel = profile.requestsLimit ? String(profile.requestsLimit) : 'Не задан';
        var deleteButton = profile.role === 'main_admin'
            ? '<button class="btn btn-neutral" type="button" disabled style="opacity:0.6;cursor:not-allowed;">Главный профиль</button>'
            : '<button class="btn btn-delete" type="button" onclick="deleteAdminProfileById(&quot;' + escapeHtml(profile.id) + '&quot;)" style="min-width:auto;">Удалить</button>';
        return '<div class="profile-card' + (profile.isCurrent ? ' current' : '') + '">' +
            '<div class="profile-card-header">' +
                '<div>' +
                    '<div class="profile-card-name">' + escapeHtml(profile.name || ('Профиль ' + profile.id)) + '</div>' +
                    '<div class="profile-card-id">ID профиля: ' + escapeHtml(profile.id) + '</div>' +
                '</div>' +
                '<span class="profile-card-badge">' + escapeHtml(roleLabel) + '</span>' +
            '</div>' +
            '<div class="profile-card-details">' +
                '<div class="profile-card-row"><span class="profile-card-label">Логин:</span> ' + escapeHtml(profile.username || 'не задан') + '</div>' +
                '<div class="profile-card-row"><span class="profile-card-label">Email:</span> ' + escapeHtml(profile.recoveryEmail || 'не задан') + '</div>' +
                '<div class="profile-card-row"><span class="profile-card-label">Промокод:</span> ' + escapeHtml(profile.promoCodeUsed || 'не использовался') + '</div>' +
                '<div class="profile-card-row"><span class="profile-card-label">Осталось минут:</span> ' + escapeHtml(remainingMinutesLabel) + '</div>' +
                '<div class="profile-card-row"><span class="profile-card-label">Лимит запросов:</span> ' + escapeHtml(requestsLimitLabel) + '</div>' +
                '<div class="profile-card-row"><span class="profile-card-label">Доступ до:</span> ' + escapeHtml(expiresLabel) + '</div>' +
            '</div>' +
            '<div class="profile-card-actions">' +
                '<button class="btn btn-info" type="button" onclick="editAdminProfile(&quot;' + escapeHtml(profile.id) + '&quot;)" style="min-width:auto;">Редактировать</button>' +
                openButton +
                deleteButton +
            '</div>' +
        '</div>';
    }).join('');
};

window.renderPromoCodes = function() {
    const listEl = document.getElementById('promoCodesList');
    if (!listEl) return;
    const allPromoCodes = window.adminDashboard.promoCodes || [];
    const search = String(document.getElementById('promoFilterSearch')?.value || '').trim().toLowerCase();
    const statusFilter = String(document.getElementById('promoFilterStatus')?.value || '').trim();
    const usesMinRaw = String(document.getElementById('promoFilterUsesMin')?.value || '').trim();
    const usesMaxRaw = String(document.getElementById('promoFilterUsesMax')?.value || '').trim();
    const usesMin = usesMinRaw ? Number(usesMinRaw) : null;
    const usesMax = usesMaxRaw ? Number(usesMaxRaw) : null;

    const filtered = allPromoCodes.filter(function(promo) {
        var haystack = [promo.code, promo.label, promo.createdAt].join(' ').toLowerCase();
        if (search && haystack.indexOf(search) === -1) return false;
        if (statusFilter === 'available' && promo.usedCount >= promo.maxUses) return false;
        if (statusFilter === 'used' && promo.usedCount < promo.maxUses) return false;
        if (usesMin !== null && promo.usedCount < usesMin) return false;
        if (usesMax !== null && promo.usedCount > usesMax) return false;
        return true;
    });

    if (!filtered.length) {
        listEl.innerHTML = '<div class="community-empty-note">Промокоды по текущим фильтрам не найдены.</div>';
        return;
    }
    listEl.innerHTML = filtered.map(function(promo) {
        var durationLabel = promo.durationMinutes ? (promo.durationMinutes + ' мин.') : 'Бессрочно';
        var requestsLimitLabel = promo.dailyRequestsLimit ? String(promo.dailyRequestsLimit) : 'Не задан';
        var isFullyUsed = promo.usedCount >= promo.maxUses;
        var isExpired = promo.expiresAt ? (new Date(promo.expiresAt).getTime() <= Date.now()) : false;
        var isDeprecated = isFullyUsed || isExpired;
        var statusBg = isDeprecated ? 'rgba(229,57,53,0.5)' : 'rgba(67,160,71,0.5)';
        var cardClass = 'profile-card promo-card--status';
        return '<div class="' + escapeHtml(cardClass) + '" style="background:' + escapeHtml(statusBg) + ';">' +
            '<div class="profile-card-header"><div><div class="profile-card-name">' + escapeHtml(promo.code) + '</div><div class="profile-card-id">' + escapeHtml(promo.label || 'Без описания') + '</div></div><span class="profile-card-badge" style="background:' + (isDeprecated ? '#e53935' : '#43a047') + ';color:#fff;">Использовано ' + escapeHtml(String(promo.usedCount)) + '/' + escapeHtml(String(promo.maxUses)) + '</span></div>' +
            '<div class="profile-card-details">' +
                '<div class="profile-card-row"><span class="profile-card-label">Срок профиля:</span> ' + escapeHtml(durationLabel) + '</div>' +
                '<div class="profile-card-row"><span class="profile-card-label">Лимит запросов:</span> ' + escapeHtml(requestsLimitLabel) + '</div>' +
                '<div class="profile-card-row"><span class="profile-card-label">Создан:</span> ' + escapeHtml(new Date(promo.createdAt).toLocaleString('ru-RU')) + '</div>' +
            '</div>' +
            '<div class="profile-card-actions">' +
                '<button class="btn btn-delete" type="button" onclick="deletePromoCodeById(&quot;' + escapeHtml(promo.id) + '&quot;)" style="min-width:auto;">Удалить</button>' +
            '</div>' +
        '</div>';
    }).join('');
};

window.renderRecoveryRequests = function() {
    const listEl = document.getElementById('recoveryRequestsList');
    const logsEl = document.getElementById('loginLogsList');
    if (listEl) {
        const requests = window.adminDashboard.recoveryRequests || [];
        if (!requests.length) {
            listEl.innerHTML = '<div class="community-empty-note">Запросов на восстановление пока нет.</div>';
        } else {
            listEl.innerHTML = requests.map(function(item) {
                var actions = item.status === 'pending'
                    ? '<button class="btn btn-save" type="button" onclick="resolveRecoveryRequestById(&quot;' + escapeHtml(item.id) + '&quot;, &quot;' + escapeHtml(item.profileId || '') + '&quot;)" style="min-width:auto;">Сбросить пароль</button>'
                    : '<button class="btn btn-neutral" type="button" disabled style="opacity:0.65;cursor:not-allowed;">Обработано</button>';
                return '<div class="profile-card">' +
                    '<div class="profile-card-header"><div><div class="profile-card-name">' + escapeHtml(item.username || 'Неизвестный профиль') + '</div><div class="profile-card-id">' + escapeHtml(item.recoveryEmail || 'email не задан') + '</div></div><span class="profile-card-badge">' + escapeHtml(item.status || 'pending') + '</span></div>' +
                    '<div class="profile-card-details">' +
                        '<div class="profile-card-row"><span class="profile-card-label">Профиль:</span> ' + escapeHtml(item.profileId || 'не найден') + '</div>' +
                        '<div class="profile-card-row"><span class="profile-card-label">Запрошен:</span> ' + escapeHtml(new Date(item.createdAt).toLocaleString('ru-RU')) + '</div>' +
                        (item.tempPassword ? '<div class="profile-card-row"><span class="profile-card-label">Временный пароль:</span> ' + escapeHtml(item.tempPassword) + '</div>' : '') +
                    '</div>' +
                    '<div class="profile-card-actions">' + actions + '</div>' +
                '</div>';
            }).join('');
        }
    }
    if (logsEl) {
        const logs = window.adminDashboard.loginLogs || [];
        logsEl.innerHTML = logs.length ? logs.map(function(item) {
            return '[' + new Date(item.createdAt).toLocaleString('ru-RU') + '] ' + (item.type || 'event') + ' | user=' + (item.username || '-') + ' | profile=' + (item.profileId || '-') + ' | reason=' + (item.reason || '-') ;
        }).join('<br>') : 'Логи пока пусты.';
    }
    renderAdminAuditPanel();
};

window.profileDashboardData = null;
window.selectedProfileSupportLimit = null;
window.groupManagerState = { search: '', editingIndex: -1 };

function formatRuDateTime(value) {
    if (!value) return '';
    try {
        return new Date(value).toLocaleString('ru-RU');
    } catch (e) {
        return String(value);
    }
}

function getCurrentProfileName() {
    try {
        return localStorage.getItem('adminProfileName') || ('Профиль ' + getCurrentProfileId());
    } catch (e) {
        return 'Профиль';
    }
}

window.exitProfileSession = function() {
    forceLogoutToLogin('Вы вышли из профиля.');
};

window.returnToPrincipalProfile = function() {
    try {
        localStorage.setItem('adminProfileId', getPrincipalProfileId());
        localStorage.setItem('adminProfileName', localStorage.getItem('adminPrincipalProfileName') || ('Профиль ' + getPrincipalProfileId()));
    } catch (e) {}
    location.reload();
};

window.openAdminTabDirect = function() {
    var adminBtn = document.getElementById('adminTabButton');
    if (adminBtn) {
        openTab({ currentTarget: adminBtn }, 'Admin');
        return;
    }
    openTab(null, 'Admin');
};

window.renderAdminAuditPanel = function() {
    var panel = document.getElementById('adminLimitRequestsPanel');
    if (!panel) return;
    var limitRequests = window.adminDashboard?.limitRequests || [];

    var limitHtml = limitRequests.length ? limitRequests.map(function(item) {
        var actions = item.status === 'pending'
            ? '<div class="profile-card-actions"><button class="btn btn-save" type="button" onclick="resolveProfileLimitRequestById(&quot;' + escapeHtml(item.id) + '&quot;, &quot;approved&quot;)">Одобрить</button><button class="btn btn-delete" type="button" onclick="resolveProfileLimitRequestById(&quot;' + escapeHtml(item.id) + '&quot;, &quot;rejected&quot;)">Не одобрять</button></div>'
            : '';
        return '<div class="app-log-card">' +
            '<div class="app-log-card-header"><div><div class="app-log-card-title">Запрос на увеличение лимита</div><div class="app-log-card-meta">' + escapeHtml(formatRuDateTime(item.createdAt)) + '</div></div><div class="app-log-card-badge">' + escapeHtml(item.status || 'pending') + '</div></div>' +
            '<div class="app-log-card-summary">' + escapeHtml(item.profileName || ('Профиль ' + item.profileId)) + ' запросил ' + escapeHtml(String(item.requestedLimit || '0')) + ' запросов в сутки</div>' +
            '<div class="app-log-card-details">' +
                '<div class="app-log-card-detail">Профиль: ' + escapeHtml(item.profileId || '') + '</div>' +
                (item.resolvedAt ? '<div class="app-log-card-detail">Обработан: ' + escapeHtml(formatRuDateTime(item.resolvedAt)) + '</div>' : '') +
                (item.note ? '<div class="app-log-card-detail">Комментарий: ' + escapeHtml(item.note) + '</div>' : '') +
            '</div>' + actions +
        '</div>';
    }).join('') : '<div class="community-empty-note">Запросов на увеличение лимита пока нет.</div>';

    panel.innerHTML = limitHtml;
};

window.resolveProfileLimitRequestById = async function(requestId, status) {
    var statusEl = document.getElementById('appLogsStatus');
    try {
        var note = '';
        if (status === 'rejected') {
            note = prompt('Комментарий для отказа (необязательно):') || '';
        }
        var baseUrl = window.location.href.split('?')[0];
        var res = await fetch(baseUrl + '?resolveProfileLimitRequest', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ requestId: requestId, status: status, note: note, principalProfileId: getPrincipalProfileId() })
        });
        var data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось обработать запрос');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', status === 'approved' ? '✅ Запрос на лимит одобрен.' : '✅ Запрос на лимит отклонён.');
        await loadAdminProfiles();
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

window.persistUploadedCommunityFileRecord = async function(payload) {
    var baseUrl = window.location.href.split('?')[0];
    var response = await fetch(baseUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            action: 'record_uploaded_file',
            profileId: getCurrentProfileId(),
            communityId: payload.communityId,
            groupId: payload.groupId,
            fileName: payload.fileName,
            fileType: payload.fileType,
            fileSize: payload.fileSize,
            attachment: payload.attachment
        })
    });
    var result = await response.json();
    if (!result.success) {
        throw new Error(result.error || 'Не удалось записать файл в каталог');
    }
    return result;
};

window.formatProfileFileSize = function(bytes) {
    var size = Number(bytes || 0);
    if (!Number.isFinite(size) || size <= 0) return '0 B';
    var units = ['B', 'KB', 'MB', 'GB'];
    var unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
        size = size / 1024;
        unitIndex += 1;
    }
    var precision = unitIndex === 0 ? 0 : 1;
    return size.toFixed(precision).replace(/\.0$/, '') + ' ' + units[unitIndex];
};

window.filterProfileFiles = function() {
    var input = document.getElementById('profileFilesFilter');
    var tbody = document.getElementById('profileFilesTableBody');
    if (!input || !tbody) return;
    var query = String(input.value || '').trim().toLowerCase();
    Array.prototype.forEach.call(tbody.querySelectorAll('tr[data-file-search]'), function(row) {
        var haystack = String(row.getAttribute('data-file-search') || '').toLowerCase();
        row.style.display = !query || haystack.indexOf(query) >= 0 ? '' : 'none';
    });
};

window.renderProfileDashboard = function() {
    var container = document.getElementById('profileDashboardContent');
    if (!container) return;
    var data = window.profileDashboardData;
    if (!data) {
        container.innerHTML = '<div class="community-empty-note">Сводка профиля пока не загружена.</div>';
        return;
    }

    var foreignProfileBanner = '';
    if (isMainAdminSession() && getCurrentProfileId() !== getPrincipalProfileId()) {
        foreignProfileBanner = '<div class="inline-notice inline-notice--accent" style="margin-bottom:14px;">Сейчас открыт чужой профиль: <strong>' + escapeHtml(getCurrentProfileName()) + '</strong>. <button class="btn btn-save" type="button" onclick="returnToPrincipalProfile()" style="margin-left:10px;">Вернуться в свой профиль</button> <button class="btn btn-neutral" type="button" onclick="openAdminTabDirect()">Открыть АДМИН</button></div>';
    }

    var communities = Array.isArray(data.communities) ? data.communities : [];
    var history = Array.isArray(data.limitHistory) ? data.limitHistory : [];
    var requests = Array.isArray(data.limitRequests) ? data.limitRequests : [];
    var packages = Array.isArray(data.supportPackages) ? data.supportPackages : [];
    var communityFiles = data.communityFiles && typeof data.communityFiles === 'object' ? data.communityFiles : {};
    var promoStatus = data.promoActivationStatus || { attempts: 0, remainingAttempts: 3, blocked: false, nextResetAt: 0 };
    var activeCommunityId = String(window.currentCommunityId || '').trim();
    if (!window.selectedProfileSupportLimit) {
        window.selectedProfileSupportLimit = packages[0] || 1000;
    }

    var selectedCommunity = communities.find(function(item) {
        var communityKey = String(item.communityId || '').trim();
        var vkGroupId = String(item.vkGroupId || item.communityId || '').trim();
        return !!activeCommunityId && (activeCommunityId === communityKey || activeCommunityId === vkGroupId);
    }) || communities[0] || null;
    var selectedCommunityVkGroupId = selectedCommunity ? String(selectedCommunity.vkGroupId || selectedCommunity.communityId || '').trim() : '';
    var selectedCommunityFiles = selectedCommunityVkGroupId && Array.isArray(communityFiles[selectedCommunityVkGroupId])
        ? communityFiles[selectedCommunityVkGroupId]
        : [];
    var filesRowsHtml = selectedCommunityFiles.length
        ? selectedCommunityFiles.map(function(item) {
            var fileName = String(item.fileName || '').trim();
            var fileType = String(item.fileType || '').trim();
            var fileSize = formatProfileFileSize(item.fileSize);
            var attachment = String(item.attachment || '').trim();
            var searchText = [fileName, fileType, fileSize, attachment].join(' ').toLowerCase();
            return '<tr data-file-search="' + escapeHtml(searchText) + '">' +
                '<td>' + escapeHtml(fileName || 'Без названия') + '</td>' +
                '<td>' + escapeHtml(fileType || 'Не указан') + '</td>' +
                '<td>' + escapeHtml(fileSize) + '</td>' +
                '<td><code>' + escapeHtml(attachment) + '</code></td>' +
            '</tr>';
        }).join('')
        : '<tr><td colspan="4" class="community-empty-note" style="text-align:left;">' + escapeHtml(selectedCommunity ? 'Для этого сообщества файлы пока не загружались.' : 'Сначала выбери или подключи сообщество.') + '</td></tr>';
    var filesSectionHtml =
        '<div class="settings-surface profile-manager"><div class="profile-manager-header"><div><h3 class="profile-manager-title">Файлы</h3><div class="profile-manager-subtitle">Каталог файлов активного сообщества. Аттачмент можно повторно использовать во вложениях без новой загрузки.</div></div></div>' +
        (selectedCommunity ? '<div class="profile-manager-subtitle" style="margin-bottom:12px;">Сейчас показано сообщество: <strong>' + escapeHtml(selectedCommunity.groupName || ('Сообщество ' + selectedCommunityVkGroupId)) + '</strong> (' + escapeHtml(selectedCommunityVkGroupId) + ')</div>' : '') +
        '<div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:12px;"><input type="text" id="profileFilesFilter" placeholder="Поиск по названию, типу, размеру или аттачменту" oninput="filterProfileFiles()" style="flex:1;min-width:280px;"></div>' +
        '<div style="overflow:auto;"><table style="width:100%;border-collapse:collapse;"><thead><tr><th>Название</th><th>Тип</th><th>Размер</th><th>Аттачмент</th></tr></thead><tbody id="profileFilesTableBody">' + filesRowsHtml + '</tbody></table></div></div>';

    var promoSectionHtml = '';
    if (data.isMainAdmin) {
        promoSectionHtml = '<div class="settings-surface profile-manager"><div class="profile-manager-header"><div><h3 class="profile-manager-title">Активация промокода</h3><div class="profile-manager-subtitle">Промокоды в этой вкладке доступны только обычным профилям.</div></div></div><div class="community-empty-note">Главному админу промокоды не требуются.</div></div>';
    } else {
        var resetLabel = promoStatus.nextResetAt ? formatRuDateTime(promoStatus.nextResetAt) : '00:00 МСК';
        var promoHint = promoStatus.blocked
            ? ('Лимит попыток ввода исчерпан до ' + resetLabel + '. Новый ввод станет доступен после 00:00 МСК.')
            : ('Осталось попыток ввода сегодня: ' + escapeHtml(String(promoStatus.remainingAttempts || 0)) + ' из 3.');
        var disabledAttr = promoStatus.blocked ? 'disabled' : '';
        promoSectionHtml = '<div class="settings-surface profile-manager"><div class="profile-manager-header"><div><h3 class="profile-manager-title">Активация промокода</h3><div class="profile-manager-subtitle">Промокод доначисляет срок жизни профиля и суточный лимит запросов, указанные в самом промокоде.</div></div></div><div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;"><input type="text" id="profilePromoCodeInput" placeholder="Введите активный промокод" ' + disabledAttr + ' style="flex:1;min-width:260px;"><button class="btn btn-accent" type="button" onclick="activateProfilePromoCode()" ' + disabledAttr + '>Активировать</button></div><div class="profile-manager-subtitle" style="margin-top:10px;">' + promoHint + '</div><div id="profilePromoActivationStatus" style="margin-top:10px;"></div></div>';
    }

    var communitiesHtml = communities.length
        ? '<div style="display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;align-items:start;">' + communities.map(function(item) {
            var communityKey = String(item.communityId || '').trim();
            var vkGroupId = String(item.vkGroupId || item.communityId || '').trim();
            var isActiveCommunity = !!activeCommunityId && (activeCommunityId === communityKey || activeCommunityId === vkGroupId);
            var cardClass = 'profile-card' + (isActiveCommunity ? ' profile-card--active-community' : '');
            var activeBadge = isActiveCommunity
                ? '<span class="profile-card-badge">🟢 Активно</span>'
                : '';
            return '<div class="' + cardClass + '">' +
                '<div class="profile-card-header"><div><div class="profile-card-name">' + escapeHtml(item.groupName || ('Сообщество ' + vkGroupId)) + '</div><div class="profile-card-id">VK Group ID: ' + escapeHtml(vkGroupId) + '</div></div>' + activeBadge + '</div>' +
                '<div class="profile-card-details">' +
                    '<div class="profile-card-row"><span class="profile-card-label">Пользователей:</span> ' + escapeHtml(String(item.usersCount || 0)) + '</div>' +
                    '<div class="profile-card-row"><span class="profile-card-label">Обработано сообщений:</span> ' + escapeHtml(String(item.messages || 0)) + '</div>' +
                    '<div class="profile-card-row"><span class="profile-card-label">Обработано комментариев:</span> ' + escapeHtml(String(item.comments || 0)) + '</div>' +
                    '<div class="profile-card-row"><span class="profile-card-label">Срабатываний триггеров:</span> ' + escapeHtml(String(item.triggers || 0)) + '</div>' +
                    '<div class="profile-card-row"><span class="profile-card-label">Запросов к PAPA BOT:</span> ' + escapeHtml(String(item.papaRequests || 0)) + '</div>' +
                    (item.lastEventAt ? '<div class="profile-card-row"><span class="profile-card-label">Последняя активность:</span> ' + escapeHtml(formatRuDateTime(item.lastEventAt)) + '</div>' : '') +
                '</div>' +
            '</div>';
        }).join('') + '</div>'
        : '<div class="community-empty-note">Пока ни одно сообщество не подключено.</div>';

    container.innerHTML = foreignProfileBanner +
        '<div class="profile-manager-header"><div><h3 class="profile-manager-title">' + escapeHtml(data.profileName || getCurrentProfileName()) + '</h3><div class="profile-manager-subtitle">Текущий профиль: ' + escapeHtml(getCurrentProfileId()) + '. Здесь можно выйти из профиля, посмотреть статистику и запросить увеличение лимита.</div></div><div class="profile-card-actions"><button class="btn btn-delete" type="button" onclick="exitProfileSession()">Выйти с профиля</button></div></div>' +
        '<div class="profile-grid">' +
            '<div class="profile-card current"><div class="profile-card-name">Лимит запросов в сутки</div><div class="profile-card-details"><div class="profile-card-row"><span class="profile-card-label">Лимит:</span> ' + escapeHtml(data.dailyLimit ? String(data.dailyLimit) : 'Без ограничений') + '</div><div class="profile-card-row"><span class="profile-card-label">Использовано сегодня:</span> ' + escapeHtml(String(data.dailyUsed || 0)) + '</div><div class="profile-card-row"><span class="profile-card-label">Осталось:</span> ' + escapeHtml(data.dailyRemaining === null ? 'Без ограничений' : String(data.dailyRemaining)) + '</div></div></div>' +
            '<div class="profile-card"><div class="profile-card-name">Суммарная активность</div><div class="profile-card-details"><div class="profile-card-row"><span class="profile-card-label">Запросов к PAPA BOT:</span> ' + escapeHtml(String(data.totalPapaRequests || 0)) + '</div><div class="profile-card-row"><span class="profile-card-label">Сообщения:</span> ' + escapeHtml(String(data.totalMessages || 0)) + '</div><div class="profile-card-row"><span class="profile-card-label">Комментарии:</span> ' + escapeHtml(String(data.totalComments || 0)) + '</div><div class="profile-card-row"><span class="profile-card-label">Триггеры:</span> ' + escapeHtml(String(data.totalTriggers || 0)) + '</div></div></div>' +
        '</div>' +
        promoSectionHtml +
        '<div class="settings-surface profile-manager"><div class="profile-manager-header"><div><h3 class="profile-manager-title">Подключённые сообщества</h3><div class="profile-manager-subtitle">По каждому сообществу показаны пользователи и накопленная статистика обработки.</div></div></div>' +
            communitiesHtml +
        '</div>' +
        filesSectionHtml +
        '<div class="settings-surface profile-manager"><div class="profile-manager-header"><div><h3 class="profile-manager-title">Поддержка автора</h3><div class="profile-manager-subtitle">Пока это ручной процесс: выбери пакет и отправь запрос главному админу.</div></div></div><div class="profile-card-row" style="margin-bottom:12px;"><span class="profile-card-label">Увеличения лимитов:</span></div><div style="display:flex;flex-wrap:wrap;gap:10px;margin-bottom:12px;">' + packages.map(function(limitValue) { var selected = Number(limitValue) === Number(window.selectedProfileSupportLimit); return '<button class="btn ' + (selected ? 'btn-save' : 'btn-neutral') + '" type="button" onclick="selectProfileLimitPackage(' + Number(limitValue) + ')">' + escapeHtml(Number(limitValue).toLocaleString('ru-RU')) + '</button>'; }).join('') + '</div><div class="profile-manager-subtitle">Выбран пакет: <strong>' + escapeHtml(Number(window.selectedProfileSupportLimit || packages[0] || 1000).toLocaleString('ru-RU')) + '</strong> запросов в сутки. Нажатие кнопки отправляет запрос главному админу.</div><div style="margin-top:12px;"><button class="btn btn-accent" type="button" onclick="requestSelectedProfileLimit()">Приобрести</button></div></div>' +
        '<div class="settings-surface profile-manager"><div class="profile-manager-header"><div><h3 class="profile-manager-title">История начислений и запросов</h3><div class="profile-manager-subtitle">Здесь видно, когда лимит менялся и какие запросы уже были отправлены.</div></div></div>' +
            (history.length ? history.map(function(item) {
                return '<div class="app-log-card"><div class="app-log-card-header"><div><div class="app-log-card-title">Начислен лимит</div><div class="app-log-card-meta">' + escapeHtml(formatRuDateTime(item.at)) + '</div></div><div class="app-log-card-badge">' + escapeHtml(String(item.limit || 0)) + '/сутки</div></div>' +
                (item.note ? '<div class="app-log-card-summary">' + escapeHtml(item.note) + '</div>' : '') + '</div>';
            }).join('') : '<div class="community-empty-note">Начислений лимита пока не было.</div>') +
            (requests.length ? '<div style="height:12px"></div>' + requests.map(function(item) {
                var deleteButton = item.status === 'pending'
                    ? '<div class="profile-card-actions" style="margin-top:10px;"><button class="btn btn-delete" type="button" onclick="deleteProfileLimitRequestById(&quot;' + escapeHtml(item.id) + '&quot;)">Удалить запрос</button></div>'
                    : '';
                return '<div class="app-log-card"><div class="app-log-card-header"><div><div class="app-log-card-title">Запрос на лимит</div><div class="app-log-card-meta">' + escapeHtml(formatRuDateTime(item.createdAt)) + '</div></div><div class="app-log-card-badge">' + escapeHtml(item.status || 'pending') + '</div></div><div class="app-log-card-summary">' + escapeHtml(String(item.requestedLimit || 0)) + ' запросов в сутки</div>' + deleteButton + '</div>';
            }).join('') : '') +
        '</div>';
};

window.loadProfileDashboard = async function() {
    var loadingEl = document.getElementById('loading-Profile');
    var statusEl = document.getElementById('profileDashboardStatus');
    try {
        if (loadingEl) loadingEl.style.display = 'block';
        var baseUrl = window.location.href.split('?')[0];
        var profileId = encodeURIComponent(getCurrentProfileId() || getPrincipalProfileId() || '1');
        var principalProfileId = encodeURIComponent(getPrincipalProfileId() || '1');
        var res = await fetch(baseUrl + '?getProfileDashboard=1&profileId=' + profileId + '&principalProfileId=' + principalProfileId);
        var data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось загрузить профиль');
        window.profileDashboardData = data.dashboard;
        renderProfileDashboard();
        if (statusEl) statusEl.innerHTML = '';
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Не удалось загрузить профиль: ' + e.message);
    } finally {
        if (loadingEl) loadingEl.style.display = 'none';
    }
};

window.activateProfilePromoCode = async function() {
    var statusEl = document.getElementById('profilePromoActivationStatus') || document.getElementById('profileDashboardStatus');
    var inputEl = document.getElementById('profilePromoCodeInput');
    var code = String(inputEl && inputEl.value || '').trim();
    if (!code) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Введите промокод.');
        return;
    }
    try {
        var baseUrl = window.location.href.split('?')[0];
        var res = await fetch(baseUrl + '?activateProfilePromoCode', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                code: code,
                profileId: getCurrentProfileId(),
                principalProfileId: getPrincipalProfileId()
            })
        });
        var data = await res.json();
        if (data.dashboard) {
            window.profileDashboardData = data.dashboard;
            renderProfileDashboard();
        } else if (window.profileDashboardData && data.promoActivationStatus) {
            window.profileDashboardData.promoActivationStatus = data.promoActivationStatus;
            renderProfileDashboard();
        }
        statusEl = document.getElementById('profilePromoActivationStatus') || document.getElementById('profileDashboardStatus');
        if (!data.success) {
            throw new Error(data.error || 'Не удалось активировать промокод');
        }
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Промокод активирован. Показатели профиля обновлены.');
        if (isMainAdminSession()) {
            await loadAdminProfiles();
        }
    } catch (e) {
        statusEl = document.getElementById('profilePromoActivationStatus') || document.getElementById('profileDashboardStatus');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

window.requestProfileLimitIncrease = async function(limitValue) {
    var statusEl = document.getElementById('profileDashboardStatus');
    try {
        var baseUrl = window.location.href.split('?')[0];
        var res = await fetch(baseUrl + '?requestProfileLimit', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ requestedLimit: limitValue, principalProfileId: getPrincipalProfileId() })
        });
        var data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось отправить запрос');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Запрос отправлен, ждите одобрения.');
        await loadProfileDashboard();
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

window.selectProfileLimitPackage = function(limitValue) {
    window.selectedProfileSupportLimit = Number(limitValue || 1000);
    renderProfileDashboard();
};

window.requestSelectedProfileLimit = function() {
    requestProfileLimitIncrease(window.selectedProfileSupportLimit || 1000);
};

window.deleteProfileLimitRequestById = async function(requestId) {
    var statusEl = document.getElementById('profileDashboardStatus');
    if (!requestId) return;
    if (!confirm('Удалить этот запрос на лимит?')) return;
    try {
        var baseUrl = window.location.href.split('?')[0];
        var res = await fetch(baseUrl + '?deleteProfileLimitRequest', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ requestId: requestId, principalProfileId: getPrincipalProfileId() })
        });
        var data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось удалить запрос');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Запрос на лимит удалён.');
        await loadProfileDashboard();
        if (isMainAdminSession()) {
            await loadAdminProfiles();
        }
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

function getGroupHistoryEntry(userRow, groupName) {
    try {
        var history = JSON.parse(userRow['_История групп'] || '{}') || {};
        return history[String(groupName || '').trim().toLowerCase()] || {};
    } catch (e) {
        return {};
    }
}

function upsertGroupHistoryEntry(userRow, groupName, actionType) {
    var normalizedGroup = String(groupName || '').trim().toLowerCase();
    if (!normalizedGroup) return;
    var history = {};
    try {
        history = JSON.parse(userRow['_История групп'] || '{}') || {};
    } catch (e) {
        history = {};
    }
    var entry = Object.assign({}, history[normalizedGroup] || {});
    var nowIso = new Date().toISOString();
    if (actionType === 'join') entry.joinedAt = nowIso;
    if (actionType === 'left') entry.leftAt = nowIso;
    history[normalizedGroup] = entry;
    userRow['_История групп'] = JSON.stringify(history);
}

function getNormalizedUserGroups(userRow) {
    return String(userRow['ГРУППА'] || '')
        .replace(/\\r/g, '\\n')
        .split(/[\\n,]+/)
        .map(function(item) {
        return item.trim().toLowerCase();
    }).filter(Boolean);
}

window.renderGroupsManager = function() {
    var container = document.getElementById('groupsManager');
    if (!container) return;
    var groups = Array.isArray(dataStore['Groups']) ? dataStore['Groups'] : [];
    var users = Array.isArray(dataStore['Users']) ? dataStore['Users'] : [];
    var query = String(window.groupManagerState.search || '').trim().toLowerCase();
    var filteredGroups = groups.filter(function(group) {
        var title = String(group['Группа'] || '').trim().toLowerCase();
        var description = String(group['Описание'] || '').trim().toLowerCase();
        if (!title && !description) return false;
        return !query || title.includes(query) || description.includes(query);
    });

    container.innerHTML = '<div class="profile-manager-header"><div><h3 class="profile-manager-title">Группы профиля</h3><div class="profile-manager-subtitle">Создавайте группы, добавляйте пользователей, смотрите кто состоит в группе и когда он был добавлен или удалён.</div></div><div class="profile-card-actions"><button class="btn btn-add" type="button" onclick="openGroupForm()">+ Новая группа</button></div></div>' +
        '<details class="settings-surface profile-manager" style="margin-bottom:12px;"><summary style="cursor:pointer;font-weight:700;">Фильтр групп</summary><div style="margin-top:12px;"><input type="text" id="groupsSearchInput" value="' + escapeHtml(window.groupManagerState.search || '') + '" placeholder="Поиск по названию или описанию" oninput="handleGroupsFilterChange()"></div></details>' +
        '<div id="groupsFormPanel"></div>' +
        (filteredGroups.length ? filteredGroups.map(function(group, idx) {
            var groupName = String(group['Группа'] || '').trim();
            var members = users.filter(function(user) { return getNormalizedUserGroups(user).includes(groupName.toLowerCase()); });
            var formerMembers = users.filter(function(user) {
                if (getNormalizedUserGroups(user).includes(groupName.toLowerCase())) return false;
                var history = getGroupHistoryEntry(user, groupName);
                return !!history.leftAt;
            });
            var membersHtml = members.length ? members.map(function(user) {
                var history = getGroupHistoryEntry(user, groupName);
                return '<div class="app-log-card-detail">' + escapeHtml(String(user['ИМЯ'] || user['ID'] || 'Пользователь')) + ' (ID ' + escapeHtml(String(user['ID'] || '')) + ')' + (history.joinedAt ? ' • вступил: ' + escapeHtml(formatRuDateTime(history.joinedAt)) : '') + (history.leftAt ? ' • выходил: ' + escapeHtml(formatRuDateTime(history.leftAt)) : '') + '</div>';
            }).join('') : '<div class="community-empty-note">В этой группе пока нет пользователей.</div>';
            var formerMembersHtml = formerMembers.length ? formerMembers.map(function(user) {
                var history = getGroupHistoryEntry(user, groupName);
                return '<div class="app-log-card-detail">' + escapeHtml(String(user['ИМЯ'] || user['ID'] || 'Пользователь')) + ' (ID ' + escapeHtml(String(user['ID'] || '')) + ')' + (history.leftAt ? ' • вышел: ' + escapeHtml(formatRuDateTime(history.leftAt)) : '') + '</div>';
            }).join('') : '<div class="community-empty-note">Выходов из группы пока не было.</div>';
            return '<div class="profile-card" style="margin-bottom:12px;">' +
                '<div class="profile-card-header"><div><div class="profile-card-name">' + escapeHtml(groupName || 'Без названия') + '</div><div class="profile-card-id">' + escapeHtml(group['Описание'] || 'Описание не заполнено') + '</div></div><span class="profile-card-badge">Участников: ' + escapeHtml(String(members.length)) + '</span></div>' +
                '<div class="profile-card-details"><div class="profile-card-row"><span class="profile-card-label">Сейчас в группе:</span></div>' + membersHtml + '<div class="profile-card-row" style="margin-top:10px;"><span class="profile-card-label">Уже вышли из группы:</span></div>' + formerMembersHtml + '</div>' +
                '<div class="profile-card-actions"><button class="btn btn-info" type="button" onclick="openGroupForm(' + idx + ')">Редактировать</button><button class="btn btn-save" type="button" onclick="manageGroupMembers(' + idx + ', true)">Добавить пользователей</button><button class="btn btn-neutral" type="button" onclick="manageGroupMembers(' + idx + ', false)">Удалить пользователей</button><button class="btn btn-delete" type="button" onclick="deleteGroupByIndex(' + idx + ')">Удалить группу</button></div>' +
            '</div>';
        }).join('') : '<div class="community-empty-note">Группы пока не созданы.</div>');

    renderGroupFormPanel();
};

function renderGroupFormPanel() {
    var panel = document.getElementById('groupsFormPanel');
    if (!panel) return;
    var groups = Array.isArray(dataStore['Groups']) ? dataStore['Groups'] : [];
    var editIndex = window.groupManagerState.editingIndex;
    if (editIndex < 0) {
        panel.innerHTML = '';
        return;
    }
    var row = groups[editIndex] || { 'Группа': '', 'Описание': '' };
    panel.innerHTML = '<div class="settings-surface profile-manager" style="margin-bottom:12px;"><div class="profile-manager-header"><div><h3 class="profile-manager-title">' + (groups[editIndex] ? 'Редактирование группы' : 'Новая группа') + '</h3></div></div><div class="profile-form-grid"><div><label><strong>Название группы</strong></label><input id="groupFormName" type="text" value="' + escapeHtml(row['Группа'] || '') + '" placeholder="Например: vip"></div><div><label><strong>Описание группы</strong></label><input id="groupFormDescription" type="text" value="' + escapeHtml(row['Описание'] || '') + '" placeholder="Кто входит в группу и зачем она нужна"></div></div><div class="profile-card-actions"><button class="btn btn-save" type="button" onclick="saveGroupForm()">Сохранить группу</button><button class="btn btn-neutral" type="button" onclick="closeGroupForm()">Отмена</button></div></div>';
}

window.openGroupForm = function(index) {
    window.groupManagerState.editingIndex = typeof index === 'number' ? index : (dataStore['Groups'] || []).length;
    if (typeof index !== 'number') {
        if (!dataStore['Groups']) dataStore['Groups'] = [];
        dataStore['Groups'].push({ 'Группа': '', 'Описание': '' });
    }
    renderGroupsManager();
};

window.closeGroupForm = function() {
    var groups = dataStore['Groups'] || [];
    var editIndex = window.groupManagerState.editingIndex;
    if (editIndex >= 0 && groups[editIndex] && !String(groups[editIndex]['Группа'] || '').trim() && !String(groups[editIndex]['Описание'] || '').trim()) {
        groups.splice(editIndex, 1);
    }
    window.groupManagerState.editingIndex = -1;
    renderGroupsManager();
};

window.saveGroupForm = async function() {
    var statusEl = document.getElementById('status-Groups');
    try {
        var editIndex = window.groupManagerState.editingIndex;
        var groups = dataStore['Groups'] || [];
        var groupName = String(document.getElementById('groupFormName')?.value || '').trim();
        var description = String(document.getElementById('groupFormDescription')?.value || '').trim();
        if (!groupName) throw new Error('Введите название группы');
        if (groups.some(function(item, idx) { return idx !== editIndex && String(item['Группа'] || '').trim().toLowerCase() === groupName.toLowerCase(); })) {
            throw new Error('Группа с таким названием уже существует');
        }
        var previousName = String(groups[editIndex]?.['Группа'] || '').trim();
        if (previousName && previousName.toLowerCase() !== groupName.toLowerCase()) {
            (dataStore['Users'] || []).forEach(function(user) {
                var currentGroups = getNormalizedUserGroups(user).map(function(item) {
                    return item === previousName.toLowerCase() ? groupName.toLowerCase() : item;
                });
                user['ГРУППА'] = currentGroups.join('\\n');
                var previousHistory = getGroupHistoryEntry(user, previousName);
                if (previousHistory && Object.keys(previousHistory).length) {
                    upsertGroupHistoryEntry(user, groupName, previousHistory.leftAt ? 'left' : 'join');
                }
            });
            await saveDataDirectly('Users');
        }
        groups[editIndex] = { 'Группа': groupName, 'Описание': description };
        await saveDataDirectly('Groups');
        window.groupManagerState.editingIndex = -1;
        renderGroupsManager();
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Группа сохранена.');
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

window.handleGroupsFilterChange = function() {
    window.groupManagerState.search = String(document.getElementById('groupsSearchInput')?.value || '').trim();
    renderGroupsManager();
};

window.manageGroupMembers = async function(groupIndex, addMode) {
    var groups = dataStore['Groups'] || [];
    var group = groups[groupIndex];
    var statusEl = document.getElementById('status-Groups');
    if (!group) return;
    var input = prompt((addMode ? 'Введите ID пользователей для добавления' : 'Введите ID пользователей для удаления') + ' (через запятую или новую строку):');
    if (!input) return;
    var ids = String(input).split(/[\\r\\n,]+/).map(function(item) { return item.trim(); }).filter(Boolean);
    var groupName = String(group['Группа'] || '').trim();
    var users = dataStore['Users'] || [];
    ids.forEach(function(id) {
        var user = users.find(function(item) { return String(item['ID'] || '').trim() === id; });
        if (!user) return;
        var currentGroups = getNormalizedUserGroups(user);
        var normalizedGroup = groupName.toLowerCase();
        if (addMode && !currentGroups.includes(normalizedGroup)) {
            currentGroups.push(normalizedGroup);
            upsertGroupHistoryEntry(user, normalizedGroup, 'join');
        }
        if (!addMode && currentGroups.includes(normalizedGroup)) {
            currentGroups = currentGroups.filter(function(item) { return item !== normalizedGroup; });
            upsertGroupHistoryEntry(user, normalizedGroup, 'left');
        }
        user['ГРУППА'] = currentGroups.join('\\n');
    });
    try {
        await saveDataDirectly('Users');
        renderGroupsManager();
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Состав группы обновлён.');
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

window.deleteGroupByIndex = async function(groupIndex) {
    var groups = dataStore['Groups'] || [];
    var users = dataStore['Users'] || [];
    var statusEl = document.getElementById('status-Groups');
    var group = groups[groupIndex];
    if (!group) return;
    var groupName = String(group['Группа'] || '').trim();
    if (!confirm('Удалить группу "' + groupName + '"? Пользователи будут исключены из неё.')) return;
    users.forEach(function(user) {
        var currentGroups = getNormalizedUserGroups(user).filter(function(item) { return item !== groupName.toLowerCase(); });
        user['ГРУППА'] = currentGroups.join('\\n');
        upsertGroupHistoryEntry(user, groupName, 'left');
    });
    groups.splice(groupIndex, 1);
    try {
        await saveDataDirectly('Users');
        await saveDataDirectly('Groups');
        renderGroupsManager();
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Группа удалена.');
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

window.loadGroupsTab = async function() {
    var loadingEl = document.getElementById('loading-Groups');
    var statusEl = document.getElementById('status-Groups');
    try {
        if (loadingEl) loadingEl.style.display = 'block';
        await loadData('Users');
        await loadData('Groups');
        renderGroupsManager();
        if (statusEl) statusEl.innerHTML = '';
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Не удалось загрузить группы: ' + e.message);
    } finally {
        if (loadingEl) loadingEl.style.display = 'none';
    }
};

const APP_LOG_TAB_OPTIONS = [
    { value: 'ALL', label: 'Все' },
    { value: 'MESSAGES', label: '💬 Сообщения' },
    { value: 'COMMENTS', label: '📝 Комментарии' },
    { value: 'USERS', label: '👤 Пользователи' },
    { value: 'PROFILE', label: '📇 Профиль' },
    { value: 'VARIABLES', label: '🧮 Переменные' },
    { value: 'MAILING', label: '📨 Рассылка' },
    { value: 'DELAYED', label: '⏳ Отложенные' },
    { value: 'TRIGGERS', label: '🎯 Триггеры' },
    { value: 'SETTINGS', label: '⚙️ Настройка' }
];

window.adminAppLogs = [];
window.appLogCurrentFilter = 'ALL';
window.appLogsEnabled = true;
window.appLogsFileName = '';
window.botVersionData = null;
window.versionEditorSelectedKey = 'global';
window.structuredTriggerFilterState = { query: '', eventCode: 'ALL' };

function renderVersionSegments(versionText, sizeClass) {
    var raw = String(versionText || '').trim();
    if (!raw) return 'version ...';

    var prefix = 'version';
    var numericPart = raw;
    if (raw.toLowerCase().indexOf('version ') === 0) {
        numericPart = raw.substring(8).trim();
    }

    var segments = numericPart.split('.').filter(Boolean);
    return '<span class="version-segments ' + escapeHtml(sizeClass || '') + '">' +
        '<span class="version-prefix">' + escapeHtml(prefix) + '</span>' +
        segments.map(function(segment, idx) {
            var dot = idx < segments.length - 1 ? '<span class="version-dot">.</span>' : '';
            return '<span class="version-segment seg-' + (idx % 12) + '">' + escapeHtml(segment) + '</span>' + dot;
        }).join('') +
    '</span>';
}

window.renderBotVersion = function() {
    var chipEl = document.getElementById('botVersionChip');
    var summaryEl = document.getElementById('botVersionSummary');
    var partsEl = document.getElementById('botVersionParts');
    var subtitleEl = document.getElementById('botVersionModalSubtitle');
    var data = window.botVersionData;

    if (!data) {
        if (chipEl) chipEl.innerHTML = renderVersionSegments('version ...', 'version-segments--chip');
        if (summaryEl) summaryEl.textContent = 'Данные о версии пока не загружены.';
        if (partsEl) partsEl.innerHTML = '';
        if (subtitleEl) subtitleEl.textContent = 'Расшифровка версии загрузится после запроса к серверу.';
        return;
    }

    if (chipEl) chipEl.innerHTML = renderVersionSegments(data.displayVersion || 'version unknown', 'version-segments--chip');
    if (subtitleEl) {
        subtitleEl.textContent = 'Обновления PAPA BOT от ' + escapeHtml(data.updatedAt || 'неизвестной даты') + ' по 14:30 мск.';
    }
    if (summaryEl) {
        summaryEl.innerHTML = '<div style="margin-bottom:6px;">' + renderVersionSegments(data.displayVersion || 'version unknown', 'version-segments--large') + '</div>' +
            '<div style="font-size:13px;line-height:1.6;color:var(--text-secondary);">' + escapeHtml(data.baseline ? 'Это базовая официальная точка отсчёта блочной версионности.' : 'Версия уже ведётся по блокам.') + '</div>';
    }
    if (partsEl) {
        var parts = Array.isArray(data.parts) ? data.parts : [];
        partsEl.innerHTML = parts.map(function(part, partIdx) {
            var history = Array.isArray(part.history) ? part.history : [];
            return '<div class="version-part-card">' +
                '<div class="version-part-value"><span class="version-segment seg-' + (partIdx % 12) + '">' + escapeHtml(part.value || '') + '</span></div>' +
                '<div class="version-part-title">' + escapeHtml(part.label || part.key || '') + '</div>' +
                '<div class="version-current-note">' + escapeHtml(part.currentSummary || '') + '</div>' +
                '<div class="version-history-box">' + history.map(function(item) {
                    return '<div class="version-history-item">' +
                        '<div class="version-history-version version-segment seg-' + ((String(item.version || '').replace(/\D/g, '').slice(-2) || '0') % 12) + '">' + escapeHtml(item.version || '') + '</div>' +
                        '<div class="version-history-text">' + escapeHtml(item.summary || '') + '</div>' +
                    '</div>';
                }).join('') + '</div>' +
            '</div>';
        }).join('');
    }

    renderVersionEditor();
    renderCapabilitiesModal();
};

window.loadBotVersion = async function() {
    try {
        var baseUrl = window.location.href.split('?')[0];
        var res = await fetch(baseUrl + '?getBotVersion');
        var data = await res.json();
        window.botVersionData = data;
        renderBotVersion();
    } catch (e) {
        console.error('[Admin] ❌ loadBotVersion error:', e.message);
        renderBotVersion();
    }
};

window.renderVersionEditor = function() {
    var listEl = document.getElementById('versionEditorPartsList');
    if (!listEl) return;
    var parts = Array.isArray(window.botVersionData?.parts) ? window.botVersionData.parts : [];
    if (!parts.length) {
        listEl.innerHTML = '<div class="community-empty-note">Нет данных по версии.</div>';
        return;
    }

    if (!parts.some(function(part) { return part.key === window.versionEditorSelectedKey; })) {
        window.versionEditorSelectedKey = parts[0].key;
    }

    listEl.innerHTML = parts.map(function(part) {
        return '<button class="version-editor-item' + (part.key === window.versionEditorSelectedKey ? ' is-active' : '') + '" type="button" onclick="selectVersionEditorPart(&quot;' + escapeHtml(part.key) + '&quot;)">' +
            '<div class="version-editor-item-title">' + escapeHtml(part.value + ' • ' + part.label) + '</div>' +
            '<div class="version-editor-item-meta">' + escapeHtml(part.currentSummary || '') + '</div>' +
        '</button>';
    }).join('');

    var selectedPart = parts.find(function(part) { return part.key === window.versionEditorSelectedKey; }) || parts[0];
    document.getElementById('versionEditorLabel').value = selectedPart.label || '';
    document.getElementById('versionEditorValue').value = selectedPart.value || '';
    document.getElementById('versionEditorCurrentSummary').value = selectedPart.currentSummary || '';
    document.getElementById('versionEditorDescription').value = selectedPart.description || '';
    document.getElementById('versionEditorHistoryVersion').value = selectedPart.value || '';
    document.getElementById('versionEditorHistorySummary').value = '';
};

window.selectVersionEditorPart = function(key) {
    window.versionEditorSelectedKey = key;
    renderVersionEditor();
};

window.appendVersionHistoryEntry = function() {
    var parts = Array.isArray(window.botVersionData?.parts) ? window.botVersionData.parts : [];
    var part = parts.find(function(item) { return item.key === window.versionEditorSelectedKey; });
    if (!part) return;
    var versionValue = String(document.getElementById('versionEditorHistoryVersion').value || '').trim();
    var summaryValue = String(document.getElementById('versionEditorHistorySummary').value || '').trim();
    if (!versionValue || !summaryValue) {
        var statusEl = document.getElementById('botVersionEditorStatus');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Заполни номер и описание записи истории.');
        return;
    }
    part.history = Array.isArray(part.history) ? part.history : [];
    var existing = part.history.find(function(item) { return item.version === versionValue; });
    if (existing) {
        existing.summary = summaryValue;
    } else {
        part.history.push({ version: versionValue, summary: summaryValue });
        part.history.sort(function(a, b) { return String(a.version).localeCompare(String(b.version)); });
    }
    part.value = String(document.getElementById('versionEditorValue').value || part.value || '').trim();
    part.currentSummary = String(document.getElementById('versionEditorCurrentSummary').value || part.currentSummary || '').trim();
    part.description = String(document.getElementById('versionEditorDescription').value || part.description || '').trim();
    renderBotVersion();
    document.getElementById('versionEditorHistorySummary').value = '';
    var statusEl2 = document.getElementById('botVersionEditorStatus');
    if (statusEl2) statusEl2.innerHTML = makeInlineNotice('success', '✅ Запись добавлена локально. Теперь нажми «Сохранить версию».');
};

window.saveVersionEditorChanges = async function() {
    var statusEl = document.getElementById('botVersionEditorStatus');
    try {
        var parts = Array.isArray(window.botVersionData?.parts) ? window.botVersionData.parts : [];
        var part = parts.find(function(item) { return item.key === window.versionEditorSelectedKey; });
        if (!part) throw new Error('Блок версии не найден');

        part.value = String(document.getElementById('versionEditorValue').value || '').trim();
        part.currentSummary = String(document.getElementById('versionEditorCurrentSummary').value || '').trim();
        part.description = String(document.getElementById('versionEditorDescription').value || '').trim();

        var baseUrl = window.location.href.split('?')[0];
        var res = await fetch(baseUrl + '?saveBotVersion', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(window.botVersionData)
        });
        var data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось сохранить версию');
        window.botVersionData = data.version;
        renderBotVersion();
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Версия сохранена.');
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

window.reloadVersionEditor = async function() {
    await loadBotVersion();
    var statusEl = document.getElementById('botVersionEditorStatus');
    if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Версия перезагружена из хранилища.');
};

function getStructuredTriggerEventOptions() {
    var events = [{ value: 'ALL', label: 'Все типы событий' }];
    Object.keys(STRUCTURED_TRIGGER_CATALOG).forEach(function(categoryKey) {
        var category = STRUCTURED_TRIGGER_CATALOG[categoryKey];
        Object.keys(category.sections).forEach(function(sectionKey) {
            var section = category.sections[sectionKey];
            Object.keys(section.events).forEach(function(eventCode) {
                events.push({ value: eventCode, label: category.label + ' / ' + section.label + ' / ' + section.events[eventCode].label });
            });
        });
    });
    return events;
}

window.renderStructuredTriggerFilters = function() {
    var selectEl = document.getElementById('structuredTriggerEventFilter');
    if (!selectEl) return;
    var options = getStructuredTriggerEventOptions();
    selectEl.innerHTML = options.map(function(option) {
        return '<option value="' + escapeHtml(option.value) + '"' + (option.value === window.structuredTriggerFilterState.eventCode ? ' selected' : '') + '>' + escapeHtml(option.label) + '</option>';
    }).join('');
    var searchEl = document.getElementById('structuredTriggerSearch');
    if (searchEl) searchEl.value = window.structuredTriggerFilterState.query || '';
};

window.handleStructuredTriggerFilterChange = function() {
    var searchEl = document.getElementById('structuredTriggerSearch');
    var eventEl = document.getElementById('structuredTriggerEventFilter');
    window.structuredTriggerFilterState = {
        query: String(searchEl?.value || '').trim().toLowerCase(),
        eventCode: String(eventEl?.value || 'ALL').trim() || 'ALL'
    };
    renderStructuredTriggerCards();
};

window.openBotVersionModal = function() {
    var overlay = document.getElementById('botVersionModalOverlay');
    if (overlay) overlay.style.display = 'flex';
};

window.closeBotVersionModal = function(evt) {
    if (evt && evt.target && evt.target.id && evt.target.id !== 'botVersionModalOverlay') return;
    var overlay = document.getElementById('botVersionModalOverlay');
    if (overlay) overlay.style.display = 'none';
};

window.renderCapabilitiesModal = function() {
    var subtitleEl = document.getElementById('capabilitiesModalSubtitle');
    var summaryEl = document.getElementById('capabilitiesSummary');
    var gridEl = document.getElementById('capabilitiesGrid');
    var data = window.botVersionData;
    if (!subtitleEl || !summaryEl || !gridEl) return;

    if (!data) {
        subtitleEl.textContent = 'Список возможностей загрузится после получения версии.';
        summaryEl.textContent = 'Данные ещё не загружены.';
        gridEl.innerHTML = '';
        return;
    }

    var capabilities = Array.isArray(data.capabilities) ? data.capabilities : [];
    subtitleEl.textContent = 'Возможности для ' + (data.displayVersion || 'текущей версии') + '.';
    summaryEl.innerHTML = '<div style="margin-bottom:6px;">' + renderVersionSegments(data.displayVersion || 'version unknown', 'version-segments--large') + '</div>' +
        '<div style="font-size:13px;line-height:1.6;color:var(--text-secondary);">Ниже перечислено, что умеет текущая версия.</div>';

    if (!capabilities.length) {
        gridEl.innerHTML = '<div class="community-empty-note">Список возможностей пока не заполнен.</div>';
        return;
    }

    gridEl.innerHTML = capabilities.map(function(group) {
        var items = Array.isArray(group.items) ? group.items : [];
        return '<div class="capabilities-card">' +
            '<div class="capabilities-card-title">' + escapeHtml(group.title || '') + '</div>' +
            '<div class="capabilities-card-list">' + items.map(function(item) {
                return '<div class="capabilities-card-item">• ' + escapeHtml(item) + '</div>';
            }).join('') + '</div>' +
        '</div>';
    }).join('');
};

window.openCapabilitiesModal = function() {
    renderCapabilitiesModal();
    var overlay = document.getElementById('capabilitiesModalOverlay');
    if (overlay) overlay.style.display = 'flex';
};

window.closeCapabilitiesModal = function(evt) {
    if (evt && evt.target && evt.target.id && evt.target.id !== 'capabilitiesModalOverlay') return;
    var overlay = document.getElementById('capabilitiesModalOverlay');
    if (overlay) overlay.style.display = 'none';
};

async function resolveActiveVkGroupIdForAdmin() {
    const baseUrl = window.location.href.split('?')[0];
    const settingsRes = await fetch(baseUrl + '?getBotSettings');
    const settingsData = await settingsRes.json();
    const communityConfig = settingsData.communities?.[window.currentCommunityId] || {};
    return communityConfig.vk_group_id || window.currentCommunityId || 'global';
}

window.renderAppLogs = function() {
    const filterRow = document.getElementById('appLogFilterRow');
    const listEl = document.getElementById('appLogsList');
    const toggleEl = document.getElementById('appLogsEnabledToggle');
    const fileLabelEl = document.getElementById('appLogsFileLabel');
    if (filterRow) {
        filterRow.innerHTML = APP_LOG_TAB_OPTIONS.map(function(item) {
            var btnClass = item.value === window.appLogCurrentFilter ? 'btn-save' : 'btn-neutral';
            return '<button class="btn ' + btnClass + ' app-log-filter-btn" type="button" onclick="setAppLogFilter(&quot;' + item.value + '&quot;)">' + escapeHtml(item.label) + '</button>';
        }).join('');
    }
    if (toggleEl) toggleEl.checked = !!window.appLogsEnabled;
    if (fileLabelEl) {
        fileLabelEl.textContent = window.appLogsFileName ? ('Файл логов: ' + window.appLogsFileName) : 'Файл логов ещё не создан.';
    }
    if (!listEl) return;

    var logs = Array.isArray(window.adminAppLogs) ? window.adminAppLogs : [];
    if (window.appLogCurrentFilter !== 'ALL') {
        logs = logs.filter(function(item) { return item.tab === window.appLogCurrentFilter; });
    }

    if (!logs.length) {
        listEl.innerHTML = '<div class="community-empty-note">Для выбранного фильтра пока нет событий.</div>';
        return;
    }

    listEl.innerHTML = logs.map(function(item) {
        var details = Array.isArray(item.details) ? item.details.filter(Boolean) : [];
        var tabLabel = (APP_LOG_TAB_OPTIONS.find(function(option) { return option.value === item.tab; }) || {}).label || item.tab || 'Система';
        return '<div class="app-log-card">' +
            '<div class="app-log-card-header">' +
                '<div>' +
                    '<div class="app-log-card-title">' + escapeHtml(item.title || 'Событие') + '</div>' +
                    '<div class="app-log-card-meta">' + escapeHtml(new Date(item.createdAt).toLocaleString('ru-RU')) + '</div>' +
                '</div>' +
                '<div class="app-log-card-badge">' + escapeHtml(tabLabel) + '</div>' +
            '</div>' +
            '<div class="app-log-card-summary">' + escapeHtml(item.summary || '') + '</div>' +
            '<div class="app-log-card-details">' + details.map(function(detail) {
                return '<div class="app-log-card-detail">' + escapeHtml(detail) + '</div>';
            }).join('') + '</div>' +
        '</div>';
    }).join('');
};

window.setAppLogFilter = function(filter) {
    window.appLogCurrentFilter = filter || 'ALL';
    renderAppLogs();
};

window.loadAppLogs = async function(showToast) {
    const statusEl = document.getElementById('appLogsStatus');
    try {
        if (showToast && statusEl) statusEl.innerHTML = makeInlineNotice('warn', 'Загружаю журнал...');
        const baseUrl = window.location.href.split('?')[0];
        const communityId = await resolveActiveVkGroupIdForAdmin();
        const res = await fetch(baseUrl + '?getAppLogs&communityId=' + encodeURIComponent(communityId) + '&limit=120');
        const data = await res.json();
        window.adminAppLogs = Array.isArray(data.logs) ? data.logs : [];
        window.appLogsEnabled = data.enabled !== false;
        window.appLogsFileName = String(data.fileName || '');
        renderAppLogs();
        if (showToast && statusEl) {
            statusEl.innerHTML = makeInlineNotice('success', '✅ Журнал обновлён.');
            setTimeout(function() {
                if (statusEl) statusEl.innerHTML = '';
            }, 5000);
        } else if (statusEl) {
            statusEl.innerHTML = '';
        }
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Не удалось загрузить журнал: ' + e.message);
    }
};

window.toggleAppLogsEnabled = async function(enabled) {
    const statusEl = document.getElementById('appLogsStatus');
    try {
        const baseUrl = window.location.href.split('?')[0];
        const communityId = await resolveActiveVkGroupIdForAdmin();
        const res = await fetch(baseUrl + '?saveAppLogsSettings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ communityId: communityId, enabled: !!enabled })
        });
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось изменить режим логирования');
        window.appLogsEnabled = !!data.enabled;
        renderAppLogs();
        if (statusEl) {
            statusEl.innerHTML = makeInlineNotice('success', window.appLogsEnabled ? '✅ Логирование включено.' : '✅ Логирование выключено.');
            setTimeout(function() { if (statusEl) statusEl.innerHTML = ''; }, 5000);
        }
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
        const toggleEl = document.getElementById('appLogsEnabledToggle');
        if (toggleEl) toggleEl.checked = !enabled;
    }
};

window.clearAppLogsFromAdmin = async function() {
    if (!confirm('Очистить журнал для текущего сообщества?')) return;
    const statusEl = document.getElementById('appLogsStatus');
    try {
        const baseUrl = window.location.href.split('?')[0];
        const communityId = await resolveActiveVkGroupIdForAdmin();
        const res = await fetch(baseUrl + '?clearAppLogs', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ communityId: communityId })
        });
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось очистить журнал');
        window.adminAppLogs = [];
        renderAppLogs();
        if (statusEl) {
            statusEl.innerHTML = makeInlineNotice('success', '✅ Логи очищены.');
            setTimeout(function() { if (statusEl) statusEl.innerHTML = ''; }, 5000);
        }
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

window.deleteAppLogsFileFromAdmin = async function() {
    if (!confirm('Удалить файл логов для текущего сообщества из хранилища?')) return;
    const statusEl = document.getElementById('appLogsStatus');
    try {
        const baseUrl = window.location.href.split('?')[0];
        const communityId = await resolveActiveVkGroupIdForAdmin();
        const res = await fetch(baseUrl + '?deleteAppLogsFile', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ communityId: communityId })
        });
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось удалить файл логов');
        window.adminAppLogs = [];
        window.appLogsFileName = String(data.fileName || '');
        renderAppLogs();
        if (statusEl) {
            statusEl.innerHTML = makeInlineNotice('success', '✅ Файл логов удалён из хранилища.');
            setTimeout(function() { if (statusEl) statusEl.innerHTML = ''; }, 5000);
        }
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ ' + e.message);
    }
};

window.loadAdminProfiles = async function() {
    if (!isMainAdminSession()) return;
    const statusEl = document.getElementById('adminProfilesStatus');
    const loadingEl = document.getElementById('loading-Admin');
    try {
        if (loadingEl) loadingEl.style.display = 'block';
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?getAdminDashboard');
        const data = await res.json();
        window.adminProfiles = Array.isArray(data.profiles) ? data.profiles : [];
        window.adminDashboard = {
            promoCodes: Array.isArray(data.promoCodes) ? data.promoCodes : [],
            recoveryRequests: Array.isArray(data.recoveryRequests) ? data.recoveryRequests : [],
            loginLogs: Array.isArray(data.loginLogs) ? data.loginLogs : [],
            limitRequests: Array.isArray(data.limitRequests) ? data.limitRequests : []
        };
        renderAdminProfiles();
        renderPromoCodes();
        renderRecoveryRequests();
        await loadAppLogs(false);
        if (statusEl) statusEl.innerHTML = '';
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Не удалось загрузить раздел администратора: ' + e.message);
    } finally {
        if (loadingEl) loadingEl.style.display = 'none';
    }
};

window.saveAdminProfile = async function() {
    const statusEl = document.getElementById('adminProfilesStatus');
    if (!isMainAdminSession()) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Только главный админ может создавать и редактировать профили.');
        return;
    }
    const payload = {
        id: (document.getElementById('profileFormId')?.value || '').trim(),
        name: (document.getElementById('profileFormName')?.value || '').trim(),
        username: (document.getElementById('profileFormUsername')?.value || '').trim(),
        password: (document.getElementById('profileFormPassword')?.value || '').trim(),
        recoveryEmail: (document.getElementById('profileFormEmail')?.value || '').trim(),
        durationMinutes: (document.getElementById('profileFormDuration')?.value || '').trim(),
        requestsLimit: (document.getElementById('profileFormRequestsLimit')?.value || '').trim(),
        principalProfileId: getPrincipalProfileId()
    };

    if (!payload.name || !payload.username || (!payload.password && !payload.id)) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Заполните название и логин профиля. Для нового профиля также обязателен пароль.');
        return;
    }

    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?saveAdminProfile', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось сохранить профиль');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Профиль сохранён.');
        closeAdminProfileForm();
        await loadAdminProfiles();
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Ошибка сохранения профиля: ' + e.message);
    }
};

window.deleteAdminProfileById = async function(profileId) {
    const statusEl = document.getElementById('adminProfilesStatus');
    if (!isMainAdminSession()) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Только главный админ может удалять профили.');
        return;
    }
    const profile = (window.adminProfiles || []).find(function(item) { return item.id === profileId; });
    if (!profileId) return;
    if (!confirm('Удалить профиль "' + (profile?.name || profileId) + '"?')) return;

    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?deleteAdminProfile', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ profileId: profileId, principalProfileId: getPrincipalProfileId() })
        });
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось удалить профиль');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Профиль удалён.');
        resetAdminProfileForm();
        await loadAdminProfiles();
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Ошибка удаления профиля: ' + e.message);
    }
};

window.savePromoCode = async function() {
    const statusEl = document.getElementById('promoCodesStatus');
    const payload = {
        id: (document.getElementById('promoFormId')?.value || '').trim(),
        code: (document.getElementById('promoFormCode')?.value || '').trim(),
        label: (document.getElementById('promoFormLabel')?.value || '').trim(),
        durationMinutes: (document.getElementById('promoFormDuration')?.value || '').trim(),
        dailyRequestsLimit: (document.getElementById('promoFormRequestsLimit')?.value || '').trim(),
        maxUses: (document.getElementById('promoFormMaxUses')?.value || '1').trim(),
        principalProfileId: getPrincipalProfileId()
    };
    if (!payload.code) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', 'Введите промокод.');
        return;
    }
    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?savePromoCode', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось сохранить промокод');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Промокод сохранён.');
        closePromoForm();
        await loadAdminProfiles();
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Ошибка сохранения промокода: ' + e.message);
    }
};

window.deletePromoCodeById = async function(id) {
    const statusEl = document.getElementById('promoCodesStatus');
    if (!id || !confirm('Удалить этот промокод?')) return;
    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?deletePromoCode', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id: id, principalProfileId: getPrincipalProfileId() })
        });
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось удалить промокод');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Промокод удалён.');
        await loadAdminProfiles();
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Ошибка удаления промокода: ' + e.message);
    }
};

window.resolveRecoveryRequestById = async function(requestId, profileId) {
    const statusEl = document.getElementById('recoveryRequestsStatus');
    var tempPassword = prompt('Введите новый временный пароль для профиля:');
    if (!tempPassword) return;
    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?resolveRecovery', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                requestId: requestId,
                profileId: profileId,
                tempPassword: tempPassword,
                principalProfileId: getPrincipalProfileId()
            })
        });
        const data = await res.json();
        if (!data.success) throw new Error(data.error || 'Не удалось обработать запрос');
        if (statusEl) statusEl.innerHTML = makeInlineNotice('success', '✅ Временный пароль выдан. Передай его пользователю: <strong>' + escapeHtml(tempPassword) + '</strong>');
        await loadAdminProfiles();
    } catch (e) {
        if (statusEl) statusEl.innerHTML = makeInlineNotice('error', '❌ Ошибка восстановления: ' + e.message);
    }
};

window.renderCommunityButtons = async function() {
    const container = document.getElementById('communityButtons');
    if (!container) return;
    
    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?getBotSettings');
        const data = await res.json();

        const communities = data.communities || {};
        const communityIds = Object.keys(communities);
        
        debug('🏘️ renderCommunityButtons: сообщества=' + communityIds.join(', '));
        debug('📊 active_community=' + data.active_community);
        debug('👤 currentCommunityId=' + window.currentCommunityId);

        // ?? ФИЛЬТРУЕМ: убираем 'default' полностью, если он есть
        const filteredIds = communityIds.filter(id => id !== 'default');

        container.innerHTML = '';

        // ?? Если сообществ нет — показываем подсказку
        if (filteredIds.length === 0) {
            container.innerHTML = '<span class="community-empty-note">Нет добавленных сообществ. Нажмите "+ Добавить Сообщество", чтобы начать.</span>';
            window.currentCommunityId = null;
            return;
        }

        // ?? Определяем активное сообщество
        // ✅ ПРИОРИТЕТ: window.currentCommunityId > active_community с сервера > первое сообщество
        let activeId;
        if (window.currentCommunityId && window.currentCommunityId !== 'default' && communities[window.currentCommunityId]) {
            activeId = window.currentCommunityId;  // ← Пользователь уже выбрал
            debug('🎯 activeId из window.currentCommunityId: ' + activeId);
        } else if (data.active_community && data.active_community !== 'default' && communities[data.active_community]) {
            activeId = data.active_community;  // ← Берём с сервера
            window.currentCommunityId = activeId;  // ← Синхронизируем
            debug('🎯 activeId из сервера: ' + activeId);
        } else {
            activeId = filteredIds[0];  // ← Первое сообщество
            window.currentCommunityId = activeId;
            debug('🎯 activeId первое сообщество: ' + activeId);
        }

        // ?? Рендерим кнопки
        for (const id of filteredIds) {
            const config = communities[id];
            const name = config?.group_name || 'Сообщество #' + id;
            const vkId = config?.vk_group_id || id;
            const isActive = id === activeId;
            
            debug('🔘 Кнопка: id=' + id + ', name=' + name + ', vk_group_id=' + (config?.vk_group_id || 'НЕТ') + ', isActive=' + isActive);

            const btn = document.createElement('button');
            btn.className = 'btn community-btn' + (isActive ? ' active' : '');
            // ?? ДОБАВЛЯЕМ data-атрибут для надёжного поиска
            btn.dataset.communityId = id;
            btn.innerHTML = '<span class="community-btn-title">' + escapeHtml(name) + '</span>' +
                '<span class="community-btn-meta">ID - ' + escapeHtml(String(vkId)) + '</span>' +
                '<span class="community-btn-state">' + (isActive ? 'Текущее сообщество' : 'Открыть сообщество') + '</span>';
            btn.onclick = function() { switchCommunity(id); };
            container.appendChild(btn);

        }

        // ✅ Update current and load settings
        // ВАЖНО: используем window.currentCommunityId если он уже установлен
        const targetCommunityId = (window.currentCommunityId && window.currentCommunityId !== 'default') 
            ? window.currentCommunityId 
            : activeId;
        
        if (!window.currentCommunityId || window.currentCommunityId === 'default') {
            window.currentCommunityId = targetCommunityId;
        }
        
        debug('🎯 renderCommunityButtons: загружаю для targetCommunityId=' + targetCommunityId);
        
        if (targetCommunityId && communities[targetCommunityId]) {
            const result = await loadCommunitySettings(targetCommunityId);
            // ✅ НЕ вызываем updateCommunityLabels здесь - это сделает loadData или switchCommunity
            debug('📋 renderCommunityButtons: настройки загружены для ' + targetCommunityId);
        }
    } catch (e) {
        console.error('Error rendering community buttons:', e);
        container.innerHTML = makeInlineText('error', 'Ошибка: ' + e.message);
    }
};






window.switchCommunity = async function(communityId) {
    // 1. Показываем оверлей с прогрессом
    showSwitchOverlay(communityId);

    try {
        window.currentCommunityId = communityId;
        debug('Switching to community: ' + communityId);

        // 2. Обновляем кнопки
        const buttons = document.querySelectorAll('#communityButtons .btn');
        buttons.forEach(btn => {
            btn.classList.remove('active');
            const stateEl = btn.querySelector('.community-btn-state');
            if (stateEl) stateEl.textContent = 'Открыть сообщество';
        });
        const activeBtn = Array.from(buttons).find(b => b.dataset.communityId === communityId);
        if (activeBtn) {
            activeBtn.classList.add('active');
            const stateEl = activeBtn.querySelector('.community-btn-state');
            if (stateEl) stateEl.textContent = 'Текущее сообщество';
        }

        // 3. Последовательно загружаем каждую вкладку с обновлением оверлея
        const allTabs = ['Messages', 'Comments', 'Users', 'Groups', 'Variables', 'Mailing', 'Delayed', 'Triggers'];
        const tabLabels = {
            Messages: '\uD83D\uDCAC \u0421\u041E\u041E\u0411\u0429\u0415\u041D\u0418\u042F',
            Comments: '\uD83D\uDCDD \u041A\u041E\u041C\u041C\u0415\u041D\u0422\u0410\u0420\u0418\u0418 \u0412 \u041F\u041E\u0421\u0422\u0410\u0425',
            Users: '\uD83D\uDC64 \u041f\u041e\u041b\u042C\u0417\u041e\u0412\u0410\u0422\u0415\u041b\u0418',
            Groups: '\uD83D\uDC65 \u0413\u0420\u0423\u041F\u041F\u042B',
            Variables: '\uD83E\uDEEE \u041f\u0415\u0420\u0415\u041c\u0415\u041d\u041d\u042b\u0415',
            Mailing: '\uD83D\uDCE8 \u0420\u0410\u0421\u0421\u042b\u041b\u041A\u0410',
            Delayed: '\u23F3 \u041e\u0422\u041b\u041e\u0416\u0415\u041d\u041d\u042b\u0415',
            Triggers: '\uD83C\uDFAF \u0422\u0420\u0418\u0413\u0413\u0415\u0420\u042B',
            Settings: '\u2699\uFE0F \u041d\u0410\u0421\u0422\u0420\u041e\u0419\u041a\u0410'
        };

        // Очищаем кэш
        for (const t of allTabs) { dataStore[t] = []; }

        // &#x1F916; Сбрасываем состояние ботов при переключении сообщества
        window.activeBot = {};
        // Очищаем кнопки ботов
        ['Messages', 'Comments'].forEach(tab => {
            setBotsForTab(tab, []);
            renderBotButtons(tab);
        });

        let successCount = 0;
        // Загрузка каждой вкладки последовательно
        for (const tabName of allTabs) {
            updateOverlayText('\u{1F4C2} \u0417\u0430\u0433\u0440\u0443\u0437\u043A\u0430... ' + (tabLabels[tabName] || tabName));
            try {
                await loadData(tabName);
                successCount++;
            } catch(e) {
                debug('\u274C Failed: ' + tabName + ': ' + e.message);
            }
        }

        // Загружаем настройки (возвращает communityName и vkGroupId)
        updateOverlayText('\u{1F4C2} \u0417\u0430\u0433\u0440\u0443\u0437\u043A\u0430... ' + (tabLabels.Settings || 'Settings'));
        let cName = communityId;
        let vkGroupId = communityId;
        try {
            const result = await loadCommunitySettings(communityId);
            cName = result.communityName;
            vkGroupId = result.vkGroupId;
            successCount++;
        } catch(e) { debug('\u274C Settings: ' + e.message); }

        debug('\u2705 Loaded tabs for community ' + communityId + ': ' + successCount);
        updateOverlayText('\u2705 \u0414\u0430\u043D\u043D\u044B\u0435 \u0441\u043E\u043E\u0431\u0449\u0435\u0441\u0442\u0432\u0430 \u0437\u0430\u0433\u0440\u0443\u0436\u0435\u043D\u044B');

        // 4. Сохраняем состояние
        saveAppState(communityId);

        // 6. НЕ вызываем renderCommunityButtons чтобы не сбрасывать выбор
        debug('✅ switchCommunity завершён для: ' + communityId);
    } finally {
        hideSwitchOverlay();
    }
};

// ✅ Полноэкранный оверлей при переключении сообщества
function showSwitchOverlay(communityId) {
    let overlay = document.getElementById('switchOverlay');
    if (!overlay) {
        overlay = document.createElement('div');
        overlay.id = 'switchOverlay';
        overlay.innerHTML = '<div class="overlay-content"><div class="spinner"></div><div class="overlay-text" id="overlayText"></div></div>';
        document.body.appendChild(overlay);
    }
    overlay.style.display = 'flex';
    document.getElementById('overlayText').textContent = '\uD83D\uDD04 \u0417\u0430\u0433\u0440\u0443\u0437\u043A\u0430 \u0434\u0430\u043D\u043D\u044B\u0445 \u0441\u043E\u043E\u0431\u0449\u0435\u0441\u0442\u0432\u0430...';
}

function hideSwitchOverlay() {
    const overlay = document.getElementById('switchOverlay');
    if (overlay) overlay.style.display = 'none';
}

function updateOverlayText(text) {
    const el = document.getElementById('overlayText');
    if (el) el.textContent = text;
}

// Сохранение состояния (сообщество + вкладка)
function saveAppState(communityId) {
    const activeTab = document.querySelector('.tablinks.active');
    let tabName = 'Messages';
    if (activeTab) {
        const match = activeTab.getAttribute('onclick')?.match(/'(\w+)'/);
        if (match) tabName = match[1];
    }
    try {
        localStorage.setItem('vkBotLastCommunity', communityId || '');
        localStorage.setItem('vkBotLastTab', tabName);
    } catch(e) {}
}

// Восстановление состояния
function restoreAppState() {
    try {
        const communityId = localStorage.getItem('vkBotLastCommunity');
        const tabName = localStorage.getItem('vkBotLastTab') || 'Messages';
        if (communityId) {
            window.currentCommunityId = communityId;
        }
        return { communityId, tabName };
    } catch(e) {
        return { communityId: null, tabName: 'Messages' };
    }
}

function highlightQuickStartTarget(target) {
    if (!target) return;
    target.scrollIntoView({ behavior: 'smooth', block: 'center' });
    var previousShadow = target.style.boxShadow;
    var previousTransition = target.style.transition;
    target.style.transition = 'box-shadow 0.2s ease, background-color 0.2s ease';
    target.style.boxShadow = '0 0 0 4px rgba(76, 175, 80, 0.55)';
    target.style.backgroundColor = 'rgba(76, 175, 80, 0.08)';
    setTimeout(function() {
        target.style.boxShadow = previousShadow;
        target.style.transition = previousTransition;
        target.style.backgroundColor = '';
    }, 2000);
}

window.runQuickStartStep = function(stepKey) {
    var targets = {
        community: document.getElementById('addCommunityBtn') || document.getElementById('communitySwitcher'),
        vkGroupId: document.getElementById('vkGroupId'),
        vkTokens: document.getElementById('vkTokens'),
        userToken: document.getElementById('userToken') || document.getElementById('getUserTokenBtn'),
        callback: document.getElementById('autoSetupCallbackBtn')
    };
    highlightQuickStartTarget(targets[stepKey]);
};






// Обновление меток активного сообщества (синхронно — имя уже в DOM)
window.updateCommunityLabels = function(communityId, communityName, vkGroupId) {
    communityName = communityName || communityId || '';
    // ✅ ПОКАЗЫВАЕМ VK_GROUP_ID вместо внутреннего ID
    const displayId = vkGroupId || communityId;
    const tabs = ['Messages', 'Comments', 'Users', 'Variables', 'Mailing', 'Delayed', 'Triggers'];
    for (const tab of tabs) {
        const labelEl = document.getElementById('activeCommunityLabel-' + tab);
        if (labelEl) {
            labelEl.style.display = 'block';
            labelEl.innerHTML = '<div class="community-btn-title" style="font-size:18px;">' + escapeHtml(communityName) + '</div>' +
                '<div class="community-btn-meta" style="font-size:13px;margin-top:4px;">ID - ' + escapeHtml(String(displayId)) + '</div>';
            debug('\uD83C\uDFF7\uFE0F updateCommunityLabels: ' + tab + ' => ' + communityName + ' / ID ' + displayId);
        }
    }
};

// ===== ПАНЕЛЬ ФИЛЬТРОВ И ДЕЙСТВИЙ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ =====
window.userFilters = {
    groups: [],      // выбранные группы
    search: '',      // поиск по имени/ID
    bot: '',         // фильтр по боту
    variable: ''     // фильтр по пользовательской переменной
};

window.userFiltersCollapsed = false;
window.userActionsCollapsed = true;

// Собирает все уникальные группы из данных пользователей
function collectAllGroups() {
    const users = dataStore['Users'] || [];
    const groups = new Set();
    users.forEach(function(u) {
        const rawGroup = (u['ГРУППА'] || '').trim();
        if (rawGroup) {
            // Поддержка запятых, точек с запятой и новых строк (обратная совместимость)
            rawGroup.split(/[\\r\\n,;]+/).forEach(function(g) {
                g = g.trim();
                if (g) groups.add(g);
            });
        }
    });
    return Array.from(groups).sort();
}

// Рендерит панель фильтров под community label на вкладке Users
function renderUserFilters() {
    const container = document.getElementById('userFiltersPanel');
    if (!container) {
        // Ищем контейнер Users и добавляем панель
        const usersTab = document.getElementById('Users');
        if (!usersTab) return;

        const panel = document.createElement('div');
        panel.id = 'userFiltersPanel';
        panel.style.cssText = 'margin:10px 0; padding:12px;';
        const anchor = document.getElementById('activeCommunityLabel-Users');
        if (anchor && anchor.parentNode === usersTab) {
            usersTab.insertBefore(panel, anchor.nextSibling);
        } else {
            usersTab.insertBefore(panel, usersTab.firstChild);
        }
    }

    const panel = document.getElementById('userFiltersPanel');
    const groups = collectAllGroups();
    const f = window.userFilters;
    const actionType = (window.userActionType || '');

    let html = '';

    // ===== ФИЛЬТРЫ =====
    html += '<div style="margin-bottom:6px;">';
    html += '<span style="cursor:pointer; font-size:13px; font-weight:bold; color:var(--text-primary);" onclick="toggleUserFiltersSection()">';
    html += window.userFiltersCollapsed ? '▶' : '▼';
    html += ' 🔍 Фильтры пользователей</span>';
    html += '<span style="float:right; font-size:11px; color:var(--text-secondary);">Показано: <strong>' + getFilteredUsers().length + '</strong> из <strong>' + (dataStore['Users'] || []).length + '</strong></span>';
    html += '</div>';

    if (!window.userFiltersCollapsed) {
        // Кнопки групп
        html += '<div style="margin-bottom:8px; padding-left:16px;">';
        html += '<span class="user-muted-text" style="margin-right:6px;">Группы:</span>';
        if (groups.length === 0) {
            html += '<span class="user-muted-text" style="font-size:11px;">нет групп</span>';
        } else {
            groups.forEach(function(g) {
                const isActive = f.groups.includes(g);
                html += '<button class="user-group-btn user-group-chip' + (isActive ? ' active' : '') + '" data-group="' + escapeHtml(g) + '">' + escapeHtml(g) + '</button>';
            });
            if (f.groups.length > 0) {
                html += '<button class="btn btn-delete" onclick="clearUserGroupFilters()" style="padding:5px 10px; font-size:11px; min-width:auto;">✕ Сбросить</button>';
            }
        }
        html += '</div>';

        // Поля фильтров
        html += '<div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:8px; padding-left:16px;">';
        html += '<input class="user-filter-input" id="userFilterSearch" type="text" placeholder="🔍 Поиск по Имени или ID..." value="' + escapeHtml(f.search) + '" oninput="applyUserFilters()">';
        html += '<input class="user-filter-input" id="userFilterBot" type="text" placeholder="🤖 Фильтр по Боту..." value="' + escapeHtml(f.bot) + '" oninput="applyUserFilters()">';
        html += '<input class="user-filter-input" id="userFilterVar" type="text" placeholder="📊 Пользовательская переменная..." value="' + escapeHtml(f.variable) + '" oninput="applyUserFilters()">';
        if (f.search || f.bot || f.variable || f.groups.length > 0) {
            html += '<button class="btn btn-neutral" onclick="clearAllUserFilters()" style="padding:8px 12px; font-size:12px;">✕ Сбросить все</button>';
        }
        html += '</div>';
    }

    // ===== ДЕЙСТВИЯ =====
    html += '<div class="user-panel-divider">';
    html += '<span style="cursor:pointer; font-size:13px; font-weight:bold; color:var(--bot-switcher-text);" onclick="toggleUserActionsSection()">';
    html += window.userActionsCollapsed ? '▶' : '▼';
    html += ' ⚡ Действия с пользователями</span>';
    html += '</div>';

    if (!window.userActionsCollapsed) {
        html += '<div style="padding:10px 0 0 16px;">';

        // Выбор действия
        html += '<div style="margin-bottom:8px;">';
        html += '<label class="user-muted-text">Выберите действие:</label><br>';
        html += '<select id="userActionType" onchange="onUserActionTypeChange()" style="min-width:220px; max-width:260px;">';
        html += '<option value="">-- Выберите --</option>';
        html += '<option value="bot_step"' + (actionType === 'bot_step' ? ' selected' : '') + '>🤔 Перевести на шаг бота</option>';
        html += '<option value="add_group"' + (actionType === 'add_group' ? ' selected' : '') + '>➕ Добавить в группу</option>';
        html += '<option value="remove_group"' + (actionType === 'remove_group' ? ' selected' : '') + '>➖ Удалить из группы</option>';
        html += '<option value="add_var"' + (actionType === 'add_var' ? ' selected' : '') + '>📊 Добавить пользовательскую переменную</option>';
        html += '<option value="remove_var"' + (actionType === 'remove_var' ? ' selected' : '') + '>🗑️ Удалить пользовательскую переменную</option>';
        html += '</select>';
        html += '</div>';

        // Поля для "Перевести на шаг бота"
        if (actionType === 'bot_step') {
            html += '<div style="margin-bottom:8px;">';
            html += '<label class="user-muted-text">Выберите бота:</label><br>';
            html += '<select id="userActionBot" style="min-width:200px; max-width:240px;"><option value="">-- Выберите бота --</option></select>';
            html += '</div>';
            html += '<div style="margin-bottom:8px;">';
            html += '<label class="user-muted-text">Выберите шаг:</label><br>';
            html += '<select id="userActionStep" style="min-width:200px; max-width:240px;"><option value="">-- Выберите шаг --</option></select>';
            html += '</div>';
        }

        // Поля для "Добавить в группу"
        if (actionType === 'add_group') {
            html += '<div style="margin-bottom:8px;">';
            html += '<label class="user-muted-text">Имя группы:</label><br>';
            html += '<input id="userActionGroupName" type="text" placeholder="Название группы..." style="min-width:200px; max-width:240px;">';
            html += '</div>';
        }

        // Поля для "Удалить из группы"
        if (actionType === 'remove_group') {
            html += '<div style="margin-bottom:8px;">';
            html += '<label class="user-muted-text">Имя группы:</label><br>';
            html += '<input id="userActionGroupName" type="text" placeholder="Название группы..." style="min-width:200px; max-width:240px;">';
            html += '</div>';
        }

        // Поля для "Добавить переменную"
        if (actionType === 'add_var') {
            html += '<div style="margin-bottom:8px;">';
            html += '<label class="user-muted-text">Имя переменной:</label><br>';
            html += '<input id="userActionVarName" type="text" placeholder="Имя переменной..." style="min-width:200px; max-width:240px;">';
            html += '</div>';
            html += '<div style="margin-bottom:8px;">';
            html += '<label class="user-muted-text">Значение:</label><br>';
            html += '<input id="userActionVarValue" type="text" placeholder="Значение..." style="min-width:200px; max-width:240px;">';
            html += '</div>';
        }

        // Поля для "Удалить переменную"
        if (actionType === 'remove_var') {
            html += '<div style="margin-bottom:8px;">';
            html += '<label class="user-muted-text">Имя переменной:</label><br>';
            html += '<input id="userActionVarName" type="text" placeholder="Имя переменной..." style="min-width:200px; max-width:240px;">';
            html += '</div>';
        }

        // Кнопка выполнить
        if (actionType) {
            html += '<button class="btn btn-accent" onclick="executeUserAction()" style="padding:8px 14px; font-size:12px;">▶ Выполнить</button>';
            html += ' <span id="userActionResult" class="user-muted-text" style="font-size:11px;"></span>';
        }

        html += '</div>';
    }

    panel.innerHTML = html;

    // Делегирование событий для кнопок групп
    if (!window.userFiltersCollapsed) {
        panel.querySelectorAll('.user-group-btn').forEach(function(btn) {
            btn.onclick = function() {
                const group = btn.getAttribute('data-group');
                toggleUserGroup(group);
            };
        });
    }

    // Загружаем список ботов если нужно
    if (actionType === 'bot_step') {
        updateUserActionBotSelect();
    }
}

// Переключить секцию фильтров
window.toggleUserFiltersSection = function() {
    window.userFiltersCollapsed = !window.userFiltersCollapsed;
    renderUserFilters();
};

// Переключить секцию действий
window.toggleUserActionsSection = function() {
    window.userActionsCollapsed = !window.userActionsCollapsed;
    renderUserFilters();
};

// Смена типа действия
window.onUserActionTypeChange = function() {
    window.userActionType = document.getElementById('userActionType')?.value || '';
    renderUserFilters();
    if (window.userActionType === 'bot_step') {
        updateUserActionBotSelect();
    }
};

// Переключить группу
window.toggleUserGroup = function(group) {
    const f = window.userFilters;
    const idx = f.groups.indexOf(group);
    if (idx >= 0) {
        f.groups.splice(idx, 1);
    } else {
        f.groups.push(group);
    }
    renderUserFilters();
    applyUserFilters();
};

// Сбросить фильтр групп
window.clearUserGroupFilters = function() {
    window.userFilters.groups = [];
    renderUserFilters();
    applyUserFilters();
};

// Сбросить все фильтры
window.clearAllUserFilters = function() {
    window.userFilters = { groups: [], search: '', bot: '', variable: '' };
    renderUserFilters();
    applyUserFilters();
};

// Применить фильтры
window.applyUserFilters = function() {
    const f = window.userFilters;
    f.search = (document.getElementById('userFilterSearch')?.value || '').trim();
    f.bot = (document.getElementById('userFilterBot')?.value || '').trim();
    f.variable = (document.getElementById('userFilterVar')?.value || '').trim();

    const filtered = getFilteredUsers();
    renderTable('Users', filtered);
};

// Получить отфильтрованных пользователей
function getFilteredUsers() {
    const users = dataStore['Users'] || [];
    const f = window.userFilters;

    if (!f.groups.length && !f.search && !f.bot && !f.variable) {
        return users;
    }

    return users.filter(function(u) {
        // Фильтр по группам (ИЛИ)
        if (f.groups.length > 0) {
            const rawGroup = (u['ГРУППА'] || '').trim();
            const userGroups = rawGroup ? rawGroup.split(/[\\r\\n,;]+/).map(function(s) { return s.trim(); }).filter(function(s) { return s; }) : [];
            const hasAnyGroup = f.groups.some(function(g) { return userGroups.includes(g); });
            if (!hasAnyGroup) return false;
        }
        // Поиск по Имени или ID
        if (f.search) {
            const q = f.search.toLowerCase();
            const name = (u['ИМЯ'] || '').toLowerCase();
            const id = (u['ID'] || '').toLowerCase();
            if (!name.includes(q) && !id.includes(q)) return false;
        }
        // Фильтр по Боту
        if (f.bot) {
            const q = f.bot.toLowerCase();
            const bot = (u['Текущий Бот'] || '').toLowerCase();
            if (!bot.includes(q)) return false;
        }
        // Фильтр по Пользовательской переменной
        if (f.variable) {
            const q = f.variable.toLowerCase();
            const pv = (u['Пользовательская'] || '').toLowerCase();
            const pvVal = (u['Значения ПП'] || '').toLowerCase();
            if (!pv.includes(q) && !pvVal.includes(q)) return false;
        }
        return true;
    });
}

// Обновить select ботов в действиях
function updateUserActionBotSelect() {
    const select = document.getElementById('userActionBot');
    if (!select) return;

    const users = dataStore['Users'] || [];
    const bots = new Set();
    users.forEach(function(u) {
        const bot = u['Текущий Бот'];
        if (bot && bot.trim()) bots.add(bot.trim());
    });

    // Также добавим ботов из Messages/Comments
    ['Messages', 'Comments'].forEach(function(tab) {
        const botsList = getBotsForTab(tab);
        botsList.forEach(function(b) { bots.add(b); });
    });

    const currentVal = select.value;
    select.innerHTML = '<option value="">-- Выберите бота --</option>';
    Array.from(bots).sort().forEach(function(b) {
        select.innerHTML += '<option value="' + escapeHtml(b) + '">' + escapeHtml(b) + '</option>';
    });
    select.value = currentVal;

    // Обработчик смены бота — загрузить шаги
    select.onchange = function() {
        updateUserActionStepSelect(select.value);
    };
}

// Обновить select шагов
function updateUserActionStepSelect(botName) {
    const select = document.getElementById('userActionStep');
    if (!select) return;

    select.innerHTML = '<option value="">-- Выберите шаг --</option>';
    if (!botName) return;

    // Ищем шаги этого бота в Messages
    const messages = dataStore['Messages'] || [];
    const steps = new Set();
    messages.forEach(function(m) {
        if (m['Бот'] === botName && m['Шаг']) {
            steps.add(m['Шаг']);
        }
    });

    Array.from(steps).sort().forEach(function(s) {
        select.innerHTML += '<option value="' + escapeHtml(s) + '">' + escapeHtml(s) + '</option>';
    });
}

// Выполнить действие с отфильтрованными пользователями
window.executeUserAction = async function() {
    const actionType = window.userActionType || '';
    const resultEl = document.getElementById('userActionResult');
    const filteredUsers = getFilteredUsers();

    if (filteredUsers.length === 0) {
        alert('Нет пользователей для действия (проверьте фильтры)!');
        return;
    }

    let confirmMsg = '';
    let actionDesc = '';

    // Валидация и описание для каждого типа
    if (actionType === 'bot_step') {
        const botName = document.getElementById('userActionBot')?.value;
        const stepName = document.getElementById('userActionStep')?.value;
        if (!botName) { alert('Выберите бота!'); return; }
        if (!stepName) { alert('Выберите шаг!'); return; }
        confirmMsg = 'Перевести ' + filteredUsers.length + ' пользователей на бот "' + botName + '", шаг "' + stepName + '"?';
        actionDesc = 'Перевод на шаг: ' + botName + '/' + stepName;
    } else if (actionType === 'add_group') {
        const groupName = (document.getElementById('userActionGroupName')?.value || '').trim();
        if (!groupName) { alert('Введите имя группы!'); return; }
        confirmMsg = 'Добавить ' + filteredUsers.length + ' пользователей в группу "' + groupName + '"?';
        actionDesc = 'Добавить группу: ' + groupName;
    } else if (actionType === 'remove_group') {
        const groupName = (document.getElementById('userActionGroupName')?.value || '').trim();
        if (!groupName) { alert('Введите имя группы!'); return; }
        confirmMsg = 'Удалить ' + filteredUsers.length + ' пользователей из группы "' + groupName + '"?';
        actionDesc = 'Удалить группу: ' + groupName;
    } else if (actionType === 'add_var') {
        const varName = (document.getElementById('userActionVarName')?.value || '').trim();
        const varValue = (document.getElementById('userActionVarValue')?.value || '').trim();
        if (!varName) { alert('Введите имя переменной!'); return; }
        confirmMsg = 'Добавить переменную "' + varName + '"="' + varValue + '" для ' + filteredUsers.length + ' пользователей?';
        actionDesc = 'Добавить переменную: ' + varName + '=' + varValue;
    } else if (actionType === 'remove_var') {
        const varName = (document.getElementById('userActionVarName')?.value || '').trim();
        if (!varName) { alert('Введите имя переменной!'); return; }
        confirmMsg = 'Удалить переменную "' + varName + '" у ' + filteredUsers.length + ' пользователей?';
        actionDesc = 'Удалить переменную: ' + varName;
    } else {
        alert('Выберите действие!');
        return;
    }

    if (!confirm(confirmMsg)) return;

    resultEl.textContent = '⏳ ' + actionDesc + '...';
    resultEl.style.color = '#2196F3';

    let successCount = 0;
    let errorCount = 0;

    try {
        const baseUrl = window.location.href.split('?')[0];
        const vkGroupId = (function() {
            var el = document.getElementById('vkGroupId');
            return el ? el.value.trim() : null;
        })();

        // ===== ТИП 1: Перевод на шаг бота =====
        if (actionType === 'bot_step') {
            const botName = document.getElementById('userActionBot').value;
            const stepName = document.getElementById('userActionStep').value;

            // Находим шаг
            const messages = dataStore['Messages'] || [];
            const targetStep = messages.find(function(m) {
                return m['Бот'] === botName && m['Шаг'] === stepName;
            });
            if (!targetStep) {
                alert('Шаг "' + stepName + '" не найден в боте "' + botName + '"!');
                resultEl.textContent = '❌ Шаг не найден';
                resultEl.style.color = 'red';
                return;
            }

            const text = targetStep['Ответ'] || ('Шаг: ' + stepName);
            const attachments = targetStep['Вложения к ответу'] || targetStep['Вложения'] || '';
            const keyboard = targetStep._keyboard ? (typeof targetStep._keyboard === 'string' ? targetStep._keyboard : JSON.stringify(targetStep._keyboard)) : null;
            const stepActionsData = {
                bot: botName, step: stepName,
                delay: targetStep['Задержка отправки на Шаг'] || '',
                addGroup: targetStep['ДОБАВИТЬ ГРУППУ'] || '',
                removeGroup: targetStep['УДАЛИТЬ ГРУППУ'] || '',
                sendToStep: targetStep['Отправить на Шаг'] || '',
                ppActions: targetStep['Действия с ПП'] || '',
                gpActions: targetStep['Действия с ГП'] || '',
                pvsActions: targetStep['Действия с ПВС'] || '',
                variableActions: targetStep['Действия с ПП/ГП/ПВК'] || ''
            };

            for (const user of filteredUsers) {
                const userId = user['ID'];
                if (!userId) { errorCount++; continue; }
                try {
                    const res = await fetch(baseUrl + '?testSend', {
                        method: 'POST', headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            userId: userId, text: text, attachments: attachments, keyboard: keyboard,
                            communityId: window.currentCommunityId, vkGroupId: vkGroupId,
                            stepActions: stepActionsData
                        })
                    });
                    const result = await res.json();
                    if (result.success) successCount++; else errorCount++;
                } catch(e) { errorCount++; }
            }
        }

        // ===== ТИП 2-5: Изменение данных пользователей =====
        else {
            const users = dataStore['Users'] || [];
            for (const user of filteredUsers) {
                const userId = user['ID'];
                if (!userId) { errorCount++; continue; }
                try {
                    const idx = users.findIndex(function(u) { return u['ID'] === userId; });
                    if (idx < 0) { errorCount++; continue; }

                    if (actionType === 'add_group') {
                        const groupName = (document.getElementById('userActionGroupName')?.value || '').trim();
                        // Парсим группы: поддержка и запятых, и новых строк (обратная совместимость)
                        const rawGroup = (users[idx]['ГРУППА'] || '').trim();
                        const currentGroups = rawGroup ? rawGroup.split(/[\\r\\n,;]+/).map(function(s) { return s.trim(); }).filter(function(s) { return s; }) : [];
                        if (!currentGroups.includes(groupName)) {
                            currentGroups.push(groupName);
                        }
                        // Запись через новую строку
                        users[idx]['ГРУППА'] = currentGroups.join(String.fromCharCode(10));
                    } else if (actionType === 'remove_group') {
                        const groupName = (document.getElementById('userActionGroupName')?.value || '').trim();
                        const rawGroup = (users[idx]['ГРУППА'] || '').trim();
                        const currentGroups = rawGroup ? rawGroup.split(/[\\r\\n,;]+/).map(function(s) { return s.trim(); }).filter(function(s) { return s && s !== groupName; }) : [];
                        // Запись через новую строку
                        users[idx]['ГРУППА'] = currentGroups.join(String.fromCharCode(10));
                    } else if (actionType === 'add_var') {
                        const varName = (document.getElementById('userActionVarName')?.value || '').trim();
                        const varValue = (document.getElementById('userActionVarValue')?.value || '').trim();

                        // Обновляем Пользовательская (список имён)
                        const currentPV = (users[idx]['Пользовательская'] || '').split(/[,;]+/).map(function(s) { return s.trim(); }).filter(function(s) { return s; });
                        if (!currentPV.includes(varName)) {
                            currentPV.push(varName);
                        }
                        users[idx]['Пользовательская'] = currentPV.join(', ');

                        // Значения ПП — ТОЛЬКО значения, привязка по позиции в currentPV
                        const currentVals = (users[idx]['Значения ПП'] || '').split(/[,;]+/).map(function(v) { return v.trim(); }).filter(function(v) { return v; });

                        // Синхронизируем длину: если currentPV длиннее — дополняем currentVals
                        while (currentVals.length < currentPV.length) {
                            currentVals.push('');
                        }

                        // Находим позицию переменной и обновляем/добавляем значение
                        const pvIdx = currentPV.indexOf(varName);
                        if (pvIdx >= 0 && pvIdx < currentVals.length) {
                            currentVals[pvIdx] = varValue;
                        }

                        users[idx]['Значения ПП'] = currentVals.join(', ');
                        console.log('[Admin] 📊 add_var DONE: PV=' + users[idx]['Пользовательская'] + ', vals=' + users[idx]['Значения ПП']);
                    } else if (actionType === 'remove_var') {
                        const varName = (document.getElementById('userActionVarName')?.value || '').trim();

                        // Удаляем из списка имён
                        const currentPV = (users[idx]['Пользовательская'] || '').split(/[,;]+/).map(function(s) { return s.trim(); }).filter(function(s) { return s; });
                        const pvIdx = currentPV.indexOf(varName);
                        if (pvIdx >= 0) {
                            currentPV.splice(pvIdx, 1);
                        }
                        users[idx]['Пользовательская'] = currentPV.join(', ');

                        // Удаляем значение по той же позиции
                        const currentVals = (users[idx]['Значения ПП'] || '').split(/[,;]+/).map(function(v) { return v.trim(); }).filter(function(v) { return v; });
                        // Синхронизируем длину
                        while (currentVals.length < currentPV.length + 1) {
                            currentVals.push('');
                        }
                        if (pvIdx >= 0 && pvIdx < currentVals.length) {
                            currentVals.splice(pvIdx, 1);
                        }
                        users[idx]['Значения ПП'] = currentVals.join(', ');
                        console.log('[Admin] 🗑️ remove_var DONE: PV=' + users[idx]['Пользовательская'] + ', vals=' + users[idx]['Значения ПП']);
                    }

                    successCount++;
                } catch(e) { errorCount++; }
            }

            // Сохраняем изменённых пользователей
            if (successCount > 0) {
                dataStore['Users'] = users;
                const saved = await saveDataDirectly('Users');
                if (saved) {
                    // Рендерим ВСЕ данные (не только отфильтрованных) чтобы таблица была в_sync
                    renderTable('Users', users);
                    renderUserFilters(); // Обновить группы
                    debug('✅ Пользователи обновлены: ' + successCount + ' из ' + filteredUsers.length);
                }
            }
        }

        resultEl.textContent = '✅ ' + actionDesc + ' — Готово! Успешно: ' + successCount + ', Ошибок: ' + errorCount;
        resultEl.style.color = 'green';

    } catch(e) {
        resultEl.textContent = '❌ Ошибка: ' + e.message;
        resultEl.style.color = 'red';
    }
};


window.loadCommunitySettings = async function(communityId) {
    const settingsDebug = document.getElementById('settings-debug');
    try {
        const baseUrl = window.location.href.split('?')[0];
        debug('🔍 loadCommunitySettings: загружаю настройки для ' + communityId);
        const res = await fetch(baseUrl + '?getBotSettings');
        const data = await res.json();
        debug('📋 Получены настройки: ' + JSON.stringify({
            active_community: data.active_community,
            communities: Object.keys(data.communities || {})
        }));
        const config = data.communities?.[communityId] || {};
        debug('⚙️ Конфиг для ' + communityId + ': ' + JSON.stringify({
            group_name: config.group_name,
            vk_group_id: config.vk_group_id,
            has_vk_tokens: !!(config.vk_tokens && config.vk_tokens.length)
        }));
        const communityName = config.group_name || communityId;
        const vkGroupId = config.vk_group_id || communityId;

        document.getElementById('communityName').value = communityName;
        document.getElementById('confirmationCode').value = config.confirmation_token || '';
        document.getElementById('secretKey').value = config.secret_key || '';
        document.getElementById('vkGroupId').value = config.vk_group_id || '';
        document.getElementById('userToken').value = config.user_token || '';

        const vkTokensTextarea = document.getElementById('vkTokens');
        if (config.vk_tokens && Array.isArray(config.vk_tokens)) {
            vkTokensTextarea.value = config.vk_tokens.join('\\n');
        } else if (config.vk_token) {
            vkTokensTextarea.value = config.vk_token;
        } else {
            vkTokensTextarea.value = '';
        }

        settingsDebug.innerHTML = '<pre>Активно: ' + communityId + '\\n' +
            JSON.stringify(config, null, 2).replace(/vk1\.[^"]+/g, '***') + '</pre>';

        // ✅ Возвращаем объект с именем и VK_GROUP_ID
        debug('✅ loadCommunitySettings возвращает: ' + JSON.stringify({ communityName, vkGroupId }));
        return { communityName, vkGroupId };
    } catch (e) {
        settingsDebug.innerHTML = '❌ Ошибка: ' + e.message;
        debug('❌ loadCommunitySettings ошибка: ' + e.message);
        return { communityName: communityId, vkGroupId: communityId };
    }
};

window.refreshTabContent = async function(tabName) {
    var loadingEl = document.getElementById('loading-' + tabName);
    try {
        if (loadingEl) loadingEl.style.display = 'block';
        if (tabName === 'Admin') {
            await loadAdminProfiles();
            return;
        }
        if (tabName === 'Settings') {
            await loadCommunitySettings(window.currentCommunityId);
            return;
        }
        await loadData(tabName);
    } catch (e) {
        console.error('[Admin] ❌ refreshTabContent error:', e.message);
    } finally {
        if (loadingEl) loadingEl.style.display = 'none';
    }
};

window.injectTabRefreshButtons = function() {
    var tabs = ['Messages', 'Comments', 'Users', 'Variables', 'Mailing', 'Delayed', 'Triggers', 'Settings', 'Admin'];
    tabs.forEach(function(tabName) {
        var tabEl = document.getElementById(tabName);
        if (!tabEl) return;
        if (tabEl.querySelector('[data-refresh-tab="' + tabName + '"]')) return;
        var titleEl = tabEl.querySelector('.tab-panel-title');
        if (!titleEl) return;
        var rowEl = titleEl.parentElement.querySelector('.tab-panel-title-row');
        if (!rowEl) {
            rowEl = document.createElement('div');
            rowEl.className = 'tab-panel-title-row';
            titleEl.parentElement.insertBefore(rowEl, titleEl);
            rowEl.appendChild(titleEl);
        }
        var button = document.createElement('button');
        button.type = 'button';
        button.className = 'btn tab-refresh-btn';
        button.setAttribute('data-refresh-tab', tabName);
        button.textContent = '↻ Обновить';
        button.onclick = function() { refreshTabContent(tabName); };
        rowEl.appendChild(button);
    });
};

window.addNewCommunity = function() {
    // Очищаем форму
    document.getElementById('communityName').value = '';
    document.getElementById('confirmationCode').value = '';
    document.getElementById('secretKey').value = '';
    document.getElementById('vkGroupId').value = '';
    document.getElementById('userToken').value = '';
    document.getElementById('vkTokens').value = '';
    document.getElementById('tokensStatus').innerHTML = '';

    // ?? Генерируем уникальный ID на основе времени + рандома
    window.currentCommunityId = 'comm_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    debug('Created new community form: ' + window.currentCommunityId);

    // ?? Сразу создаём кнопку "NO NAME"
    const container = document.getElementById('communityButtons');
    if (container) {
        const existingNoName = container.querySelector('.btn-temp');
        if (existingNoName) existingNoName.remove();

        const tempBtn = document.createElement('button');
        tempBtn.className = 'btn btn-temp community-btn community-btn--temp active';
        tempBtn.dataset.communityId = window.currentCommunityId;
        tempBtn.innerHTML = '<span class="community-btn-title">Новое сообщество</span>' +
            '<span class="community-btn-meta">ID - будет после сохранения</span>' +
            '<span class="community-btn-state">Черновик</span>';
        tempBtn.onclick = function() {
            window.currentCommunityId = window.currentCommunityId;
            loadCommunitySettings(window.currentCommunityId);
        };
        container.appendChild(tempBtn);
    }

    // ?? Показываем подсказку что это новое сообщество
    const debugEl = document.getElementById('settings-debug');
    if (debugEl) {
        debugEl.innerHTML = makeInlineNotice('warn', '✨ Новое сообщество: <strong>' + window.currentCommunityId + '</strong><br>Заполните поля и нажмите "Сохранить настройки сообщества"');
    }
};

window.saveCommunitySettings = async function() {
    showSaveOverlay();
    
    const getValue = function(id) {
        const el = document.getElementById(id);
        return el ? el.value.trim() : '';
    };
    
    const vkTokensText = getValue('vkTokens');
    const vkTokensArray = vkTokensText.split('\\n').map(t => t.trim()).filter(t => t);
    
    const settings = {
        community_id: window.currentCommunityId,
        group_name: getValue('communityName'),
        vk_tokens: vkTokensArray,
        vk_token: vkTokensArray[0] || '',
        confirmation_token: getValue('confirmationCode'),
        secret_key: getValue('secretKey'),
        vk_group_id: getValue('vkGroupId') ? parseInt(getValue('vkGroupId'), 10) : null,
        user_token: getValue('userToken')
    };
    
    const statusDiv = document.getElementById('save-status');
    statusDiv.innerHTML = makeInlineNotice('warn', '&#x1F4BE; Сохранение...');
    
    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?saveBotSettings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(settings)
        });
        const data = await res.json();
        
        if (data.success) {
            statusDiv.innerHTML = makeInlineNotice('success', '✅ Настройки сохранены! Обновляем список...');
            await renderCommunityButtons();
            setTimeout(function() { statusDiv.innerHTML = ''; }, 3000);
        } else {
            statusDiv.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + (data.error || 'неизвестная'));
        }
    } catch (e) {
        statusDiv.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + e.message);
    }
};

window.saveAllCommunities = async function() {
    showSaveOverlay();
    const statusDiv = document.getElementById('save-status');
    statusDiv.innerHTML = makeInlineNotice('warn', '&#x1F4BE; Сохранение всех сообществ...');
    
    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?getBotSettings');
        const currentData = await res.json();
        
        const saveRes = await fetch(baseUrl + '?saveAllCommunities', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(currentData)
        });
        const result = await saveRes.json();
        
        if (result.success) {
            statusDiv.innerHTML = makeInlineNotice('success', '✅ Все сообщества сохранены!');
            await renderCommunityButtons();
        } else {
            throw new Error(result.error || 'Ошибка сохранения');
        }
    } catch (e) {
        statusDiv.innerHTML = makeInlineNotice('error', '❌ Ошибка: ' + e.message);
    }
};

window.deleteCurrentCommunity = async function() {
    if (!window.currentCommunityId || window.currentCommunityId === 'default') {
        alert('Нельзя удалить сообщество по умолчанию');
        return;
    }
    
    if (!confirm('?? Удалить настройки сообщества "' + window.currentCommunityId + '"?\\nЭто действие нельзя отменить!')) {
        return;
    }
    
    try {
        const baseUrl = window.location.href.split('?')[0];
        const res = await fetch(baseUrl + '?deleteCommunity', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ community_id: window.currentCommunityId })
        });
        const data = await res.json();
        
        if (data.success) {
            alert('&#x1F4E6; Сообщество удалено');
            await renderCommunityButtons();
        } else {
            alert('❌ Ошибка: ' + (data.error || 'неизвестная'));
        }
    } catch (e) {
        alert('❌ Ошибка: ' + e.message);
    }
};

// ?? Вызов рендера кнопок при загрузке вкладки
const originalOpenTab = window.openTab;
window.openTab = function(evt, name) {
    originalOpenTab(evt, name);
    if (name === 'Settings') {
        renderCommunityButtons();
    }
};




// Автоматическое извлечение токена из полной строки
function setupTokenExtractor(inputId) {
    const input = document.getElementById(inputId);
    if (!input) return;
    input.addEventListener('input', function() {
        let val = this.value.trim();
        // Ищем access_token=...
        const match = val.match(/access_token=([^&]+)/);
        if (match && match[1]) {
            this.value = match[1]; // заменяем на извлечённый токен
            // можно добавить небольшой визуальный эффект
            this.style.backgroundColor = '#e8f5e9';
            setTimeout(() => this.style.backgroundColor = '', 1000);
        }
    });
}

// Вызываем после загрузки страницы
document.addEventListener('DOMContentLoaded', () => {
    setupTokenExtractor('userToken');
    // Если нужно для VK Token (на всякий случай), раскомментируйте:
    // setupTokenExtractor('vkToken');

    // Инициализация темы при загрузке
    initTheme();
    window.updateThemeDocking && window.updateThemeDocking();
});

// ===== УПРАВЛЕНИЕ ТЕМАМИ =====
window.setTheme = function(theme) {
    if (!theme) theme = 'light';
    document.body.setAttribute('data-theme', theme);
    localStorage.setItem('adminPanelTheme', theme);
    // Подсвечиваем активную кнопку
    document.querySelectorAll('.theme-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-theme') === theme);
    });
};

function initTheme() {
    const saved = localStorage.getItem('adminPanelTheme') || 'light';
    document.body.setAttribute('data-theme', saved);
    // Подсвечиваем активную кнопку
    document.querySelectorAll('.theme-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-theme') === saved);
    });
}

window.updateThemeDocking = function() {
    const themeHost = document.getElementById('headerThemeHost');
    const themeDockSlot = document.getElementById('themeDockSlot');
    const themeSwitcher = document.getElementById('globalThemeSwitcher');
    const tabs = document.querySelector('.tab');
    if (!themeHost || !themeDockSlot || !themeSwitcher || !tabs) return;

    if (themeDockSlot.parentElement !== document.body) {
        document.body.appendChild(themeDockSlot);
    }

    if (window.innerWidth <= 980) {
        if (themeSwitcher.parentElement !== themeHost) {
            themeHost.appendChild(themeSwitcher);
        }
        themeDockSlot.classList.remove('is-visible');
        themeDockSlot.style.top = '';
        return;
    }

    const tabsRect = tabs.getBoundingClientRect();
    const shouldDock = window.scrollY > 12;
    if (shouldDock) {
        if (themeSwitcher.parentElement !== themeDockSlot) {
            themeDockSlot.appendChild(themeSwitcher);
        }
        themeDockSlot.classList.add('is-visible');
        themeDockSlot.style.top = (tabsRect.bottom + 2) + 'px';
    } else {
        if (themeSwitcher.parentElement !== themeHost) {
            themeHost.appendChild(themeSwitcher);
        }
        themeDockSlot.classList.remove('is-visible');
        themeDockSlot.style.top = '';
    }
};

window.addEventListener('scroll', function() {
    window.updateThemeDocking && window.updateThemeDocking();
}, { passive: true });

window.addEventListener('resize', function() {
    window.updateThemeDocking && window.updateThemeDocking();
});
// ===== КОНЕЦ УПРАВЛЕНИЯ ТЕМАМИ =====



})(); // ? Конец IIFE
</script>

</div> <!-- ? Закрытие container -->

</body>
</html>`;


module.exports = { adminPanelHTML }
