# VK OAuth Token Viewer

This Chrome extension helps an operator obtain a VK OAuth user token for manual configuration flows.

## Install

1. Open `chrome://extensions/`
2. Enable developer mode
3. Click `Load unpacked`
4. Select the [vk-token-extension](</C:/PROJECT/GPT/PAPA BOT 2/vk-token-extension>) folder

## Usage

1. Make sure you are already signed in to VK in the browser
2. Open the extension popup
3. Click `Get User Token`
4. The extension opens `https://vkhost.github.io/`
5. Complete the OAuth flow in the opened tab
6. When the redirect URL contains `access_token=...`, the popup captures and displays the token
7. Copy the token and paste it into the relevant bot configuration

## How It Works

- the popup opens the VK OAuth helper flow
- the background logic tracks the created tab
- the popup polls the tab URL and extracts `access_token` from the redirect fragment

## Notes

- this tool is for operator setup and recovery workflows
- it is not part of the production bot runtime
- the token should be stored only in the intended admin or deployment configuration path
