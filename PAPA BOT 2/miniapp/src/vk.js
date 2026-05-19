import bridge from '@vkontakte/vk-bridge';

export function parseLaunchParams() {
  const params = new URLSearchParams(window.location.search);
  const result = {};
  params.forEach((value, key) => {
    result[key] = value;
  });
  return result;
}

export function parseRouteHash() {
  const raw = window.location.hash.replace(/^#/, '');
  const params = new URLSearchParams(raw);
  return {
    communityId: params.get('c') || '',
    slug: params.get('g') || ''
  };
}

export function setGroupHash(communityId, slug = '') {
  const params = new URLSearchParams();
  if (communityId) params.set('c', communityId);
  if (slug) params.set('g', slug);
  window.location.hash = params.toString();
}

export async function initVkBridge() {
  try {
    await bridge.send('VKWebAppInit');
  } catch (error) {
    return false;
  }
  return true;
}

export async function allowMessagesFromGroup(communityId) {
  const groupId = Number(communityId);
  if (!Number.isFinite(groupId) || groupId <= 0) {
    throw new Error('Не удалось определить VK ID сообщества');
  }
  return bridge.send('VKWebAppAllowMessagesFromGroup', { group_id: groupId });
}
