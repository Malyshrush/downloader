import { useCallback, useEffect, useMemo, useState } from 'react';
import { loadGroup, loadGroups, subscribeGroup, unsubscribeGroup } from './api.js';
import { allowMessagesFromGroup, initVkBridge, parseLaunchParams, parseRouteHash, setGroupHash } from './vk.js';

const EMPTY_STATE = {
  loading: true,
  error: '',
  communityId: '',
  slug: '',
  groups: [],
  group: null
};

function PlaceholderImage({ type }) {
  return <div className={`placeholder placeholder-${type}`}>{type === 'banner' ? 'PAPA BOT' : 'PB'}</div>;
}

function GroupImage({ src, alt, type }) {
  if (!src) return <PlaceholderImage type={type} />;
  return <img className={`group-${type}`} src={src} alt={alt} loading="lazy" />;
}

function StatusView({ title, text }) {
  return (
    <main className="app-shell app-shell-center">
      <section className="notice">
        <h1>{title}</h1>
        <p>{text}</p>
      </section>
    </main>
  );
}

function GroupList({ groups, onOpen }) {
  return (
    <div className="group-list">
      {groups.map((group) => (
        <button className="group-card" type="button" key={group.slug} onClick={() => onOpen(group.slug)}>
          <GroupImage src={group.iconUrl} alt={group.title} type="icon" />
          <span className="group-card-copy">
            <strong>{group.title}</strong>
            {group.description ? <span>{group.description}</span> : null}
          </span>
          {group.subscribed ? <span className="subscribed-mark">Вы в группе</span> : null}
        </button>
      ))}
    </div>
  );
}

function GroupDetail({ group, busy, onBack, onToggle }) {
  const buttonText = group.subscribed ? group.unsubscribeText : group.subscribeText;
  return (
    <article className="detail">
      <button className="back-button" type="button" onClick={onBack}>Назад</button>
      <GroupImage src={group.bannerUrl} alt={group.title} type="banner" />
      <div className="detail-copy">
        <h1>{group.title}</h1>
        {group.description ? <p>{group.description}</p> : null}
      </div>
      <button className="primary-button" type="button" disabled={busy} onClick={onToggle}>
        {busy ? 'Сохраняем...' : buttonText}
      </button>
    </article>
  );
}

export default function App() {
  const launchParams = useMemo(() => parseLaunchParams(), []);
  const [state, setState] = useState(EMPTY_STATE);
  const [busy, setBusy] = useState(false);

  const loadCurrentRoute = useCallback(async () => {
    const route = parseRouteHash();
    if (!route.communityId) {
      setState({ ...EMPTY_STATE, loading: false, error: 'Откройте Mini App по ссылке сообщества' });
      return;
    }

    setState((prev) => ({ ...prev, loading: true, error: '', communityId: route.communityId, slug: route.slug }));
    try {
      if (route.slug) {
        const data = await loadGroup(route.communityId, route.slug, launchParams);
        setState({ loading: false, error: '', communityId: route.communityId, slug: route.slug, groups: [], group: data.group });
      } else {
        const data = await loadGroups(route.communityId, launchParams);
        setState({ loading: false, error: '', communityId: route.communityId, slug: '', groups: data.groups || [], group: null });
      }
    } catch (error) {
      setState((prev) => ({ ...prev, loading: false, error: error.message || 'Не удалось загрузить данные' }));
    }
  }, [launchParams]);

  useEffect(() => {
    initVkBridge();
    loadCurrentRoute();
    window.addEventListener('hashchange', loadCurrentRoute);
    return () => window.removeEventListener('hashchange', loadCurrentRoute);
  }, [loadCurrentRoute]);

  const openGroup = (slug) => setGroupHash(state.communityId, slug);
  const backToList = () => setGroupHash(state.communityId);

  const toggleSubscription = async () => {
    if (!state.group || !state.communityId) return;
    setBusy(true);
    try {
      if (!state.group.subscribed) {
        await allowMessagesFromGroup(state.communityId);
        const data = await subscribeGroup(state.communityId, state.group.slug, launchParams);
        setState((prev) => ({ ...prev, group: data.group || { ...prev.group, subscribed: true } }));
      } else {
        const data = await unsubscribeGroup(state.communityId, state.group.slug, launchParams);
        setState((prev) => ({ ...prev, group: data.group || { ...prev.group, subscribed: false } }));
      }
    } catch (error) {
      setState((prev) => ({
        ...prev,
        error: state.group.subscribed
          ? (error.message || 'Не удалось отписаться')
          : (error.message || 'Для подписки разрешите сообщения')
      }));
    } finally {
      setBusy(false);
    }
  };

  if (state.loading) {
    return <StatusView title="Загрузка" text="Получаем группы сообщества" />;
  }

  if (state.error && !state.group && state.groups.length === 0) {
    return <StatusView title="Mini App" text={state.error} />;
  }

  return (
    <main className="app-shell">
      {state.group ? (
        <>
          {state.error ? <div className="inline-error">{state.error}</div> : null}
          <GroupDetail group={state.group} busy={busy} onBack={backToList} onToggle={toggleSubscription} />
        </>
      ) : (
        <>
          <header className="list-header">
            <h1>Группы сообщества</h1>
          </header>
          {state.error ? <div className="inline-error">{state.error}</div> : null}
          {state.groups.length ? (
            <GroupList groups={state.groups} onOpen={openGroup} />
          ) : (
            <section className="notice"><p>Доступных групп пока нет.</p></section>
          )}
        </>
      )}
    </main>
  );
}
