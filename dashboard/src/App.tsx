import { Show, createSignal, createEffect, onCleanup, createMemo } from 'solid-js';
import AuditList from './routes/AuditList';
import AuditDetail from './routes/AuditDetail';

const parsePath = (): string | null => {
  if (typeof window === 'undefined') return null;
  const segments = window.location.pathname.split('/').filter(Boolean);
  if (segments.length === 2 && segments[0] === 'audits') {
    return segments[1];
  }
  return null;
};

const App = () => {
  const [selectedId, setSelectedId] = createSignal<string | null>(parsePath());

  if (typeof window !== 'undefined') {
    createEffect(() => {
      const handlePopState = () => {
        setSelectedId(parsePath());
      };
      window.addEventListener('popstate', handlePopState);
      onCleanup(() => window.removeEventListener('popstate', handlePopState));
    });
  }

  const navigateToList = () => {
    if (typeof window !== 'undefined') {
      window.history.pushState(null, '', '/');
    }
    setSelectedId(null);
  };

  const navigateToDetail = (id: string) => {
    if (typeof window !== 'undefined') {
      window.history.pushState(null, '', `/audits/${id}`);
    }
    setSelectedId(id);
  };

  const isListActive = createMemo(() => selectedId() === null);

  return (
    <div class="app-shell">
      <header class="app-header">
        <button type="button" class="brand" onClick={navigateToList}>
          Audit Discovery
        </button>
        <nav class="nav-links">
          <button
            type="button"
            class={isListActive() ? 'active' : ''}
            onClick={navigateToList}
          >
            Audits
          </button>
        </nav>
      </header>
      <main class="app-main">
        <Show
          when={selectedId()}
          fallback={<AuditList onSelect={navigateToDetail} />}
        >
          {(id) => <AuditDetail auditId={id()} onBack={navigateToList} />}
        </Show>
      </main>
    </div>
  );
};

export default App;
