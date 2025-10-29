import { Show, createSignal, createEffect, onCleanup } from 'solid-js';
import LocationList from './routes/LocationList';
import LocationDetail from './routes/LocationDetail';
import AuditDetail from './routes/AuditDetail';

const parsePath = () => {
  if (typeof window === 'undefined') return null;
  const segments = window.location.pathname.split('/').filter(Boolean);
  if (segments.length === 2 && segments[0] === 'audits') {
    return { audit: segments[1] } as const;
  }
  if (segments.length === 2 && segments[0] === 'locations') {
    try {
      return { location: decodeURIComponent(segments[1]) } as const;
    } catch (err) {
      return { location: segments[1] } as const;
    }
  }
  return null;
};

const App = () => {
  const initial = parsePath();
  const [selectedLocation, setSelectedLocation] = createSignal<string | null>(initial && 'location' in initial ? initial.location : null);
  const [selectedAudit, setSelectedAudit] = createSignal<string | null>(initial && 'audit' in initial ? initial.audit : null);
  const [lastLocation, setLastLocation] = createSignal<string | null>(initial && 'location' in initial ? initial.location : null);

  if (typeof window !== 'undefined') {
    createEffect(() => {
      const handlePopState = () => {
        const state = parsePath();
        if (state && 'audit' in state) {
          setSelectedAudit(state.audit);
          setSelectedLocation(null);
        } else if (state && 'location' in state) {
          setSelectedLocation(state.location);
          setSelectedAudit(null);
          setLastLocation(state.location);
        } else {
          setSelectedLocation(null);
          setSelectedAudit(null);
        }
      };
      window.addEventListener('popstate', handlePopState);
      onCleanup(() => window.removeEventListener('popstate', handlePopState));
    });
  }

  const navigateToList = () => {
    if (typeof window !== 'undefined') {
      window.history.pushState(null, '', '/');
    }
    setSelectedLocation(null);
    setSelectedAudit(null);
  };

  const navigateToLocation = (address: string) => {
    if (typeof window !== 'undefined') {
      window.history.pushState(null, '', `/locations/${encodeURIComponent(address)}`);
    }
    setSelectedLocation(address);
    setSelectedAudit(null);
    setLastLocation(address);
  };

  const navigateToAudit = (id: string, originLocation?: string | null) => {
    if (typeof window !== 'undefined') {
      window.history.pushState(null, '', `/audits/${id}`);
    }
    setSelectedAudit(id);
    if (originLocation) {
      setLastLocation(originLocation);
    }
  };

  const handleBackFromAudit = () => {
    const previousLocation = lastLocation();
    if (previousLocation) {
      navigateToLocation(previousLocation);
    } else {
      navigateToList();
    }
  };

  return (
    <div class="app-shell">
      <header class="app-header">
        <button type="button" class="brand" onClick={navigateToList}>
          <img src="/square.png" alt="Citywide logo" />
          <span>Citywide Audit Services</span>
        </button>
        <nav class="nav-links">
          <button type="button" class={!selectedLocation() && !selectedAudit() ? 'active' : ''} onClick={navigateToList}>
            Locations
          </button>
          <Show when={selectedLocation()}>
            {(address) => (
              <button type="button" class={!selectedAudit() ? 'active' : ''} onClick={() => navigateToLocation(address())}>
                {address()}
              </button>
            )}
          </Show>
        </nav>
      </header>
      <main class="app-main">
        <Show
          when={selectedAudit()}
          fallback={
            <Show
              when={selectedLocation()}
              fallback={<LocationList onSelect={navigateToLocation} />}
            >
              {(address) => (
                <LocationDetail
                  address={address()}
                  onBack={navigateToList}
                  onSelectAudit={(id) => navigateToAudit(id, address())}
                />
              )}
            </Show>
          }
        >
          {(id) => <AuditDetail auditId={id()} onBack={handleBackFromAudit} />}
        </Show>
      </main>
    </div>
  );
};

export default App;
