import { Show, createSignal, createEffect, onCleanup } from 'solid-js';
import LocationList from './routes/LocationList';
import LocationDetail from './routes/LocationDetail';
import AuditDetail from './routes/AuditDetail';
import type { LocationSummary } from './types';

interface LocationSelection {
  address: string;
  locationRowId?: number | null;
  locationCode?: string | null;
  siteName?: string | null;
}

const parsePath = () => {
  if (typeof window === 'undefined') return null;
  const params = new URLSearchParams(window.location.search);
  const locParam = params.get('loc');
  const parsedLoc = locParam ? Number(locParam) : null;
  const segments = window.location.pathname.split('/').filter(Boolean);
  if (segments.length === 2 && segments[0] === 'audits') {
    return { audit: segments[1] } as const;
  }
  if (segments.length === 2 && segments[0] === 'locations') {
    const decoded = (() => {
      try {
        return decodeURIComponent(segments[1]);
      } catch (err) {
        return segments[1];
      }
    })();
    return {
      location: {
        address: decoded,
        locationRowId: Number.isFinite(parsedLoc) ? parsedLoc : undefined
      }
    } as const;
  }
  return null;
};

const App = () => {
  const initial = parsePath();
  const [selectedLocation, setSelectedLocation] = createSignal<LocationSelection | null>(
    initial && 'location' in initial ? initial.location : null
  );
  const [selectedAudit, setSelectedAudit] = createSignal<string | null>(initial && 'audit' in initial ? initial.audit : null);
  const [lastLocation, setLastLocation] = createSignal<LocationSelection | null>(
    initial && 'location' in initial ? initial.location : null
  );

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

  const navigateToLocation = (location: LocationSummary | LocationSelection) => {
    const address = location.address;
    const locId =
      'location_row_id' in location
        ? location.location_row_id ?? null
        : location.locationRowId ?? null;
    const query = locId !== null && locId !== undefined ? `?loc=${encodeURIComponent(String(locId))}` : '';
    if (typeof window !== 'undefined') {
      window.history.pushState(null, '', `/locations/${encodeURIComponent(address)}${query}`);
    }
    const selection: LocationSelection = {
      address,
      locationRowId: locId ?? undefined,
      locationCode:
        'location_code' in location ? location.location_code ?? undefined : location.locationCode,
      siteName: 'site_name' in location ? location.site_name ?? undefined : location.siteName
    };
    setSelectedLocation(selection);
    setSelectedAudit(null);
    setLastLocation(selection);
  };

  const navigateToAudit = (id: string, originLocation?: LocationSelection | null) => {
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
            {(loc) => (
              <button type="button" class={!selectedAudit() ? 'active' : ''} onClick={() => navigateToLocation(loc())}>
                {loc().siteName ?? loc().address}
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
              {(location) => (
                <LocationDetail
                  location={location()}
                  onBack={navigateToList}
                  onSelectAudit={(id) => navigateToAudit(id, location())}
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
