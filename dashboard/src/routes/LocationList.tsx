import type { Component } from 'solid-js';
import { For, Match, Show, Switch, createMemo, createResource, createSignal } from 'solid-js';
import { fetchLocations } from '../api';
import LoadingIndicator from '../components/LoadingIndicator';
import ErrorMessage from '../components/ErrorMessage';
import { formatDateTime } from '../utils';
import type { LocationSummary } from '../types';

interface LocationListProps {
  onSelect(address: string): void;
}

const LocationList: Component<LocationListProps> = (props) => {
  const [search, setSearch] = createSignal('');
  const [locations, { refetch }] = createResource(fetchLocations);

  const filteredAudits = createMemo(() => {
    const list = locations() ?? [];
    const term = search().trim().toLowerCase();
    if (!term) {
      return list;
    }
    return list.filter((entry) => {
      const parts: (string | null | undefined)[] = [
        entry.address,
        entry.building_owner,
        entry.elevator_contractor,
        entry.city_id
      ];
      return parts.some((value) => value && value.toLowerCase().includes(term));
    });
  });

  const handleSearchInput = (event: InputEvent & { currentTarget: HTMLInputElement }) => {
    setSearch(event.currentTarget.value);
  };

  return (
    <section class="page-section" aria-labelledby="locations-heading">
      <div class="section-header">
        <div>
          <h1 id="locations-heading">Locations</h1>
          <p class="section-subtitle">Review all audited properties, see open issues at a glance, and drill into device-level details.</p>
        </div>
        <div class="section-actions">
          <input
            type="search"
            placeholder="Search by address, owner, contractor, city…"
            value={search()}
            onInput={handleSearchInput}
            autoFocus
          />
          <button type="button" class="action-button refresh-button" onClick={() => refetch()}>
            Refresh
          </button>
        </div>
      </div>

      <Switch>
        <Match when={locations.error}>
          <ErrorMessage message={(locations.error as Error).message} onRetry={() => refetch()} />
        </Match>
        <Match when={locations.loading}>
          <LoadingIndicator message="Loading locations…" />
        </Match>
        <Match when={filteredAudits().length === 0}>
          <div class="empty-state">No locations found. Try a different search or refresh the list.</div>
        </Match>
        <Match when={filteredAudits().length > 0}>
          <div class="table-wrapper" role="region" aria-live="polite">
            <table>
              <thead>
                <tr>
                  <th scope="col">Location</th>
                  <th scope="col">Owner</th>
                  <th scope="col">Contractor</th>
                  <th scope="col">City ID</th>
                  <th scope="col">Devices</th>
                  <th scope="col">Audits</th>
                  <th scope="col">Open Deficiencies</th>
                  <th scope="col">Last Audit</th>
                </tr>
              </thead>
              <tbody>
                <For each={filteredAudits()}>
                  {(location: LocationSummary) => {
                    const openCount = location.open_deficiencies ?? 0;

                    return (
                      <tr
                        class="table-row"
                        role="link"
                        tabIndex={0}
                        aria-label={`View location ${location.address}`}
                        onClick={() => props.onSelect(location.address)}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault();
                            props.onSelect(location.address);
                          }
                        }}
                      >
                        <td>{location.address}</td>
                        <td>{location.building_owner ?? '—'}</td>
                        <td>{location.elevator_contractor ?? '—'}</td>
                        <td>{location.city_id ?? '—'}</td>
                        <td>{location.device_count}</td>
                        <td>{location.audit_count}</td>
                        <td>
                          <span class="deficiency-count">{openCount}</span>
                          <Show when={openCount > 0}>
                            <span class="deficiency-indicator" aria-hidden="true" title="Open deficiencies" />
                          </Show>
                        </td>
                        <td>{formatDateTime(location.last_audit)}</td>
                      </tr>
                    );
                  }}
                </For>
              </tbody>
            </table>
          </div>
        </Match>
      </Switch>
    </section>
  );
};

export default LocationList;
