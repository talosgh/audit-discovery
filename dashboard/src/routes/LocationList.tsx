import type { Component } from 'solid-js';
import { For, Match, Show, Switch, createEffect, createMemo, createResource, createSignal } from 'solid-js';
import { fetchLocations } from '../api';
import LoadingIndicator from '../components/LoadingIndicator';
import ErrorMessage from '../components/ErrorMessage';
import type { LocationSummary } from '../types';

interface LocationListProps {
  onSelect(location: LocationSummary): void;
}

const PAGE_SIZE = 25;

const LocationList: Component<LocationListProps> = (props) => {
  const [page, setPage] = createSignal(1);
  const [searchInput, setSearchInput] = createSignal('');
  const [searchTerm, setSearchTerm] = createSignal('');

  const [locations, { refetch }] = createResource(
    () => ({ page: page(), pageSize: PAGE_SIZE, search: searchTerm() }),
    fetchLocations
  );

  createEffect(() => {
    const result = locations();
    if (result && typeof result.page === 'number' && result.page !== page()) {
      setPage(result.page);
    }
  });

  const items = createMemo(() => locations()?.items ?? []);
  const total = createMemo(() => locations()?.total ?? 0);
  const effectivePageSize = createMemo(() => locations()?.page_size ?? PAGE_SIZE);
  const totalPages = createMemo(() => {
    const size = effectivePageSize();
    return size > 0 ? Math.max(1, Math.ceil(total() / size)) : 1;
  });

  const startIndex = createMemo(() => {
    if (total() === 0) return 0;
    return (page() - 1) * effectivePageSize() + 1;
  });

  const endIndex = createMemo(() => {
    const start = startIndex();
    if (start === 0) return 0;
    return start + items().length - 1;
  });

  const handleSearchInput = (event: InputEvent & { currentTarget: HTMLInputElement }) => {
    const value = event.currentTarget.value;
    setSearchInput(value);
    setSearchTerm(value.trim());
    setPage(1);
  };

  const handlePrev = () => {
    if (page() > 1) {
      setPage(page() - 1);
    }
  };

  const handleNext = () => {
    if (page() < totalPages()) {
      setPage(page() + 1);
    }
  };

  const isLoading = () => locations.loading;

  return (
    <section class="page-section" aria-labelledby="locations-heading">
      <div class="section-header">
        <div>
          <h1 id="locations-heading">Locations</h1>
          <p class="section-subtitle">Browse every property and drill into device-level history when you need more detail.</p>
        </div>
        <div class="section-actions">
          <input
            type="search"
            placeholder="Search by location, owner, vendor…"
            value={searchInput()}
            onInput={handleSearchInput}
            autoFocus
          />
          <button type="button" class="action-button refresh-button" onClick={() => refetch()} disabled={isLoading()}>
            Refresh
          </button>
        </div>
      </div>

      <Switch>
        <Match when={locations.error}>
          <ErrorMessage message={(locations.error as Error).message} onRetry={() => refetch()} />
        </Match>
        <Match when={isLoading()}>
          <LoadingIndicator message="Loading locations…" />
        </Match>
        <Match when={items().length === 0}>
          <div class="empty-state">No locations found. Try updating your search or refresh the list.</div>
        </Match>
        <Match when={items().length > 0}>
          <div class="table-wrapper" role="region" aria-live="polite">
            <table>
              <thead>
                <tr>
                  <th scope="col">Location</th>
                  <th scope="col">Owner</th>
                  <th scope="col">Vendor</th>
                  <th scope="col">Devices</th>
                  <th scope="col">Open Deficiencies</th>
                </tr>
              </thead>
              <tbody>
                <For each={items()}>
                  {(location) => {
                    const openCount = location.open_deficiencies ?? 0;
                    const deviceCount = location.device_count ?? 0;

                    return (
                      <tr
                        class="table-row"
                        role="link"
                        tabIndex={0}
                        aria-label={`View location ${location.site_name ?? location.address}`}
                        onClick={() => props.onSelect(location)}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault();
                            props.onSelect(location);
                          }
                        }}
                      >
                        <td class={openCount > 0 ? 'location-cell has-open-deficiencies' : 'location-cell'}>
                          <span class="location-name">{location.site_name ?? location.address}</span>
                          <Show when={location.site_name && location.site_name !== location.address}>
                            <span class="location-subtext">{location.address}</span>
                          </Show>
                          <Show when={location.location_code}>
                            <span class="location-subtext">ID: {location.location_code}</span>
                          </Show>
                          <Show when={openCount > 0}>
                            <span class="deficiency-chip" role="img" aria-label="Open deficiencies">⚠</span>
                          </Show>
                        </td>
                        <td>{location.building_owner ?? '—'}</td>
                        <td>{location.vendor_name ?? '—'}</td>
                        <td>{deviceCount}</td>
                        <td>
                          <span class="deficiency-count">{openCount}</span>
                          <Show when={openCount > 0}>
                            <span class="deficiency-indicator" aria-hidden="true" title="Open deficiencies" />
                          </Show>
                        </td>
                      </tr>
                    );
                  }}
                </For>
              </tbody>
            </table>
          </div>

          <div class="pagination-controls">
            <div class="page-info">
              <Show when={total() > 0} fallback={<span>No matching locations</span>}>
                <span>
                  Showing {startIndex()}–{endIndex()} of {total()} locations
                </span>
              </Show>
            </div>
            <div class="page-buttons">
              <button type="button" class="action-button" disabled={page() <= 1 || isLoading()} onClick={handlePrev}>
                Previous
              </button>
              <button type="button" class="action-button" disabled={page() >= totalPages() || isLoading()} onClick={handleNext}>
                Next
              </button>
            </div>
          </div>
        </Match>
      </Switch>
    </section>
  );
};

export default LocationList;
