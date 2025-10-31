import type { Component } from 'solid-js';
import { For, Show, createMemo, createResource, createSignal } from 'solid-js';
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
  const [sortMode, setSortMode] = createSignal<'alpha' | 'risk'>('alpha');
  const [riskFilter, setRiskFilter] = createSignal<'all' | 'critical' | 'warning' | 'stable'>('all');

  const [locations, { refetch }] = createResource(
    () => ({
      page: page(),
      pageSize: PAGE_SIZE,
      search: searchTerm(),
      sort: sortMode() === 'risk' ? 'risk_desc' : undefined
    }),
    fetchLocations
  );

  const items = createMemo(() => locations()?.items ?? []);
  const total = createMemo(() => locations()?.total ?? 0);
  const effectivePageSize = createMemo(() => locations()?.page_size ?? PAGE_SIZE);
  const totalPages = createMemo(() => {
    const size = effectivePageSize();
    return size > 0 ? Math.max(1, Math.ceil(total() / size)) : 1;
  });

  const filteredItems = createMemo(() => {
    const filter = riskFilter();
    const list = items();
    if (filter === 'all') return list;
    return list.filter((entry) => (entry.risk_level ?? 'stable') === filter);
  });

  const topRiskLocations = createMemo(() => {
    const sorted = [...items()].sort((a, b) => {
      const scoreA = typeof a.risk_score === 'number' ? a.risk_score : Number(a.risk_score ?? 0);
      const scoreB = typeof b.risk_score === 'number' ? b.risk_score : Number(b.risk_score ?? 0);
      return scoreB - scoreA;
    });
    return sorted
      .filter((entry) => entry.risk_level === 'critical' || entry.risk_level === 'warning')
      .slice(0, 5);
  });

  const startIndex = createMemo(() => {
    if (total() === 0) return 0;
    if (filteredItems().length === 0) return 0;
    return (page() - 1) * effectivePageSize() + 1;
  });

  const endIndex = createMemo(() => {
    const start = startIndex();
    if (start === 0) return 0;
    const visible = filteredItems().length;
    return start + visible - 1;
  });

  const paginationRange = createMemo(() => {
    const current = page();
    const totalPageCount = totalPages();
    const range: Array<number | 'ellipsis'> = [];
    if (totalPageCount <= 7) {
      for (let i = 1; i <= totalPageCount; i += 1) range.push(i);
      return range;
    }
    const showLeftEllipsis = current > 4;
    const showRightEllipsis = current < totalPageCount - 3;
    range.push(1);
    if (showLeftEllipsis) range.push('ellipsis');
    const start = Math.max(2, Math.min(current - 1, totalPageCount - 3));
    const end = Math.min(totalPageCount - 1, start + 2);
    for (let i = start; i <= end; i += 1) {
      range.push(i);
    }
    if (showRightEllipsis) range.push('ellipsis');
    range.push(totalPageCount);
    return range;
  });

  const formatCurrency = (value: number | string | null | undefined) => {
    if (value == null) return '—';
    const numeric = typeof value === 'number' ? value : Number(value);
    if (!Number.isFinite(numeric)) return '—';
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(numeric);
  };

  const riskBadgeClass = (level?: string | null) => {
    switch (level) {
      case 'critical':
        return 'risk-pill risk-pill--critical';
      case 'warning':
        return 'risk-pill risk-pill--warning';
      case 'stable':
        return 'risk-pill risk-pill--stable';
      default:
        return 'risk-pill risk-pill--muted';
    }
  };

  const riskLabel = (level?: string | null) => {
    switch (level) {
      case 'critical':
        return 'Critical';
      case 'warning':
        return 'Warning';
      case 'stable':
        return 'Stable';
      default:
        return 'Unknown';
    }
  };

  const riskTooltip = (location: LocationSummary) => {
    const failures = typeof location.service_failures === 'number' ? location.service_failures : Number(location.service_failures ?? 0);
    const pm = typeof location.service_pm === 'number' ? location.service_pm : Number(location.service_pm ?? 0);
    const opex = typeof location.open_spend === 'number' ? location.open_spend : Number(location.open_spend ?? 0);
    const openPerRaw = typeof location.open_per_device === 'number' ? location.open_per_device : Number(location.open_per_device ?? 0);
    const openPer = Number.isFinite(openPerRaw) ? openPerRaw : 0;
    return `Failures (12m): ${failures} • PM Visits: ${pm} • Open spend: ${formatCurrency(opex)} • Deficiencies/device: ${openPer.toFixed(2)}`;
  };

  const rowRiskClass = (level?: string | null) => {
    switch (level) {
      case 'critical':
        return 'table-row table-row--risk-critical';
      case 'warning':
        return 'table-row table-row--risk-warning';
      default:
        return 'table-row';
    }
  };

  const coverageIcon = (type: 'audits' | 'service' | 'financial', available: boolean) => {
    if (!available) return '◻';
    switch (type) {
      case 'audits':
        return '🔍';
      case 'service':
        return '📄';
      case 'financial':
        return '💰';
      default:
        return '◻';
    }
  };

  const handleSearchInput = (event: InputEvent & { currentTarget: HTMLInputElement }) => {
    const value = event.currentTarget.value;
    setSearchInput(value);
    setSearchTerm(value.trim());
    setPage(1);
  };

  const handleSortChange = (mode: 'alpha' | 'risk') => {
    if (sortMode() === mode) return;
    setSortMode(mode);
    setPage(1);
  };

  const handleRiskFilterChange = (value: 'all' | 'critical' | 'warning' | 'stable') => {
    setRiskFilter(value);
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

      {(() => {
        if (locations.error) {
          return <ErrorMessage message={(locations.error as Error).message} onRetry={() => refetch()} />;
        }
        if (isLoading()) {
          return <LoadingIndicator message="Loading locations…" />;
        }
        if (items().length === 0) {
          return <div class="empty-state">No locations found. Try updating your search or refresh the list.</div>;
        }
        return (
          <>
            <div class="list-toolbar">
              <div class="toolbar-group">
                <span class="toolbar-label">Sort</span>
                <div class="toolbar-buttons">
                  <button
                    type="button"
                    class={sortMode() === 'alpha' ? 'toolbar-button toolbar-button--active' : 'toolbar-button'}
                    onClick={() => handleSortChange('alpha')}
                  >
                    Alphabetical
                  </button>
                  <button
                    type="button"
                    class={sortMode() === 'risk' ? 'toolbar-button toolbar-button--active' : 'toolbar-button'}
                    onClick={() => handleSortChange('risk')}
                  >
                    Highest Risk
                  </button>
                </div>
              </div>
              <div class="toolbar-group">
                <label class="toolbar-label" for="risk-filter">Risk Filter</label>
                <select
                  id="risk-filter"
                  class="toolbar-select"
                  value={riskFilter()}
                  onInput={(event) => handleRiskFilterChange(event.currentTarget.value as 'all' | 'critical' | 'warning' | 'stable')}
                >
                  <option value="all">All</option>
                  <option value="critical">Critical</option>
                  <option value="warning">Warning</option>
                  <option value="stable">Stable</option>
                </select>
              </div>
            </div>

            <Show when={filteredItems().length > 0} fallback={<div class="empty-state">No locations match the current risk filter.</div>}>
              <Show when={topRiskLocations().length > 0}>
                <aside class="risk-leaderboard" aria-label="Top risk locations">
                  <h2>Top Risk Locations</h2>
                  <ol>
                    <For each={topRiskLocations()}>
                      {(location) => {
                        const score = typeof location.risk_score === 'number' ? location.risk_score : Number(location.risk_score ?? 0);
                        const label = location.site_name ?? location.address;
                        return (
                          <li
                            role="link"
                            tabIndex={0}
                            onClick={() => props.onSelect(location)}
                            onKeyDown={(event) => {
                              if (event.key === 'Enter' || event.key === ' ') {
                                event.preventDefault();
                                props.onSelect(location);
                              }
                            }}
                          >
                            <span class="risk-rank-name">{label}</span>
                            <span class="risk-rank-meta">{riskLabel(location.risk_level)} · Score {score.toFixed(1)}</span>
                          </li>
                        );
                      }}
                    </For>
                  </ol>
                </aside>
              </Show>
              <div class="table-wrapper" role="region" aria-live="polite">
                <table>
                  <thead>
                    <tr>
                      <th scope="col">Location</th>
                      <th scope="col">Risk</th>
                      <th scope="col" aria-label="Data availability">Data</th>
                      <th scope="col">Street</th>
                      <th scope="col">City</th>
                      <th scope="col">State</th>
                      <th scope="col">Owner</th>
                      <th scope="col">Vendor</th>
                      <th scope="col">Devices</th>
                      <th scope="col">Failures (12m)</th>
                      <th scope="col">Open Spend</th>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={filteredItems()}>
                      {(location) => {
                    const deviceCount = location.device_count ?? 0;
                    const detailParts = [location.street, location.city, location.state].filter(Boolean).join(', ');
                    const displayName = location.site_name ?? location.formatted_address ?? location.address;
                    const hasAudits = Boolean(location.has_audits);
                    const hasService = Boolean(location.has_service_records);
                        const hasFinancial = Boolean(location.has_financial_records);
                        const failures12m = typeof location.service_failures === 'number' ? location.service_failures : Number(location.service_failures ?? 0);
                        const openSpendValue = typeof location.open_spend === 'number' ? location.open_spend : Number(location.open_spend ?? 0);

                        return (
                          <tr
                            class={rowRiskClass(location.risk_level)}
                            role="link"
                            tabIndex={0}
                            aria-label={`View location ${displayName}`}
                            onClick={() => props.onSelect(location)}
                            onKeyDown={(event) => {
                              if (event.key === 'Enter' || event.key === ' ') {
                                event.preventDefault();
                                props.onSelect(location);
                              }
                            }}
                          >
                        <td class="location-cell">
                          <div class="location-name-row">
                            <span class="location-name">{displayName ?? '—'}</span>
                          </div>
                              <Show when={detailParts}>
                                <span class="location-subtext">{detailParts}</span>
                              </Show>
                              <Show when={location.location_code}>
                                <span class="location-subtext">ID: {location.location_code}</span>
                              </Show>
                            </td>
                            <td>
                              <span class={riskBadgeClass(location.risk_level)} title={riskTooltip(location)}>{riskLabel(location.risk_level)}</span>
                            </td>
                        <td class="coverage-cell" aria-label="Data coverage">
                          <span class="sr-only">
                            Data coverage: audits {hasAudits ? 'available' : 'missing'}, service {hasService ? 'available' : 'missing'}, financial {hasFinancial ? 'available' : 'missing'}
                          </span>
                          <span class="coverage-icon" title={hasAudits ? 'Audit data available' : 'Audit data missing'} aria-hidden="true">
                            {coverageIcon('audits', hasAudits)}
                          </span>
                          <span class="coverage-icon" title={hasService ? 'Service data available' : 'Service data missing'} aria-hidden="true">
                            {coverageIcon('service', hasService)}
                          </span>
                          <span class="coverage-icon" title={hasFinancial ? 'Financial data available' : 'Financial data missing'} aria-hidden="true">
                            {coverageIcon('financial', hasFinancial)}
                          </span>
                        </td>
                            <td>{location.street ?? '—'}</td>
                            <td>{location.city ?? '—'}</td>
                            <td>{location.state ?? '—'}</td>
                            <td>{location.building_owner ?? '—'}</td>
                            <td>{location.vendor_name ?? '—'}</td>
                            <td>{deviceCount}</td>
                            <td>{failures12m}</td>
                            <td>{formatCurrency(openSpendValue)}</td>
                          </tr>
                        );
                      }}
                    </For>
                  </tbody>
                </table>
              </div>
            </Show>

            <nav class="pagination-nav" aria-label="Pagination">
              <div class="page-info">
                <Show when={startIndex() > 0} fallback={<span>No matching locations</span>}>
                  <span>
                    Showing {startIndex()}–{endIndex()} of {total()} locations
                  </span>
                </Show>
              </div>
              <div class="page-links">
                <button
                  type="button"
                  class="pagination-button"
                  disabled={page() === 1 || isLoading()}
                  onClick={() => setPage(1)}
                  aria-label="First page"
                >
                  «
                </button>
                <button
                  type="button"
                  class="pagination-button"
                  disabled={page() === 1 || isLoading()}
                  onClick={handlePrev}
                  aria-label="Previous page"
                >
                  ‹
                </button>
                <For each={paginationRange()}>
                  {(entry) =>
                    typeof entry === 'number' ? (
                      <button
                        type="button"
                        class={entry === page() ? 'pagination-button pagination-button--active' : 'pagination-button'}
                        aria-current={entry === page() ? 'page' : undefined}
                        disabled={entry === page() || isLoading()}
                        onClick={() => setPage(entry)}
                      >
                        {entry}
                      </button>
                    ) : (
                      <span class="pagination-ellipsis" aria-hidden="true">
                        …
                      </span>
                    )
                  }
                </For>
                <button
                  type="button"
                  class="pagination-button"
                  disabled={page() === totalPages() || isLoading()}
                  onClick={handleNext}
                  aria-label="Next page"
                >
                  ›
                </button>
                <button
                  type="button"
                  class="pagination-button"
                  disabled={page() === totalPages() || isLoading()}
                  onClick={() => setPage(totalPages())}
                  aria-label="Last page"
                >
                  »
                </button>
              </div>
            </nav>
          </>
        );
      })()}
    </section>
  );
};

export default LocationList;
