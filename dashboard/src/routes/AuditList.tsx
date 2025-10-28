import type { Component } from 'solid-js';
import { For, Match, Show, Switch, createMemo, createResource, createSignal } from 'solid-js';
import { fetchAudits } from '../api';
import LoadingIndicator from '../components/LoadingIndicator';
import ErrorMessage from '../components/ErrorMessage';
import { formatDateTime } from '../utils';
import type { AuditSummary } from '../types';

interface AuditListProps {
  onSelect(id: string): void;
}

const AuditList: Component<AuditListProps> = (props) => {
  const [search, setSearch] = createSignal('');
  const [audits, { refetch }] = createResource(fetchAudits);

  const filteredAudits = createMemo(() => {
    const list = audits() ?? [];
    const term = search().trim().toLowerCase();
    if (!term) {
      return list;
    }
    return list.filter((entry) => {
      const parts: (string | null | undefined)[] = [
        entry.building_address,
        entry.building_owner,
        entry.city_id,
        entry.bank_name,
        entry.device_type
      ];
      return parts.some((value) => value && value.toLowerCase().includes(term));
    });
  });

  const handleSearchInput = (event: InputEvent & { currentTarget: HTMLInputElement }) => {
    setSearch(event.currentTarget.value);
  };

  return (
    <section class="page-section" aria-labelledby="audits-heading">
      <div class="section-header">
        <div>
          <h1 id="audits-heading">Stored audits</h1>
          <p class="section-subtitle">Review processed elevator audits and drill deeper into individual reports.</p>
        </div>
        <div class="section-actions">
          <input
            type="search"
            placeholder="Search by building, owner, city, device…"
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
        <Match when={audits.error}>
          <ErrorMessage message={(audits.error as Error).message} onRetry={() => refetch()} />
        </Match>
        <Match when={audits.loading}>
          <LoadingIndicator message="Loading audits…" />
        </Match>
        <Match when={filteredAudits().length === 0}>
          <div class="empty-state">No audits found. Try a different search or refresh the list.</div>
        </Match>
        <Match when={filteredAudits().length > 0}>
          <div class="table-wrapper" role="region" aria-live="polite">
            <table>
              <thead>
                <tr>
                  <th scope="col">Building</th>
                  <th scope="col">Owner</th>
                  <th scope="col">Device</th>
                  <th scope="col">Bank</th>
                  <th scope="col">City ID</th>
                  <th scope="col">Submitted</th>
                  <th scope="col">Updated</th>
                </tr>
              </thead>
              <tbody>
                <For each={filteredAudits()}>
                  {(audit: AuditSummary) => (
                    <tr
                      class="table-row"
                      role="link"
                      tabIndex={0}
                      aria-label={`View audit ${audit.building_address ?? audit.audit_uuid}`}
                      onClick={() => props.onSelect(audit.audit_uuid)}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                          event.preventDefault();
                          props.onSelect(audit.audit_uuid);
                        }
                      }}
                    >
                      <td>{audit.building_address ?? '—'}</td>
                      <td>{audit.building_owner ?? '—'}</td>
                      <td>{audit.device_type ?? '—'}</td>
                      <td>{audit.bank_name ?? '—'}</td>
                      <td>{audit.city_id ?? '—'}</td>
                      <td>{formatDateTime(audit.submitted_on)}</td>
                      <td>{formatDateTime(audit.updated_at)}</td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </div>
        </Match>
      </Switch>
    </section>
  );
};

export default AuditList;
