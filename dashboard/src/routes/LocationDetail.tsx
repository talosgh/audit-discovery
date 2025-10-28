import type { Component } from 'solid-js';
import { For, Match, Show, Switch, createResource, createSignal, createMemo } from 'solid-js';
import { fetchLocationDetail, downloadReport } from '../api';
import LoadingIndicator from '../components/LoadingIndicator';
import ErrorMessage from '../components/ErrorMessage';
import { formatDateTime, slugify } from '../utils';
import type { LocationDetail as LocationDetailType, LocationDevice } from '../types';

interface LocationDetailProps {
  address: string;
  onBack(): void;
  onSelectAudit(id: string): void;
}

const LocationDetail: Component<LocationDetailProps> = (props) => {
  const [message, setMessage] = createSignal<string | null>(null);
  const [errorMessage, setErrorMessage] = createSignal<string | null>(null);
  const [isGenerating, setIsGenerating] = createSignal(false);
  const [detail, { refetch }] = createResource<LocationDetailType, string>(() => props.address, fetchLocationDetail);

  const summary = createMemo(() => detail()?.summary);
  const devices = createMemo(() => detail()?.devices ?? []);

  const handleGenerateReport = async () => {
    setMessage(null);
    setErrorMessage(null);
    setIsGenerating(true);
    try {
      const blob = await downloadReport(props.address);
      const filename = `audit-report-${slugify(props.address)}.json`;
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      setMessage(`Report data saved as ${filename}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to generate report';
      setErrorMessage(message);
    } finally {
      setIsGenerating(false);
    }
  };

  const topDeficiencyCodes = createMemo(() => {
    const summaryData = summary();
    if (!summaryData) return [] as Array<[string, number]>;
    const entries = Object.entries(summaryData.deficiencies_by_code ?? {});
    return entries.sort((a, b) => b[1] - a[1]).slice(0, 5);
  });

  return (
    <section class="page-section" aria-labelledby="location-heading">
      <div class="section-header">
        <div>
          <button type="button" class="back-button" onClick={props.onBack}>
            ← Locations
          </button>
          <h1 id="location-heading">{props.address}</h1>
          <Show when={summary()?.building_owner}>
            {(owner) => <p class="section-subtitle">Owned by {owner()}</p>}
          </Show>
        </div>
        <div class="section-actions">
          <button
            type="button"
            class="action-button refresh-button"
            onClick={() => {
              setMessage(null);
              setErrorMessage(null);
              refetch();
            }}
          >
            Refresh
          </button>
          <button
            type="button"
            class="action-button"
            disabled={isGenerating()}
            onClick={handleGenerateReport}
          >
            {isGenerating() ? 'Generating…' : 'Generate Report Data'}
          </button>
        </div>
      </div>

      <Switch>
        <Match when={detail.error}>
          <ErrorMessage message={(detail.error as Error).message} onRetry={() => refetch()} />
        </Match>
        <Match when={detail.loading}>
          <LoadingIndicator message="Loading location…" />
        </Match>
        <Match when={summary()}>
          <div class="location-summary">
            <div class="summary-grid">
              <div class="summary-card">
                <span class="summary-label">Owner</span>
                <span class="summary-value">{summary()?.building_owner ?? '—'}</span>
              </div>
              <div class="summary-card">
                <span class="summary-label">Contractor</span>
                <span class="summary-value">{summary()?.elevator_contractor ?? '—'}</span>
              </div>
              <div class="summary-card">
                <span class="summary-label">City ID</span>
                <span class="summary-value">{summary()?.city_id ?? '—'}</span>
              </div>
              <div class="summary-card">
                <span class="summary-label">Devices</span>
                <span class="summary-value">{summary()?.device_count ?? 0}</span>
              </div>
              <div class="summary-card">
                <span class="summary-label">Audits</span>
                <span class="summary-value">{summary()?.audit_count ?? 0}</span>
              </div>
              <div class="summary-card">
                <span class="summary-label">Open deficiencies</span>
                <span class="summary-value warning">{summary()?.open_deficiencies ?? 0}</span>
              </div>
              <div class="summary-card">
                <span class="summary-label">Last audit</span>
                <span class="summary-value">{formatDateTime(summary()?.last_audit)}</span>
              </div>
              <div class="summary-card">
                <span class="summary-label">First audit</span>
                <span class="summary-value">{formatDateTime(summary()?.first_audit)}</span>
              </div>
            </div>

            <Show when={topDeficiencyCodes().length > 0}>
              <div class="summary-section">
                <h2>Most common conditions</h2>
                <ul class="tag-list">
                  <For each={topDeficiencyCodes()}>
                    {([code, count]) => (
                      <li>
                        <span class="tag">
                          {code}
                          <span class="tag-count">{count}</span>
                        </span>
                      </li>
                    )}
                  </For>
                </ul>
              </div>
            </Show>

            <Show when={message()}>
              {(msg) => <div class="success-banner" role="status">{msg()}</div>}
            </Show>
            <Show when={errorMessage()}>
              {(msg) => <div class="error-banner" role="alert">{msg()}</div>}
            </Show>
          </div>

          <div class="table-wrapper" role="region" aria-live="polite">
            <table>
              <thead>
                <tr>
                  <th scope="col">Device</th>
                  <th scope="col">Type</th>
                  <th scope="col">Bank</th>
                  <th scope="col">City ID</th>
                  <th scope="col">Controller</th>
                  <th scope="col">Tests</th>
                  <th scope="col">Open Deficiencies</th>
                  <th scope="col">Last Audit</th>
                  <th scope="col">Actions</th>
                </tr>
              </thead>
              <tbody>
                <For each={devices()}>
                  {(device: LocationDevice) => {
                    const total = device.total_deficiencies ?? 0;
                    const open = device.open_deficiencies ?? 0;
                    const controllerInfo = device.controller_install_year ?? device.controller_age
                      ? `${device.controller_install_year ?? '—'} (${device.controller_age ?? '—'} yrs)`
                      : '—';

                    return (
                      <tr>
                        <td>{device.device_id ?? '—'}</td>
                        <td>{device.device_type ?? '—'}</td>
                        <td>{device.bank_name ?? '—'}</td>
                        <td>{device.city_id ?? '—'}</td>
                        <td>{controllerInfo}</td>
                        <td>
                          <div class="status-stack">
                            <span>Cat 1: {device.cat1_tag_current === null ? '—' : device.cat1_tag_current ? '✓' : '✗'}</span>
                            <span>Cat 5: {device.cat5_tag_current === null ? '—' : device.cat5_tag_current ? '✓' : '✗'}</span>
                            <span>DLM: {device.dlm_compliant === null ? '—' : device.dlm_compliant ? '✓' : '✗'}</span>
                          </div>
                        </td>
                        <td>
                          <span class="deficiency-count">{open}</span>
                          <Show when={total > open}>
                            <span class="deficiency-note"> / {total}</span>
                          </Show>
                        </td>
                        <td>{formatDateTime(device.submitted_on)}</td>
                        <td>
                          <div class="action-group">
                            <button type="button" onClick={() => props.onSelectAudit(device.audit_uuid)}>
                              View audit
                            </button>
                          </div>
                        </td>
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

export default LocationDetail;
