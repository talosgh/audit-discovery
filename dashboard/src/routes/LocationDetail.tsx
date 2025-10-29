import type { Component } from 'solid-js';
import { For, Match, Show, Switch, createEffect, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { fetchLocationDetail, downloadReport, createReportJob, fetchReportJob } from '../api';
import LoadingIndicator from '../components/LoadingIndicator';
import ErrorMessage from '../components/ErrorMessage';
import { formatDateTime, formatFileSize, slugify } from '../utils';
import type { LocationDetail as LocationDetailType, LocationDevice, ReportJobStatus, ReportVersion } from '../types';

interface LocationDetailProps {
  address: string;
  onBack(): void;
  onSelectAudit(id: string): void;
}

const LocationDetail: Component<LocationDetailProps> = (props) => {
  const [message, setMessage] = createSignal<string | null>(null);
  const [errorMessage, setErrorMessage] = createSignal<string | null>(null);
  const [isGenerating, setIsGenerating] = createSignal(false);
  const [jobId, setJobId] = createSignal<string | null>(null);
  const [jobStatus, setJobStatus] = createSignal<ReportJobStatus | null>(null);
  const [statusMessage, setStatusMessage] = createSignal<string | null>(null);
  const [lastDownload, setLastDownload] = createSignal<{ jobId: string; filename: string } | null>(null);
  const [showReportModal, setShowReportModal] = createSignal(false);
  const [coverOwner, setCoverOwner] = createSignal('');
  const [coverStreet, setCoverStreet] = createSignal('');
  const [coverCity, setCoverCity] = createSignal('');
  const [coverState, setCoverState] = createSignal('');
  const [coverZip, setCoverZip] = createSignal('');
  const [coverContactName, setCoverContactName] = createSignal('');
  const [coverContactEmail, setCoverContactEmail] = createSignal('');
  const [narrativeSeed, setNarrativeSeed] = createSignal('');
  const [recommendationsSeed, setRecommendationsSeed] = createSignal('');
  const [formError, setFormError] = createSignal<string | null>(null);
  const [detail, { refetch }] = createResource<LocationDetailType, string>(() => props.address, fetchLocationDetail);
  let ownerInputRef: HTMLInputElement | undefined;

  const summary = createMemo(() => detail()?.summary);
  const devices = createMemo(() => detail()?.devices ?? []);
  const reports = createMemo<ReportVersion[]>(() => detail()?.reports ?? []);

  let pollTimer: number | null = null;

  const stopPolling = () => {
    if (pollTimer !== null) {
      window.clearInterval(pollTimer);
      pollTimer = null;
    }
  };

  const updateStatusFromPoll = (status: ReportJobStatus) => {
    setJobStatus(status);
    if (status.status === 'failed') {
      setStatusMessage(null);
      setErrorMessage(status.error ?? 'Report generation failed');
      setIsGenerating(false);
      stopPolling();
      return;
    }
    if (status.status === 'completed' && status.download_ready) {
      setStatusMessage(null);
      setMessage('Report ready for download.');
      setIsGenerating(false);
      stopPolling();
      setLastDownload(null);
      void refetch();
      return;
    }
    setMessage(null);
    if (status.status === 'processing') {
      setStatusMessage('Report generation in progress…');
    } else if (status.status === 'queued') {
      setStatusMessage('Report queued for generation…');
    } else {
      setStatusMessage(`Status: ${status.status}`);
    }
  };

  const startPolling = (id: string) => {
    stopPolling();
    setIsGenerating(true);
    const poll = async () => {
      try {
        const status = await fetchReportJob(id);
        updateStatusFromPoll(status);
      } catch (err) {
        setErrorMessage((err as Error).message);
        setIsGenerating(false);
        stopPolling();
      }
    };
    poll();
    pollTimer = window.setInterval(poll, 5000);
  };

  onCleanup(() => {
    stopPolling();
  });

  createEffect(() => {
    props.address;
    stopPolling();
    setJobId(null);
    setJobStatus(null);
    setIsGenerating(false);
    setMessage(null);
    setErrorMessage(null);
    setStatusMessage(null);
    setLastDownload(null);
    setShowReportModal(false);
    setFormError(null);
    setCoverOwner('');
    setCoverStreet('');
    setCoverCity('');
    setCoverState('');
    setCoverZip('');
    setCoverContactName('');
    setCoverContactEmail('');
    setNarrativeSeed('');
    setRecommendationsSeed('');
  });

  createEffect(() => {
    if (!showReportModal()) {
      return;
    }
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !isGenerating()) {
        setFormError(null);
        setShowReportModal(false);
      }
    };
    window.addEventListener('keydown', handleKey);
    onCleanup(() => window.removeEventListener('keydown', handleKey));
  });

  createEffect(() => {
    if (!showReportModal()) {
      return;
    }
    requestAnimationFrame(() => {
      if (showReportModal()) {
        ownerInputRef?.focus();
        ownerInputRef?.select();
      }
    });
  });

  const openReportModal = () => {
    if (isGenerating()) {
      return;
    }
    setMessage(null);
    setErrorMessage(null);
    setFormError(null);
    const ownerSuggestion = summary()?.building_owner ?? '';
    if (!coverOwner() && ownerSuggestion) {
      setCoverOwner(ownerSuggestion);
    }
    const addressSuggestion = summary()?.address ?? '';
    if (!coverStreet() && addressSuggestion) {
      setCoverStreet(addressSuggestion);
    }
    setShowReportModal(true);
  };

  const closeReportModal = () => {
    if (isGenerating()) {
      return;
    }
    setShowReportModal(false);
    setFormError(null);
  };

  const handleSubmitReportForm = async (event: SubmitEvent) => {
    event.preventDefault();
    if (isGenerating()) {
      return;
    }
    setFormError(null);

    const owner = coverOwner().trim();
    const street = coverStreet().trim();
    const city = coverCity().trim();
    const state = coverState().trim();
    const zip = coverZip().trim();
    const contactName = coverContactName().trim();
    const contactEmail = coverContactEmail().trim();
    const notesSeed = narrativeSeed().trim();
    const recsSeed = recommendationsSeed().trim();

    if (!owner || !street || !city || !state || !zip || !contactName || !contactEmail) {
      setFormError('Please complete all cover page fields.');
      return;
    }

    const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailPattern.test(contactEmail)) {
      setFormError('Please provide a valid contact email address.');
      return;
    }

    setCoverOwner(owner);
    setCoverStreet(street);
    setCoverCity(city);
    setCoverState(state);
    setCoverZip(zip);
    setCoverContactName(contactName);
    setCoverContactEmail(contactEmail);
    setNarrativeSeed(notesSeed);
    setRecommendationsSeed(recsSeed);

    setMessage(null);
    setErrorMessage(null);
    setJobStatus(null);
    setJobId(null);
    stopPolling();

    try {
      setIsGenerating(true);
      setStatusMessage('Report queued for generation…');
      setLastDownload(null);
      const response = await createReportJob({
        address: props.address,
        coverBuildingOwner: owner,
        coverStreet: street,
        coverCity: city,
        coverState: state,
        coverZip: zip,
        coverContactName: contactName,
        coverContactEmail: contactEmail,
        notes: notesSeed,
        recommendations: recsSeed
      });
      setJobId(response.job_id);
      setShowReportModal(false);
      setFormError(null);
      startPolling(response.job_id);
    } catch (error) {
      setIsGenerating(false);
      setStatusMessage(null);
      const message = error instanceof Error ? error.message : 'Failed to queue report';
      setErrorMessage(message);
      setFormError(message);
    }
  };

  const downloadReportFor = async (targetJobId: string) => {
    setErrorMessage(null);
    setMessage(null);
    setStatusMessage(null);
    try {
      const { blob, filename: serverFilename, contentType } = await downloadReport(targetJobId);
      const inferredExt = (() => {
        if (serverFilename) {
          const dot = serverFilename.lastIndexOf('.');
          if (dot !== -1) {
            return serverFilename.slice(dot + 1).toLowerCase();
          }
        }
        if (contentType && contentType.includes('zip')) return 'zip';
        if (contentType && contentType.includes('pdf')) return 'pdf';
        return 'zip';
      })();
      const fallbackName = `audit-report-${slugify(props.address)}.${inferredExt}`;
      const filename = serverFilename ?? fallbackName;
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      setLastDownload({ jobId: targetJobId, filename });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to download report';
      setErrorMessage(message);
    }
  };

  const handleDownloadReport = async () => {
    const id = jobId();
    if (!id) return;
    await downloadReportFor(id);
  };

  const handleDownloadExisting = async (id: string) => {
    await downloadReportFor(id);
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
            onClick={openReportModal}
          >
            {isGenerating() ? 'Generating…' : 'Generate Report'}
          </button>
          <Show when={jobStatus()?.download_ready}>
            <button type="button" class="action-button" onClick={handleDownloadReport}>
              Download Report
            </button>
          </Show>
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

            <Show when={reports().length > 0}>
              <div class="summary-section">
                <h2>Generated reports</h2>
                <ul class="reports-list">
                  <For each={reports()}>
                    {(report: ReportVersion, index) => {
                      const isCurrent = index() === 0;
                      return (
                        <li class={`report-item${isCurrent ? ' report-item--current' : ''}`}>
                          <div class="report-meta">
                            <span class="report-title">
                              Version {report.version ?? '—'}
                              <Show when={isCurrent}>
                                <span class="report-badge">Current</span>
                              </Show>
                            </span>
                            <span class="report-date">{formatDateTime(report.completed_at ?? report.created_at)}</span>
                          </div>
                          <div class="report-actions">
                            <span class="report-size">{formatFileSize(report.size_bytes)}</span>
                            <button type="button" class="action-button" onClick={() => void handleDownloadExisting(report.job_id)}>
                              Download
                            </button>
                          </div>
                        </li>
                      );
                    }}
                  </For>
                </ul>
              </div>
            </Show>

            <Show when={message()}>
              {(msg) => <div class="success-banner" role="status">{msg()}</div>}
            </Show>
            <Show when={lastDownload()}>
              {(info) => (
                <div class="success-banner" role="status">
                  <span>Report downloaded as {info().filename}</span>
                  <button type="button" class="link-button" onClick={() => void handleDownloadExisting(info().jobId)}>
                    Download again
                  </button>
                </div>
              )}
            </Show>
            <Show when={errorMessage()}>
              {(msg) => <div class="error-banner" role="alert">{msg()}</div>}
            </Show>
            <Show when={statusMessage()}>
              {(msg) => <div class="info-banner" role="status">{msg()}</div>}
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
      <Show when={showReportModal()}>
        <div
          class="report-modal"
          role="dialog"
          aria-modal="true"
          aria-labelledby="report-modal-title"
          onClick={closeReportModal}
        >
          <div class="report-modal-content" role="document" onClick={(event) => event.stopPropagation()}>
            <header class="report-modal-header">
              <h2 id="report-modal-title">Generate Report</h2>
              <p>Provide the cover page information for this location&apos;s report.</p>
            </header>
            <form class="report-modal-form" noValidate onSubmit={handleSubmitReportForm}>
              <div class="report-modal-grid">
                <label class="modal-field">
                  <span>Building Owner</span>
                  <input
                    ref={ownerInputRef}
                    type="text"
                    value={coverOwner()}
                    onInput={(event) => setCoverOwner(event.currentTarget.value)}
                    autoComplete="organization"
                    required
                  />
                </label>
                <label class="modal-field">
                  <span>Street Address</span>
                  <input
                    type="text"
                    value={coverStreet()}
                    onInput={(event) => setCoverStreet(event.currentTarget.value)}
                    autoComplete="address-line1"
                    required
                  />
                </label>
                <label class="modal-field">
                  <span>City</span>
                  <input
                    type="text"
                    value={coverCity()}
                    onInput={(event) => setCoverCity(event.currentTarget.value)}
                    autoComplete="address-level2"
                    required
                  />
                </label>
                <label class="modal-field">
                  <span>State</span>
                  <input
                    type="text"
                    value={coverState()}
                    onInput={(event) => setCoverState(event.currentTarget.value)}
                    autoComplete="address-level1"
                    maxLength={64}
                    required
                  />
                </label>
                <label class="modal-field">
                  <span>ZIP Code</span>
                  <input
                    type="text"
                    value={coverZip()}
                    onInput={(event) => setCoverZip(event.currentTarget.value)}
                    autoComplete="postal-code"
                    required
                  />
                </label>
                <label class="modal-field">
                  <span>Contact Name</span>
                  <input
                    type="text"
                    value={coverContactName()}
                    onInput={(event) => setCoverContactName(event.currentTarget.value)}
                    autoComplete="name"
                    required
                  />
                </label>
                <label class="modal-field">
                  <span>Contact Email</span>
                  <input
                    type="email"
                    value={coverContactEmail()}
                    onInput={(event) => setCoverContactEmail(event.currentTarget.value)}
                    autoComplete="email"
                    required
                  />
                </label>
              </div>
              <label class="modal-field modal-field--full">
                <span>Narrative Seed</span>
                <textarea
                  value={narrativeSeed()}
                  onInput={(event) => setNarrativeSeed(event.currentTarget.value)}
                  rows={4}
                  placeholder="Key observations to emphasize in the executive summary and findings."
                />
              </label>
              <label class="modal-field modal-field--full">
                <span>Recommendations Seed</span>
                <textarea
                  value={recommendationsSeed()}
                  onInput={(event) => setRecommendationsSeed(event.currentTarget.value)}
                  rows={4}
                  placeholder="Notes about remediation priorities or client directives."
                />
              </label>
              <Show when={formError()}>
                {(msg) => <p class="modal-error" role="alert">{msg()}</p>}
              </Show>
              <div class="report-modal-actions">
                <button type="button" class="modal-button" onClick={closeReportModal} disabled={isGenerating()}>
                  Cancel
                </button>
                <button type="submit" class="action-button" disabled={isGenerating()}>
                  {isGenerating() ? 'Submitting…' : 'Queue Report'}
                </button>
              </div>
            </form>
          </div>
        </div>
      </Show>
    </section>
  );
};

export default LocationDetail;
