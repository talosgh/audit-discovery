import type { Component } from 'solid-js';
import { For, Match, Show, Switch, createEffect, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { fetchLocationDetail, downloadReport, createReportJob, fetchReportJob, updateDeficiencyStatus } from '../api';
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
  const [lastDownload, setLastDownload] = createSignal<{ jobId: string; filename: string; label: string } | null>(null);
  const [reportMode, setReportMode] = createSignal<'full' | 'deficiency'>('full');
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
  const [includeAllVisits, setIncludeAllVisits] = createSignal(true);
  const [selectedVisitIds, setSelectedVisitIds] = createSignal<Set<string>>(new Set());
  const [detail, { refetch }] = createResource<LocationDetailType, string>(() => props.address, fetchLocationDetail);
  let ownerInputRef: HTMLInputElement | undefined;

  const dismissMessage = () => setMessage(null);
  const dismissLastDownload = () => setLastDownload(null);

  const summary = createMemo(() => detail()?.summary);
  const devices = createMemo(() => detail()?.devices ?? []);
  const auditReports = createMemo<ReportVersion[]>(() => detail()?.reports ?? []);
  const deficiencyReports = createMemo<ReportVersion[]>(() => detail()?.deficiency_reports ?? []);
  const profile = createMemo(() => detail()?.profile);
  const serviceSummary = createMemo(() => detail()?.service);
  const financialSummary = createMemo(() => detail()?.financial);
  const visits = createMemo(() => detail()?.visits ?? []);

  const [showResolvedDeficiencies, setShowResolvedDeficiencies] = createSignal(false);
  const [pendingDeficiencyKey, setPendingDeficiencyKey] = createSignal<string | null>(null);
  const [showDeficiencySection, setShowDeficiencySection] = createSignal(false);
  const [collapsedDevices, setCollapsedDevices] = createSignal<Record<string, boolean>>({});
  const selectedVisitList = createMemo(() => Array.from(selectedVisitIds()));
  const selectedVisitCount = createMemo(() => selectedVisitList().length);

  const clearVisitSelection = () => setSelectedVisitIds(() => new Set());

  const toggleVisitSelection = (visitId: string | null | undefined) => {
    if (!visitId) return;
    setSelectedVisitIds((prev) => {
      const next = new Set(prev);
      if (next.has(visitId)) {
        next.delete(visitId);
      } else {
        next.add(visitId);
      }
      return next;
    });
  };

  const setVisitSelection = (visitIds: string[]) => {
    setSelectedVisitIds(() => new Set(visitIds));
  };

  const selectLatestVisit = () => {
    const latest = visits()[0]?.visit_id;
    if (!latest) return;
    setIncludeAllVisits(false);
    setVisitSelection([latest]);
  };

  createEffect(() => {
    if (includeAllVisits()) {
      clearVisitSelection();
    }
  });

  let pollTimer: number | null = null;

  const stopPolling = () => {
    if (pollTimer !== null) {
      window.clearInterval(pollTimer);
      pollTimer = null;
    }
  };

  const updateStatusFromPoll = (status: ReportJobStatus) => {
    setJobStatus(status);
    const isDeficiency = Boolean(status.deficiency_only);
    const label = isDeficiency ? 'Deficiency list' : 'Report';
    const selectionDescriptor = status.include_all ? 'entire history' : `${status.selected_audit_count ?? 0} audits`;
    if (status.status === 'failed') {
      setStatusMessage(null);
      setErrorMessage(status.error ?? `${label} generation failed`);
      setIsGenerating(false);
      stopPolling();
      return;
    }
    if (status.status === 'completed' && status.download_ready) {
      setStatusMessage(null);
      setMessage(`${label} ready for download (${selectionDescriptor}).`);
      setIsGenerating(false);
      stopPolling();
      setLastDownload(null);
      void refetch();
      return;
    }
    setMessage(null);
    if (status.status === 'processing') {
      setStatusMessage(`${label} generation in progress (${selectionDescriptor})…`);
    } else if (status.status === 'queued') {
      setStatusMessage(`${label} queued for generation (${selectionDescriptor})…`);
    } else {
      setStatusMessage(`${label} status: ${status.status}`);
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
    setReportMode('full');
    setShowDeficiencySection(false);
    setShowResolvedDeficiencies(false);
    setIncludeAllVisits(true);
    clearVisitSelection();
  });

  createEffect(() => {
    const currentDevices = devices();
    setCollapsedDevices((prev) => {
      const next: Record<string, boolean> = {};
      currentDevices.forEach((device, index) => {
        const key = device.audit_uuid ?? device.device_id ?? `device-${index}`;
        next[key] = prev[key] ?? true;
      });
      return next;
    });
  });

  const isDeviceCollapsed = (key: string) => collapsedDevices()[key] ?? true;

  const toggleDeviceCollapsed = (key: string) => {
    setCollapsedDevices((prev) => ({
      ...prev,
      [key]: !(prev[key] ?? true)
    }));
  };

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

  const openReportModal = (mode: 'full' | 'deficiency') => {
    if (isGenerating()) {
      return;
    }
    setReportMode(mode);
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

    const mode = reportMode();
    const isDeficiency = mode === 'deficiency';
    const label = isDeficiency ? 'Deficiency list' : 'Report';

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

    const visitSelection = includeAllVisits() ? [] : selectedVisitList();
    if (!includeAllVisits() && visitSelection.length === 0) {
      setFormError('Select at least one visit to include in this report.');
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

    const selectionLabel = includeAllVisits() ? 'entire history' : `${visitSelection.length} visits`;

    try {
      setIsGenerating(true);
      setStatusMessage(`${label} queued for generation (${selectionLabel})…`);
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
        recommendations: recsSeed,
        deficiencyOnly: isDeficiency,
        visitIds: includeAllVisits() ? undefined : visitSelection
      });
      setJobId(response.job_id);
      setShowReportModal(false);
      setFormError(null);
      startPolling(response.job_id);
    } catch (error) {
      setIsGenerating(false);
      setStatusMessage(null);
      const message = error instanceof Error ? error.message : `Failed to queue ${label.toLowerCase()}`;
      setErrorMessage(message);
      setFormError(message);
    }
  };

  const handleGenerateDeficiencyList = async () => {
    if (isGenerating()) {
      return;
    }
    stopPolling();
    setReportMode('deficiency');
    setMessage(null);
    setErrorMessage(null);
    setStatusMessage('Deficiency list queued for generation…');
    setJobId(null);
    setJobStatus(null);
    setLastDownload(null);
    const visitSelection = includeAllVisits() ? [] : selectedVisitList();
    if (!includeAllVisits() && visitSelection.length === 0) {
      setIsGenerating(false);
      setStatusMessage(null);
      setErrorMessage('Select at least one visit to include in the deficiency list.');
      return;
    }
    try {
      const selectionLabel = includeAllVisits() ? 'entire history' : `${visitSelection.length} visits`;
      setIsGenerating(true);
      setStatusMessage(`Deficiency list queued for generation (${selectionLabel})…`);
      const response = await createReportJob({
        address: props.address,
        deficiencyOnly: true,
        visitIds: includeAllVisits() ? undefined : visitSelection
      });
      setFormError(null);
      setShowReportModal(false);
      setJobId(response.job_id);
      startPolling(response.job_id);
    } catch (error) {
      setIsGenerating(false);
      setStatusMessage(null);
      const message = error instanceof Error ? error.message : 'Failed to queue deficiency list';
      setErrorMessage(message);
    }
  };

  const downloadReportFor = async (targetJobId: string, label: string) => {
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
      setMessage(`${label} downloaded as ${filename}`);
      setLastDownload({ jobId: targetJobId, filename, label });
    } catch (error) {
      const message = error instanceof Error ? error.message : `Failed to download ${label.toLowerCase()}`;
      setErrorMessage(message);
    }
  };

  const handleDownloadReport = async () => {
    const id = jobId();
    if (!id) return;
    const status = jobStatus();
    const isDeficiency = Boolean(status?.deficiency_only);
    const label = isDeficiency ? 'Deficiency list' : 'Report';
    await downloadReportFor(id, label);
  };

  const handleDownloadExisting = async (id: string, label: string) => {
    await downloadReportFor(id, label);
  };

  const handleToggleDeficiency = async (auditId: string, deficiencyId: number, resolved: boolean) => {
    const key = `${auditId}:${deficiencyId}`;
    setPendingDeficiencyKey(key);
    setErrorMessage(null);
    setMessage(null);
    setStatusMessage(null);
    try {
      await updateDeficiencyStatus(auditId, deficiencyId, resolved);
      setMessage(resolved ? 'Deficiency marked closed.' : 'Deficiency reopened.');
      await refetch();
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to update deficiency status';
      setErrorMessage(message);
    } finally {
      setPendingDeficiencyKey(null);
    }
  };

  const topDeficiencyCodes = createMemo(() => {
    const summaryData = summary();
    if (!summaryData) return [] as Array<[string, number]>;
    const entries = Object.entries(summaryData.deficiencies_by_code ?? {});
    return entries.sort((a, b) => b[1] - a[1]).slice(0, 5);
  });

  const formatCurrency = (value: number | null | undefined) => {
    const numeric = typeof value === 'number' ? value : 0;
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(numeric);
  };

  const serviceTrend = createMemo(() => serviceSummary()?.monthly_trend ?? []);
  const serviceTrendMax = createMemo(() => {
    const trend = serviceTrend();
    if (trend.length === 0) return 0;
    return Math.max(...trend.map((point) => (typeof point.tickets === 'number' ? point.tickets : 0)));
  });

  const financialTrend = createMemo(() => financialSummary()?.monthly_trend ?? []);
  const financialTrendMax = createMemo(() => {
    const points = financialTrend();
    if (points.length === 0) return 0;
    return Math.max(...points.map((point) => (typeof point.spend === 'number' ? point.spend : 0)));
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
            onClick={() => openReportModal('full')}
          >
            {isGenerating() && reportMode() === 'full' ? 'Generating…' : 'Generate Report'}
          </button>
          <button
            type="button"
            class="action-button"
            disabled={isGenerating()}
            onClick={handleGenerateDeficiencyList}
          >
            {isGenerating() && reportMode() === 'deficiency' ? 'Generating…' : 'Generate Deficiency List'}
          </button>
          <Show when={jobStatus()?.download_ready}>
            {(status) => (
              <button type="button" class="action-button" onClick={handleDownloadReport}>
                {status().deficiency_only ? 'Download Deficiency List' : 'Download Report'}
              </button>
            )}
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
          <div class="location-dashboard">
            <section class="profile-card">
              <h2>Location Overview</h2>
              <div class="profile-grid">
                <div>
                  <span class="profile-label">Address</span>
                  <span class="profile-value">{profile()?.address_label ?? summary()?.address ?? props.address}</span>
                </div>
                <div>
                  <span class="profile-label">Site Name</span>
                  <span class="profile-value">{profile()?.site_name ?? '—'}</span>
                </div>
                <div>
                  <span class="profile-label">Owner</span>
                  <span class="profile-value">{profile()?.owner.name ?? summary()?.building_owner ?? '—'}</span>
                </div>
                <div>
                  <span class="profile-label">Operator</span>
                  <span class="profile-value">{profile()?.operator.name ?? '—'}</span>
                </div>
                <div>
                  <span class="profile-label">Vendor</span>
                  <span class="profile-value">{profile()?.vendor.name ?? summary()?.elevator_contractor ?? '—'}</span>
                </div>
                <div>
                  <span class="profile-label">Location ID</span>
                  <span class="profile-value">{profile()?.location_code ?? '—'}</span>
                </div>
              </div>
            </section>

            <section class="stat-grid">
              <div class="stat-card">
                <h3>Audit Coverage</h3>
                <dl>
                  <div>
                    <dt>Devices</dt>
                    <dd>{summary()?.device_count ?? 0}</dd>
                  </div>
                  <div>
                    <dt>Total Audits</dt>
                    <dd>{summary()?.audit_count ?? 0}</dd>
                  </div>
                  <div>
                    <dt>Last Audit</dt>
                    <dd>{formatDateTime(summary()?.last_audit)}</dd>
                  </div>
                  <div>
                    <dt>First Audit</dt>
                    <dd>{formatDateTime(summary()?.first_audit)}</dd>
                  </div>
                  <div>
                    <dt>Open Deficiencies</dt>
                    <dd class="warning">{summary()?.open_deficiencies ?? 0}</dd>
                  </div>
                </dl>
              </div>

              <div class="stat-card">
                <h3>Service Activity</h3>
                <dl>
                  <div>
                    <dt>Tickets</dt>
                    <dd>{serviceSummary()?.total_tickets ?? 0}</dd>
                  </div>
                  <div>
                    <dt>Total Hours</dt>
                    <dd>{(serviceSummary()?.total_hours ?? 0).toFixed(1)}</dd>
                  </div>
                  <div>
                    <dt>Last Service</dt>
                    <dd>{formatDateTime(serviceSummary()?.last_service)}</dd>
                  </div>
                </dl>
              </div>

              <div class="stat-card">
                <h3>Financial Snapshot</h3>
                <dl>
                  <div>
                    <dt>Total Records</dt>
                    <dd>{financialSummary()?.total_records ?? 0}</dd>
                  </div>
                  <div>
                    <dt>Total Spend</dt>
                    <dd>{formatCurrency(financialSummary()?.total_spend)}</dd>
                  </div>
                  <div>
                    <dt>Approved</dt>
                    <dd>{formatCurrency(financialSummary()?.approved_spend)}</dd>
                  </div>
                  <div>
                    <dt>Open</dt>
                    <dd>{formatCurrency(financialSummary()?.open_spend)}</dd>
                  </div>
                  <div>
                    <dt>Last Statement</dt>
                    <dd>{formatDateTime(financialSummary()?.last_statement)}</dd>
                  </div>
                </dl>
              </div>
            </section>

            <section class="insights-panel">
              <div class="insight-card">
                <h2>Deficiency Hotspots</h2>
                <Show when={topDeficiencyCodes().length > 0} fallback={<p class="muted">No deficiencies recorded yet.</p>}>
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
                </Show>
              </div>

              <div class="insight-card">
                <h2>Service Trends</h2>
                <Show when={serviceSummary()} fallback={<p class="muted">No service history recorded.</p>}>
                  <div class="trend-list">
                    <For each={serviceTrend()}>
                      {(point) => {
                        const width = serviceTrendMax() > 0 ? Math.max(8, ((point.tickets ?? 0) / serviceTrendMax()) * 100) : 8;
                        return (
                          <div class="trend-row">
                            <span class="trend-label">{point.month ?? '—'}</span>
                            <div class="trend-bar"><div class="trend-fill" style={{ width: `${width}%` }} /></div>
                            <span class="trend-value">{point.tickets ?? 0}</span>
                          </div>
                        );
                      }}
                    </For>
                    <Show when={serviceSummary()?.top_problems?.length}>
                      <div class="trend-sublist">
                        <h3>Top Issues</h3>
                        <ul>
                          <For each={serviceSummary()?.top_problems ?? []}>
                            {(item) => (
                              <li>{item.problem ?? 'Unspecified'} — {item.count}</li>
                            )}
                          </For>
                        </ul>
                      </div>
                    </Show>
                  </div>
                </Show>
              </div>

              <div class="insight-card">
                <h2>Financial Overview</h2>
                <Show when={financialSummary()} fallback={<p class="muted">No financial records available.</p>}>
                  <div class="trend-list">
                    <For each={financialTrend()}>
                      {(point) => {
                        const width = financialTrendMax() > 0 ? Math.max(8, ((point.spend ?? 0) / financialTrendMax()) * 100) : 8;
                        return (
                          <div class="trend-row">
                            <span class="trend-label">{point.month ?? '—'}</span>
                            <div class="trend-bar"><div class="trend-fill" style={{ width: `${width}%` }} /></div>
                            <span class="trend-value">{formatCurrency(point.spend)}</span>
                          </div>
                        );
                      }}
                    </For>
                    <Show when={financialSummary()?.category_breakdown?.length}>
                      <div class="trend-sublist">
                        <h3>Top Categories</h3>
                        <ul>
                          <For each={financialSummary()?.category_breakdown ?? []}>
                            {(item) => (
                              <li>{item.category ?? 'Uncategorized'} — {formatCurrency(item.spend)}</li>
                            )}
                          </For>
                        </ul>
                      </div>
                    </Show>
                  </div>
                </Show>
              </div>
            </section>

            <section class="reports-section">
              <div>
                <h2>Audit Reports</h2>
                <Show when={auditReports().length > 0} fallback={<p class="muted">No audit reports generated yet.</p>}>
                  <ul class="reports-list">
                    <For each={auditReports()}>
                      {(report: ReportVersion, index) => {
                        const isCurrent = index() === 0;
                        const versionLabel = report.version != null ? `Audit v${report.version}` : 'Audit';
                        const selectionLabel = report.include_all !== false ? 'All visits' : `${report.selected_count ?? 0} audits`;
                        return (
                          <li class={`report-item${isCurrent ? ' report-item--current' : ''}`}>
                            <div class="report-meta">
                              <span class="report-title">
                                {versionLabel}
                                <Show when={isCurrent}>
                                  <span class="report-badge">Current</span>
                                </Show>
                              </span>
                              <span class="report-date">{formatDateTime(report.completed_at ?? report.created_at)}</span>
                              <span class="report-note">{selectionLabel}</span>
                            </div>
                            <div class="report-actions">
                              <span class="report-size">{report.size_bytes != null ? formatFileSize(report.size_bytes) : '—'}</span>
                              <button type="button" class="action-button" onClick={() => void handleDownloadExisting(report.job_id, 'Report')}>
                                Download
                              </button>
                            </div>
                          </li>
                        );
                      }}
                    </For>
                  </ul>
                </Show>
              </div>
              <div>
                <h2>Deficiency Lists</h2>
                <Show when={deficiencyReports().length > 0} fallback={<p class="muted">No deficiency lists generated yet.</p>}>
                  <ul class="reports-list">
                    <For each={deficiencyReports()}>
                      {(report: ReportVersion, index) => {
                        const isCurrent = index() === 0;
                        const versionLabel = report.version != null ? `Deficiency v${report.version}` : 'Deficiency';
                        const selectionLabel = report.include_all !== false ? 'All visits' : `${report.selected_count ?? 0} audits`;
                        return (
                          <li class={`report-item${isCurrent ? ' report-item--current' : ''}`}>
                            <div class="report-meta">
                              <span class="report-title">
                                {versionLabel}
                                <Show when={isCurrent}>
                                  <span class="report-badge">Current</span>
                                </Show>
                              </span>
                              <span class="report-date">{formatDateTime(report.completed_at ?? report.created_at)}</span>
                              <span class="report-note">{selectionLabel}</span>
                            </div>
                            <div class="report-actions">
                              <span class="report-size">{report.size_bytes != null ? formatFileSize(report.size_bytes) : '—'}</span>
                              <button type="button" class="action-button" onClick={() => void handleDownloadExisting(report.job_id, 'Deficiency list')}>
                                Download
                              </button>
                            </div>
                          </li>
                        );
                      }}
                    </For>
                  </ul>
                </Show>
              </div>
            </section>

            <section class="visit-card">
              <h2>Recent Visits</h2>
              <div class="visit-selection-controls">
                <label class="visit-toggle">
                  <input
                    type="checkbox"
                    checked={includeAllVisits()}
                    onChange={(event) => setIncludeAllVisits(event.currentTarget.checked)}
                  />
                  <span>Include all visits</span>
                </label>
                <Show when={!includeAllVisits()}>
                  <div class="visit-selection-toolbar">
                    <span class="visit-selection-count">{selectedVisitCount()} selected</span>
                    <div class="visit-selection-buttons">
                      <button type="button" onClick={selectLatestVisit}>Latest</button>
                      <button
                        type="button"
                        onClick={() => setVisitSelection(visits()
                          .map((visit) => visit.visit_id)
                          .filter((id): id is string => Boolean(id)))}
                      >
                        Select all
                      </button>
                      <button type="button" onClick={clearVisitSelection}>Clear</button>
                    </div>
                  </div>
                </Show>
              </div>
              <Show when={visits().length > 0} fallback={<p class="muted">No visits recorded yet.</p>}>
                <table class="visit-table">
                  <thead>
                    <tr>
                      <th scope="col">Include</th>
                      <th scope="col">Visit</th>
                      <th scope="col">Start</th>
                      <th scope="col">Completion</th>
                      <th scope="col">Audits</th>
                      <th scope="col">Devices</th>
                      <th scope="col">Open Deficiencies</th>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={visits()}>
                      {(visit) => {
                        const visitId = visit.visit_id ?? '';
                        const isChecked = visitId ? selectedVisitIds().has(visitId) : false;
                        return (
                          <tr>
                            <td>
                              <input
                                type="checkbox"
                                disabled={includeAllVisits() || !visitId}
                                checked={isChecked}
                                onChange={() => toggleVisitSelection(visitId)}
                              />
                            </td>
                            <td>{visit.visit_id?.slice(0, 8) ?? '—'}</td>
                            <td>{formatDateTime(visit.started_at)}</td>
                            <td>{formatDateTime(visit.completed_at)}</td>
                            <td>{visit.audit_count ?? 0}</td>
                            <td>{visit.device_count ?? 0}</td>
                            <td>{visit.open_deficiencies ?? 0}</td>
                          </tr>
                        );
                      }}
                    </For>
                  </tbody>
                </table>
              </Show>
            </section>

            <div class="notification-stack">
              <Show when={message()}>
                {(msg) => (
                  <div class="success-banner" role="status">
                    <span>{msg()}</span>
                    <button type="button" class="banner-close" aria-label="Dismiss notification" onClick={dismissMessage}>
                      <span aria-hidden="true">&times;</span>
                      <span class="sr-only">Dismiss notification</span>
                    </button>
                  </div>
                )}
              </Show>
              <Show when={lastDownload()}>
                {(info) => (
                  <div class="info-banner" role="status">
                    <span>{info().label} downloaded as {info().filename}</span>
                    <button type="button" class="link-button" onClick={() => void downloadReportFor(info().jobId, info().label)}>
                      Download again
                    </button>
                    <button type="button" class="banner-close" aria-label="Dismiss download notification" onClick={dismissLastDownload}>
                      <span aria-hidden="true">&times;</span>
                      <span class="sr-only">Dismiss download notification</span>
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

            <div class="detail-card full-span deficiency-section">
              <div class="deficiency-header">
                <div>
                  <h2>Deficiencies by device</h2>
                  <p class="deficiency-subtitle">Manage open and resolved findings across every unit.</p>
                </div>
                <div class="deficiency-header-actions">
                  <button
                    type="button"
                    class="deficiency-toggle"
                    onClick={() => setShowDeficiencySection((value) => !value)}
                  >
                    {showDeficiencySection() ? 'Collapse section' : 'Expand section'}
                  </button>
                  <button
                    type="button"
                    class="deficiency-toggle"
                    onClick={() => setShowResolvedDeficiencies((value) => !value)}
                    disabled={!showDeficiencySection()}
                  >
                    {showResolvedDeficiencies() ? 'Hide resolved' : 'Show resolved'}
                  </button>
                </div>
              </div>
              <Show when={showDeficiencySection()} fallback={<p class="deficiency-empty">Deficiency details are collapsed.</p>}>
                <Show when={devices().length > 0} fallback={<p class="deficiency-empty">No devices found.</p>}>
                  <For each={devices()}>
                    {(device: LocationDevice) => {
                      const filtered = (device.deficiencies ?? []).filter((item) =>
                        showResolvedDeficiencies() ? true : !item.resolved
                      );
                      const totalDeficiencies = device.deficiencies?.length ?? 0;
                      const openDeficiencies = device.deficiencies?.filter((item) => !item.resolved).length ?? 0;
                      const deviceKey = device.audit_uuid ?? device.device_id ?? 'device';
                      const collapsed = isDeviceCollapsed(deviceKey);

                      return (
                        <section class="deficiency-device" data-collapsed={collapsed}>
                          <div class="deficiency-device-heading">
                            <div>
                              <div class="deficiency-device-title">Unit {device.device_id ?? '—'}</div>
                              <div class="deficiency-device-meta">
                                <span>{device.device_type ?? '—'}</span>
                                <span>{device.bank_name ?? '—'}</span>
                              </div>
                            </div>
                            <div class="deficiency-device-controls">
                              <span class="deficiency-count-badge">Open {openDeficiencies} / {totalDeficiencies}</span>
                              <button
                                type="button"
                                class="deficiency-device-toggle"
                                onClick={() => toggleDeviceCollapsed(deviceKey)}
                              >
                                {collapsed ? 'Expand' : 'Collapse'}
                              </button>
                            </div>
                          </div>
                          <Show when={!collapsed} fallback={<p class="deficiency-empty">Device collapsed.</p>}>
                            <Show
                              when={filtered().length > 0}
                              fallback={<p class="deficiency-empty">No deficiencies to display.</p>}
                            >
                              <ul class="deficiency-list">
                                <For each={filtered()}>
                                  {(def) => {
                                    const key = `${device.audit_uuid}:${def.id}`;
                                    return (
                                      <li class={`deficiency-item${def.resolved ? ' deficiency-item--resolved' : ''}`}>
                                        <div class="deficiency-main">
                                          <div class="deficiency-title">{def.condition ?? 'Condition unspecified'}</div>
                                          <div class="deficiency-meta">
                                            <Show when={def.condition_code}>
                                              {(code) => <span class="deficiency-code">{code()}</span>}
                                            </Show>
                                            <Show when={def.equipment}>
                                              {(equipment) => <span>{equipment()}</span>}
                                            </Show>
                                            <span>Audit ID {device.audit_uuid.slice(0, 8)}…</span>
                                            <Show when={def.resolved && def.resolved_at}>
                                              {(resolvedAt) => <span>Closed {formatDateTime(resolvedAt())}</span>}
                                            </Show>
                                          </div>
                                          <Show when={def.remedy}>
                                            {(remedy) => (
                                              <p class="deficiency-note">
                                                <strong>Remedy:</strong> {remedy()}
                                              </p>
                                            )}
                                          </Show>
                                          <Show when={def.note}>
                                            {(note) => <p class="deficiency-note">{note()}</p>}
                                          </Show>
                                        </div>
                                        <div class="deficiency-actions">
                                          <button
                                            type="button"
                                            class="deficiency-action-button"
                                            aria-label={def.resolved ? 'Reopen deficiency' : 'Mark deficiency closed'}
                                            disabled={pendingDeficiencyKey() === key}
                                            onClick={() => void handleToggleDeficiency(device.audit_uuid, def.id, !def.resolved)}
                                          >
                                            {pendingDeficiencyKey() === key
                                              ? 'Updating…'
                                              : def.resolved
                                              ? 'Reopen'
                                              : 'Mark closed'}
                                          </button>
                                        </div>
                                      </li>
                                    );
                                  }}
                                </For>
                              </ul>
                            </Show>
                          </Show>
                        </section>
                      );
                    }}
                  </For>
                </Show>
              </Show>
            </div>

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
