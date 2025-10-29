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
  const [detail, { refetch }] = createResource<LocationDetailType, string>(() => props.address, fetchLocationDetail);
  let ownerInputRef: HTMLInputElement | undefined;

  const dismissMessage = () => setMessage(null);
  const dismissLastDownload = () => setLastDownload(null);

  const summary = createMemo(() => detail()?.summary);
  const devices = createMemo(() => detail()?.devices ?? []);
  const reports = createMemo<ReportVersion[]>(() => detail()?.reports ?? []);
  const deficiencyReports = createMemo<ReportVersion[]>(() => detail()?.deficiency_reports ?? []);
  const currentAuditId = createMemo(() => reports()[0]?.job_id ?? null);
  const currentDeficiencyId = createMemo(() => deficiencyReports()[0]?.job_id ?? null);
  const reportTimeline = createMemo(() => {
    const auditEntries = reports().map((report) => ({ type: 'audit' as const, report }));
    const deficiencyEntries = deficiencyReports().map((report) => ({ type: 'deficiency' as const, report }));
    const combined = [...auditEntries, ...deficiencyEntries];
    return combined.sort((a, b) => {
      const aTimestamp = a.report.completed_at ?? a.report.created_at ?? '';
      const bTimestamp = b.report.completed_at ?? b.report.created_at ?? '';
      const aDate = aTimestamp ? Date.parse(aTimestamp) : 0;
      const bDate = bTimestamp ? Date.parse(bTimestamp) : 0;
      return bDate - aDate;
    });
  });

  const [showResolvedDeficiencies, setShowResolvedDeficiencies] = createSignal(false);
  const [pendingDeficiencyKey, setPendingDeficiencyKey] = createSignal<string | null>(null);
  const [showDeficiencySection, setShowDeficiencySection] = createSignal(false);
  const [collapsedDevices, setCollapsedDevices] = createSignal<Record<string, boolean>>({});

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
    if (status.status === 'failed') {
      setStatusMessage(null);
      setErrorMessage(status.error ?? `${label} generation failed`);
      setIsGenerating(false);
      stopPolling();
      return;
    }
    if (status.status === 'completed' && status.download_ready) {
      setStatusMessage(null);
      setMessage(`${label} ready for download.`);
      setIsGenerating(false);
      stopPolling();
      setLastDownload(null);
      void refetch();
      return;
    }
    setMessage(null);
    if (status.status === 'processing') {
      setStatusMessage(`${label} generation in progress…`);
    } else if (status.status === 'queued') {
      setStatusMessage(`${label} queued for generation…`);
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
      setStatusMessage(`${label} queued for generation…`);
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
        deficiencyOnly: isDeficiency
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
    try {
      setIsGenerating(true);
      const response = await createReportJob({
        address: props.address,
        deficiencyOnly: true
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

            <Show when={reportTimeline().length > 0}>
              <div class="summary-section">
                <h2>Generated reports</h2>
                <ul class="reports-list">
                  <For each={reportTimeline()}>
                    {(entry) => {
                      const report = entry.report;
                      const isCurrent =
                        (entry.type === 'audit' && currentAuditId() === report.job_id) ||
                        (entry.type === 'deficiency' && currentDeficiencyId() === report.job_id);
                      const versionLabel = report.version != null
                        ? `${entry.type === 'audit' ? 'Audit' : 'Deficiency'} v${report.version}`
                        : entry.type === 'audit' ? 'Audit' : 'Deficiency';
                      const buttonLabel = entry.type === 'audit' ? 'Report' : 'Deficiency list';
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
                          </div>
                          <div class="report-actions">
                            <span class="report-size">{report.size_bytes != null ? formatFileSize(report.size_bytes) : '—'}</span>
                            <button
                              type="button"
                              class="action-button"
                              onClick={() => void handleDownloadExisting(report.job_id, buttonLabel)}
                            >
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
                <div class="success-banner" role="status">
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
                {(device: LocationDevice, index) => {
                  const deviceKey = device.audit_uuid ?? device.device_id ?? `device-${index()}`;
                  const filtered = () =>
                    (device.deficiencies ?? []).filter((def) => showResolvedDeficiencies() || !def.resolved);
                  const totalDeficiencies = device.total_deficiencies ?? (device.deficiencies?.length ?? 0);
                  const openDeficiencies = device.open_deficiencies ?? (device.deficiencies ?? []).filter((def) => !def.resolved).length;
                  const collapsed = () => isDeviceCollapsed(deviceKey);
                  return (
                    <section class="deficiency-device-card">
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
                            {collapsed() ? 'Expand' : 'Collapse'}
                          </button>
                        </div>
                      </div>
                      <Show when={!collapsed()} fallback={<p class="deficiency-empty">Device collapsed.</p>}>
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
