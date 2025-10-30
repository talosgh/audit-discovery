import type { Component } from 'solid-js';
import { For, Match, Show, Switch, createEffect, createMemo, createResource, createSignal, onCleanup } from 'solid-js';
import { fetchLocationDetail, downloadReport, createReportJob, fetchReportJob, updateDeficiencyStatus, type LocationQuery } from '../api';
import LoadingIndicator from '../components/LoadingIndicator';
import ErrorMessage from '../components/ErrorMessage';
import { formatCurrency, formatDateTime, formatFileSize, slugify } from '../utils';
import type { CoverageStatus, LocationAnalytics, LocationDetail as LocationDetailType, LocationDevice, ReportJobStatus, ReportVersion } from '../types';

interface LocationSelection {
  address: string;
  locationRowId?: number | null;
  locationCode?: string | null;
  siteName?: string | null;
}

interface LocationDetailProps {
  location: LocationSelection;
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
  const [activePanel, setActivePanel] = createSignal<'overview' | 'deficiencies' | 'service' | 'financial'>('overview');
  const [detail, { refetch }] = createResource<LocationDetailType, LocationQuery>(
    () => ({ address: props.location.address, locationId: props.location.locationRowId ?? null }),
    fetchLocationDetail
  );
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
  const analytics = createMemo<LocationAnalytics | undefined>(() => detail()?.analytics);
  const analyticsOverview = createMemo(() => analytics()?.overview);
  const deficiencyAnalytics = createMemo(() => analytics()?.deficiencies);
  const serviceAnalyticsSection = createMemo(() => analytics()?.service);
  const financialAnalyticsSection = createMemo(() => analytics()?.financial);
  const deficiencyMetrics = createMemo(() => deficiencyAnalytics()?.metrics);
  const serviceMetrics = createMemo(() => serviceAnalyticsSection()?.metrics);
  const financialMetrics = createMemo(() => financialAnalyticsSection()?.metrics);
  const serviceTrendInfo = createMemo(() => serviceAnalyticsSection()?.trend);
  const financialTrendInfo = createMemo(() => financialAnalyticsSection()?.trend);
  const serviceActivitiesAnalytics = createMemo(() => serviceAnalyticsSection()?.activities ?? []);
  const financialSavingsTrendInfo = createMemo(() => financialAnalyticsSection()?.savings_trend);
  const deficiencyStatus = createMemo<CoverageStatus>(() => deficiencyAnalytics()?.status ?? 'missing');
  const serviceStatus = createMemo<CoverageStatus>(() => serviceAnalyticsSection()?.status ?? 'missing');
  const financialStatus = createMemo<CoverageStatus>(() => financialAnalyticsSection()?.status ?? 'missing');
  const serviceActivitySummary = createMemo(() => serviceSummary()?.activity_summary ?? []);
  const serviceActivityBreakdown = createMemo(() => serviceSummary()?.activity_breakdown ?? []);
  const financialSavingsTrend = createMemo(() => financialSummary()?.monthly_savings ?? []);
  const financialCumulativeSavings = createMemo(() => financialSummary()?.cumulative_savings ?? []);
  const financialSavingsMax = createMemo(() => {
    const points = financialSavingsTrend();
    if (!points.length) return 0;
    return Math.max(...points.map((point) => Math.abs(point.savings ?? 0)));
  });
  const financialCumulativeMax = createMemo(() => {
    const points = financialCumulativeSavings();
    if (!points.length) return 0;
    return Math.max(...points.map((point) => Math.abs(point.savings ?? 0)));
  });
  const totalSavings = createMemo(() => financialSummary()?.total_savings ?? 0);
  const savingsRatePercent = createMemo(() => (financialSummary()?.savings_rate ?? 0) * 100);
  const savingsPerDevice = createMemo(() => {
    const metrics = financialMetrics();
    return metrics?.savings_per_device ?? null;
  });
  const preventativeSharePercent = createMemo(() => {
    const summary = serviceActivitySummary();
    const preventative = summary.find((item) => item.category?.toLowerCase().includes('preventative'));
    return preventative ? preventative.share * 100 : null;
  });
  const [timelineAutoScrolled, setTimelineAutoScrolled] = createSignal(false);
  const [timelineHintVisible, setTimelineHintVisible] = createSignal(false);
  let timelineChartRef: HTMLDivElement | undefined;
  let timelineHintTimer: number | undefined;

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
    if (timelineHintTimer !== undefined) {
      window.clearTimeout(timelineHintTimer);
      timelineHintTimer = undefined;
    }
  });

  createEffect(() => {
    props.location.address;
    props.location.locationRowId;
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
    setActivePanel('overview');
    setTimelineAutoScrolled(false);
    setTimelineHintVisible(false);
    if (timelineHintTimer !== undefined) {
      window.clearTimeout(timelineHintTimer);
      timelineHintTimer = undefined;
    }
    if (timelineChartRef) {
      timelineChartRef.scrollLeft = 0;
    }
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
    const ownerSuggestion = summary()?.owner_name ?? summary()?.building_owner ?? '';
    if (!coverOwner() && ownerSuggestion) {
      setCoverOwner(ownerSuggestion);
    }
    const addressSuggestion = summary()?.street ?? summary()?.address ?? '';
    if (!coverStreet() && addressSuggestion) {
      setCoverStreet(addressSuggestion);
    }
    const citySuggestion = summary()?.city ?? '';
    if (!coverCity() && citySuggestion) {
      setCoverCity(citySuggestion);
    }
    const stateSuggestion = summary()?.state ?? '';
    if (!coverState() && stateSuggestion) {
      setCoverState(stateSuggestion);
    }
    const zipSuggestion = summary()?.zip ?? '';
    if (!coverZip() && zipSuggestion) {
      setCoverZip(zipSuggestion);
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
        address: props.location.address,
        locationRowId: detail()?.summary.location_row_id ?? props.location.locationRowId ?? null,
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
        address: props.location.address,
        locationRowId: detail()?.summary.location_row_id ?? props.location.locationRowId ?? null,
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
      const fallbackName = `audit-report-${slugify(props.location.address)}.${inferredExt}`;
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

  const formatCurrency = (value: number | string | null | undefined) => {
    if (value === null || value === undefined) return '—';
    const numeric = typeof value === 'number' ? value : Number(value);
    if (!Number.isFinite(numeric)) return '—';
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(numeric);
  };

  const serviceTrend = createMemo(() => serviceSummary()?.monthly_trend ?? []);
  const serviceTrendMax = createMemo(() => {
    const trend = serviceTrend();
    if (trend.length === 0) return 0;
    const totals = trend.map((point) => {
      const pm = Math.max(point.pm ?? 0, 0);
      const cbEmergency = Math.max(point.cb_emergency ?? 0, 0);
      const cbEnv = Math.max(point.cb_env ?? 0, 0);
      const tst = Math.max(point.tst ?? 0, 0);
      const rp = Math.max(point.rp ?? 0, 0);
      const totalTickets = Math.max(point.tickets ?? pm + cbEmergency + cbEnv + tst + rp, 0);
      const tracked = pm + cbEmergency + cbEnv + tst + rp;
      const other = Math.max(totalTickets - tracked, 0);
      return tracked + other;
    });
    return Math.max(1, ...totals);
  });

  const financialTrend = createMemo(() => financialSummary()?.monthly_trend ?? []);
  const financialTrendMax = createMemo(() => {
    const points = financialTrend();
    if (points.length === 0) return 0;
    return Math.max(...points.map((point) => (typeof point.spend === 'number' ? point.spend : 0)));
  });

  const totalDevices = createMemo(() => {
    const analyticsDevices = analyticsOverview()?.device_count;
    if (typeof analyticsDevices === 'number' && !Number.isNaN(analyticsDevices)) {
      return analyticsDevices;
    }
    const profileDevices = profile()?.device_count;
    if (typeof profileDevices === 'number' && !Number.isNaN(profileDevices)) {
      return profileDevices;
    }
    const summaryDevices = summary()?.device_count;
    if (typeof summaryDevices === 'number' && !Number.isNaN(summaryDevices)) {
      return summaryDevices;
    }
    return devices().length;
  });

  const totalDeficiencies = createMemo(() => deficiencyAnalytics()?.metrics.total ?? summary()?.total_deficiencies ?? 0);
  const openDeficiencies = createMemo(() => deficiencyAnalytics()?.metrics.open ?? summary()?.open_deficiencies ?? 0);
  const deficiencyClosureRate = createMemo(() => {
    const rate = deficiencyAnalytics()?.metrics.closure_rate;
    if (typeof rate === 'number' && !Number.isNaN(rate)) {
      return rate * 100;
    }
    const total = totalDeficiencies();
    if (total === 0) return 0;
    const closed = total - openDeficiencies();
    return (closed / total) * 100;
  });

  const serviceTickets = createMemo(() => serviceAnalyticsSection()?.metrics.tickets ?? serviceSummary()?.total_tickets ?? 0);
  const serviceHours = createMemo(() => serviceAnalyticsSection()?.metrics.hours ?? serviceSummary()?.total_hours ?? 0);
  const financialTotals = createMemo(() => ({
    total: financialAnalyticsSection()?.metrics.total_spend ?? financialSummary()?.total_spend ?? 0,
    proposed: financialAnalyticsSection()?.metrics.proposed_spend ?? financialSummary()?.proposed_spend ?? 0,
    approved: financialAnalyticsSection()?.metrics.approved_spend ?? financialSummary()?.approved_spend ?? 0,
    open: financialAnalyticsSection()?.metrics.open_spend ?? financialSummary()?.open_spend ?? 0,
    savings: financialAnalyticsSection()?.metrics.savings_total ?? financialSummary()?.total_savings ?? 0
  }));
  const proposedTotal = createMemo(() => financialTotals().proposed);
  const approvedTotal = createMemo(() => financialTotals().approved);
  const openTotal = createMemo(() => financialTotals().open);
  const actualSpend = createMemo(() => financialTotals().total);
  const negotiatedSavings = createMemo(() => financialTotals().savings ?? totalSavings());
  const savingsDeltaGap = createMemo(() => Math.abs((proposedTotal() - approvedTotal()) - negotiatedSavings()));
  const serviceCorrelation = createMemo(() => financialAnalyticsSection()?.service_correlation ?? null);
  const financialClassifications = createMemo(() => financialSummary()?.classification_breakdown ?? []);
  const financialTypes = createMemo(() => financialSummary()?.type_breakdown ?? []);
  const financialVendors = createMemo(() => financialSummary()?.vendor_breakdown ?? []);
  const financialWorkSummary = createMemo(() => financialSummary()?.work_summary ?? []);
const classificationTotal = createMemo(() => financialClassifications().reduce((sum, item) => sum + (item.spend ?? 0), 0));
const typeTotal = createMemo(() => financialTypes().reduce((sum, item) => sum + (item.spend ?? 0), 0));

const handleTimelineScroll = () => {
  if (timelineHintVisible()) {
    setTimelineHintVisible(false);
    if (timelineHintTimer !== undefined) {
      window.clearTimeout(timelineHintTimer);
      timelineHintTimer = undefined;
    }
  }
};

const fallbackTimeline = createMemo(() => detail()?.timeline ?? null);
const timelineBundle = createMemo(() => analytics()?.timeline ?? fallbackTimeline() ?? null);
const timelineData = createMemo(() => timelineBundle()?.data ?? []);
const serviceTotals = createMemo(() => {
  const data = timelineData();
  if (!data.length) return [] as number[];
  return data.map((point) => {
    const pm = Math.max(point.pm ?? 0, 0);
    const cbEmergency = Math.max(point.cb_emergency ?? 0, 0);
    const cbEnv = Math.max(point.cb_env ?? 0, 0);
    const cbOther = Math.max(point.cb_other ?? 0, 0);
    const tst = Math.max(point.tst ?? 0, 0);
    const rp = Math.max(point.rp ?? 0, 0);
    const misc = Math.max(point.misc ?? 0, 0);
    return point.total ?? pm + cbEmergency + cbEnv + cbOther + tst + rp + misc;
  });
});
const financeTotals = createMemo(() => {
  const data = timelineData();
  if (!data.length) return [] as number[];
  return data.map((point) => {
    const bc = Math.max(point.bc ?? 0, 0);
    const opex = Math.max(point.opex ?? 0, 0);
    const capex = Math.max(point.capex ?? 0, 0);
    const other = Math.max(point.other ?? 0, 0);
    return bc + opex + capex + other;
  });
});
const timelineMaxVisits = createMemo(() => {
  const totals = serviceTotals();
  if (!totals.length) return 1;
  return Math.max(1, ...totals);
});
const timelineMaxSpend = createMemo(() => {
  const data = timelineData();
  if (!data.length) return 0;
  const spendTotals = data.map((point) => Math.max(point.spend ?? 0, 0));
  const financeBuckets = financeTotals();
  const combined = spendTotals.concat(financeBuckets.length ? financeBuckets : []);
  return combined.length ? Math.max(...combined) : 0;
});
const hasServiceData = createMemo(() => serviceTotals().some((total) => total > 0));
const hasFinanceData = createMemo(() => financeTotals().some((total) => total > 0));
const serviceSegmentLabels = {
  pm: 'Preventative maintenance',
  cbEmergency: 'Emergency callbacks',
  cbEnv: 'Environmental callbacks',
  cbOther: 'Other callbacks',
  tst: 'Testing',
  rp: 'Repair / modernization',
  misc: 'Site visits / misc'
} as const;

  const financialSegmentLabels = {
    bc: 'Base contract spend',
    opex: 'Operational expenses',
  capex: 'Capital expenditures',
  other: 'Other spend'
} as const;

const showService = createMemo(() => hasServiceData());
const showFinance = createMemo(() => hasFinanceData());

  const timelineGeometry = createMemo(() => {
    const data = timelineData();
    const maxSpend = timelineMaxSpend();
    const maxVisits = timelineMaxVisits();
    const financePresence = showFinance();
    const columnWidth = 70;
    const columnGap = 26;
    const baseHeight = maxVisits * 28;
    const financeHeight = financePresence && maxSpend > 0 ? 220 : 0;
    const chartHeight = Math.min(360, Math.max(160, Math.max(baseHeight, financeHeight)));
    const segments: string[] = [];
    const financeSegments = {
      total: [] as string[],
      bc: [] as string[],
      opex: [] as string[],
      capex: [] as string[],
      other: [] as string[]
    };
    const financeActive = {
      total: false,
      bc: false,
      opex: false,
      capex: false,
      other: false
    };
    const points: Array<{ x: number; y: number; spend: number }> = [];
    data.forEach((point, index) => {
      const spendValue = point.spend ?? 0;
      const ratio = maxSpend > 0 ? Math.min(1, spendValue / maxSpend) : 0;
      const x = index * (columnWidth + columnGap) + columnWidth / 2;
      const y = chartHeight - ratio * chartHeight;
      segments.push(`${index === 0 ? 'M' : 'L'}${x},${y.toFixed(2)}`);
      points.push({ x, y, spend: spendValue });

      const bcValue = point.bc ?? 0;
      const opexValue = point.opex ?? 0;
      const capexValue = point.capex ?? 0;
      const otherValue = point.other ?? 0;

      const pushFinance = (type: keyof typeof financeSegments, value: number) => {
        const ratioValue = maxSpend > 0 ? Math.min(1, value / maxSpend) : 0;
        const yValue = chartHeight - ratioValue * chartHeight;
        financeSegments[type].push(`${index === 0 ? 'M' : 'L'}${x},${yValue.toFixed(2)}`);
        if (value > 0) {
          financeActive[type] = true;
        }
      };

      pushFinance('total', spendValue);
      pushFinance('bc', bcValue);
      pushFinance('opex', opexValue);
      pushFinance('capex', capexValue);
      pushFinance('other', otherValue);
    });
    return {
      width: Math.max(data.length > 0 ? columnWidth * data.length + columnGap * Math.max(data.length - 1, 0) : columnWidth, columnWidth),
      height: chartHeight,
      columnWidth,
      columnGap,
      path: segments.join(' '),
      financePaths: {
        total: financeSegments.total.join(' '),
        bc: financeSegments.bc.join(' '),
        opex: financeSegments.opex.join(' '),
        capex: financeSegments.capex.join(' '),
        other: financeSegments.other.join(' ')
      },
      financeActive,
      points
    };
  });
  const formatMonthLabel = (bucket: string) => {
    if (!bucket) return '—';
    const [year, month] = bucket.split('-');
    const idx = Number(month) - 1;
    const names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    if (!Number.isFinite(idx) || idx < 0 || idx >= names.length) {
      return bucket;
    }
    return `${names[idx]} ${year}`;
  };
  const formatMonthRange = (start?: string | null, end?: string | null) => {
    if (!start || !end) {
      return null;
    }
    if (start === end) {
      return formatMonthLabel(start);
    }
    return `${formatMonthLabel(start)} – ${formatMonthLabel(end)}`;
  };

  createEffect(() => {
    const container = timelineChartRef;
    const data = timelineData();
    if (!container || data.length === 0) {
      return;
    }
    const geometry = timelineGeometry();
    const perColumn = geometry.columnWidth + geometry.columnGap || 0;
    const contentWidth = geometry.width;
    const viewportWidth = container.clientWidth;
    const inferredVisible = perColumn > 0 ? Math.max(Math.floor(viewportWidth / perColumn), 1) : 12;
    const windowSize = Math.max(12, inferredVisible);
    let lastServiceIndex = -1;
    let lastFinanceIndex = -1;
    data.forEach((point, index) => {
      const serviceTotal =
        (point.pm ?? 0) +
        (point.cb_emergency ?? 0) +
        (point.cb_env ?? 0) +
        (point.cb_other ?? 0) +
        (point.tst ?? 0) +
        (point.rp ?? 0) +
        (point.misc ?? 0);
      if (serviceTotal > 0) {
        lastServiceIndex = index;
      }
      const financeTotal = (point.bc ?? 0) + (point.opex ?? 0) + (point.capex ?? 0) + (point.other ?? 0);
      if (financeTotal > 0) {
        lastFinanceIndex = index;
      }
    });
    const lastActiveIndex = lastServiceIndex >= 0 ? lastServiceIndex : lastFinanceIndex;
    const targetIndex = lastActiveIndex >= 0 ? Math.max(lastActiveIndex - windowSize + 1, 0) : Math.max(data.length - windowSize, 0);
    const rawTarget = perColumn > 0 ? targetIndex * perColumn : 0;
    const maxScroll = Math.max(contentWidth - viewportWidth, 0);
    const targetScroll = Math.max(Math.min(rawTarget, maxScroll), 0);
    if (timelineAutoScrolled()) {
      return;
    }
    if (targetScroll === 0) {
      setTimelineAutoScrolled(true);
      return;
    }
    requestAnimationFrame(() => {
      if (!timelineAutoScrolled() && container) {
        container.scrollLeft = targetScroll;
        setTimelineAutoScrolled(true);
        if (targetScroll > 0) {
          setTimelineHintVisible(true);
          if (timelineHintTimer !== undefined) {
            window.clearTimeout(timelineHintTimer);
          }
          timelineHintTimer = window.setTimeout(() => {
            setTimelineHintVisible(false);
            timelineHintTimer = undefined;
          }, 4000);
        }
      }
    });
  });

  const serviceTimelineMonths = createMemo(() => {
    const data = timelineData();
    return data
      .filter((point) => {
        const total =
          (point.pm ?? 0) +
          (point.cb_emergency ?? 0) +
          (point.cb_env ?? 0) +
          (point.cb_other ?? 0) +
          (point.tst ?? 0) +
          (point.rp ?? 0) +
          (point.misc ?? 0);
        return total > 0;
      })
      .map((point) => point.month);
  });

  const financeTimelineMonths = createMemo(() => {
    const data = timelineData();
    return data
      .filter((point) => {
        const total = (point.bc ?? 0) + (point.opex ?? 0) + (point.capex ?? 0) + (point.other ?? 0);
        return total > 0;
      })
      .map((point) => point.month);
  });

  const serviceTimelineSummary = createMemo(() => {
    const months = serviceTimelineMonths();
    if (!months.length) return null;
    return { count: months.length, first: months[0], last: months[months.length - 1] };
  });

  const financeTimelineSummary = createMemo(() => {
    const months = financeTimelineMonths();
    if (!months.length) return null;
    return { count: months.length, first: months[0], last: months[months.length - 1] };
  });

  const summaryCardClass = (panel: 'deficiencies' | 'service' | 'financial', status?: CoverageStatus) => {
    const classes = ['summary-card'];
    if (status === 'missing') {
      classes.push('summary-card--inactive');
    } else if (status === 'partial') {
      classes.push('summary-card--partial');
    }
    if (activePanel() === panel) {
      classes.push('summary-card--active');
    }
    return classes.join(' ');
  };

  const formatPercent = (value: number | null | undefined) => (value == null || Number.isNaN(value) ? '—' : `${value.toFixed(1)}%`);
  const formatMaybeNumber = (value: number | null | undefined, decimals = 2) => (value == null || Number.isNaN(value) ? '—' : value.toFixed(decimals));
  const describeTrend = (trend?: { direction: string; percent_change: number | null }) => {
    if (!trend) {
      return '—';
    }
    if (trend.direction === 'insufficient') {
      return 'Insufficient data';
    }
    if (trend.direction === 'flat') {
      return 'Flat';
    }
    const label = trend.direction === 'up' ? 'Rising' : 'Declining';
    const change = trend.percent_change != null ? `${trend.percent_change.toFixed(1)}%` : '';
    return change ? `${label} (${change})` : label;
  };
  const formatShare = (value: number, total: number) => {
    if (!total || !Number.isFinite(total) || total <= 0) return '—';
    const ratio = (value / total) * 100;
    if (!Number.isFinite(ratio)) return '—';
    return `${ratio.toFixed(1)}%`;
  };

  const analyticsMetrics = createMemo(() => {
    const devicesCount = Math.max(totalDevices(), 1);
    const deficiencyMetrics = deficiencyAnalytics()?.metrics;
    const serviceMetrics = serviceAnalyticsSection()?.metrics;
    const financialMetrics = financialAnalyticsSection()?.metrics;
    const savingsRate = savingsRatePercent();
    const savingsPerDeviceValue = savingsPerDevice();
    const preventativeShare = preventativeSharePercent();
    const correlation = serviceCorrelation();

    const items = [
      {
        key: 'open-def',
        label: 'Open deficiencies per device',
        tooltip: 'Average open deficiencies divided by the total devices assigned to this location.',
        value: deficiencyMetrics?.open_per_device ?? 0,
        formatted: deficiencyMetrics?.open_per_device != null ? deficiencyMetrics.open_per_device.toFixed(2) : '—',
        hasValue: deficiencyMetrics?.open_per_device != null
      },
      {
        key: 'closure-rate',
        label: 'Deficiency closure rate',
        tooltip: 'Percentage of recorded deficiencies that have been resolved.',
        value: deficiencyMetrics?.closure_rate != null ? deficiencyMetrics.closure_rate * 100 : 0,
        formatted: deficiencyMetrics?.closure_rate != null ? `${(deficiencyMetrics.closure_rate * 100).toFixed(1)}%` : '—',
        hasValue: deficiencyMetrics?.closure_rate != null
      },
      {
        key: 'tickets-device',
        label: 'Service tickets per device',
        tooltip: 'Average number of service tickets logged per device (ticket volume ÷ devices).',
        value: serviceMetrics?.per_device ?? (serviceTickets() / devicesCount),
        formatted: serviceMetrics?.per_device != null ? serviceMetrics.per_device.toFixed(2) : '—',
        hasValue: serviceMetrics?.per_device != null
      },
      {
        key: 'preventative-share',
        label: 'Preventative maintenance share',
        tooltip: 'Percentage of service tickets classified as preventative maintenance (PM).',
        value: preventativeShare ?? 0,
        formatted: preventativeShare != null ? `${preventativeShare.toFixed(1)}%` : '—',
        hasValue: preventativeShare != null
      },
      {
        key: 'spend-device',
        label: 'Annual spend per device',
        tooltip: 'Total approved spend divided by devices under management.',
        value: financialMetrics?.per_device ?? (financialTotals().total / devicesCount),
        formatted: financialMetrics?.per_device != null ? formatCurrency(financialMetrics.per_device) : '—',
        hasValue: financialMetrics?.per_device != null
      },
      {
        key: 'savings-device',
        label: 'Savings per device',
        tooltip: 'Negotiated savings (delta) divided by devices at this location.',
        value: savingsPerDeviceValue ?? 0,
        formatted: savingsPerDeviceValue != null ? formatCurrency(savingsPerDeviceValue) : '—',
        hasValue: savingsPerDeviceValue != null
      },
      {
        key: 'savings-rate',
        label: 'Savings rate',
        tooltip: 'Percentage of proposed spend reduced through negotiation or dismissal.',
        value: savingsRate ?? 0,
        formatted: savingsRate != null ? `${(savingsRate).toFixed(1)}%` : '—',
        hasValue: savingsRate != null
      }
    ];

    const serviceTimeline = serviceTimelineSummary();
    if (serviceTimeline) {
      const rangeLabel = formatMonthRange(serviceTimeline.first, serviceTimeline.last);
      items.push({
        key: 'service-timeline',
        label: 'Service months analyzed',
        tooltip: rangeLabel
          ? `Months with recorded service activity in the timeline (${rangeLabel}).`
          : 'Months with recorded service activity in the timeline.',
        value: serviceTimeline.count,
        formatted: `${serviceTimeline.count}`,
        hasValue: true
      });
    }

    const financeTimeline = financeTimelineSummary();
    if (financeTimeline) {
      const rangeLabel = formatMonthRange(financeTimeline.first, financeTimeline.last);
      items.push({
        key: 'finance-timeline',
        label: 'Financial months analyzed',
        tooltip: rangeLabel
          ? `Months with recorded spend in the timeline (${rangeLabel}).`
          : 'Months with recorded spend in the timeline.',
        value: financeTimeline.count,
        formatted: `${financeTimeline.count}`,
        hasValue: true
      });
    }

    if (correlation && typeof correlation.coefficient === 'number' && !Number.isNaN(correlation.coefficient)) {
      items.push({
        key: 'callbacks-spend-corr',
        label: 'Callbacks vs spend correlation',
        tooltip: 'Pearson correlation between monthly callback volume and spend (−1 to 1).',
        value: correlation.coefficient,
        formatted: correlation.coefficient.toFixed(2),
        hasValue: true
      });
    }

    return items;
  });

  const analyticsMaxValue = createMemo(() => {
    const values = analyticsMetrics().map((metric) => (Number.isFinite(metric.value) ? Math.abs(metric.value) : 0));
    if (values.length === 0) {
      return 1;
    }
    const max = Math.max(...values);
    return max > 0 ? max : 1;
  });

  const performanceSeries = createMemo(() => {
    const ranked = devices()
      .map((device) => {
        const open = device.open_deficiencies ?? 0;
        const total = device.total_deficiencies ?? 0;
        const compliancePenalty = (device.cat1_tag_current === false ? 1 : 0)
          + (device.cat5_tag_current === false ? 1 : 0)
          + (device.dlm_compliant === false ? 1 : 0);
        const agePenalty = device.controller_age ? device.controller_age / 10 : 0;
        const score = open * 4 + (total - open) * 2 + compliancePenalty * 3 + agePenalty;
        return { device, score };
      })
      .sort((a, b) => b.score - a.score)
      .slice(0, 5);
    const maxScore = ranked.length > 0 ? Math.max(...ranked.map((item) => item.score)) : 1;
    return { maxScore: maxScore || 1, ranked };
  });

  const worstUnits = createMemo(() => {
    const devicesList = [...devices()];
    devicesList.sort((a, b) => (b.open_deficiencies ?? 0) - (a.open_deficiencies ?? 0));
    return devicesList.slice(0, 3);
  });

  const costlyUnits = createMemo(() => performanceSeries().ranked.slice(0, 3));

  const financialCostDrivers = createMemo(() => (financialSummary()?.category_breakdown ?? []).slice(0, 3));

  const hasDeficiencies = createMemo(() => totalDeficiencies() > 0);

  const combinedStateZip = createMemo(() => {
    const state = summary()?.state ?? profile()?.state ?? '';
    const zip = summary()?.zip ?? profile()?.zip ?? '';
    if (state && zip) return `${state} ${zip}`;
    if (state) return state;
    if (zip) return zip;
    return '—';
  });

  return (
    <section class="page-section" aria-labelledby="location-heading">
      <div class="section-header">
        <div>
          <button type="button" class="back-button" onClick={props.onBack}>
            ← Locations
          </button>
          <h1 id="location-heading">
            {detail()?.profile?.site_name ?? summary()?.site_name ?? props.location.siteName ?? summary()?.address ?? props.location.address}
          </h1>
          <Show when={detail()?.profile?.address_label ?? summary()?.address ?? props.location.address}>
            {(addr) => <p class="section-subtitle">{addr()}</p>}
          </Show>
          <Show when={detail()?.profile?.owner.name ?? summary()?.owner_name}>
            {(owner) => <p class="section-subtitle">Owner: {owner()}</p>}
          </Show>
          <Show when={detail()?.profile?.vendor.name ?? summary()?.vendor_name}>
            {(vendor) => <p class="section-subtitle">Vendor: {vendor()}</p>}
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
                  <span class="profile-label">Site Name</span>
                  <span class="profile-value">{profile()?.site_name ?? summary()?.site_name ?? props.location.siteName ?? '—'}</span>
                </div>
                <div>
                  <span class="profile-label">Location Code</span>
                  <span class="profile-value">{profile()?.location_code ?? summary()?.location_code ?? props.location.locationCode ?? '—'}</span>
                </div>
                <div>
                  <span class="profile-label">Devices</span>
                  <span class="profile-value">{analyticsOverview()?.device_count ?? profile()?.device_count ?? summary()?.device_count ?? 0}</span>
                </div>
                <div>
                  <span class="profile-label">Address</span>
                  <span class="profile-value">{profile()?.address_label ?? summary()?.address ?? props.location.address}</span>
                </div>
                <div>
                  <span class="profile-label">City</span>
                  <span class="profile-value">{summary()?.city ?? profile()?.city ?? '—'}</span>
                </div>
                <div>
                  <span class="profile-label">State / ZIP</span>
                  <span class="profile-value">{combinedStateZip()}</span>
                </div>
                <div>
                  <span class="profile-label">Owner</span>
                  <span class="profile-value">{profile()?.owner.name ?? summary()?.owner_name ?? '—'}</span>
                </div>
                <div>
                  <span class="profile-label">Operator</span>
                  <span class="profile-value">{profile()?.operator.name ?? summary()?.operator_name ?? '—'}</span>
                </div>
                <div>
                  <span class="profile-label">Vendor</span>
                  <span class="profile-value">{profile()?.vendor.name ?? summary()?.vendor_name ?? '—'}</span>
                </div>
              </div>
            </section>

            <Show when={showService() || showFinance()}>
              <section class="timeline-card" aria-labelledby="timeline-title">
                <header class="timeline-header">
                  <h2 id="timeline-title">Service &amp; Spend Timeline</h2>
                  <p>Monthly preventative maintenance (PM) and callback activity compared with invoiced spend.</p>
                </header>
                <Show when={timelineData().length > 0} fallback={<p class="muted">No combined service or financial history recorded yet.</p>}>
                  {(() => {
                    const data = timelineData();
                    const maxVisits = timelineMaxVisits();
                    const geometry = timelineGeometry();
                    const maxSpend = timelineMaxSpend();
                    return (
                      <>
                        <Show when={showService() || showFinance()}>
                          <div class="timeline-legend">
                            <Show when={showService()}>
                              <div class="timeline-legend-row" aria-label="Service legend">
                                <span class="legend-item legend-heading">Service</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--pm" /> {serviceSegmentLabels.pm}</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--cb-emergency" /> {serviceSegmentLabels.cbEmergency}</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--cb-env" /> {serviceSegmentLabels.cbEnv}</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--cb-other" /> {serviceSegmentLabels.cbOther}</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--tst" /> {serviceSegmentLabels.tst}</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--rp" /> {serviceSegmentLabels.rp}</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--misc" /> {serviceSegmentLabels.misc}</span>
                              </div>
                            </Show>
                            <Show when={showFinance()}>
                              <div class="timeline-legend-row" aria-label="Financial legend">
                                <span class="legend-item legend-heading">Financial</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--finance-bc" /> {financialSegmentLabels.bc}</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--finance-opex" /> {financialSegmentLabels.opex}</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--finance-capex" /> {financialSegmentLabels.capex}</span>
                                <span class="legend-item"><span class="legend-swatch legend-swatch--finance-other" /> {financialSegmentLabels.other}</span>
                                <span class="legend-item"><span class="legend-line" /> Total spend</span>
                              </div>
                            </Show>
                          </div>
                        </Show>
                        <div
                          class="timeline-chart"
                          role="img"
                          aria-label="Timeline of service visits and spend"
                          ref={(el) => {
                            timelineChartRef = el;
                          }}
                          onScroll={handleTimelineScroll}
                        >
                          <Show when={timelineHintVisible()}>
                            <div class="timeline-hint">
                              Latest activity <span aria-hidden="true">→</span>
                            </div>
                          </Show>
                          <div class="timeline-content" style={{ width: `${geometry.width}px` }}>
                            <Show when={showService() || showFinance()}>
                              <div class="timeline-bars" style={{ height: `${geometry.height}px` }}>
                                <For each={data}>
                                  {(entry) => {
                                    const pmValue = Math.max(entry.pm ?? 0, 0);
                                    const cbEmergencyValue = Math.max(entry.cb_emergency ?? 0, 0);
                                    const cbEnvValue = Math.max(entry.cb_env ?? 0, 0);
                                    const cbOtherValue = Math.max(entry.cb_other ?? 0, 0);
                                    const tstValue = Math.max(entry.tst ?? 0, 0);
                                    const rpValue = Math.max(entry.rp ?? 0, 0);
                                    const miscValue = Math.max(entry.misc ?? 0, 0);
                                    const totalValue = Math.max(entry.total ?? pmValue + cbEmergencyValue + cbEnvValue + cbOtherValue + tstValue + rpValue + miscValue, 0);
                                    const serviceSegments = [
                                      { value: miscValue, className: 'timeline-bar--misc', label: serviceSegmentLabels.misc },
                                      { value: rpValue, className: 'timeline-bar--rp', label: serviceSegmentLabels.rp },
                                      { value: tstValue, className: 'timeline-bar--tst', label: serviceSegmentLabels.tst },
                                      { value: cbEnvValue, className: 'timeline-bar--cb-env', label: serviceSegmentLabels.cbEnv },
                                      { value: cbOtherValue, className: 'timeline-bar--cb-other', label: serviceSegmentLabels.cbOther },
                                      { value: cbEmergencyValue, className: 'timeline-bar--cb-emergency', label: serviceSegmentLabels.cbEmergency },
                                      { value: pmValue, className: 'timeline-bar--pm', label: serviceSegmentLabels.pm }
                                    ];
                                    const bcSpend = Math.max(entry.bc ?? 0, 0);
                                    const opexSpend = Math.max(entry.opex ?? 0, 0);
                                    const capexSpend = Math.max(entry.capex ?? 0, 0);
                                    const otherSpend = Math.max(entry.other ?? 0, 0);
                                    const financeSegments = [
                                      { value: bcSpend, className: 'timeline-bar--finance-bc', label: financialSegmentLabels.bc },
                                      { value: opexSpend, className: 'timeline-bar--finance-opex', label: financialSegmentLabels.opex },
                                      { value: capexSpend, className: 'timeline-bar--finance-capex', label: financialSegmentLabels.capex },
                                      { value: otherSpend, className: 'timeline-bar--finance-other', label: financialSegmentLabels.other }
                                    ];
                                    const spendValue = entry.spend ?? 0;
                                    const columnTitleParts = [];
                                    columnTitleParts.push(`${serviceSegmentLabels.pm}: ${pmValue}`);
                                    columnTitleParts.push(`${serviceSegmentLabels.cbEmergency}: ${cbEmergencyValue}`);
                                    columnTitleParts.push(`${serviceSegmentLabels.cbEnv}: ${cbEnvValue}`);
                                    columnTitleParts.push(`${serviceSegmentLabels.cbOther}: ${cbOtherValue}`);
                                    columnTitleParts.push(`${serviceSegmentLabels.tst}: ${tstValue}`);
                                    columnTitleParts.push(`${serviceSegmentLabels.rp}: ${rpValue}`);
                                    columnTitleParts.push(`${serviceSegmentLabels.misc}: ${miscValue}`);
                                    columnTitleParts.push(`Total tracked: ${totalValue}`);
                                    columnTitleParts.push(`${financialSegmentLabels.bc}: ${formatCurrency(bcSpend)}`);
                                    columnTitleParts.push(`${financialSegmentLabels.opex}: ${formatCurrency(opexSpend)}`);
                                    columnTitleParts.push(`${financialSegmentLabels.capex}: ${formatCurrency(capexSpend)}`);
                                    columnTitleParts.push(`${financialSegmentLabels.other}: ${formatCurrency(otherSpend)}`);
                                    columnTitleParts.push(`Spend: ${formatCurrency(spendValue)}`);
                                    const columnTitle = `${formatMonthLabel(entry.month)} — ${columnTitleParts.join(', ')}`;
                                    return (
                                      <div class="timeline-column" style={{ width: `${geometry.columnWidth}px` }} title={columnTitle}>
                                        <div class="timeline-bars-stack">
                                          <Show when={showService()}>
                                            <div class="timeline-stack timeline-stack--service" aria-hidden="true">
                                              <For each={serviceSegments}>
                                                {(segment) => {
                                                  if (segment.value <= 0) return null;
                                                  const rawHeight = maxVisits > 0 ? (segment.value / maxVisits) * 100 : 0;
                                                  const segmentHeight = rawHeight > 0 ? Math.max(4, Math.min(100, rawHeight)) : 0;
                                                  return <div class={`timeline-bar ${segment.className}`} style={{ height: `${segmentHeight}%` }} />;
                                                }}
                                              </For>
                                            </div>
                                          </Show>
                                          <Show when={showFinance()}>
                                            <div class={`timeline-stack timeline-stack--finance${showService() ? '' : ' timeline-stack--finance-only'}`} aria-hidden="true">
                                              <For each={financeSegments}>
                                                {(segment) => {
                                                  if (segment.value <= 0 || maxSpend <= 0) return null;
                                                  const rawHeight = (segment.value / maxSpend) * 100;
                                                  const segmentHeight = rawHeight > 0 ? Math.max(4, Math.min(100, rawHeight)) : 0;
                                                  return <div class={`timeline-bar ${segment.className}`} style={{ height: `${segmentHeight}%` }} />;
                                                }}
                                              </For>
                                            </div>
                                          </Show>
                                        </div>
                                        <span class="timeline-month">{formatMonthLabel(entry.month)}</span>
                                      </div>
                                    );
                                  }}
                                </For>
                              </div>
                            </Show>
                            <Show when={showFinance()}>
                              <svg
                                class="timeline-svg"
                                width={geometry.width}
                                height={geometry.height}
                                viewBox={`0 0 ${geometry.width} ${geometry.height}`}
                                preserveAspectRatio="none"
                                aria-hidden="true"
                              >
                                <path class="timeline-line timeline-line--total" d={geometry.path.length > 0 ? geometry.path : `M0,${geometry.height} L${geometry.width},${geometry.height}`} />
                                <Show when={geometry.financeActive.bc}>
                                  <path class="timeline-line timeline-line--bc" d={geometry.financePaths.bc.length > 0 ? geometry.financePaths.bc : `M0,${geometry.height} L${geometry.width},${geometry.height}`} />
                                </Show>
                                <Show when={geometry.financeActive.opex}>
                                  <path class="timeline-line timeline-line--opex" d={geometry.financePaths.opex.length > 0 ? geometry.financePaths.opex : `M0,${geometry.height} L${geometry.width},${geometry.height}`} />
                                </Show>
                                <Show when={geometry.financeActive.capex}>
                                  <path class="timeline-line timeline-line--capex" d={geometry.financePaths.capex.length > 0 ? geometry.financePaths.capex : `M0,${geometry.height} L${geometry.width},${geometry.height}`} />
                                </Show>
                                <Show when={geometry.financeActive.other}>
                                  <path class="timeline-line timeline-line--other" d={geometry.financePaths.other.length > 0 ? geometry.financePaths.other : `M0,${geometry.height} L${geometry.width},${geometry.height}`} />
                                </Show>
                                <For each={geometry.points}>
                                  {(point) => (
                                    <circle class="timeline-dot" cx={point.x} cy={point.y} r={4} />
                                  )}
                                </For>
                              </svg>
                            </Show>
                          </div>
                        </div>
                      </>
                    );
                  })()}
                </Show>
              </section>
            </Show>

            <section class="summary-grid" aria-label="Location summary cards">
              <article
                class={summaryCardClass('deficiencies', deficiencyStatus())}
                data-active={activePanel() === 'deficiencies'}
                title="Audit-derived deficiency metrics for this location."
              >
                <header class="summary-card-header">
                  <button type="button" class="summary-card-trigger" onClick={() => setActivePanel('deficiencies')}>
                    Deficiencies &amp; Violations
                  </button>
                  <span class="summary-pill" title="Open deficiencies currently requiring follow-up.">
                    {deficiencyMetrics()?.open != null ? `${deficiencyMetrics()?.open ?? 0} open` : `${openDeficiencies()} open`}
                  </span>
                </header>
                <dl class="summary-stats">
                  <div>
                    <dt>Total deficiencies</dt>
                    <dd title="Total deficiencies recorded across all device audits.">{deficiencyMetrics()?.total ?? totalDeficiencies()}</dd>
                  </div>
                  <div>
                    <dt>Open deficiencies</dt>
                    <dd title="Outstanding deficiencies not yet marked resolved.">{deficiencyMetrics()?.open ?? openDeficiencies()}</dd>
                  </div>
                  <div>
                    <dt>Closure rate</dt>
                    <dd>
                      {deficiencyMetrics()?.closure_rate != null
                        ? formatPercent(deficiencyMetrics()!.closure_rate * 100)
                        : totalDeficiencies() === 0
                        ? '—'
                        : formatPercent(deficiencyClosureRate())}
                    </dd>
                  </div>
                  <div>
                    <dt>Visits tracked</dt>
                    <dd title="Number of audit visits associated with this location.">{visits().length}</dd>
                  </div>
                  <div>
                    <dt>Latest visit</dt>
                    <dd title="Most recent audit visit completion timestamp.">{visits().length > 0 ? formatDateTime(visits()[0]?.completed_at ?? visits()[0]?.started_at) : '—'}</dd>
                  </div>
                </dl>
              </article>

              <article
                class={summaryCardClass('service', serviceStatus())}
                data-active={activePanel() === 'service'}
                title="Service records aggregated from dispatch logs (esa_in_progress)."
              >
                <header class="summary-card-header">
                  <button type="button" class="summary-card-trigger" onClick={() => setActivePanel('service')}>
                    Service Records
                  </button>
                  <span class="summary-pill" title="Total service tickets logged for this location.">
                    {serviceMetrics()?.tickets != null ? `${serviceMetrics()?.tickets ?? 0} tickets` : `${serviceSummary()?.total_tickets ?? 0} tickets`}
                  </span>
                </header>
                <dl class="summary-stats">
                  <div>
                    <dt>Total hours</dt>
                    <dd title="Total technician hours charged to this location.">{formatMaybeNumber(serviceMetrics()?.hours ?? serviceSummary()?.total_hours, 1)}</dd>
                  </div>
                  <div>
                    <dt>Tickets per device</dt>
                    <dd title="Average number of service tickets per device.">{formatMaybeNumber(serviceMetrics()?.per_device)}</dd>
                  </div>
                  <div>
                    <dt>Last service</dt>
                    <dd title="Most recent service ticket work date.">{formatDateTime(serviceSummary()?.last_service)}</dd>
                  </div>
                  <div>
                    <dt>Vendors engaged</dt>
                    <dd title="Distinct vendors logging service tickets.">{serviceSummary()?.vendor_mix?.length ?? 0}</dd>
                  </div>
                  <div>
                    <dt>Top issue</dt>
                    <dd title="Most frequent problem description logged.">{serviceSummary()?.top_problems?.[0]?.problem ?? '—'}</dd>
                  </div>
                </dl>
              </article>

              <article
                class={summaryCardClass('financial', financialStatus())}
                data-active={activePanel() === 'financial'}
                title="Financial records and negotiated savings for this location."
              >
                <header class="summary-card-header">
                  <button type="button" class="summary-card-trigger" onClick={() => setActivePanel('financial')}>
                    Financial Records
                  </button>
                  <span class="summary-pill" title="Unapproved or open spend awaiting action.">
                    {financialMetrics()?.open_spend != null ? `${formatCurrency(financialMetrics()?.open_spend ?? 0)} open` : `${formatCurrency(financialTotals().open)} open`}
                  </span>
                </header>
                <dl class="summary-stats">
                  <div>
                    <dt>Proposed spend</dt>
                    <dd title="Total proposed cost submitted for approval.">{formatCurrency(proposedTotal())}</dd>
                  </div>
                  <div>
                    <dt>Approved spend</dt>
                    <dd title="Spend approved for payment.">{formatCurrency(approvedTotal())}</dd>
                  </div>
                  <div>
                    <dt>Negotiated savings</dt>
                    <dd title="Cumulative negotiated savings (delta).">{formatCurrency(negotiatedSavings())}</dd>
                  </div>
                  <div>
                    <dt>Open spend</dt>
                    <dd title="Open or pending spend awaiting action.">{formatCurrency(openTotal())}</dd>
                  </div>
                  <div>
                    <dt>Invoiced spend</dt>
                    <dd title="Total invoiced spend recorded in financial data.">{formatCurrency(actualSpend())}</dd>
                  </div>
                  <div>
                    <dt>Savings rate</dt>
                    <dd title="Savings as a percentage of proposed cost.">{formatPercent(savingsRatePercent())}</dd>
                  </div>
                  <div>
                    <dt>Statements</dt>
                    <dd title="Number of financial statements on record.">{financialSummary()?.total_records ?? 0}</dd>
                  </div>
                </dl>
                <Show when={savingsDeltaGap() > 1}>
                  <p class="summary-note warning">Note: Recorded savings differ from proposed minus approved by {formatCurrency(savingsDeltaGap())}. Review open invoices for alignment.</p>
                </Show>
              </article>
            </section>

            <section class="analytics-card" aria-labelledby="performance-insights-title">
              <header class="analytics-header">
                <h2 id="performance-insights-title">Performance Insights</h2>
                <p>Combined health scoring across audits, service workload, and financial exposure.</p>
              </header>
              <div class="analytics-body">
                {(() => {
                  const metrics = analyticsMetrics();
                  const max = analyticsMaxValue();
                  const ranking = performanceSeries();
                  return (
                    <>
                      <div class="analytics-metrics">
                        <For each={metrics}>
                      {(metric) => {
                        const width = max > 0 ? Math.max(8, Math.min(100, Math.round((Math.abs(metric.value) / max) * 100))) : 8;
                        return (
                              <div
                                class={`analytics-metric${metric.hasValue ? '' : ' analytics-metric--inactive'}`}
                                title={metric.tooltip ?? metric.label}
                              >
                                <div class="analytics-metric-label">{metric.label}</div>
                                <div class="analytics-bar">
                                  <div class="analytics-bar-fill" style={{ width: `${width}%` }} />
                                </div>
                                <div class="analytics-metric-value">{metric.formatted}</div>
                              </div>
                            );
                          }}
                        </For>
                      </div>
                      <div class="analytics-chart" role="img" aria-label="Highest risk units">
                        <Show when={ranking.ranked.length > 0} fallback={<p class="muted">No device risk data yet.</p>}>
                          <For each={ranking.ranked}>
                            {(item) => {
                              const width = ranking.maxScore > 0 ? Math.max(8, Math.min(100, (item.score / ranking.maxScore) * 100)) : 8;
                              return (
                                <div class="analytics-chart-row" title={`Unit ${item.device.device_id ?? '—'} risk score ${item.score.toFixed(1)}`}>
                                  <span class="chart-label">{item.device.device_id ?? '—'}</span>
                                  <div class="chart-bar">
                                    <div class="chart-bar-fill" style={{ width: `${width}%` }} />
                                  </div>
                                  <span class="chart-score">{item.score.toFixed(1)}</span>
                                </div>
                              );
                            }}
                          </For>
                        </Show>
                      </div>
                      <div class="analytics-lists">
                        <div class="analytics-column">
                          <h3>Worst performing units</h3>
                          <Show when={worstUnits().length > 0} fallback={<p class="muted">No device audits recorded yet.</p>}>
                            <ul class="analytics-list">
                              <For each={worstUnits()}>
                                {(device) => (
                                  <li>
                                    <span class="list-primary">{device.device_id ?? 'Unknown device'}</span>
                                    <span class="list-secondary">{device.open_deficiencies ?? 0} open / {device.total_deficiencies ?? 0} total</span>
                                  </li>
                                )}
                              </For>
                            </ul>
                          </Show>
                        </div>
                        <div class="analytics-column">
                          <h3>Projected cost drivers</h3>
                          <Show when={costlyUnits().length > 0} fallback={<p class="muted">No projected cost risk yet.</p>}>
                            <ul class="analytics-list">
                              <For each={costlyUnits()}>
                                {(item) => (
                                  <li>
                                    <span class="list-primary">{item.device.device_id ?? 'Unknown device'}</span>
                                    <span class="list-secondary">Risk score {item.score.toFixed(1)}</span>
                                  </li>
                                )}
                              </For>
                            </ul>
                          </Show>
                        </div>
                        <div class="analytics-column">
                          <h3>Top spending categories</h3>
                          <Show when={financialCostDrivers().length > 0} fallback={<p class="muted">No spend on record.</p>}>
                            <ul class="analytics-list">
                              <For each={financialCostDrivers()}>
                                {(entry) => (
                                  <li>
                                    <span class="list-primary">{entry.category ?? 'Uncategorized'}</span>
                                    <span class="list-secondary">{formatCurrency(entry.spend)}</span>
                                  </li>
                                )}
                              </For>
                            </ul>
                          </Show>
                        </div>
                      </div>
                    </>
                  );
                })()}
              </div>
            </section>

            <section class="detail-panel">
              <Switch>
                <Match when={activePanel() === 'overview'}>
                  <section class="overview-grid">
                    <article class="insight-card">
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
                    </article>

                    <article class="insight-card">
                      <h2>Service Trends</h2>
                      <Show when={serviceSummary()} fallback={<p class="muted">No service history recorded.</p>}>
                        <div class="trend-list">
                          <For each={serviceTrend()}>
                            {(point) => {
                              const pmValue = Math.max(point.pm ?? 0, 0);
                              const cbEmergencyValue = Math.max(point.cb_emergency ?? 0, 0);
                              const cbEnvValue = Math.max(point.cb_env ?? 0, 0);
                              const tstValue = Math.max(point.tst ?? 0, 0);
                              const rpValue = Math.max(point.rp ?? 0, 0);
                              const totalTickets = Math.max(point.tickets ?? 0, 0);
                              const trackedTotal = pmValue + cbEmergencyValue + cbEnvValue + tstValue + rpValue;
                              const otherValue = Math.max(totalTickets - trackedTotal, 0);
                              const segments = [
                                { value: pmValue, className: 'trend-segment--pm', label: serviceSegmentLabels.pm },
                                { value: cbEmergencyValue, className: 'trend-segment--cb-emergency', label: serviceSegmentLabels.cbEmergency },
                                { value: cbEnvValue, className: 'trend-segment--cb-env', label: serviceSegmentLabels.cbEnv },
                                { value: tstValue, className: 'trend-segment--tst', label: serviceSegmentLabels.tst },
                                { value: rpValue, className: 'trend-segment--rp', label: serviceSegmentLabels.rp }
                              ];
                              if (otherValue > 0) {
                                segments.push({ value: otherValue, className: 'trend-segment--misc', label: serviceSegmentLabels.misc });
                              }
                              const segmentTotal = segments.reduce((sum, seg) => sum + Math.max(seg.value, 0), 0);
                              const width = segmentTotal > 0 && serviceTrendMax() > 0 ? Math.max(8, (segmentTotal / serviceTrendMax()) * 100) : 0;
                              const tooltipBits = [] as string[];
                              if (totalTickets > 0) tooltipBits.push(`${totalTickets} tickets`);
                              if (typeof point.hours === 'number') tooltipBits.push(`${point.hours.toFixed(1)} hours`);
                              tooltipBits.push(`${serviceSegmentLabels.pm}: ${pmValue}`);
                              tooltipBits.push(`${serviceSegmentLabels.cbEmergency}: ${cbEmergencyValue}`);
                              tooltipBits.push(`${serviceSegmentLabels.cbEnv}: ${cbEnvValue}`);
                              tooltipBits.push(`${serviceSegmentLabels.tst}: ${tstValue}`);
                              tooltipBits.push(`${serviceSegmentLabels.rp}: ${rpValue}`);
                              if (otherValue > 0) tooltipBits.push(`${serviceSegmentLabels.misc}: ${otherValue}`);
                              return (
                                <div class="trend-row" title={`${point.month ?? '—'}: ${tooltipBits.join(' · ') || 'No data'}`}>
                                  <span class="trend-label">{point.month ?? '—'}</span>
                                  <div class="trend-bar">
                                    <div class="trend-stack" style={{ width: `${Math.min(100, width)}%` }}>
                                      <For each={segments}>
                                        {(segment) => {
                                          if (segment.value <= 0 || segmentTotal <= 0) return null;
                                          const segmentWidth = (segment.value / segmentTotal) * 100;
                                          return (
                                            <span
                                              class={`trend-segment ${segment.className}`}
                                              style={{ width: `${segmentWidth}%` }}
                                              aria-hidden="true"
                                            />
                                          );
                                        }}
                                      </For>
                                    </div>
                                  </div>
                                  <span class="trend-value">{totalTickets}</span>
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
                    </article>

                    <article class="insight-card">
                      <h2>Financial Overview</h2>
                      <Show when={financialSummary()} fallback={<p class="muted">No financial records available.</p>}>
                        <div class="trend-list">
                          <For each={financialTrend()}>
                            {(point) => {
                              const bc = Math.max(point.bc ?? 0, 0);
                              const opex = Math.max(point.opex ?? 0, 0);
                              const capex = Math.max(point.capex ?? 0, 0);
                              const other = Math.max(point.other ?? 0, 0);
                              const total = Math.max(point.spend ?? bc + opex + capex + other, 0);
                              const segments = [
                                { value: bc, className: 'trend-segment--finance-bc', label: financialSegmentLabels.bc },
                                { value: opex, className: 'trend-segment--finance-opex', label: financialSegmentLabels.opex },
                                { value: capex, className: 'trend-segment--finance-capex', label: financialSegmentLabels.capex },
                                { value: other, className: 'trend-segment--finance-other', label: financialSegmentLabels.other }
                              ];
                              const width = total > 0 && financialTrendMax() > 0 ? Math.max(8, (total / financialTrendMax()) * 100) : 0;
                              const tooltipParts = [
                                `${formatCurrency(total)} total`,
                                `${financialSegmentLabels.bc}: ${formatCurrency(bc)}`,
                                `${financialSegmentLabels.opex}: ${formatCurrency(opex)}`,
                                `${financialSegmentLabels.capex}: ${formatCurrency(capex)}`
                              ];
                              if (other > 0) tooltipParts.push(`${financialSegmentLabels.other}: ${formatCurrency(other)}`);
                              return (
                                <div class="trend-row" title={`${point.month ?? '—'}: ${tooltipParts.join(' · ')}`}>
                                  <span class="trend-label">{point.month ?? '—'}</span>
                                  <div class="trend-bar">
                                    <div class="trend-stack" style={{ width: `${Math.min(100, width)}%` }}>
                                      <For each={segments}>
                                        {(segment) => {
                                          if (segment.value <= 0 || total <= 0) return null;
                                          const segmentWidth = (segment.value / total) * 100;
                                          return <span class={`trend-segment ${segment.className}`} style={{ width: `${segmentWidth}%` }} />;
                                        }}
                                      </For>
                                    </div>
                                  </div>
                                  <span class="trend-value">{formatCurrency(total)}</span>
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
                    </article>
                  </section>

                  <p class="muted">Dive into individual devices and audit history by selecting the “Deficiencies &amp; Violations” panel above.</p>
                </Match>

                <Match when={activePanel() === 'deficiencies'}>
                  <div class="panel-header">
                    <div>
                      <h2>Deficiencies &amp; Violations</h2>
                      <p class="panel-subtitle">Track compliance status, remediation, and reporting history.</p>
                    </div>
                    <Show when={hasDeficiencies()}>
                      <div class="panel-actions">
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
                    </Show>
                  </div>

                  <div class="panel-metrics">
                    <div>
                      <span class="metric-label">Open</span>
                      <span class="metric-value warning">{openDeficiencies()}</span>
                    </div>
                    <div>
                      <span class="metric-label">Total</span>
                      <span class="metric-value">{totalDeficiencies()}</span>
                    </div>
                    <div>
                      <span class="metric-label">Closure rate</span>
                      <span class="metric-value">{deficiencyClosureRate().toFixed(1)}%</span>
                    </div>
                  </div>

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
                            const totalDeficienciesDevice = device.deficiencies?.length ?? 0;
                            const openDeficienciesDevice = device.deficiencies?.filter((item) => !item.resolved).length ?? 0;
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
                                    <span class="deficiency-count-badge">Open {openDeficienciesDevice} / {totalDeficienciesDevice}</span>
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
                </Match>

                <Match when={activePanel() === 'service'}>
                  <div class="panel-header">
                    <div>
                      <h2>Service Records</h2>
                      <p class="panel-subtitle">Ticket load, technician hours, and vendor mix across the location.</p>
                    </div>
                  </div>
                  <div class="panel-metrics">
                    <div>
                      <span class="metric-label">Tickets</span>
                      <span class="metric-value">{serviceStatus() === 'missing' ? '—' : serviceTickets()}</span>
                    </div>
                    <div>
                      <span class="metric-label">Technician hours</span>
                      <span class="metric-value">{serviceStatus() === 'missing' ? '—' : serviceHours().toFixed(1)}</span>
                    </div>
                    <div>
                      <span class="metric-label">Active vendors</span>
                      <span class="metric-value">{serviceStatus() === 'missing' ? '—' : (serviceSummary()?.vendor_mix?.length ?? 0)}</span>
                    </div>
                    <div>
                      <span class="metric-label">Trend</span>
                      <span class="metric-value">{describeTrend(serviceTrendInfo())}</span>
                    </div>
                </div>
                  <div class="insight-card full-width">
                    <h3>Monthly ticket volume</h3>
                    <Show when={serviceTrend().length > 0} fallback={<p class="muted">No service history recorded.</p>}>
                      <div class="trend-list">
                        <For each={serviceTrend()}>
                          {(point) => {
                            const width = serviceTrendMax() > 0 ? Math.max(8, ((point.tickets ?? 0) / serviceTrendMax()) * 100) : 8;
                            const tooltipBits = [] as string[];
                            if (point.tickets != null) tooltipBits.push(`${point.tickets} tickets`);
                            if (typeof point.hours === 'number') tooltipBits.push(`${point.hours.toFixed(1)} hours`);
                            return (
                              <div class="trend-row" title={`${point.month ?? '—'}: ${tooltipBits.join(' · ') || 'No data'}`}>
                                <span class="trend-label">{point.month ?? '—'}</span>
                                <div class="trend-bar"><div class="trend-fill" style={{ width: `${width}%` }} /></div>
                                <span class="trend-value">{point.tickets ?? 0}</span>
                              </div>
                            );
                          }}
                        </For>
                      </div>
                    </Show>
                  </div>
                  <div class="insight-card full-width">
                    <h3>Vendor mix</h3>
                    <Show when={serviceSummary()?.vendor_mix?.length} fallback={<p class="muted">No vendor data available.</p>}>
                      <ul class="vendor-mix-list">
                        <For each={serviceSummary()?.vendor_mix ?? []}>
                          {(item) => (
                            <li>
                              <span class="list-primary">{item.vendor ?? 'Unknown vendor'}</span>
                              <span class="list-secondary">{item.tickets} tickets</span>
                            </li>
                          )}
                        </For>
                      </ul>
                    </Show>
                  </div>
                  <div class="insight-card full-width">
                    <h3>Top problems</h3>
                    <Show when={serviceSummary()?.top_problems?.length} fallback={<p class="muted">No recurring problems logged.</p>}>
                      <ul class="analytics-list">
                        <For each={serviceSummary()?.top_problems ?? []}>
                          {(item) => (
                            <li>
                              <span class="list-primary">{item.problem ?? 'Unspecified'}</span>
                              <span class="list-secondary">{item.count} tickets</span>
                            </li>
                          )}
                        </For>
                      </ul>
                    </Show>
                  </div>
                  <div class="insight-card full-width" title="Share of logged service by activity category (sd_cw_at).">
                    <h3>Activity mix</h3>
                    <Show when={serviceActivitySummary().length > 0} fallback={<p class="muted">No activity mix recorded.</p>}>
                      <table class="activity-table">
                        <thead>
                          <tr>
                            <th scope="col">Category</th>
                            <th scope="col">Tickets</th>
                            <th scope="col">Hours</th>
                            <th scope="col">Share</th>
                          </tr>
                        </thead>
                        <tbody>
                          <For each={serviceActivitySummary()}>
                            {(item) => (
                              <tr title={`${item.category ?? 'Unclassified'} · ${item.tickets} tickets · ${item.hours.toFixed(1)} hours`}>
                                <td>{item.category ?? 'Unclassified'}</td>
                                <td>{item.tickets}</td>
                                <td>{item.hours.toFixed(1)}</td>
                                <td>{formatPercent(item.share * 100)}</td>
                              </tr>
                            )}
                          </For>
                        </tbody>
                      </table>
                    </Show>
                  </div>
                  <div class="insight-card full-width" title="Most recent activity codes driving service volume.">
                    <h3>Top activity codes</h3>
                    <Show when={serviceActivityBreakdown().length > 0} fallback={<p class="muted">No activity codes captured.</p>}>
                     <ul class="analytics-list activity-code-list">
                        <For each={serviceActivityBreakdown().slice(0, 6)}>
                          {(item) => (
                            <li title={`Code: ${item.code ?? 'N/A'} · ${item.tickets} tickets · ${item.hours.toFixed(1)} hours`}>
                              <span class="list-primary">{item.label ?? 'Unclassified'}</span>
                              <span class="list-secondary">{item.tickets} tickets · {item.hours.toFixed(1)} hours</span>
                            </li>
                          )}
                        </For>
                      </ul>
                    </Show>
                  </div>
                </Match>

                <Match when={activePanel() === 'financial'}>
                  <div class="panel-header">
                    <div>
                      <h2>Financial Records</h2>
                      <p class="panel-subtitle">Spending trends, approval pipeline, and exposure for this location.</p>
                    </div>
                  </div>
                  <div class="panel-metrics">
                    <div>
                      <span class="metric-label">Total spend</span>
                      <span class="metric-value">{financialStatus() === 'missing' ? '—' : formatCurrency(financialTotals().total)}</span>
                    </div>
                    <div>
                      <span class="metric-label">Approved</span>
                      <span class="metric-value">{financialStatus() === 'missing' ? '—' : formatCurrency(financialTotals().approved)}</span>
                    </div>
                    <div>
                      <span class="metric-label">Open</span>
                      <span class="metric-value warning">{financialStatus() === 'missing' ? '—' : formatCurrency(financialTotals().open)}</span>
                    </div>
                    <div>
                      <span class="metric-label">Last statement</span>
                      <span class="metric-value">{formatDateTime(financialSummary()?.last_statement)}</span>
                    </div>
                    <div>
                      <span class="metric-label">Total savings</span>
                      <span class="metric-value">{financialStatus() === 'missing' ? '—' : formatCurrency(negotiatedSavings())}</span>
                    </div>
                    <div>
                      <span class="metric-label">Savings trend</span>
                      <span class="metric-value">{describeTrend(financialSavingsTrendInfo())}</span>
                    </div>
                    <div>
                      <span class="metric-label">Savings rate</span>
                      <span class="metric-value">{financialStatus() === 'missing' ? '—' : formatPercent(savingsRatePercent())}</span>
                    </div>
                    <div>
                      <span class="metric-label">Savings per device</span>
                      <span class="metric-value">{savingsPerDevice() != null ? formatCurrency(savingsPerDevice() ?? 0) : '—'}</span>
                    </div>
                    <div>
                      <span class="metric-label">Trend</span>
                      <span class="metric-value">{describeTrend(financialTrendInfo())}</span>
                    </div>
                  </div>
                  <div class="insight-card full-width">
                    <h3>Monthly spend</h3>
                    <Show when={financialTrend().length > 0} fallback={<p class="muted">No financial records available.</p>}>
                      <div class="trend-list">
                        <For each={financialTrend()}>
                          {(point) => {
                            const width = financialTrendMax() > 0 ? Math.max(8, ((point.spend ?? 0) / financialTrendMax()) * 100) : 8;
                            return (
                              <div class="trend-row" title={`${point.month ?? '—'}: ${formatCurrency(point.spend ?? 0)}`}>
                                <span class="trend-label">{point.month ?? '—'}</span>
                                <div class="trend-bar"><div class="trend-fill" style={{ width: `${width}%` }} /></div>
                                <span class="trend-value">{formatCurrency(point.spend)}</span>
                              </div>
                            );
                          }}
                        </For>
                      </div>
                    </Show>
                  </div>
                  <div class="insight-card full-width" title="Monthly savings captured from financial delta values.">
                    <h3>Monthly savings</h3>
                    <Show when={financialSavingsTrend().length > 0} fallback={<p class="muted">No savings captured yet.</p>}>
                      <div class="trend-list">
                        <For each={financialSavingsTrend()}>
                          {(point) => {
                            const label = point.month ?? '—';
                            const value = point.savings ?? 0;
                            const maxSavings = financialSavingsMax();
                            const width = maxSavings > 0 ? Math.max(8, Math.min(100, Math.round((Math.abs(value) / maxSavings) * 100))) : 8;
                            return (
                              <div class="trend-row" title={`${label}: ${formatCurrency(value)}`}>
                                <span class="trend-label">{label}</span>
                                <div class="trend-bar"><div class="trend-fill" style={{ width: `${width}%` }} /></div>
                                <span class="trend-value">{formatCurrency(value)}</span>
                              </div>
                            );
                          }}
                        </For>
                      </div>
                    </Show>
                  </div>
                  <div class="insight-card full-width" title="Cumulative savings over time.">
                    <h3>Cumulative savings</h3>
                    <Show when={financialCumulativeSavings().length > 0} fallback={<p class="muted">Savings will appear as financial records accumulate.</p>}>
                      <div class="trend-list">
                        <For each={financialCumulativeSavings()}>
                          {(point) => {
                            const label = point.month ?? '—';
                            const value = point.savings ?? 0;
                            const maxSavings = financialCumulativeMax();
                            const width = maxSavings > 0 ? Math.max(8, Math.min(100, Math.round((Math.abs(value) / maxSavings) * 100))) : 8;
                            return (
                              <div class="trend-row" title={`${label}: ${formatCurrency(value)}`}>
                                <span class="trend-label">{label}</span>
                                <div class="trend-bar"><div class="trend-fill" style={{ width: `${width}%` }} /></div>
                                <span class="trend-value">{formatCurrency(value)}</span>
                              </div>
                            );
                          }}
                        </For>
                      </div>
                    </Show>
                  </div>
                  <div class="detail-columns">
                    <div class="insight-card">
                      <h3>By classification</h3>
                      <Show when={financialClassifications().length > 0} fallback={<p class="muted">No classification data.</p>}>
                        <ul class="analytics-list">
                          <For each={financialClassifications()}>
                            {(item) => (
                              <li>
                                <span class="list-primary">{item.classification ?? 'Unclassified'}</span>
                                <span class="list-secondary">{formatCurrency(item.spend)} · {formatShare(item.spend ?? 0, classificationTotal() || actualSpend() || 0)}</span>
                              </li>
                            )}
                          </For>
                        </ul>
                      </Show>
                    </div>
                    <div class="insight-card">
                      <h3>By record type</h3>
                      <Show when={financialTypes().length > 0} fallback={<p class="muted">No type data.</p>}>
                        <ul class="analytics-list">
                          <For each={financialTypes()}>
                            {(item) => (
                              <li>
                                <span class="list-primary">{item.type ?? 'Unspecified'}</span>
                                <span class="list-secondary">{formatCurrency(item.spend)} · {formatShare(item.spend ?? 0, typeTotal() || actualSpend() || 0)}</span>
                              </li>
                            )}
                          </For>
                        </ul>
                      </Show>
                    </div>
                    <div class="insight-card">
                      <h3>Spend by category</h3>
                      <Show when={financialSummary()?.category_breakdown?.length} fallback={<p class="muted">No category data.</p>}>
                        <ul class="analytics-list">
                          <For each={financialSummary()?.category_breakdown ?? []}>
                            {(item) => (
                              <li>
                                <span class="list-primary">{item.category ?? 'Uncategorized'}</span>
                                <span class="list-secondary">{formatCurrency(item.spend)} · {formatShare(item.spend ?? 0, actualSpend() || 0)}</span>
                              </li>
                            )}
                          </For>
                        </ul>
                      </Show>
                    </div>
                    <div class="insight-card">
                      <h3>Spend by status</h3>
                      <Show when={financialSummary()?.status_breakdown?.length} fallback={<p class="muted">No status data.</p>}>
                        <ul class="analytics-list">
                          <For each={financialSummary()?.status_breakdown ?? []}>
                            {(item) => (
                              <li>
                                <span class="list-primary">{item.status ?? 'Unknown'}</span>
                                <span class="list-secondary">{formatCurrency(item.spend)} · {formatShare(item.spend ?? 0, actualSpend() || 0)}</span>
                              </li>
                            )}
                          </For>
                        </ul>
                      </Show>
                    </div>
                  </div>
                  <div class="detail-columns">
                    <div class="insight-card">
                      <h3>Top vendors</h3>
                      <Show when={financialVendors().length > 0} fallback={<p class="muted">No vendor records yet.</p>}>
                        <ul class="analytics-list">
                          <For each={financialVendors()}>
                            {(item) => (
                              <li>
                                <span class="list-primary">{item.vendor_name}</span>
                                <span class="list-secondary">{formatCurrency(item.spend)}</span>
                              </li>
                            )}
                          </For>
                        </ul>
                      </Show>
                    </div>
                    <div class="insight-card">
                      <h3>Work statements</h3>
                      <Show when={financialWorkSummary().length > 0} fallback={<p class="muted">Work statements will appear as records accumulate.</p>}>
                        <ul class="analytics-list">
                          <For each={financialWorkSummary()}>
                            {(item) => (
                              <li>
                                <span class="list-primary">{item.description}</span>
                                <span class="list-secondary">{item.records} records · {formatCurrency(item.spend)}</span>
                              </li>
                            )}
                          </For>
                        </ul>
                      </Show>
                    </div>
                  </div>
                </Match>
              </Switch>
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
