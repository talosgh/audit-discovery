import type {
  AuditDetailResponse,
  AuditSummary,
  LocationDetail,
  LocationSummary,
  ReportJobCreateResponse,
  ReportJobStatus
} from './types';

const rawBase = import.meta.env.VITE_API_BASE_URL?.trim() ?? '';
const rawPath = import.meta.env.VITE_API_PATH?.trim() ?? '/webhook';

const API_BASE = rawBase.replace(/\/$/, '');

let normalizedPath = rawPath === '' ? '/webhook' : rawPath;
if (!normalizedPath.startsWith('/')) {
  normalizedPath = `/${normalizedPath}`;
}
normalizedPath = normalizedPath.replace(/\/$/, '');
const API_PATH = normalizedPath === '/' ? '' : normalizedPath;

function buildUrl(path: string): string {
  const segments = [API_BASE, API_PATH, path].filter((segment) => segment && segment.length > 0);
  const joined = segments.join('');
  return joined.replace(/([^:]\/)(\/)+/g, '$1');
}

async function fetchJSON<T>(path: string): Promise<T> {
  const response = await fetch(buildUrl(path), {
    headers: {
      Accept: 'application/json'
    }
  });

  if (!response.ok) {
    let message = response.statusText;
    try {
      const data = await response.json();
      if (typeof data?.message === 'string' && data.message.trim().length > 0) {
        message = data.message;
      }
    } catch (err) {
      // ignore json parse errors
    }
    throw new Error(message || `Request failed with status ${response.status}`);
  }

  return (await response.json()) as T;
}

export function fetchAuditDetail(id: string): Promise<AuditDetailResponse> {
  return fetchJSON<AuditDetailResponse>(`/audits/${id}`);
}

export function fetchLocations(): Promise<LocationSummary[]> {
  return fetchJSON<LocationSummary[]>('/locations');
}

export function fetchLocationDetail(address: string): Promise<LocationDetail> {
  const query = new URLSearchParams({ address }).toString();
  return fetchJSON<LocationDetail>(`/locations?${query}`);
}

export async function createReportJob(address: string, notes?: string, recommendations?: string): Promise<ReportJobCreateResponse> {
  const payload: Record<string, unknown> = { address };
  if (notes && notes.trim().length > 0) {
    payload.notes = notes.trim();
  }
  if (recommendations && recommendations.trim().length > 0) {
    payload.recommendations = recommendations.trim();
  }

  const response = await fetch(buildUrl('/reports'), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json'
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    let message = response.statusText;
    try {
      const data = await response.json();
      if (typeof data?.message === 'string' && data.message.trim().length > 0) {
        message = data.message;
      }
    } catch (err) {
      /* ignore */
    }
    throw new Error(message || `Request failed with status ${response.status}`);
  }

  return (await response.json()) as ReportJobCreateResponse;
}

export function fetchReportJob(jobId: string): Promise<ReportJobStatus> {
  return fetchJSON<ReportJobStatus>(`/reports/${jobId}`);
}

interface DeficiencyUpdateResponse {
  status: 'ok';
  resolved: boolean;
  resolved_at: string | null;
}

export async function updateDeficiencyStatus(auditId: string, deficiencyId: number, resolved: boolean): Promise<DeficiencyUpdateResponse> {
  const response = await fetch(buildUrl(`/audits/${auditId}/deficiencies/${deficiencyId}`), {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json'
    },
    body: JSON.stringify({ resolved })
  });

  if (!response.ok) {
    let message = response.statusText;
    try {
      const data = await response.json();
      if (typeof data?.message === 'string' && data.message.trim().length > 0) {
        message = data.message;
      }
    } catch (err) {
      // ignore json parse errors
    }
    throw new Error(message || `Request failed with status ${response.status}`);
  }

  return (await response.json()) as DeficiencyUpdateResponse;
}

export async function downloadReport(jobId: string): Promise<Blob> {
  const url = buildUrl(`/reports/${encodeURIComponent(jobId)}/download`);
  const response = await fetch(url, {
    headers: {
      Accept: 'application/pdf'
    }
  });

  if (!response.ok) {
    let message = response.statusText;
    try {
      const data = await response.json();
      if (typeof data?.message === 'string' && data.message.trim().length > 0) {
        message = data.message;
      }
    } catch (err) {
      // ignore JSON parse errors
    }
    throw new Error(message || `Request failed with status ${response.status}`);
  }

  return await response.blob();
}
