import type {
  AuditDetailResponse,
  AuditSummary,
  LocationDetail,
  LocationListParams,
  LocationListResponse,
  LocationSummary,
  ReportJobCreateRequest,
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

export function fetchLocations(params: LocationListParams = {}): Promise<LocationListResponse> {
  const search = new URLSearchParams();
  if (params.page && params.page > 0) {
    search.set('page', String(params.page));
  }
  if (params.pageSize && params.pageSize > 0) {
    search.set('page_size', String(params.pageSize));
  }
  if (params.search && params.search.trim().length > 0) {
    search.set('search', params.search.trim());
  }
  const query = search.toString();
  return fetchJSON<LocationListResponse>(query ? `/locations?${query}` : '/locations');
}

export interface LocationQuery {
  address: string;
  locationId?: number | string | null;
}

export function fetchLocationDetail(params: LocationQuery): Promise<LocationDetail> {
  const search = new URLSearchParams();
  if (params.address && params.address.length > 0) {
    search.set('address', params.address);
  }
  if (params.locationId !== undefined && params.locationId !== null) {
    search.set('location_id', String(params.locationId));
  }
  return fetchJSON<LocationDetail>(`/locations?${search.toString()}`);
}

export async function createReportJob(request: ReportJobCreateRequest): Promise<ReportJobCreateResponse> {
  const trimmedAddress = request.address?.trim() ?? '';
  if (!trimmedAddress) {
    throw new Error('Address is required');
  }

  const payload: Record<string, unknown> = { address: trimmedAddress };

  const trimmedNotes = request.notes?.trim();
  if (trimmedNotes) {
    payload.notes = trimmedNotes;
  }

  const trimmedRecommendations = request.recommendations?.trim();
  if (trimmedRecommendations) {
    payload.recommendations = trimmedRecommendations;
  }

  const owner = request.coverBuildingOwner?.trim();
  if (owner) {
    payload.cover_building_owner = owner;
  }

  const street = request.coverStreet?.trim();
  if (street) {
    payload.cover_street = street;
  }

  const city = request.coverCity?.trim();
  if (city) {
    payload.cover_city = city;
  }

  const state = request.coverState?.trim();
  if (state) {
    payload.cover_state = state;
  }

  const zip = request.coverZip?.trim();
  if (zip) {
    payload.cover_zip = zip;
  }

  const contactName = request.coverContactName?.trim();
  if (contactName) {
    payload.cover_contact_name = contactName;
  }

  const contactEmail = request.coverContactEmail?.trim();
  if (contactEmail) {
    payload.cover_contact_email = contactEmail;
  }

  if (request.deficiencyOnly) {
    payload.deficiency_only = true;
  }

  if (request.visitIds && request.visitIds.length > 0) {
    payload.visit_ids = request.visitIds;
  }

  if (request.auditIds && request.auditIds.length > 0) {
    payload.audit_ids = request.auditIds;
  }

  if (request.locationRowId !== undefined && request.locationRowId !== null) {
    payload.location_id = request.locationRowId;
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

export interface DownloadedReport {
  blob: Blob;
  filename?: string;
  contentType: string | null;
}

function extractFilename(headerValue: string | null): string | undefined {
  if (!headerValue) return undefined;
  const match = /filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i.exec(headerValue);
  if (!match) return undefined;
  const value = match[1] || match[2];
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export async function downloadReport(jobId: string): Promise<DownloadedReport> {
  const url = buildUrl(`/reports/${encodeURIComponent(jobId)}/download`);
  const response = await fetch(url, {
    headers: {
      Accept: 'application/zip, application/pdf;q=0.9, */*;q=0.5'
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

  const blob = await response.blob();
  const contentType = response.headers.get('Content-Type');
  const filename = extractFilename(response.headers.get('Content-Disposition'));

  return { blob, filename: filename ?? undefined, contentType };
}
