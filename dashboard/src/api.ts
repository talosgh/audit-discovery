import type { AuditDetailResponse, AuditSummary } from './types';

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

export function fetchAudits(): Promise<AuditSummary[]> {
  return fetchJSON<AuditSummary[]>('/audits');
}

export function fetchAuditDetail(id: string): Promise<AuditDetailResponse> {
  return fetchJSON<AuditDetailResponse>(`/audits/${id}`);
}
