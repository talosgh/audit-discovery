export function formatDateTime(value: string | null | undefined): string {
  if (!value) return '—';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short'
  }).format(parsed);
}

export function formatArray(values: (string | null)[] | null | undefined, fallback = '—'): string {
  if (!values || values.length === 0) {
    return fallback;
  }
  const filtered = values.map((value) => value?.trim()).filter((value): value is string => Boolean(value && value.length > 0));
  return filtered.length > 0 ? filtered.join(', ') : fallback;
}

export function formatBoolean(value: boolean | null | undefined): string {
  if (value === null || value === undefined) {
    return '—';
  }
  return value ? 'Yes' : 'No';
}

export function formatNumber(value: number | null | undefined, options?: Intl.NumberFormatOptions): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return new Intl.NumberFormat(undefined, options).format(value);
}

export function formatCurrency(value: number | null | undefined, currency = 'USD'): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return new Intl.NumberFormat(undefined, {
    style: 'currency',
    currency,
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
  }).format(value);
}

export function formatFileSize(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  const units = ['bytes', 'KB', 'MB', 'GB', 'TB'];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

export function dataUrl(contentType: string, base64: string): string {
  return `data:${contentType};base64,${base64}`;
}

export function slugify(value: string): string {
  return value
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .substring(0, 64) || 'report';
}
