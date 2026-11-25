import type { DatasetInfo } from '../types/api';

export interface DateRange {
  start: string;
  end: string;
}

export function toDateTimeLocal(iso: string): string {
  const date = new Date(iso);
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

export function localInputToIso(value: string): string | null {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

export function defaultDateRange(dataset: DatasetInfo, hours = 24): DateRange {
  const first = new Date(dataset.first_window_start ?? dataset.training_start);
  const latest = new Date(dataset.latest_window_end ?? dataset.training_end);
  const startAt = Math.max(first.getTime(), latest.getTime() - hours * 60 * 60 * 1000);
  return { start: new Date(startAt).toISOString(), end: latest.toISOString() };
}

export function readableTimestamp(value: string | null | undefined): string {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '—';
  return new Intl.DateTimeFormat(undefined, {
    month: 'short', day: 'numeric', year: 'numeric',
    hour: '2-digit', minute: '2-digit',
  }).format(date);
}

export function compactTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat('en-US', { timeZone: 'UTC', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }).format(date);
}

export function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds)) return 'Unavailable';
  if (seconds < 60) return `${Math.round(seconds)} sec ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} hr ago`;
  return `${Math.floor(seconds / 86400)} days ago`;
}
