import type {
  ApiFailureBody,
  Coordinate,
  CongestionTrends,
  DashboardKpis,
  DatasetId,
  DatasetInfo,
  GeocodeResponse,
  LatestForecast,
  MetricsPage,
  ModelMetadata,
  Region,
  RegionInfo,
  RouteResponse,
  TrafficEvidenceResponse,
} from '../types/api';

const configuredBase = import.meta.env.VITE_API_BASE_URL?.trim();
export const API_BASE_URL = (configuredBase || 'http://127.0.0.1:8000').replace(/\/$/, '');

export class ApiError extends Error {
  status: number;
  code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
  }
}

async function get<T>(
  path: string,
  params: Record<string, string | number | undefined> = {},
  signal?: AbortSignal,
): Promise<T> {
  const url = new URL(`${API_BASE_URL}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) url.searchParams.set(key, String(value));
  }

  let response: Response;
  try {
    response = await fetch(url, {
      method: 'GET',
      headers: { Accept: 'application/json' },
      signal,
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error;
    throw new ApiError(0, 'network_error', 'Could not reach the traffic analytics API.');
  }

  if (!response.ok) {
    const body = (await response.json().catch(() => ({}))) as ApiFailureBody;
    const detail = body.error?.message ?? body.details?.[0]?.message;
    throw new ApiError(
      response.status,
      body.error?.code ?? 'http_error',
      detail ?? `The API returned HTTP ${response.status}.`,
    );
  }
  return (await response.json()) as T;
}

export const api = {
  ready: (signal?: AbortSignal) => get<{ status: 'ready' }>('/health/ready', {}, signal),
  datasets: (signal?: AbortSignal) => get<DatasetInfo[]>('/api/v1/datasets', {}, signal),
  dataset: (dataset: DatasetId, signal?: AbortSignal) =>
    get<DatasetInfo>(`/api/v1/datasets/${dataset}`, {}, signal),
  kpis: (dataset: DatasetId, start: string, end: string, signal?: AbortSignal) =>
    get<DashboardKpis>('/api/v1/dashboard/kpis', { dataset, start, end }, signal),
  trends: (
    dataset: DatasetId,
    start: string,
    end: string,
    bucket: CongestionTrends['bucket'],
    signal?: AbortSignal,
  ) => get<CongestionTrends>('/api/v1/congestion/trends', { dataset, start, end, bucket }, signal),
  metrics: (
    dataset: DatasetId,
    start: string,
    end: string,
    limit: number,
    after?: string | null,
    signal?: AbortSignal,
  ) => get<MetricsPage>('/api/v1/metrics', { dataset, start, end, limit, after: after ?? undefined }, signal),
  forecast: (dataset: DatasetId, signal?: AbortSignal) =>
    get<LatestForecast>('/api/v1/forecasts/latest', { dataset }, signal),
  modelMetadata: (dataset: DatasetId, signal?: AbortSignal) =>
    get<ModelMetadata>('/api/v1/forecasts/model-metadata', { dataset }, signal),
  regions: (signal?: AbortSignal) => get<RegionInfo[]>('/api/v1/route/regions', {}, signal),
  geocode: (
    q: string,
    region: Region,
    signal?: AbortSignal,
  ) => get<GeocodeResponse>('/api/v1/route/geocode', { q, region }, signal),
  route: (
    source: Coordinate,
    destination: Coordinate,
    region: Region,
    signal?: AbortSignal,
  ) => get<RouteResponse>('/api/v1/route/route', {
    source_lat: source.lat,
    source_lon: source.lon,
    dest_lat: destination.lat,
    dest_lon: destination.lon,
    region,
  }, signal),
  trafficEvidence: (dataset: DatasetId, signal?: AbortSignal) =>
    get<TrafficEvidenceResponse>('/api/v1/route/traffic-evidence', { dataset }, signal),
};
