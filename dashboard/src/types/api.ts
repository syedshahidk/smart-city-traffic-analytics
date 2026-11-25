export type DatasetId = 'metr-la' | 'pems-bay';

export interface DatasetInfo {
  dataset: DatasetId;
  display_name: string;
  sensor_count: number;
  training_start: string;
  training_end: string;
  metric_row_count: number;
  first_window_start: string | null;
  latest_window_end: string | null;
  last_processed_at: string | null;
  freshness_seconds: number | null;
  model_available: boolean;
  model_version: string | null;
}

export interface MetricRecord {
  dataset: DatasetId;
  window_start: string;
  window_end: string;
  average_speed: number;
  total_sensor_readings: number;
  congested_sensor_count: number;
  congestion_percentage: number;
  processed_at: string;
}

export interface MetricsPage {
  dataset: DatasetId;
  range_start: string;
  range_end: string;
  limit: number;
  has_more: boolean;
  next_cursor: string | null;
  latest_metric_at: string | null;
  freshness_seconds: number | null;
  items: MetricRecord[];
}

export interface DashboardKpis {
  dataset: DatasetId;
  range_start: string;
  range_end: string;
  window_count: number;
  total_sensor_readings: number;
  congested_sensor_count: number;
  weighted_average_speed: number | null;
  weighted_congestion_percentage: number | null;
  latest_metric: MetricRecord | null;
  freshness_seconds: number | null;
}

export interface CongestionTrendPoint {
  bucket_start: string;
  window_count: number;
  total_sensor_readings: number;
  congested_sensor_count: number;
  weighted_average_speed: number | null;
  weighted_congestion_percentage: number | null;
}

export interface CongestionTrends {
  dataset: DatasetId;
  bucket: '5m' | '15m' | '1h' | '1d';
  range_start: string;
  range_end: string;
  latest_metric_at: string | null;
  freshness_seconds: number | null;
  items: CongestionTrendPoint[];
}

export interface LatestForecast {
  dataset: DatasetId;
  forecast_origin: string;
  target_timestamp: string;
  prediction_horizon_minutes: 15;
  predicted_average_speed: number;
  predicted_congestion_percentage: number;
  congestion_status: 'congested' | 'not_congested';
  congestion_threshold_speed_below: number;
  congestion_percentage_threshold: number;
  speed_model: string;
  congestion_model: string;
  model_version: string;
  input_window_count: number;
  input_window_start: string;
  input_window_end: string;
  latest_event_timestamp: string;
  freshness_seconds: number;
  freshness_limit_seconds: number;
  data_provenance: 'historical_dataset_replay_simulated';
  is_live_traffic: false;
}

export interface ModelEvaluationMetadata {
  selected_model: string;
  mae: number;
  r2: number;
  test_samples: number;
  parameters: Record<string, unknown>;
}

export interface ModelMetadata {
  dataset: DatasetId;
  model_version: string;
  artifact_path: string;
  artifact_size_bytes: number;
  report_path: string;
  prediction_horizon_minutes: number;
  window_minutes: number;
  speed: ModelEvaluationMetadata;
  congestion: ModelEvaluationMetadata;
  training_samples: number;
  validation_samples: number;
  feature_count: number;
  speed_congestion_threshold: number;
  default_congestion_percentage_threshold: number;
  status_logic: string;
  source: 'offline_evaluation_report_and_model_artifact';
}

export interface ApiFailureBody {
  error?: { code?: string; message?: string };
  details?: Array<{ location?: string; message?: string }>;
}

export type Region = 'metr-la' | 'pems-bay';

export interface RegionInfo {
  region: Region;
  display_name: string;
  south: number;
  west: number;
  north: number;
  east: number;
}

export interface Coordinate {
  lat: number;
  lon: number;
}

export interface GeocodeCandidate {
  label: string;
  lat: number;
  lon: number;
  region: Region;
}

export interface GeocodeResponse {
  region: Region;
  query: string;
  candidates: GeocodeCandidate[];
}

export interface RouteAlternative {
  id: string;
  distance_km: number;
  duration_seconds: number;
  duration_minutes: number;
  geometry: [number, number][]; // [lat, lon]
}

export interface RouteResponse {
  region: Region;
  source: Coordinate;
  destination: Coordinate;
  alternatives: RouteAlternative[];
  selected_route_id: string;
}

export interface TrafficEvidenceResponse {
  available: boolean;
  message: string;
  reason: string;
}
