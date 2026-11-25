import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from 'react';
import {
  Activity,
  AlertCircle,
  AlertTriangle,
  BarChart3,
  CalendarDays,
  Check,
  CheckCircle2,
  ChevronDown,
  Clock3,
  Cpu,
  Database,
  Gauge,
  HardDrive,
  Info,
  Menu,
  Radio,
  RefreshCw,
  Route,
  Search,
  Server,
  ShieldAlert,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  Waves,
  X,
} from 'lucide-react';

import { MetricTable } from './components/MetricTable';
import { NetworkCoverage } from './components/NetworkCoverage';
import { ApiError, api, API_BASE_URL } from './lib/api';
import {
  defaultDateRange,
  localInputToIso,
  readableTimestamp,
  toDateTimeLocal,
} from './lib/dates';
import type {
  CongestionTrends,
  DashboardKpis,
  DatasetId,
  DatasetInfo,
  LatestForecast,
  MetricRecord,
  MetricsPage,
  ModelMetadata,
} from './types/api';

const TrafficChart = lazy(() =>
  import('./components/TrafficChart').then((module) => ({ default: module.TrafficChart }))
);

const RouteRecommendation = lazy(() =>
  import('./components/RouteRecommendation').then((module) => ({ default: module.RouteRecommendation }))
);

type Section = 'command-center' | 'analytics' | 'forecasting' | 'explorer' | 'route-planner' | 'system-intelligence';
type LoadState = 'idle' | 'loading' | 'loaded' | 'error';
type RangeDraft = { start: string; end: string };

const navigation: Array<{ id: Section; label: string; icon: typeof Activity; badge?: string }> = [
  { id: 'command-center', label: 'Command Center', icon: Activity },
  { id: 'analytics', label: 'Traffic Analytics', icon: BarChart3 },
  { id: 'forecasting', label: 'Predictive Intelligence', icon: Sparkles, badge: 'ML +15m' },
  { id: 'route-planner', label: 'Route Planner', icon: Route, badge: 'OSRM' },
  { id: 'explorer', label: 'Data Explorer', icon: Search },
  { id: 'system-intelligence', label: 'System & Architecture', icon: Server },
];

const sectionCopy: Record<Section, { eyebrow: string; title: string; subtitle: string }> = {
  'command-center': {
    eyebrow: 'Operations & Replay Telemetry',
    title: 'Command Center',
    subtitle: 'Unified operational view of highway sensor networks, velocity indices, and streaming status.',
  },
  analytics: {
    eyebrow: 'Time-Series & Volume Workbench',
    title: 'Traffic Analytics',
    subtitle: 'Sensor-weighted velocity distributions, congestion severity, and multi-scale temporal trends.',
  },
  forecasting: {
    eyebrow: 'Machine Learning Serving Layer',
    title: 'Predictive Intelligence',
    subtitle: '15-minute ahead speed and congestion forecasts evaluated with strict chronological horizons.',
  },
  'route-planner': {
    eyebrow: 'Road Network & Route Planning',
    title: 'Route Planner',
    subtitle: 'Real road routes via OpenStreetMap/OSRM for Los Angeles County and the San Francisco Bay Area.',
  },
  explorer: {
    eyebrow: 'Historical Records Store',
    title: 'Data Explorer',
    subtitle: 'Inspect, filter, and export verified 5-minute metric windows from the PostgreSQL analytics lake.',
  },
  'system-intelligence': {
    eyebrow: 'Pipeline Architecture & Provenance',
    title: 'System Intelligence',
    subtitle: 'Actual API readiness, configured data-flow topology, and benchmark replay provenance.',
  },
};

function errorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message;
  return 'Unable to communicate with the analytics API.';
}

function numberValue(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return value.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function formatArtifactSize(bytes: number | null | undefined): string {
  if (bytes == null || !Number.isFinite(bytes) || bytes < 0) return 'Not verified';
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes / 1024;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(1)} ${units[unitIndex]}`;
}

function formatModelParameters(parameters: Record<string, unknown>): string {
  const entries = Object.entries(parameters).slice(0, 3);
  if (entries.length === 0) return 'No parameters reported';
  return entries
    .map(([key, value]) => `${key}: ${typeof value === 'number' ? value : String(value)}`)
    .join(' · ');
}

function StatusPill({
  tone,
  children,
}: {
  tone: 'healthy' | 'configured' | 'probing' | 'unknown';
  children: string;
}) {
  const tones = {
    healthy: 'bg-emerald-950 text-emerald-300 border border-emerald-800',
    configured: 'bg-slate-800 text-slate-300 border border-slate-700',
    probing: 'bg-indigo-950 text-indigo-300 border border-indigo-800',
    unknown: 'bg-amber-950 text-amber-300 border border-amber-800',
  } as const;
  return (
    <span className={`text-[10px] font-mono font-bold px-1.5 py-0.5 rounded ${tones[tone]}`}>
      {children}
    </span>
  );
}

function dateRangeFor(dataset: DatasetInfo, hours: number): { start: string; end: string } {
  return defaultDateRange(dataset, hours);
}

function App() {
  const [section, setSection] = useState<Section>('command-center');
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
  const [datasetState, setDatasetState] = useState<LoadState>('loading');
  const [datasetError, setDatasetError] = useState<string | null>(null);
  const [selectedDataset, setSelectedDataset] = useState<DatasetId | null>(null);
  const [datasetDetail, setDatasetDetail] = useState<DatasetInfo | null>(null);

  const [apiReady, setApiReady] = useState<boolean | null>(null);
  const [apiLatency, setApiLatency] = useState<number | null>(null);
  const [apiError, setApiError] = useState<string | null>(null);

  const [draftRange, setDraftRange] = useState<RangeDraft | null>(null);
  const [appliedRange, setAppliedRange] = useState<{ start: string; end: string } | null>(null);
  const [quickHours, setQuickHours] = useState<number | null>(24);
  const [rangeError, setRangeError] = useState<string | null>(null);

  const [bucket, setBucket] = useState<CongestionTrends['bucket']>('15m');

  const [kpis, setKpis] = useState<DashboardKpis | null>(null);
  const [kpiState, setKpiState] = useState<LoadState>('idle');
  const [kpiError, setKpiError] = useState<string | null>(null);

  const [trends, setTrends] = useState<CongestionTrends | null>(null);
  const [trendState, setTrendState] = useState<LoadState>('idle');
  const [trendError, setTrendError] = useState<string | null>(null);

  const [recentRows, setRecentRows] = useState<MetricRecord[]>([]);
  const [recentState, setRecentState] = useState<LoadState>('idle');
  const [recentError, setRecentError] = useState<string | null>(null);

  const [forecast, setForecast] = useState<LatestForecast | null>(null);
  const [forecastState, setForecastState] = useState<LoadState>('idle');
  const [forecastError, setForecastError] = useState<string | null>(null);

  const [modelMetadata, setModelMetadata] = useState<
    Partial<Record<DatasetId, ModelMetadata | null>>
  >({});
  const [modelMetadataState, setModelMetadataState] = useState<
    Partial<Record<DatasetId, LoadState>>
  >({});
  const [modelMetadataError, setModelMetadataError] = useState<
    Partial<Record<DatasetId, string | null>>
  >({});

  const [explorerRows, setExplorerRows] = useState<MetricRecord[]>([]);
  const [explorerState, setExplorerState] = useState<LoadState>('idle');
  const [explorerError, setExplorerError] = useState<string | null>(null);
  const [explorerHasMore, setExplorerHasMore] = useState(false);
  const [explorerCursor, setExplorerCursor] = useState<string | null>(null);
  const [explorerPage, setExplorerPage] = useState(0);
  const [explorerLoadingMore, setExplorerLoadingMore] = useState(false);

  const [refreshToken, setRefreshToken] = useState(0);
  const [overviewRefresh, setOverviewRefresh] = useState(0);
  const [forecastRetry, setForecastRetry] = useState(0);

  const selectedInfo = useMemo(
    () => datasets.find((dataset) => dataset.dataset === selectedDataset) ?? null,
    [datasets, selectedDataset]
  );
  const activeCopy = sectionCopy[section];

  // API readiness check with latency measurement
  const checkApi = useCallback((signal?: AbortSignal) => {
    const t0 = performance.now();
    api
      .ready(signal)
      .then(() => {
        setApiLatency(Math.round(performance.now() - t0));
        setApiReady(true);
        setApiError(null);
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') return;
        setApiReady(false);
        setApiLatency(null);
        setApiError(errorMessage(error));
      });
  }, []);

  // Initial mount: load datasets and initial date ranges
  useEffect(() => {
    const controller = new AbortController();
    checkApi(controller.signal);
    setDatasetState('loading');

    api
      .datasets(controller.signal)
      .then((items) => {
        setDatasets(items);
        setDatasetState('loaded');
        setDatasetError(null);
        if (items.length > 0) {
          const initial = items.find((item) => item.dataset === 'metr-la') ?? items[0]!;
          setSelectedDataset(initial.dataset);
          const initialRange = dateRangeFor(initial, 24);
          setAppliedRange(initialRange);
          setQuickHours(24);
          setDraftRange({
            start: toDateTimeLocal(initialRange.start),
            end: toDateTimeLocal(initialRange.end),
          });
        }
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') return;
        setDatasetState('error');
        setDatasetError(errorMessage(error));
      });

    return () => controller.abort();
  }, [checkApi, refreshToken]);

  // Load detailed metadata for the selected dataset
  useEffect(() => {
    if (!selectedDataset) return;
    const controller = new AbortController();
    setDatasetDetail(null);
    api
      .dataset(selectedDataset, controller.signal)
      .then(setDatasetDetail)
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') return;
        setDatasetDetail(null);
      });
    return () => controller.abort();
  }, [selectedDataset]);

  // Load KPIs, trends, and recent records for Command Center and Analytics
  useEffect(() => {
    if (!selectedDataset || !appliedRange || !['command-center', 'analytics'].includes(section)) return;
    const controller = new AbortController();

    if (section === 'command-center') {
      setKpiState('loading');
      setKpiError(null);
      api
        .kpis(selectedDataset, appliedRange.start, appliedRange.end, controller.signal)
        .then((value) => {
          setKpis(value);
          setKpiState('loaded');
        })
        .catch((error: unknown) => {
          if (error instanceof DOMException && error.name === 'AbortError') return;
          setKpiState('error');
          setKpiError(errorMessage(error));
        });

      const endAt = new Date(appliedRange.end).getTime();
      const rangeStartAt = new Date(appliedRange.start).getTime();
      const recentStart = new Date(Math.max(rangeStartAt, endAt - 6 * 60 * 60 * 1000)).toISOString();
      setRecentState('loading');
      setRecentError(null);
      api
        .metrics(selectedDataset, recentStart, appliedRange.end, 50, null, controller.signal)
        .then((page) => {
          setRecentRows(page.items);
          setRecentState('loaded');
        })
        .catch((error: unknown) => {
          if (error instanceof DOMException && error.name === 'AbortError') return;
          setRecentState('error');
          setRecentError(errorMessage(error));
        });
    }

    setTrendState('loading');
    setTrendError(null);
    api
      .trends(selectedDataset, appliedRange.start, appliedRange.end, bucket, controller.signal)
      .then((value) => {
        setTrends(value);
        setTrendState('loaded');
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') return;
        setTrendState('error');
        setTrendError(errorMessage(error));
      });

    return () => controller.abort();
  }, [selectedDataset, appliedRange, bucket, section, overviewRefresh]);

  // Load forecast when visiting Predictive Intelligence
  useEffect(() => {
    if (section !== 'forecasting' || !selectedDataset) return;
    const controller = new AbortController();
    setForecastState('loading');
    setForecastError(null);
    setForecast(null);

    api
      .forecast(selectedDataset, controller.signal)
      .then((value) => {
        setForecast(value);
        setForecastState('loaded');
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') return;
        setForecast(null);
        setForecastState('error');
        setForecastError(errorMessage(error));
      });

    return () => controller.abort();
  }, [section, selectedDataset, forecastRetry]);

  // Load report-derived model metadata for Predictive Intelligence and System pages.
  useEffect(() => {
    if (section !== 'forecasting' && section !== 'system-intelligence') return;
    const datasetsToCheck: DatasetId[] =
      section === 'system-intelligence'
        ? ['metr-la', 'pems-bay']
        : selectedDataset
          ? [selectedDataset]
          : [];
    if (datasetsToCheck.length === 0) return;

    const controller = new AbortController();
    datasetsToCheck.forEach((datasetId) => {
      setModelMetadata((current) => ({ ...current, [datasetId]: null }));
      setModelMetadataState((current) => ({ ...current, [datasetId]: 'loading' }));
      setModelMetadataError((current) => ({ ...current, [datasetId]: null }));
    });

    Promise.all(
      datasetsToCheck.map(async (datasetId) => {
        const value = await api.modelMetadata(datasetId, controller.signal);
        return { datasetId, value };
      }),
    )
      .then((results) => {
        results.forEach(({ datasetId, value }) => {
          setModelMetadata((current) => ({ ...current, [datasetId]: value }));
          setModelMetadataState((current) => ({ ...current, [datasetId]: 'loaded' }));
        });
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') return;
        const message = errorMessage(error);
        datasetsToCheck.forEach((datasetId) => {
          setModelMetadataError((current) => ({ ...current, [datasetId]: message }));
          setModelMetadataState((current) => ({ ...current, [datasetId]: 'error' }));
        });
      });

    return () => controller.abort();
  }, [section, selectedDataset, forecastRetry, refreshToken]);

  // Keyset cursor pagination for Data Explorer
  useEffect(() => {
    if (section !== 'explorer' || !selectedDataset || !appliedRange) return;
    const controller = new AbortController();
    const after = explorerPage === 0 ? null : explorerCursor;

    if (explorerPage === 0) {
      setExplorerRows([]);
      setExplorerCursor(null);
      setExplorerHasMore(false);
      setExplorerError(null);
      setExplorerState('loading');
    } else {
      setExplorerLoadingMore(true);
    }

    api
      .metrics(selectedDataset, appliedRange.start, appliedRange.end, 100, after, controller.signal)
      .then((page: MetricsPage) => {
        setExplorerRows((rows) => (explorerPage === 0 ? page.items : [...rows, ...page.items]));
        setExplorerCursor(page.next_cursor);
        setExplorerHasMore(page.has_more);
        setExplorerState('loaded');
        setExplorerLoadingMore(false);
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') return;
        setExplorerError(errorMessage(error));
        setExplorerState('error');
        setExplorerLoadingMore(false);
      });

    return () => controller.abort();
  }, [section, selectedDataset, appliedRange, explorerPage]);

  const chooseDataset = (value: string) => {
    if (value !== 'metr-la' && value !== 'pems-bay') return;
    const info = datasets.find((item) => item.dataset === value);
    setSelectedDataset(value);
    setExplorerPage(0);
    if (info) {
      const range = dateRangeFor(info, 24);
      setAppliedRange(range);
      setQuickHours(24);
      setDraftRange({
        start: toDateTimeLocal(range.start),
        end: toDateTimeLocal(range.end),
      });
    }
  };

  const updateDraftRange = (key: keyof RangeDraft, value: string) => {
    setQuickHours(null);
    setDraftRange((current) => (current ? { ...current, [key]: value } : null));
  };

  const applyRange = () => {
    if (!draftRange) return;
    const start = localInputToIso(draftRange.start);
    const end = localInputToIso(draftRange.end);
    if (!start || !end || new Date(end) <= new Date(start)) {
      setRangeError('Choose a valid start and end time; the end must be later than the start.');
      return;
    }
    setRangeError(null);
    setQuickHours(null);
    setAppliedRange({ start, end });
    setExplorerPage(0);
  };

  const chooseQuickRange = (hours: number) => {
    if (!selectedInfo) return;
    const range = dateRangeFor(selectedInfo, hours);
    setDraftRange({ start: toDateTimeLocal(range.start), end: toDateTimeLocal(range.end) });
    setAppliedRange(range);
    setQuickHours(hours);
    setRangeError(null);
    setExplorerPage(0);
  };

  const loadMore = () => {
    if (explorerHasMore && explorerCursor && !explorerLoadingMore) {
      setExplorerPage((page) => page + 1);
    }
  };

  const switchSection = (next: Section) => {
    if (next === 'explorer' && section !== 'explorer') {
      setExplorerRows([]);
      setExplorerCursor(null);
      setExplorerHasMore(false);
      setExplorerPage(0);
    }
    setSection(next);
    setMobileNavOpen(false);
  };

  const activeDataset = datasetDetail ?? selectedInfo;

  return (
    <div className="app-shell min-h-screen bg-slate-950 text-slate-100 flex">
      {/* Sidebar Command Console */}
      <aside className={`sidebar ${mobileNavOpen ? 'mobile-open' : ''}`}>
        <div className="brand-lockup">
          <div className="brand-mark">
            <Waves size={20} strokeWidth={2.4} className="text-cyan-400" />
          </div>
          <div>
            <span className="brand-name font-bold tracking-tight text-slate-100">FLOWSTATE</span>
            <span className="brand-caption text-[9px] font-bold tracking-widest text-cyan-400 uppercase">
              COMMAND CENTER
            </span>
          </div>
          <button
            className="mobile-close"
            type="button"
            aria-label="Close navigation"
            onClick={() => setMobileNavOpen(false)}
          >
            <X size={18} />
          </button>
        </div>

        <div className="workspace-label">COMMAND WORKSPACES</div>
        <nav className="nav-list" aria-label="Main navigation">
          {navigation.map(({ id, label, icon: Icon, badge }) => (
            <button
              key={id}
              type="button"
              className={`nav-item ${section === id ? 'selected' : ''}`}
              onClick={() => switchSection(id)}
            >
              <Icon size={17} strokeWidth={1.8} />
              <span className="flex-1 text-left">{label}</span>
              {badge && (
                <span className="text-[9px] font-mono px-1.5 py-0.5 rounded bg-indigo-950/80 text-indigo-300 border border-indigo-700/50">
                  {badge}
                </span>
              )}
            </button>
          ))}
        </nav>

        <div className="sidebar-spacer" />

        {/* Live Architecture Status Card */}
        <div className="pipeline-card">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-1.5 text-xs font-semibold text-slate-200">
              <Database size={14} className="text-cyan-400" />
              <span>Big Data Engine</span>
            </div>
            <span className="text-[10px] font-mono text-cyan-400 font-medium">v1.0</span>
          </div>

          <div className="pipeline-track my-2.5">
            <span className="pipeline-node" title="Kafka 4.0 Streaming Ingestion (configured)" />
            <i />
            <span className="pipeline-node" title="Spark 4.0 Deduplication Engine (configured)" />
            <i />
            <span className={`pipeline-node ${apiReady ? 'done' : ''}`} title="FastAPI Analytics Gateway" />
            <i />
            <span className="pipeline-node model" title="Gradient Boosting ML (inference only)" />
          </div>
          <div className="pipeline-labels text-[9px] font-mono text-slate-400 flex justify-between">
            <span>Kafka</span>
            <span>Spark</span>
            <span>FastAPI</span>
            <span>ML</span>
          </div>

          <div className="mt-3 pt-2 border-t border-slate-800/80 text-[10px] text-slate-400 flex items-center justify-between">
            <span>Streaming Mode</span>
            <span className="text-amber-400 font-mono font-medium">Historical Replay</span>
          </div>
        </div>

        <div className="sidebar-foot flex items-center justify-between pt-3 text-xs text-slate-400">
          <div className="flex items-center gap-2">
            <span className={`connection-dot ${apiReady ? 'connected' : ''}`} />
            <span className="text-[11px]">
              {apiReady ? 'API Core Active' : apiReady === false ? 'API Core Offline' : 'Probing Core'}
            </span>
          </div>
          {apiLatency !== null && (
            <span className="font-mono text-[10px] text-slate-500">{apiLatency}ms</span>
          )}
        </div>
      </aside>

      {mobileNavOpen && (
        <button
          className="mobile-scrim"
          aria-label="Close navigation"
          onClick={() => setMobileNavOpen(false)}
        />
      )}

      {/* Main Command Center Shell */}
      <main className="main-shell flex-1 flex flex-col min-w-0">
        <header className="topbar">
          <button
            className="mobile-menu"
            type="button"
            aria-label="Open navigation"
            onClick={() => setMobileNavOpen(true)}
          >
            <Menu size={20} />
          </button>

          <div className="breadcrumb flex items-center gap-2 text-xs">
            <span className="text-slate-400 font-medium">Traffic Intelligence</span>
            <span className="crumb-divider text-slate-600">/</span>
            <strong className="text-slate-200 font-semibold">{activeCopy.title}</strong>
          </div>

          <div className="topbar-right flex items-center gap-3">
            <div className="replay-badge flex items-center gap-1.5 px-2.5 py-1 rounded bg-amber-950/40 border border-amber-800/60 text-amber-300 text-[11px] font-mono">
              <span className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse" />
              <span>SIMULATED STREAMING REPLAY</span>
            </div>

            <div className="topbar-divider" />

            <button
              type="button"
              className="health-button flex items-center gap-1.5 px-2 py-1 rounded hover:bg-slate-800 text-xs text-slate-300 transition-colors"
              onClick={() => checkApi()}
              title="Refresh API readiness"
            >
              <span className={`connection-dot ${apiReady ? 'connected' : ''}`} />
              <span className="hidden sm:inline text-[11px]">
                {apiReady
                  ? 'API READY · INFRASTRUCTURE PARTIALLY VERIFIED'
                  : apiReady === false
                    ? 'API UNAVAILABLE · INFRASTRUCTURE UNVERIFIED'
                    : 'CONNECTING · INFRASTRUCTURE PENDING'}
              </span>
              <RefreshCw size={12} className="text-slate-400 ml-0.5" />
            </button>
          </div>
        </header>

        <div className="page-content flex-1 p-6 max-w-7xl w-full mx-auto space-y-6">
          {/* Header & Dataset Selector */}
          <div className="page-title-row flex flex-wrap items-end justify-between gap-4">
            <div>
              <div className="eyebrow text-xs font-bold font-mono tracking-wider text-cyan-400 uppercase">
                {activeCopy.eyebrow}
              </div>
              <h1 className="text-2xl sm:text-3xl font-bold tracking-tight text-slate-100 mt-1">
                {activeCopy.title}
              </h1>
              <p className="page-subtitle text-xs sm:text-sm text-slate-400 mt-1">
                {activeCopy.subtitle}
              </p>
            </div>

            {/* Dataset Switcher with Badge */}
            <div className="dataset-control flex items-center gap-2">
              <label htmlFor="dataset-select" className="text-xs font-semibold text-slate-400 font-mono">
                DATASET:
              </label>
              <div className="select-wrap">
                <span className="dataset-avatar font-mono font-bold">
                  {selectedDataset === 'pems-bay' ? 'PB' : 'LA'}
                </span>
                <select
                  id="dataset-select"
                  value={selectedDataset ?? ''}
                  onChange={(event) => chooseDataset(event.target.value)}
                  disabled={datasetState !== 'loaded'}
                  className="bg-transparent text-xs font-medium text-slate-100 cursor-pointer"
                >
                  {datasets.map((item) => (
                    <option key={item.dataset} value={item.dataset} className="bg-slate-900 text-slate-100">
                      {item.display_name} ({item.sensor_count} Sensors)
                    </option>
                  ))}
                </select>
                <ChevronDown size={14} className="text-slate-400 pointer-events-none" />
              </div>
            </div>
          </div>

          {/* Global Alert Banners */}
          {apiError && (
            <div className="notice danger" role="status">
              <AlertTriangle size={18} className="text-rose-400 shrink-0" />
              <div>
                <strong>API Gateway Disconnected</strong>
                <span className="block text-xs mt-0.5">{apiError}</span>
              </div>
              <button
                type="button"
                onClick={() => checkApi()}
                className="ml-auto text-xs px-2.5 py-1 rounded bg-rose-950/60 border border-rose-800 text-rose-300 font-medium"
              >
                <RefreshCw size={12} className="inline mr-1" /> Reconnect
              </button>
            </div>
          )}

          {datasetError && (
            <div className="notice danger" role="alert">
              <AlertTriangle size={18} className="text-rose-400 shrink-0" />
              <div>
                <strong>Dataset Metadata Missing</strong>
                <span className="block text-xs mt-0.5">{datasetError}</span>
              </div>
              <button
                type="button"
                onClick={() => setRefreshToken((v) => v + 1)}
                className="ml-auto text-xs px-2.5 py-1 rounded bg-rose-950/60 border border-rose-800 text-rose-300 font-medium"
              >
                <RefreshCw size={12} className="inline mr-1" /> Retry
              </button>
            </div>
          )}

          {/* Time Window Filter Bar (shown on Command Center, Analytics, Explorer) */}
          {section !== 'forecasting' && section !== 'system-intelligence' && (
            <section className="filterbar flex flex-wrap items-center gap-3 p-3 rounded-xl bg-slate-900/80 border border-slate-800 shadow-lg">
              <div className="filter-title flex items-center gap-1.5 text-xs font-semibold text-slate-300">
                <SlidersHorizontal size={14} className="text-cyan-400" />
                <span>Time Horizon:</span>
              </div>

              <div className="date-field flex items-center gap-2 px-2.5 py-1.5 rounded-lg bg-slate-950 border border-slate-800 text-xs">
                <CalendarDays size={13} className="text-slate-400" />
                <label htmlFor="range-start" className="text-slate-400 text-[11px]">
                  From
                </label>
                <input
                  id="range-start"
                  type="datetime-local"
                  value={draftRange?.start ?? ''}
                  onChange={(event) => updateDraftRange('start', event.target.value)}
                  disabled={!draftRange}
                  className="bg-transparent border-0 text-slate-200 text-xs focus:outline-none"
                />
              </div>

              <span className="text-slate-500 text-xs font-medium">to</span>
<span className="text-[10px] text-slate-500 ml-2 opacity-60">start inclusive, end exclusive</span>

              <div className="date-field flex items-center gap-2 px-2.5 py-1.5 rounded-lg bg-slate-950 border border-slate-800 text-xs">
                <label htmlFor="range-end" className="text-slate-400 text-[11px]">
                  To
                </label>
                <input
                  id="range-end"
                  type="datetime-local"
                  value={draftRange?.end ?? ''}
                  onChange={(event) => updateDraftRange('end', event.target.value)}
                  disabled={!draftRange}
                  className="bg-transparent border-0 text-slate-200 text-xs focus:outline-none"
                />
              </div>

              {/* Quick Presets */}
              <div className="quick-filters flex rounded-lg bg-slate-950 p-1 border border-slate-800 text-xs">
                <button
                  type="button"
                  className={quickHours === 1 ? 'quick-selected' : ''}
                  onClick={() => chooseQuickRange(1)}
                >
                  1h
                </button>
                <button
                  type="button"
                  className={quickHours === 6 ? 'quick-selected' : ''}
                  onClick={() => chooseQuickRange(6)}
                >
                  6h
                </button>
                <button
                  type="button"
                  className={quickHours === 24 ? 'quick-selected' : ''}
                  onClick={() => chooseQuickRange(24)}
                >
                  24h
                </button>
                <button
                  type="button"
                  className={quickHours === 168 ? 'quick-selected' : ''}
                  onClick={() => chooseQuickRange(168)}
                >
                  7d
                </button>
              </div>

              <button
                type="button"
                className="apply-button flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-cyan-500/20 hover:bg-cyan-500/30 text-cyan-300 border border-cyan-500/40 text-xs font-semibold transition-all disabled:opacity-40"
                onClick={applyRange}
                disabled={!draftRange}
              >
                <Check size={13} /> Apply Range
              </button>

              {rangeError && (
                <span className="text-xs text-rose-400 w-full pl-1 mt-1" role="alert">
                  {rangeError}
                </span>
              )}
            </section>
          )}

          {/* Section 1: Command Center / Overview */}
          {selectedDataset && appliedRange && section === 'command-center' && (
            <OverviewSection
              dataset={activeDataset}
              kpis={kpis}
              kpiState={kpiState}
              kpiError={kpiError}
              trends={trends}
              trendState={trendState}
              trendError={trendError}
              recentRows={recentRows}
              recentState={recentState}
              recentError={recentError}
              onRefresh={() => setOverviewRefresh((v) => v + 1)}
            />
          )}

          {/* Section 2: Traffic Analytics */}
          {selectedDataset && appliedRange && section === 'analytics' && (
            <AnalyticsSection
              dataset={activeDataset}
              trends={trends}
              state={trendState}
              error={trendError}
              bucket={bucket}
              setBucket={setBucket}
            />
          )}

          {/* Section 3: Predictive Intelligence */}
          {section === 'forecasting' && (
            <>
              <ForecastSection
                dataset={activeDataset}
                forecast={forecast}
                state={forecastState}
                error={forecastError}
                metadata={selectedDataset ? modelMetadata[selectedDataset] ?? null : null}
                metadataState={selectedDataset ? modelMetadataState[selectedDataset] ?? 'idle' : 'idle'}
                metadataError={selectedDataset ? modelMetadataError[selectedDataset] ?? null : null}
                onRetry={() => setForecastRetry((v) => v + 1)}
              />
              {/* Offline Model Evaluation Metrics Section */}
              {selectedDataset && modelMetadata[selectedDataset] && (
                <div className="mt-6 space-y-4">
                  <div className="bg-slate-950/80 border border-slate-700 rounded-lg p-4">
                    <div className="text-[10px] font-bold font-mono text-cyan-400 tracking-wider uppercase">Speed Model Evaluation</div>
                    <div className="text-xs text-slate-400 mt-2">
                      <strong>Selected model:</strong> {modelMetadata[selectedDataset].speed.selected_model}<br />
                      <strong>MAE:</strong> {modelMetadata[selectedDataset].speed.mae} mph — mean absolute error (offline test set)<br />
                      <strong>R²:</strong> {modelMetadata[selectedDataset].speed.r2} — coefficient of determination (offline test set)<br />
                      <strong>Test samples:</strong> {modelMetadata[selectedDataset].speed.test_samples} windows
                    </div>
                  </div>

                  <div className="bg-slate-950/80 border border-slate-700 rounded-lg p-4">
                    <div className="text-[10px] font-bold font-mono text-amber-400 tracking-wider uppercase">Congestion Model Evaluation</div>
                    <div className="text-xs text-slate-400 mt-2">
                      <strong>Selected model:</strong> {modelMetadata[selectedDataset].congestion.selected_model}<br />
                      <strong>MAE:</strong> {modelMetadata[selectedDataset].congestion.mae}% — mean absolute error in percentage points (offline test set)<br />
                      <strong>R²:</strong> {modelMetadata[selectedDataset].congestion.r2} — coefficient of determination (offline test set)<br />
                      <strong>Test samples:</strong> {modelMetadata[selectedDataset].congestion.test_samples} windows
                    </div>
                  </div>

                  <div className="bg-slate-950/80 border border-slate-700 rounded-lg p-4">
                    <div className="text-[10px] font-bold font-mono text-emerald-400 tracking-wider uppercase">Model Training Information</div>
                    <div className="text-xs text-slate-400 mt-2">
                      <strong>Training samples:</strong> {modelMetadata[selectedDataset].training_samples} windows<br />
                      <strong>Validation samples:</strong> {modelMetadata[selectedDataset].validation_samples} windows<br />
                      <strong>Input features:</strong> {modelMetadata[selectedDataset].feature_count}<br />
                      <strong>Model version:</strong> {modelMetadata[selectedDataset].model_version}<br />
                      <strong>Prediction horizon:</strong> {modelMetadata[selectedDataset].prediction_horizon_minutes} minutes
                    </div>
                  </div>

                  <div className="bg-rose-950/20 border border-rose-800/50 rounded-lg p-4 text-xs text-rose-300">
                    <strong>Disclaimer:</strong> These are offline test evaluation metrics from historical dataset evaluation. They describe performance on a fixed test set and do not represent live prediction accuracy, prediction confidence, or guaranteed future performance. This project uses historical dataset replay with simulated streaming, not live real-world traffic.
                  </div>
                </div>
              )}
            </>
          )}

          {/* Section 4: Route Planner */}
          {section === 'route-planner' && (
            <Suspense fallback={<div className="panel chart-message">Loading Route Planner…</div>}>
              <RouteRecommendation />
            </Suspense>
          )}

          {/* Section 5: Data Explorer */}
          {selectedDataset && appliedRange && section === 'explorer' && (
            <ExplorerSection
              rows={explorerRows}
              state={explorerState}
              error={explorerError}
              hasMore={explorerHasMore}
              loadingMore={explorerLoadingMore}
              onLoadMore={loadMore}
            />
          )}

          {/* Section 5: System Intelligence & Architecture */}
          {section === 'system-intelligence' && (
            <SystemIntelligenceSection
              datasets={datasets}
              selectedDataset={selectedDataset}
              apiReady={apiReady}
              apiLatency={apiLatency}
              apiBase={API_BASE_URL}
              modelMetadata={modelMetadata}
              metadataStates={modelMetadataState}
              metadataErrors={modelMetadataError}
            />
          )}
        </div>
      </main>
    </div>
  );
}

// -------------------------------------------------------------
// SECTION 1: COMMAND CENTER / OVERVIEW
// -------------------------------------------------------------
function OverviewSection({
  dataset,
  kpis,
  kpiState: _kpiState,
  kpiError,
  trends,
  trendState,
  trendError,
  recentRows,
  recentState,
  recentError,
  onRefresh,
}: {
  dataset: DatasetInfo | null;
  kpis: DashboardKpis | null;
  kpiState?: LoadState;
  kpiError: string | null;
  trends: CongestionTrends | null;
  trendState: LoadState;
  trendError: string | null;
  recentRows: MetricRecord[];
  recentState: LoadState;
  recentError: string | null;
  onRefresh: () => void;
}) {
  const metricFreshness = kpis?.freshness_seconds ?? dataset?.freshness_seconds;
  const isHighCongestion = (kpis?.weighted_congestion_percentage ?? 0) >= 50;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between text-xs text-slate-400">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
          <span className="font-mono text-slate-300">Historical Replay Metrics Store</span>
          <span>·</span>
          <span>5-minute tumbling windows</span>
          {metricFreshness != null && (
            <>
              <span>·</span>
              <span className="text-slate-400 font-mono">
                Event-time age: {metricFreshness > 86400 ? `${Math.floor(metricFreshness / 86400)}d` : `${metricFreshness}s ago`} since latest window end
              </span>
            </>
          )}
        </div>
        <button
          type="button"
          className="flex items-center gap-1.5 px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs transition-colors"
          onClick={onRefresh}
        >
          <RefreshCw size={13} /> Refresh KPIs
        </button>
      </div>

      {kpiError && (
        <div className="notice danger">
          <AlertTriangle size={16} />
          <span>{kpiError}</span>
        </div>
      )}

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard
          title="Network Average Speed"
          value={numberValue(kpis?.weighted_average_speed)}
          unit="mph"
          icon={Gauge}
          tone="cyan"
          hint={kpis ? `${kpis.window_count.toLocaleString()} windows (reading-weighted average)` : 'Network mean velocity (reading-weighted)'}
          status={
            kpis?.weighted_average_speed != null
              ? kpis.weighted_average_speed >= 45
                ? 'Free Flow (UI: speed band)'
                : kpis.weighted_average_speed >= 20
                ? 'Moderate Flow (UI: speed band)'
                : 'Heavy Congestion'
              : undefined
          }
        />

        <KpiCard
          title="Congestion Share"
          value={numberValue(kpis?.weighted_congestion_percentage)}
          unit="%"
          icon={Activity}
          tone="amber"
          hint="Readings below 20.0 mph threshold"
          warning={isHighCongestion}
          status={isHighCongestion ? 'Severe Congestion' : 'Optimal Band'}
        />

        <KpiCard
          title="Total Sensor Readings"
          value={kpis?.total_sensor_readings ? kpis.total_sensor_readings.toLocaleString() : '—'}
          unit="readings"
          icon={Radio}
          tone="blue"
          hint="Aggregated valid measurements"
        />

        <KpiCard
          title="Congested Readings"
          value={kpis?.congested_sensor_count ? kpis.congested_sensor_count.toLocaleString() : '—'}
          unit="readings"
          icon={AlertCircle}
          tone="orange"
          hint="Individual sensor readings < 20 mph"
          warning={kpis ? kpis.congested_sensor_count > 0 : false}
        />
      </div>

        {/* Congestion threshold note */}
        <div className="text-[10px] text-slate-500 mt-1">
          <span className="font-medium text-slate-300">20 mph:</span> individual reading congestion threshold &bull; <span className="font-medium text-slate-400">50%</span>: network congestion classification threshold
        </div>

      {/* Dual Command Center Main Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        <div className="lg:col-span-8">
          <Suspense fallback={<div className="panel chart-message">Initializing Recharts Telemetry…</div>}>
            <TrafficChart
              data={trends?.items ?? []}
              title="Highway Velocity & Congestion Profile"
              description="Weighted average speed (mph) vs congestion share (%) across the active horizon."
              loading={trendState === 'loading'}
              error={trendState === 'error' ? trendError : null}
            />
          </Suspense>
        </div>

        {/* Replaced Illustrative Schematic with Real Network Telemetry */}
        <div className="lg:col-span-4">
          <NetworkCoverage dataset={dataset} kpis={kpis} />
        </div>
      </div>

      {/* Lower Grid: Recent Windows Preview + Dataset Registry Card */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        <section className="panel recent-panel lg:col-span-8">
          <div className="panel-heading">
            <div>
              <div className="section-tag-inline">
                <Clock3 size={12} className="text-cyan-400" />
                <span>MICRO-BATCH STREAM PREVIEW</span>
              </div>
              <h2 className="mt-0.5 text-base font-semibold text-slate-100">
                Recent 5-Minute Metric Windows
              </h2>
              <p className="text-xs text-slate-400">
                Latest aggregated snapshots written by Spark Structured Streaming
              </p>
            </div>
            {dataset && (
              <span className="px-2 py-0.5 rounded text-xs font-mono font-medium bg-cyan-950/60 border border-cyan-800 text-cyan-300">
                {dataset.display_name}
              </span>
            )}
          </div>

          <MetricTable
            rows={recentRows}
            loading={recentState === 'loading'}
            error={recentState === 'error' ? recentError : null}
            hasMore={false}
            loadingMore={false}
            compact
          />
        </section>

        {/* Dataset Registry Card */}
        <section className="panel context-panel lg:col-span-4">
          <div className="panel-heading">
            <div>
              <div className="section-tag-inline">
                <Database size={12} className="text-cyan-400" />
                <span>DATASET REGISTRY</span>
              </div>
              <h2 className="mt-0.5 text-base font-semibold text-slate-100">Active Benchmark</h2>
              <p className="text-xs text-slate-400">Metadata from analytics catalog</p>
            </div>
          </div>

          {dataset ? (
            <div className="space-y-3 px-4 pb-4 mt-2">
              <div className="dataset-identity flex items-center gap-3 p-3 rounded-lg bg-slate-900/80 border border-slate-800">
                <span className="dataset-avatar large font-mono font-bold text-sm">
                  {dataset.dataset === 'metr-la' ? 'LA' : 'PB'}
                </span>
                <div>
                  <strong className="text-sm font-semibold text-slate-100">{dataset.display_name}</strong>
                  <div className="text-xs text-slate-400 font-mono">
                    {dataset.sensor_count} loop detectors · {dataset.dataset.toUpperCase()}
                  </div>
                </div>
              </div>

              <div className="space-y-2 text-xs">
                <div className="flex justify-between py-1.5 border-b border-slate-800/80">
                  <span className="text-slate-400">Aggregated Windows</span>
                  <span className="font-mono font-bold text-slate-200">
                    {dataset.metric_row_count.toLocaleString()}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-800/80">
                  <span className="text-slate-400">Latest Completed Window</span>
                  <span className="font-mono text-slate-300">
                    {readableTimestamp(dataset.latest_window_end)}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-800/80">
                  <span className="text-slate-400">ML Model Status</span>
                  <span
                    className={`font-mono font-semibold ${
                      dataset.model_available ? 'text-emerald-400' : 'text-slate-500'
                    }`}
                  >
                    {dataset.model_available ? `v${dataset.model_version}` : 'Unavailable'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-800/80">
                  <span className="text-slate-400">Data Provenance</span>
                  <span className="font-mono text-amber-400 font-medium">Historical Benchmark</span>
                </div>
              </div>

              <div className="p-2.5 rounded bg-slate-950/80 border border-slate-800/80 text-[11px] text-slate-400 leading-relaxed">
                Source data from Caltrans PeMS Highway Sensor Network. Simulated stream ingestion via Kafka.
              </div>
            </div>
          ) : (
            <div className="table-state">Loading dataset metadata…</div>
          )}
        </section>
      </div>

      <div className="bottom-provenance text-xs text-slate-400 flex items-center gap-2 pt-2">
        <ShieldCheck size={14} className="text-cyan-400 shrink-0" />
        <span>
          Academic demonstration environment. Historical benchmark datasets replayed through simulated Kafka & Spark pipelines.
        </span>
      </div>
    </div>
  );
}

function KpiCard({
  title,
  value,
  unit,
  icon: Icon,
  tone,
  hint,
  warning = false,
  status,
}: {
  title: string;
  value: string;
  unit: string;
  icon: typeof Activity;
  tone: 'cyan' | 'amber' | 'blue' | 'orange';
  hint: string;
  warning?: boolean;
  status?: string;
}) {
  return (
    <article className={`kpi-card tone-${tone} p-4 rounded-xl bg-slate-900/90 border border-slate-800 shadow-md`}>
      <div className="flex items-center justify-between text-xs text-slate-400">
        <span className="font-medium text-slate-300">{title}</span>
        <span className="kpi-icon p-1.5 rounded-lg">
          <Icon size={16} />
        </span>
      </div>

      <div className="my-2.5 flex items-baseline gap-1.5">
        <span className="text-2xl sm:text-3xl font-bold font-mono tracking-tight text-slate-100">
          {value}
        </span>
        <span className="text-xs font-mono text-slate-400">{unit}</span>
      </div>

      <div className="flex items-center justify-between text-xs pt-1 border-t border-slate-800/60">
        <span className="text-[11px] text-slate-400">{hint}</span>
        {status && (
          <span
            className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${
              warning
                ? 'bg-rose-950/80 text-rose-300 border border-rose-800/60 font-semibold'
                : 'bg-emerald-950/80 text-emerald-300 border border-emerald-800/60'
            }`}
          >
            {status}
          </span>
        )}
      </div>
    </article>
  );
}

// -------------------------------------------------------------
// SECTION 2: TRAFFIC ANALYTICS
// -------------------------------------------------------------
function AnalyticsSection({
  dataset,
  trends,
  state,
  error,
  bucket,
  setBucket,
}: {
  dataset: DatasetInfo | null;
  trends: CongestionTrends | null;
  state: LoadState;
  error: string | null;
  bucket: CongestionTrends['bucket'];
  setBucket: (bucket: CongestionTrends['bucket']) => void;
}) {
  return (
    <div className="space-y-6">
      {/* Analytics Toolbar */}
      <div className="flex flex-wrap items-center justify-between gap-4 p-3 rounded-xl bg-slate-900/80 border border-slate-800">
        <div className="flex items-center gap-2 text-xs">
          <BarChart3 size={15} className="text-cyan-400" />
          <span className="font-semibold text-slate-200">
            {dataset?.display_name ?? 'Dataset'} Multi-Scale Time Aggregation
          </span>
          <span className="text-slate-500">·</span>
          <span className="font-mono text-cyan-400">{trends?.items.length ?? 0} sample buckets</span>
        </div>

        {/* Bucket Selector */}
        <div className="flex items-center gap-2 text-xs">
          <label className="text-slate-400 text-[11px] font-semibold font-mono">
            AGGREGATION BUCKET:
          </label>
          <div className="select-wrap" style={{ height: 32 }}>
            <select
              value={bucket}
              onChange={(e) => setBucket(e.target.value as CongestionTrends['bucket'])}
              className="bg-transparent text-xs font-mono text-slate-200 cursor-pointer"
            >
              <option value="5m" className="bg-slate-900">
                5-Minute Aggregate Window
              </option>
              <option value="15m" className="bg-slate-900">
                15 Minutes (Quarter-Hour)
              </option>
              <option value="1h" className="bg-slate-900">
                1 Hour (Hourly Aggregation)
              </option>
              <option value="1d" className="bg-slate-900">
                1 Day (Daily Average)
              </option>
            </select>
            <ChevronDown size={13} className="text-slate-400 pointer-events-none" />
          </div>
        </div>
      </div>

      {/* Dual Detailed Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <Suspense fallback={<div className="panel chart-message">Rendering Velocity Curve…</div>}>
          <TrafficChart
            data={trends?.items ?? []}
            title="Highway Average Speed Profile"
            description="Unweighted average of displayed 5-minute bucket averages (mph)."
            loading={state === 'loading'}
            error={state === 'error' ? error : null}
            variant="speed"
          />
        </Suspense>

        <Suspense fallback={<div className="panel chart-message">Rendering Congestion Distribution…</div>}>
          <TrafficChart
            data={trends?.items ?? []}
            title="Congestion Severity Rate"
            description="Proportion of valid readings below 20.0 mph."
            loading={state === 'loading'}
            error={state === 'error' ? error : null}
            variant="congestion"
          />
        </Suspense>
      </div>

      {/* Analytics Methodology Card */}
      <section className="panel p-5 rounded-xl bg-slate-900/80 border border-slate-800">
        <div className="flex items-center gap-2 mb-3">
          <Info size={16} className="text-cyan-400" />
          <h3 className="text-sm font-semibold text-slate-100">
            Weighted Aggregation & Methodology Reference
          </h3>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
          <div className="p-3 rounded-lg bg-slate-950/60 border border-slate-800/80">
            <strong className="text-cyan-400 font-semibold block mb-1">
              Sensor-Weighted Velocity
            </strong>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              Speeds are aggregated via weighted sums: Speed = &Sigma;(speed &times; readings) / &Sigma;(readings).
              Eliminates bias from sensors with intermittent packet loss.
            </p>
          </div>

          <div className="p-3 rounded-lg bg-slate-950/60 border border-slate-800/80">
            <strong className="text-amber-400 font-semibold block mb-1">
              Congestion Index (Cutoff: 20 mph)
            </strong>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              Any reading strictly below 20.0 mph increments the congested sensor counter. Congestion % =
              congested / total valid readings &times; 100.
            </p>
          </div>

          <div className="p-3 rounded-lg bg-slate-950/60 border border-slate-800/80">
            <strong className="text-indigo-400 font-semibold block mb-1">
              PostgreSQL date_bin Windowing
            </strong>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              Bucketing leverages PostgreSQL 16 <code className="text-slate-300">date_bin()</code> over window_start.
              Maintains strict UTC boundaries independent of client display timezone.
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}

// -------------------------------------------------------------
// SECTION 3: PREDICTIVE INTELLIGENCE (FORECASTING)
// -------------------------------------------------------------
function ForecastSection({
  dataset,
  forecast,
  state,
  error,
  metadata,
  metadataState,
  metadataError,
  onRetry,
}: {
  dataset: DatasetInfo | null;
  forecast: LatestForecast | null;
  state: LoadState;
  error: string | null;
  metadata: ModelMetadata | null;
  metadataState: LoadState;
  metadataError: string | null;
  onRetry: () => void;
}) {
  const isMetr = dataset?.dataset === 'metr-la';
  const shownMetadata = metadata?.dataset === dataset?.dataset ? metadata : null;

  return (
    <div className="space-y-6">
      {/* Provenance Banner */}
      <div className="p-4 rounded-xl bg-amber-950/30 border border-amber-800/50 flex items-start gap-3">
        <AlertTriangle size={18} className="text-amber-400 shrink-0 mt-0.5" />
        <div className="space-y-1.5">
          <h4 className="text-sm font-semibold text-amber-200">
            Historical benchmark replay — not live real-world traffic
          </h4>
          <p className="text-xs text-slate-300 leading-relaxed">
            Forecasts are model predictions, not guaranteed outcomes. Inputs arrive through a simulated
            streaming replay of historical benchmark snapshots.
          </p>
          <div className="flex flex-wrap gap-1.5 text-[10px] font-mono text-amber-300">
            <span className="px-2 py-0.5 rounded bg-amber-950/70 border border-amber-800/60">
              Historical benchmark replay
            </span>
            <span className="px-2 py-0.5 rounded bg-amber-950/70 border border-amber-800/60">
              Simulated streaming provenance
            </span>
            <span className="px-2 py-0.5 rounded bg-amber-950/70 border border-amber-800/60">
              Not live real-world traffic
            </span>
            <span className="px-2 py-0.5 rounded bg-amber-950/70 border border-amber-800/60">
              Predictions, not guaranteed outcomes
            </span>
          </div>
        </div>
      </div>

      {/* Header Context Bar */}
      <div className="p-4 rounded-xl bg-gradient-to-r from-indigo-950/60 via-slate-900 to-slate-900 border border-indigo-800/40 flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-lg bg-indigo-500/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
            <Sparkles size={18} />
          </div>
          <div>
            <h3 className="text-sm font-semibold text-slate-100">
              15-Minute Ahead Network Velocity & Congestion Forecaster
            </h3>
            <p className="text-xs text-slate-400">
              Report-selected models with time-safe lags and chronological 70/15/15 evaluation splits
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <span className="px-2.5 py-1 rounded bg-indigo-950 border border-indigo-700/60 font-mono text-xs text-indigo-300 font-semibold flex items-center gap-1.5">
            <Clock3 size={13} /> +15 MIN HORIZON
          </span>
          <button
            type="button"
            onClick={onRetry}
            className="flex items-center gap-1 px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs transition-colors"
          >
            <RefreshCw size={12} /> Check Inference
          </button>
        </div>
      </div>

      {/* State A: Loading */}
      {state === 'loading' && (
        <div className="panel p-8 rounded-xl bg-slate-900/80 border border-slate-800 flex items-center justify-center gap-3 text-slate-300">
          <div className="w-5 h-5 border-2 border-indigo-400 border-t-transparent rounded-full animate-spin" />
          <span className="text-sm font-medium">
            Validating 13 consecutive 5-minute input windows from PostgreSQL…
          </span>
        </div>
      )}

      {/* State B: Forecast Available */}
      {state === 'loaded' && forecast && (
        <div className="space-y-4">
          <div className="p-3 rounded-xl bg-emerald-950/40 border border-emerald-700/50 flex items-center justify-between">
            <div className="flex items-center gap-2 text-xs text-emerald-300">
              <CheckCircle2 size={16} className="text-emerald-400" />
              <span>
                Inference on historical replay windows completed across 13 completed five-minute windows.
              </span>
            </div>
            <span className="text-xs font-mono text-emerald-400">Target: {readableTimestamp(forecast.target_timestamp)}</span>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
              <span className="text-xs text-slate-400 font-medium">Predicted Average Network Speed</span>
              <div className="flex items-baseline gap-2">
                <span className="text-4xl font-bold font-mono text-cyan-400">
                  {forecast.predicted_average_speed.toFixed(1)}
                </span>
                <span className="text-sm font-mono text-slate-400">mph</span>
              </div>
              <div className="text-xs text-slate-400">
                Model: <span className="font-mono text-slate-300">{forecast.speed_model}</span>
              </div>
            </div>

            <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
              <span className="text-xs text-slate-400 font-medium">Predicted Congestion Rate</span>
              <div className="flex items-baseline gap-2">
                <span className={`text-4xl font-bold font-mono ${
                  forecast.predicted_congestion_percentage >= forecast.congestion_percentage_threshold
                    ? 'text-rose-400'
                    : 'text-amber-400'
                }`}>
                  {forecast.predicted_congestion_percentage.toFixed(1)}%
                </span>
                <span className={`text-xs px-2 py-0.5 rounded font-mono ${
                  forecast.congestion_status === 'congested'
                    ? 'bg-rose-950 text-rose-300 border border-rose-800'
                    : 'bg-emerald-950 text-emerald-300 border border-emerald-800'
                }`}>
                  {forecast.congestion_status.toUpperCase()}
                </span>
              </div>
              <div className="text-xs text-slate-400">
                Status rule: predicted ≥ {forecast.congestion_percentage_threshold}% of readings congested
              </div>
            </div>
          </div>

          <div className="p-3 rounded-lg bg-slate-950/60 border border-slate-800 space-y-1.5 text-[11px] text-slate-400 leading-relaxed">
            <p>
              <strong className="text-slate-300">Forecast horizon:</strong> the target is 15 minutes after
              the last window’s start timestamp, which is approximately 10 minutes after that five-minute
              window ends.
            </p>
            <p>
              <strong className="text-slate-300">Two distinct thresholds:</strong>{' '}
              {forecast.congestion_threshold_speed_below} mph classifies an individual reading as
              congested; {forecast.congestion_percentage_threshold}% is the network congestion-status
              threshold applied to the predicted share of congested readings.
            </p>
          </div>
        </div>
      )}

      {/* State C: Stale replay windows / simulated stream gate */}
      {(state === 'error' || (!forecast && state !== 'loading')) && (
        <div className="p-5 rounded-xl bg-slate-900/90 border border-slate-800 space-y-4">
          <div className="flex items-start gap-3">
            <div className="w-8 h-8 rounded-lg bg-amber-500/10 border border-amber-500/20 flex items-center justify-center text-amber-400 shrink-0 mt-0.5">
              <Clock3 size={18} />
            </div>
            <div className="space-y-1">
              <h4 className="text-sm font-semibold text-slate-100">
                Historical Replay Forecast Standby — Freshness Gate
              </h4>
              <p className="text-xs text-slate-400 leading-relaxed">
                The API requires 13 consecutive five-minute windows with the latest{' '}
                <code className="text-slate-300">window_end</code> no more than{' '}
                <strong>20 minutes</strong> (
                <code className="text-slate-300">API_FORECAST_MAX_INPUT_AGE_SECONDS=1200</code>) behind
                wall-clock UTC. It also requires the 15-minute target to remain in the future. For a
                five-minute window, that makes the effective limit <strong>under 10 minutes</strong>.
                Historical benchmark timestamps ({isMetr ? '2012' : '2017'}) cannot satisfy this gate, so no
                forecast is fabricated.
              </p>
              <div className="text-[11px] text-amber-400/90 font-mono mt-1">
                API Diagnostic: {error ?? 'Latest traffic metrics are stale for forecasting.'}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Report-derived model metadata card */}
      <section className="panel p-5 rounded-xl bg-slate-900/80 border border-slate-800 space-y-4">
        <div className="flex items-center justify-between border-b border-slate-800/80 pb-3">
          <div className="flex items-center gap-2">
            <Cpu size={16} className="text-indigo-400" />
            <h3 className="text-sm font-semibold text-slate-100">
              Offline Model Metadata & Test MAE / R²
            </h3>
          </div>
          <span className="text-xs font-mono text-indigo-300 px-2 py-0.5 rounded bg-indigo-950/60 border border-indigo-800/60">
            {shownMetadata ? `Artifact: ${shownMetadata.model_version}` : 'Artifact metadata unavailable'}
          </span>
        </div>

        {metadataState === 'loading' && (
          <div className="text-xs text-slate-400 font-mono">
            Loading report-derived metadata from the analytics API…
          </div>
        )}

        {metadataError && (
          <div className="flex items-start gap-2 text-xs text-amber-300 bg-amber-950/30 border border-amber-800/50 rounded-lg p-3">
            <AlertCircle size={14} className="shrink-0 mt-0.5" />
            <span>{metadataError}</span>
          </div>
        )}

        {shownMetadata && (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3 text-xs">
              <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800/80">
                <span className="text-slate-400 text-[11px] block">Velocity Model</span>
                <strong className="text-cyan-400 font-mono text-sm block mt-0.5">
                  {shownMetadata.speed.selected_model}
                </strong>
                <span className="text-[10px] text-slate-500 font-mono mt-1 block">
                  {formatModelParameters(shownMetadata.speed.parameters)}
                </span>
              </div>

              <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800/80">
                <span className="text-slate-400 text-[11px] block">Congestion Model</span>
                <strong className="text-amber-400 font-mono text-sm block mt-0.5">
                  {shownMetadata.congestion.selected_model}
                </strong>
                <span className="text-[10px] text-slate-500 font-mono mt-1 block">
                  {formatModelParameters(shownMetadata.congestion.parameters)}
                </span>
              </div>

              <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800/80">
                <span className="text-slate-400 text-[11px] block">Offline Test MAE / R²</span>
                <strong className="text-emerald-400 font-mono text-xs block mt-0.5">
                  Speed: MAE {numberValue(shownMetadata.speed.mae, 3)} · R² {numberValue(shownMetadata.speed.r2, 3)}
                </strong>
                <span className="text-[10px] text-slate-400 font-mono mt-1 block">
                  Congestion: MAE {numberValue(shownMetadata.congestion.mae, 3)} · R²{' '}
                  {numberValue(shownMetadata.congestion.r2, 3)}
                </span>
                <span className="text-[10px] text-slate-500 mt-1 block">
                  {shownMetadata.speed.test_samples.toLocaleString()} held-out windows · offline metrics,
                  not prediction confidence
                </span>
              </div>

              <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800/80">
                <span className="text-slate-400 text-[11px] block">Feature Engineering</span>
                <strong className="text-slate-200 font-mono text-sm block mt-0.5">
                  {shownMetadata.feature_count} engineered features
                </strong>
                <span className="text-[10px] text-slate-500 mt-1 block">
                  {shownMetadata.training_samples.toLocaleString()} train ·{' '}
                  {shownMetadata.validation_samples.toLocaleString()} validation
                </span>
              </div>
            </div>

            {/* Feature Pipeline Breakdown */}
            <div className="pt-3 border-t border-slate-800/80 text-xs space-y-2">
              <span className="text-slate-300 font-semibold text-[11px] block">
                Time-Safe Lag and Cyclical Feature Pipeline:
              </span>
              <div className="flex flex-wrap gap-1.5 text-[11px] font-mono">
                {[
                  'speed_lag_0',
                  'speed_lag_1 (5m)',
                  'speed_lag_2 (10m)',
                  'speed_lag_3 (15m)',
                  'speed_lag_6 (30m)',
                  'speed_lag_12 (60m)',
                  'congestion_lag_0..12',
                  'speed_past_15m_mean',
                  'speed_past_15m_std',
                  'time_of_day_sin/cos',
                  'day_of_week_sin/cos',
                ].map((f) => (
                  <span key={f} className="px-2 py-0.5 rounded bg-slate-800/70 border border-slate-700/60 text-slate-300">
                    {f}
                  </span>
                ))}
              </div>
              <p className="text-[11px] text-slate-400 leading-relaxed pt-1">
                Labels are looked up at the exact timestamp {shownMetadata.prediction_horizon_minutes} minutes
                after each feature timestamp —{' '}
                {shownMetadata.prediction_horizon_minutes - shownMetadata.window_minutes} minutes after the
                last input window ends.
              </p>
            </div>

            <div className="pt-3 border-t border-slate-800/80 text-[10px] text-slate-500 font-mono space-y-1">
              <p>
                Source: evaluation report {shownMetadata.report_path.split(/[\\/]/).pop()} and served artifact{' '}
                {shownMetadata.artifact_path.split(/[\\/]/).pop()} ({formatArtifactSize(shownMetadata.artifact_size_bytes)}).
              </p>
              <p>{shownMetadata.status_logic}</p>
            </div>
          </>
        )}

        {!shownMetadata && metadataState !== 'loading' && !metadataError && (
          <div className="text-xs text-slate-500">
            Model metadata is not available for this dataset.
          </div>
        )}
      </section>
    </div>
  );
}

// -------------------------------------------------------------
// SECTION 4: DATA EXPLORER
// -------------------------------------------------------------
function ExplorerSection({
  rows,
  state,
  error,
  hasMore,
  loadingMore,
  onLoadMore,
}: {
  rows: MetricRecord[];
  state: LoadState;
  error: string | null;
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore: () => void;
}) {
  return (
    <div className="space-y-4">
      <div className="panel p-5 rounded-xl bg-slate-900/80 border border-slate-800">
        <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
          <div>
            <div className="section-tag-inline">
              <Search size={12} className="text-cyan-400" />
              <span>HISTORICAL LAKE INSPECTOR</span>
            </div>
            <h2 className="text-base font-semibold text-slate-100 mt-0.5">
              PostgreSQL 5-Minute Metric Windows
            </h2>
            <p className="text-xs text-slate-400">
              Persisted aggregated records computed from Kafka events by Spark Structured Streaming
            </p>
          </div>

          <span className="text-xs font-mono px-2.5 py-1 rounded bg-slate-800 border border-slate-700 text-slate-300">
            {rows.length.toLocaleString()} records loaded
          </span>
        </div>

        <MetricTable
          rows={rows}
          loading={state === 'loading'}
          error={state === 'error' ? error : null}
          hasMore={hasMore}
          loadingMore={loadingMore}
          onLoadMore={onLoadMore}
        />
      </div>
    </div>
  );
}

// -------------------------------------------------------------
// SECTION 5: SYSTEM INTELLIGENCE & ARCHITECTURE
// -------------------------------------------------------------
function SystemIntelligenceSection({
  datasets,
  selectedDataset,
  apiReady,
  apiLatency,
  apiBase,
  modelMetadata,
  metadataStates,
  metadataErrors,
}: {
  datasets: DatasetInfo[];
  selectedDataset: DatasetId | null;
  apiReady: boolean | null;
  apiLatency: number | null;
  apiBase: string;
  modelMetadata: Partial<Record<DatasetId, ModelMetadata | null>>;
  metadataStates: Partial<Record<DatasetId, LoadState>>;
  metadataErrors: Partial<Record<DatasetId, string | null>>;
}) {
  const dataset = datasets.find((d) => d.dataset === selectedDataset);
  const apiTone = apiReady === null ? 'probing' : apiReady ? 'healthy' : 'unknown';
  const apiLabel = apiReady === null ? 'PROBING' : apiReady ? 'VERIFIED HEALTHY' : 'NOT VERIFIED';
  const postgresLabel = apiReady === null
    ? 'PROBING'
    : apiReady
      ? 'READY VIA /HEALTH/READY'
      : 'NOT VERIFIED';
  const metadataChecks = ['metr-la', 'pems-bay'].map((id) => metadataStates[id as DatasetId]);
  const mlTone = metadataChecks.includes('loading')
    ? 'probing'
    : metadataChecks.every((state) => state === 'loaded')
      ? 'healthy'
      : metadataChecks.includes('loaded')
        ? 'configured'
        : metadataChecks.includes('error')
          ? 'unknown'
          : 'configured';
  const mlLabel = metadataChecks.includes('loading')
    ? 'PROBING'
    : metadataChecks.every((state) => state === 'loaded')
      ? 'METADATA VERIFIED'
      : metadataChecks.includes('loaded')
        ? 'PARTIAL METADATA CHECK'
        : metadataChecks.includes('error')
          ? 'NOT VERIFIED'
          : 'NOT MONITORED';

  return (
    <div className="space-y-6">
      {/* Configured architecture and actual API readiness */}
      <section className="panel p-5 rounded-xl bg-slate-900/80 border border-slate-800 space-y-4">
        <div className="flex items-center justify-between border-b border-slate-800 pb-3">
          <div className="flex items-center gap-2">
            <Server size={18} className="text-cyan-400" />
            <h3 className="text-base font-semibold text-slate-100">
              Configured Architecture & API Readiness
            </h3>
          </div>
          <span className="text-xs font-mono text-cyan-400">
            Active Context: {dataset?.display_name ?? 'METR-LA'} ({dataset?.sensor_count ?? 207} sensors)
          </span>
        </div>
        <p className="text-[11px] text-slate-500">
          Status pills reflect actual checks, a pending check, or an explicit statement that no runtime
          check exists. All other details are static architecture documentation.
        </p>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {/* Service 1: FastAPI Core */}
          <div className="p-3.5 rounded-lg bg-slate-950/70 border border-slate-800/80 space-y-2">
            <div className="flex items-center justify-between">
              <span className="font-semibold text-xs text-slate-200 flex items-center gap-1.5">
                <Radio size={14} className="text-cyan-400" /> FastAPI Analytics Core
              </span>
              <StatusPill tone={apiTone}>{apiLabel}</StatusPill>
            </div>
            <div className="text-[11px] text-slate-400 space-y-0.5 font-mono">
              <div>Runtime check: GET /health/ready</div>
              <div>Configured: port 8000 · workers 1</div>
              <div>Response Latency: {apiLatency !== null ? `${apiLatency} ms` : '—'}</div>
              <div>Connection Pool: Read-Only (min 1, max 5)</div>
            </div>
          </div>

          {/* Service 2: PostgreSQL 16 Store */}
          <div className="p-3.5 rounded-lg bg-slate-950/70 border border-slate-800/80 space-y-2">
            <div className="flex items-center justify-between">
              <span className="font-semibold text-xs text-slate-200 flex items-center gap-1.5">
                <Database size={14} className="text-emerald-400" /> PostgreSQL 16 Store
              </span>
              <StatusPill tone={apiTone}>{postgresLabel}</StatusPill>
            </div>
            <div className="text-[11px] text-slate-400 space-y-0.5 font-mono">
              <div>Runtime check: API table readiness only</div>
              <div>Configured: port 5432 · DB traffic_analytics</div>
              <div>Table: public.traffic_metrics</div>
              <div>Unique: (dataset, window_start, window_end)</div>
            </div>
          </div>

          {/* Service 3: Apache Kafka 4.0 */}
          <div className="p-3.5 rounded-lg bg-slate-950/70 border border-slate-800/80 space-y-2">
            <div className="flex items-center justify-between">
              <span className="font-semibold text-xs text-slate-200 flex items-center gap-1.5">
                <Radio size={14} className="text-indigo-400" /> Apache Kafka 4.0 (KRaft)
              </span>
              <StatusPill tone="unknown">NOT MONITORED</StatusPill>
            </div>
            <div className="text-[11px] text-slate-400 space-y-0.5 font-mono">
              <div>Configured: port 29092 (Ext) / 9092 (Int)</div>
              <div>Topic: traffic-events</div>
              <div>Partitions: Dataset-keyed ordering</div>
            </div>
          </div>

          {/* Service 4: Spark 4.0 Streaming */}
          <div className="p-3.5 rounded-lg bg-slate-950/70 border border-slate-800/80 space-y-2">
            <div className="flex items-center justify-between">
              <span className="font-semibold text-xs text-slate-200 flex items-center gap-1.5">
                <Cpu size={14} className="text-amber-400" /> Spark 4.0 Streaming
              </span>
              <StatusPill tone="unknown">NOT MONITORED</StatusPill>
            </div>
            <div className="text-[11px] text-slate-400 space-y-0.5 font-mono">
              <div>Configured: master local[2] · Ivy 2.5.2 cache</div>
              <div>Watermark: 10 minutes lateness</div>
              <div>Deduplication: dropDuplicatesWithinWatermark</div>
            </div>
          </div>

          {/* Service 5: Hadoop 3.5 HDFS */}
          <div className="p-3.5 rounded-lg bg-slate-950/70 border border-slate-800/80 space-y-2">
            <div className="flex items-center justify-between">
              <span className="font-semibold text-xs text-slate-200 flex items-center gap-1.5">
                <HardDrive size={14} className="text-blue-400" /> Hadoop 3.5.0 HDFS
              </span>
              <StatusPill tone="unknown">NOT MONITORED</StatusPill>
            </div>
            <div className="text-[11px] text-slate-400 space-y-0.5 font-mono">
              <div>Configured: NameNode RPC 8020</div>
              <div>Sink: Parquet /traffic/streaming/...</div>
              <div>Partitioning: partitionBy("dataset")</div>
            </div>
          </div>

          {/* Service 6: ML Model Registry */}
          <div className="p-3.5 rounded-lg bg-slate-950/70 border border-slate-800/80 space-y-2">
            <div className="flex items-center justify-between">
              <span className="font-semibold text-xs text-slate-200 flex items-center gap-1.5">
                <Sparkles size={14} className="text-purple-400" /> ML Model Registry
              </span>
              <StatusPill tone={mlTone}>{mlLabel}</StatusPill>
            </div>
            <div className="text-[11px] text-slate-400 space-y-0.5 font-mono">
              <div>METR-LA: {formatArtifactSize(modelMetadata['metr-la']?.artifact_size_bytes)}</div>
              <div>PEMS-BAY: {formatArtifactSize(modelMetadata['pems-bay']?.artifact_size_bytes)}</div>
              <div>Size source: API filesystem stat</div>
              {(metadataErrors['metr-la'] || metadataErrors['pems-bay']) && (
                <div className="text-amber-400">Check failed for one or more artifacts</div>
              )}
            </div>
          </div>
        </div>
      </section>

      {/* Pipeline Dataflow Architecture */}
      <section className="panel p-5 rounded-xl bg-slate-900/80 border border-slate-800 space-y-4">
        <h3 className="text-sm font-semibold text-slate-100">
          Streaming Architecture Dataflow
        </h3>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-3 text-xs">
          <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800 space-y-1">
            <span className="text-[10px] font-mono text-cyan-400 font-bold block">1. INGESTION</span>
            <strong className="text-slate-200 block">Producer Replay</strong>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              Reads processed CSV, validates ISO timestamps, checks Spark HTTP readiness (:8081), publishes to Kafka topic.
            </p>
          </div>

          <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800 space-y-1">
            <span className="text-[10px] font-mono text-cyan-400 font-bold block">2. STREAM PROCESSING</span>
            <strong className="text-slate-200 block">Spark Structured Streaming</strong>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              Consumes Kafka micro-batches, deduplicates snapshots within 10m watermark, computes 5-minute tumbling aggregations.
            </p>
          </div>

          <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800 space-y-1">
            <span className="text-[10px] font-mono text-cyan-400 font-bold block">3. DUAL STORAGE</span>
            <strong className="text-slate-200 block">PostgreSQL & HDFS</strong>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              Idempotent upsert into PostgreSQL table traffic_metrics via foreachBatch; parallel partitioned Parquet sink to HDFS.
            </p>
          </div>

          <div className="p-3 rounded-lg bg-slate-950/70 border border-slate-800 space-y-1">
            <span className="text-[10px] font-mono text-cyan-400 font-bold block">4. SERVING & PREDICTION</span>
            <strong className="text-slate-200 block">FastAPI & Command Center</strong>
            <p className="text-slate-400 text-[11px] leading-relaxed">
              Read-only pool serves KPIs and trends. Executes configured 15-minute-ahead inference using the
              models selected in the served artifact over 13 past windows.
            </p>
          </div>
        </div>
      </section>

      {/* Academic Integrity & Benchmark Provenance Statement */}
      <section className="panel p-5 rounded-xl bg-slate-900/80 border border-slate-800 space-y-3">
        <div className="flex items-center gap-2">
          <ShieldAlert size={16} className="text-amber-400" />
          <h3 className="text-sm font-semibold text-slate-100">
            Academic Provenance & Simulation Transparency Statement
          </h3>
        </div>

        <p className="text-xs text-slate-300 leading-relaxed">
          This system is an academic Big Data Analytics demonstration project. The telemetry records visualised in this
          dashboard originate from standard highway sensor benchmark datasets (
          <strong>METR-LA:</strong> 207 road loop detectors in Los Angeles County, March–June 2012;{' '}
          <strong>PEMS-BAY:</strong> 325 freeway sensors in the San Francisco Bay Area, January–June 2017) published by the
          California Department of Transportation (Caltrans) Performance Measurement System (PeMS).
        </p>

        <p className="text-xs text-slate-400 leading-relaxed">
          The streaming ingestion pipeline simulates real-time data flow by replaying these historical snapshots through
          Apache Kafka 4.0 into Apache Spark 4.0. <strong>This dashboard does not monitor real-time Bengaluru traffic</strong>,
          and sensor geographic coordinates are unmapped in the raw data files.
        </p>

        <div className="pt-2 text-[11px] font-mono text-slate-400 border-t border-slate-800 flex justify-between">
          <span>Technology Stack: Python 3.12 · Kafka 4 · Spark 4 · PostgreSQL 16 · Hadoop 3.5 · React 19</span>
          <span>Gateway: {apiBase}</span>
        </div>
      </section>
    </div>
  );
}

export default App;
