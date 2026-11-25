import { useCallback, useEffect, useRef, useState } from 'react';
import {
  AlertCircle,
  ChevronDown,
  Clock3,
  MapPin,
  RefreshCw,
  Route,
  Search,
  ShieldAlert,
  X,
} from 'lucide-react';

import { api } from '../lib/api';
import type {
  Coordinate,
  GeocodeCandidate,
  Region,
  RegionInfo,
  RouteResponse,
  TrafficEvidenceResponse,
} from '../types/api';
import { RouteMap } from './RouteMap';

const DEBOUNCE_MS = 300;
const MIN_QUERY_LENGTH = 2;

const REGION_LABELS: Record<Region, string> = {
  'metr-la': 'Los Angeles County',
  'pems-bay': 'San Francisco Bay Area',
};

function AutocompleteList({
  candidates,
  selectedValue,
  onSelect,
}: {
  candidates: GeocodeCandidate[];
  selectedValue: string;
  onSelect: (candidate: GeocodeCandidate) => void;
}) {
  if (candidates.length === 0) return null;
  return (
    <ul className="absolute z-20 mt-1 w-full max-h-48 overflow-y-auto rounded-lg bg-slate-900 border border-slate-700 shadow-2xl">
      {candidates.map((candidate) => (
        <li
          key={`${candidate.lat},${candidate.lon}`}
          onClick={() => onSelect(candidate)}
          className={`cursor-pointer px-3 py-2 text-sm ${
            candidate.label === selectedValue
              ? 'bg-cyan-950/30 text-cyan-300 font-medium'
              : 'text-slate-300 hover:bg-slate-800'
          }`}
        >
          <MapPin className="w-4 h-4 mr-2 text-slate-500 shrink-0" />
          {candidate.label}
        </li>
      ))}
    </ul>
  );
}

export function RouteRecommendation() {
  const [regions, setRegions] = useState<RegionInfo[]>([]);
  const [region, setRegion] = useState<Region>('metr-la');

  const [sourceQuery, setSourceQuery] = useState('');
  const [destQuery, setDestQuery] = useState('');
  const [source, setSource] = useState<Coordinate | null>(null);
  const [dest, setDest] = useState<Coordinate | null>(null);
  const [sourceLabel, setSourceLabel] = useState('');
  const [destLabel, setDestLabel] = useState('');
  const [sourceCandidates, setSourceCandidates] = useState<GeocodeCandidate[]>([]);
  const [destCandidates, setDestCandidates] = useState<GeocodeCandidate[]>([]);
  const [sourceFocused, setSourceFocused] = useState(false);
  const [destFocused, setDestFocused] = useState(false);

  const [routes, setRoutes] = useState<RouteResponse | null>(null);
  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(null);
  const [trafficEvidence, setTrafficEvidence] = useState<TrafficEvidenceResponse | null>(null);

  const [loading, setLoading] = useState(false);
  const [geocoding, setGeocoding] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const sourceDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const destDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const geocodeSeqRef = useRef(0);

  const clearSelection = () => {
    setSource(null);
    setDest(null);
    setSourceLabel('');
    setDestLabel('');
    setRoutes(null);
    setSelectedRouteId(null);
    setError(null);
  };

  // Load supported regions on mount.
  useEffect(() => {
    let cancelled = false;
    api.regions().then((items) => {
      if (!cancelled) {
        setRegions(items);
        if (items.length > 0) setRegion(items[0]!.region);
      }
    }).catch(() => {
      // Regions are static; fall back to defaults if the endpoint is unreachable.
      if (!cancelled) setRegions([
        { region: 'metr-la', display_name: 'Los Angeles County', south: 33.7, west: -118.8, north: 34.85, east: -117.6 },
        { region: 'pems-bay', display_name: 'San Francisco Bay Area', south: 37.05, west: -122.8, north: 38.3, east: -121.8 },
      ]);
    });
    return () => { cancelled = true; };
  }, []);

  // Load traffic-evidence availability (region-scoped).
  useEffect(() => {
    let cancelled = false;
    api.trafficEvidence(region).then((evidence) => {
      if (!cancelled) setTrafficEvidence(evidence);
    }).catch(() => {
      if (!cancelled) setTrafficEvidence({
        available: false,
        message: 'Traffic recommendation unavailable — insufficient geographically matched traffic observations.',
        reason: 'service_unavailable',
      });
    });
    return () => { cancelled = true; };
  }, [region]);

  const debouncedGeocode = useCallback(
    (field: 'source' | 'dest', value: string) => {
      if (sourceDebounceRef.current) clearTimeout(sourceDebounceRef.current);
      if (destDebounceRef.current) clearTimeout(destDebounceRef.current);
      const seq = ++geocodeSeqRef.current;
      setGeocoding(true);
      const timeout = setTimeout(async () => {
        if (seq !== geocodeSeqRef.current) return;
        try {
          const response = await api.geocode(value.trim(), region);
          if (seq !== geocodeSeqRef.current) return;
          setGeocoding(false);
          if (field === 'source') {
            setSourceCandidates(response.candidates);
          } else {
            setDestCandidates(response.candidates);
          }
        } catch {
          if (seq !== geocodeSeqRef.current) return;
          setGeocoding(false);
          if (field === 'source') setSourceCandidates([]);
          else setDestCandidates([]);
        }
      }, DEBOUNCE_MS);
      if (field === 'source') sourceDebounceRef.current = timeout;
      else destDebounceRef.current = timeout;
    },
    [region]
  );

  const handleSourceChange = (value: string) => {
    setSourceQuery(value);
    setSource(null);
    setSourceLabel('');
    setRoutes(null);
    setSelectedRouteId(null);
    setError(null);
    setSourceCandidates([]);
    if (value.trim().length >= MIN_QUERY_LENGTH) {
      debouncedGeocode('source', value);
    }
  };

  const handleDestChange = (value: string) => {
    setDestQuery(value);
    setDest(null);
    setDestLabel('');
    setRoutes(null);
    setSelectedRouteId(null);
    setError(null);
    setDestCandidates([]);
    if (value.trim().length >= MIN_QUERY_LENGTH) {
      debouncedGeocode('dest', value);
    }
  };

  const selectCandidate = (candidate: GeocodeCandidate, field: 'source' | 'dest') => {
    if (field === 'source') {
      setSource({ lat: candidate.lat, lon: candidate.lon });
      setSourceLabel(candidate.label);
      setSourceQuery(candidate.label);
      setSourceCandidates([]);
      setSourceFocused(false);
    } else {
      setDest({ lat: candidate.lat, lon: candidate.lon });
      setDestLabel(candidate.label);
      setDestQuery(candidate.label);
      setDestCandidates([]);
      setDestFocused(false);
    }
    setRoutes(null);
    setSelectedRouteId(null);
    setError(null);
  };

  const handleAnalyze = async () => {
    if (!source || !dest) {
      setError('Please select both a source and a destination from the search results.');
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await api.route(source, dest, region);
      setRoutes(response);
      setSelectedRouteId(response.alternatives[0]?.id ?? null);
    } catch (err: any) {
      setError(
        err?.message ?? 'Unable to compute routes. Please check the locations and try again.',
      );
    } finally {
      setLoading(false);
    }
  };

  const handleRetry = () => {
    setError(null);
    if (source && dest) handleAnalyze();
  };

  const selectedRoute = routes?.alternatives.find((r) => r.id === selectedRouteId)
    ?? routes?.alternatives[0] ?? null;

  return (
    <section className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="eyebrow text-xs font-bold font-mono tracking-wider text-cyan-400 uppercase">
            Route Planner &mdash; Road Network
          </div>
          <h1 className="text-2xl sm:text-3xl font-bold tracking-tight text-slate-100 mt-1">
            Route Recommendation
          </h1>
          <p className="page-subtitle text-xs sm:text-sm text-slate-400 mt-1">
            Real road routes via OpenStreetMap/OSRM for {REGION_LABELS[region]}. OSRM durations are
            routing-engine estimates, not live traffic.
          </p>
        </div>
        <div className="select-wrap w-full sm:w-64">
          <label className="text-xs font-semibold text-slate-400 font-mono mb-1 block">
            Region
          </label>
          <select
            value={region}
            onChange={(e) => {
              setRegion(e.target.value as Region);
              clearSelection();
            }}
            className="bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-xs text-slate-100 focus:border-cyan-500"
          >
            {regions.map((r) => (
              <option key={r.region} value={r.region}>
                {REGION_LABELS[r.region]}
              </option>
            ))}
          </select>
        </div>
      </div>

      {error && (
        <div className="notice danger" role="alert">
          <AlertCircle size={18} className="text-rose-400 shrink-0" />
          <div>
            <strong>{error}</strong>
          </div>
          <button
            type="button"
            onClick={handleRetry}
            disabled={loading}
            className="ml-auto text-xs px-2.5 py-1 rounded bg-rose-950/60 border border-rose-800 text-rose-300 font-medium"
          >
            <RefreshCw size={12} className="inline mr-1" /> Retry
          </button>
        </div>
      )}

      {/* Source / destination search */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <div className="relative">
          <label className="text-xs font-semibold text-slate-400 font-mono mb-1 block">
            Source
          </label>
          <div className="relative">
            <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-slate-500" />
            <input
              id="source-input"
              type="text"
              value={sourceQuery}
              onChange={(e) => handleSourceChange(e.target.value)}
              onFocus={() => setSourceFocused(true)}
              onBlur={() => setSourceFocused(false)}
              placeholder="e.g. Downtown Los Angeles"
              disabled={loading}
              className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 pl-9 py-2 text-xs text-slate-100 focus:border-cyan-500 disabled:opacity-60"
            />
            {source && (
              <button
                type="button"
                onClick={() => { setSource(null); setSourceLabel(''); setSourceQuery(''); setRoutes(null); setSelectedRouteId(null); setError(null); }}
                className="absolute right-2.5 top-2.5 text-slate-500 hover:text-slate-200"
                title="Clear source"
              >
                <X size={14} />
              </button>
            )}
            {geocoding && (
              <div className="absolute right-2.5 top-2.5">
                <Clock3 size={14} className="animate-spin text-cyan-400" />
              </div>
            )}
          </div>
          {source && (
            <div className="mt-1 px-2 py-1.5 rounded-lg bg-cyan-950/20 border border-cyan-800/50">
              <span className="text-[10px] font-mono text-cyan-300">
                {sourceLabel} &middot; {source.lat.toFixed(4)}, {source.lon.toFixed(4)}
              </span>
            </div>
          )}
          <AutocompleteList
            candidates={sourceCandidates}
            selectedValue={sourceLabel}
            onSelect={(c) => selectCandidate(c, 'source')}
          />
          {sourceFocused && sourceCandidates.length === 0 && sourceQuery.trim().length >= MIN_QUERY_LENGTH && (
            <div className="mt-1 px-3 py-1 text-[10px] text-slate-500">No results</div>
          )}
        </div>

        <div className="relative">
          <label className="text-xs font-semibold text-slate-400 font-mono mb-1 block">
            Destination
          </label>
          <div className="relative">
            <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-slate-500" />
            <input
              id="dest-input"
              type="text"
              value={destQuery}
              onChange={(e) => handleDestChange(e.target.value)}
              onFocus={() => setDestFocused(true)}
              onBlur={() => setDestFocused(false)}
              placeholder="e.g. Santa Monica Pier"
              disabled={loading}
              className="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 pl-9 py-2 text-xs text-slate-100 focus:border-cyan-500 disabled:opacity-60"
            />
            {dest && (
              <button
                type="button"
                onClick={() => { setDest(null); setDestLabel(''); setDestQuery(''); setRoutes(null); setSelectedRouteId(null); setError(null); }}
                className="absolute right-2.5 top-2.5 text-slate-500 hover:text-slate-200"
                title="Clear destination"
              >
                <X size={14} />
              </button>
            )}
            {geocoding && (
              <div className="absolute right-2.5 top-2.5">
                <Clock3 size={14} className="animate-spin text-cyan-400" />
              </div>
            )}
          </div>
          {dest && (
            <div className="mt-1 px-2 py-1.5 rounded-lg bg-cyan-950/20 border border-cyan-800/50">
              <span className="text-[10px] font-mono text-cyan-300">
                {destLabel} &middot; {dest.lat.toFixed(4)}, {dest.lon.toFixed(4)}
              </span>
            </div>
          )}
          <AutocompleteList
            candidates={destCandidates}
            selectedValue={destLabel}
            onSelect={(c) => selectCandidate(c, 'dest')}
          />
          {destFocused && destCandidates.length === 0 && destQuery.trim().length >= MIN_QUERY_LENGTH && (
            <div className="mt-1 px-3 py-1 text-[10px] text-slate-500">No results</div>
          )}
        </div>
      </div>

      <div className="flex items-end gap-3">
        <button
          type="button"
          onClick={handleAnalyze}
          disabled={loading || !source || !dest}
          className="flex items-center gap-2 px-4 py-2 rounded-lg bg-cyan-600 hover:bg-cyan-500 text-xs font-semibold text-white disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          <Route size={16} />
          {loading ? <Clock3 size={14} className="animate-spin" /> : <span>Analyze Routes</span>}
        </button>
        <span className="text-[10px] text-slate-500 font-mono">
          Regions are validated by coordinates, not text matching.
        </span>
      </div>

      {!loading && !error && source && dest && (
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <div className="lg:col-span-7">
            <section className="panel p-0">
              <div className="panel-heading p-4 border-b border-slate-800">
                <div className="flex items-center gap-2">
                  <MapPin size={14} className="text-cyan-400" />
                  <h2 className="text-sm font-semibold text-slate-100">Road Routes (OpenStreetMap / OSRM)</h2>
                </div>
                <p className="text-[10px] text-slate-400 mt-1">
                  Polylines are actual road-route geometries from OSRM. Distances and durations are
                  routing-engine estimates.
                </p>
              </div>
              <div className="p-4">
                {routes ? (
                  <RouteMap
                    routes={routes.alternatives}
                    source={source}
                    destination={dest}
                    selectedRouteId={selectedRouteId}
                    height="420px"
                  />
                ) : (
                  <div className="h-64 flex items-center justify-center rounded-lg bg-slate-950/50 border border-dashed border-slate-700">
                    <span className="text-slate-500 text-sm">Select source and destination, then analyze routes.</span>
                  </div>
                )}
              </div>
            </section>
          </div>

          <div className="lg:col-span-5">
            {/* Route comparison */}
            <section className="panel p-5">
              <div className="flex items-center gap-2 mb-3">
                <Route size={16} className="text-cyan-400" />
                <h3 className="text-sm font-semibold text-slate-100">Route Comparison</h3>
              </div>
              {!routes ? (
                <div className="text-slate-500 text-sm text-center py-6">No routes computed yet.</div>
              ) : (
                <div className="space-y-2">
                  {routes.alternatives.map((route) => {
                    const selected = route.id === selectedRouteId;
                    return (
                      <button
                        key={route.id}
                        onClick={() => setSelectedRouteId(route.id)}
                        className={`flex items-center justify-between p-3 rounded-lg border transition-all ${
                          selected
                            ? 'bg-cyan-950/40 border-cyan-500/50 shadow-[0_0_0_1px_rgba(34,211,238,0.25)]'
                            : 'bg-slate-950/60 border-slate-700/60 hover:border-slate-600'
                        }`}
                      >
                        <div>
                          <div className="flex items-center gap-2 text-xs">
                            <span className={`font-mono font-bold ${selected ? 'text-cyan-400' : 'text-slate-300'}`}>
                              {route.id.toUpperCase()}
                            </span>
                            <span className="text-slate-500">selected route</span>
                          </div>
                          <div className="flex justify-between text-sm mt-0.5">
                            <span className="text-slate-400">Distance</span>
                            <span className="font-mono text-slate-200">{route.distance_km} km</span>
                          </div>
                          <div className="flex justify-between text-sm">
                            <span className="text-slate-400">OSRM est. duration</span>
                            <span className="font-mono text-slate-200">{route.duration_minutes} min</span>
                          </div>
                        </div>
                        <ChevronDown size={14} className={`text-slate-500 ${selected ? 'text-cyan-400' : ''}`} />
                      </button>
                    );
                  })}
                </div>
              )}
              {selectedRoute && (
                <div className="mt-4 p-3 rounded-lg bg-slate-950/60 border border-slate-700">
                  <div className="text-[10px] font-mono text-slate-400 mb-1">Selected route summary</div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">Distance</span>
                    <span className="font-mono text-cyan-400">{selectedRoute.distance_km} km</span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-slate-400">OSRM est. duration</span>
                    <span className="font-mono text-cyan-400">{selectedRoute.duration_minutes} min</span>
                  </div>
                  <div className="text-[10px] text-slate-500 mt-1">
                    Label: OSRM routing-engine estimate — not live traffic travel time.
                  </div>
                </div>
              )}
            </section>

            {/* Traffic evidence */}
            <section className="panel p-5 mt-4">
              <div className="flex items-center gap-2 mb-3">
                <ShieldAlert size={16} className="text-amber-400" />
                <h3 className="text-sm font-semibold text-slate-100">Historical Traffic Evidence</h3>
              </div>
              <div className="rounded-lg bg-rose-950/20 border border-rose-800/50 p-3">
                <div className="flex items-start gap-2 mb-2">
                  <AlertCircle size={14} className="text-rose-400 mt-0.5 shrink-0" />
                  <div>
                    <div className="text-xs font-semibold text-rose-300">
                      Traffic recommendation unavailable
                    </div>
                    <div className="text-[10px] text-rose-400/70 mt-0.5 leading-relaxed">
                      {trafficEvidence?.message ??
                        'Traffic recommendation unavailable — insufficient geographically matched traffic observations.'}
                    </div>
                  </div>
                </div>
                <div className="text-[10px] text-slate-500 mt-2 font-mono">
                  Reason: {trafficEvidence?.reason ?? 'no_sensor_coordinates_in_dataset'}{' '}
                  &middot; dataset: {region.toUpperCase()}
                </div>
                <div className="text-[10px] text-slate-400 mt-2 leading-relaxed">
                  Road-route alternatives above are still displayed with distance and OSRM estimated
                  duration. Congestion-based ranking is intentionally not offered: the repository
                  contains no genuine sensor coordinates, so historical observations cannot be
                  matched to individual route corridors.
                </div>
              </div>
            </section>
          </div>
        </div>
      )}

      {!loading && !error && (!source || !dest) && (
        <div className="rounded-lg bg-slate-950/50 border border-dashed border-slate-700 p-8 text-center">
          <Search size={32} className="mx-auto mb-3 text-slate-600" />
          <h3 className="text-sm font-semibold text-slate-200 mb-1">Search for a source and destination</h3>
          <p className="text-xs text-slate-400 max-w-xs mx-auto">
            Type a place name (e.g. "Downtown Los Angeles") to geocode within the selected region,
            then select a result for both source and destination.
          </p>
        </div>
      )}
    </section>
  );
}
