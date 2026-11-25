import { Cpu, Gauge, Radio, ShieldCheck } from 'lucide-react';
import type { DashboardKpis, DatasetInfo } from '../types/api';

interface NetworkCoverageProps {
  dataset: DatasetInfo | null;
  kpis: DashboardKpis | null;
}

export function NetworkCoverage({ dataset, kpis }: NetworkCoverageProps) {
  const sensorCount = dataset?.sensor_count ?? 207;
  const isMetr = dataset?.dataset === 'metr-la';
  const datasetName = dataset?.display_name ?? 'METR-LA';

  // Calculate telemetry metrics from real API data
  const totalReadings = kpis?.total_sensor_readings ?? 0;
  const congestedReadings = kpis?.congested_sensor_count ?? 0;
  const normalReadings = Math.max(0, totalReadings - congestedReadings);
  const congestionShare = kpis?.weighted_congestion_percentage ?? 0;
  const averageSpeed = kpis?.weighted_average_speed ?? 0;
  const windowCount = kpis?.window_count ?? 0;

  // totalReadings is the sum of per-window valid observation counts across the selected range.
  // It cannot be used to derive the number of unique reporting sensors from aggregated windows alone,
  // so no reporting-density percentage is computed (and none is ever reported above 100%).
  const averageReadingsPerWindow = windowCount > 0 ? Math.round(totalReadings / windowCount) : 0;

  return (
    <section className="panel network-panel">
      <div className="panel-heading">
        <div>
          <div className="section-tag-inline">
            <Radio size={12} className="text-cyan-400 animate-pulse" />
            <span>SENSOR TELEMETRY & NETWORK COVERAGE</span>
          </div>
          <h2 className="mt-1 text-base font-semibold text-slate-100">
            {datasetName} Highway Detection Grid
          </h2>
          <p className="text-xs text-slate-400">
            Real sensor telemetry aggregated across 5-minute tumbling windows
          </p>
        </div>
        <span className="source-pill">
          {sensorCount} Total Source Sensors
        </span>
      </div>

      {/* Sensor Fleet Summary */}
      <div className="grid grid-cols-2 gap-2 my-3">
        <div className="p-2.5 rounded-lg bg-slate-900/80 border border-slate-800/80">
          <div className="flex items-center gap-1.5 text-xs text-slate-400">
            <Cpu size={13} className="text-cyan-400" />
            <span>Loop Detectors</span>
          </div>
          <div className="mt-1 text-lg font-bold font-mono text-slate-100">
            {sensorCount.toLocaleString()}
            <span className="ml-1 text-xs font-normal text-slate-400">units</span>
          </div>
          <div className="text-[10px] text-slate-400">
            {isMetr ? 'Los Angeles County Loop Grid' : 'SF Bay Area Expressway Loop Grid'}
          </div>
        </div>

        <div className="p-2.5 rounded-lg bg-slate-900/80 border border-slate-800/80">
          <div className="flex items-center gap-1.5 text-xs text-slate-400">
            <Gauge size={13} className="text-emerald-400" />
            <span>Sampling Cadence</span>
          </div>
          <div className="mt-1 text-lg font-bold font-mono text-emerald-400">
            300s
            <span className="ml-1 text-xs font-normal text-slate-400">(5 min)</span>
          </div>
          <div className="text-[10px] text-slate-400">
            {sensorCount * 12} nominal readings/hour
          </div>
        </div>
      </div>

      {/* Network Traffic Flow Breakdown */}
      <div className="p-3 my-2 rounded-lg bg-slate-900/60 border border-slate-800/80">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs font-medium text-slate-300">
            Traffic State Distribution (Selected Range)
          </span>
          <span className="text-xs font-mono text-cyan-400">
            {totalReadings.toLocaleString()} total readings
          </span>
        </div>

        {/* Stacked Telemetry Bar */}
        <div className="h-3 w-full rounded-full bg-slate-800/90 overflow-hidden flex">
          {totalReadings > 0 ? (
            <>
              <div
                className="bg-gradient-to-r from-emerald-500 to-cyan-500 h-full transition-all duration-500"
                style={{ width: `${Math.max(2, 100 - congestionShare)}%` }}
                title={`Normal Flow (≥ 20 mph): ${(100 - congestionShare).toFixed(1)}%`}
              />
              <div
                className="bg-gradient-to-r from-amber-500 to-rose-500 h-full transition-all duration-500"
                style={{ width: `${Math.max(2, congestionShare)}%` }}
                title={`Congested Flow (< 20 mph): ${congestionShare.toFixed(1)}%`}
              />
            </>
          ) : (
            <div className="w-full h-full bg-slate-800 flex items-center justify-center text-[10px] text-slate-500">
              No data in window
            </div>
          )}
        </div>

        {/* Legend / Metrics */}
        <div className="flex items-center justify-between mt-2.5 text-xs">
          <div className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-sm bg-cyan-400" />
            <span className="text-slate-300">Free / Moderate Flow (&ge; 20 mph)</span>
            <span className="font-mono text-slate-400">
              {totalReadings > 0 ? `${(100 - congestionShare).toFixed(1)}% (${normalReadings.toLocaleString()} readings)` : '—'}
            </span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-sm bg-amber-400" />
            <span className="text-slate-300">Congested (&lt; 20 mph)</span>
            <span className="font-mono text-amber-400">
              {totalReadings > 0 ? `${congestionShare.toFixed(1)}%` : '—'}
            </span>
          </div>
        </div>
      </div>

      {/* Sensor Coverage Stats */}
      <div className="space-y-2 text-xs">
        <div className="flex items-center justify-between py-1.5 border-b border-slate-800/60">
          <span className="text-slate-400">Fleet Mean Velocity</span>
          <span className="font-mono font-medium text-emerald-400">
            {averageSpeed > 0 ? `${averageSpeed.toFixed(1)} mph` : '—'}
          </span>
        </div>

        <div className="flex items-center justify-between py-1.5 border-b border-slate-800/60">
          <span className="text-slate-400">Congestion Speed Threshold</span>
          <span className="font-mono font-medium text-slate-200">
            &lt; 20.0 mph (fixed benchmark)
          </span>
        </div>

        <div className="flex items-center justify-between py-1.5 border-b border-slate-800/60">
          <span className="text-slate-400">Avg Valid Observations per Window</span>
          <span className="font-mono font-medium text-cyan-400">
            {averageReadingsPerWindow} observations
          </span>
        </div>

        <div className="flex items-center justify-between py-1.5 border-b border-slate-800/60">
          <span className="text-slate-400">Unique Reporting Sensors</span>
          <span className="text-slate-400 italic">
            Not derivable from aggregated windows
          </span>
        </div>

        <div className="flex items-center justify-between py-1.5 border-b border-slate-800/60">
          <span className="text-slate-400">Data Cleansing & Zero Policy</span>
          <span className="text-slate-300">
            Zero speeds preserved; nulls/inf converted to NaN
          </span>
        </div>

        <div className="flex items-center justify-between py-1.5 border-b border-slate-800/60">
          <span className="text-slate-400">Traffic-State Basis</span>
          <span className="text-slate-300">
            Congested: valid speed &lt; 20 mph; normal: &ge; 20 mph; shares sum to 100% of valid readings
          </span>
        </div>

        <div className="flex items-center justify-between py-1.5">
          <span className="text-slate-400">Sensor Geographical Coordinates</span>
          <span className="text-slate-400 italic">
            Not included in dataset; unmapped loop topology
          </span>
        </div>
      </div>

      <div className="mt-3.5 pt-2.5 border-t border-slate-800/80 flex items-center gap-2 text-[11px] text-slate-400">
        <ShieldCheck size={14} className="text-cyan-400 shrink-0" />
        <span>
          True benchmark sensor network. No artificial coordinates or fabricated map overlays are displayed.
        </span>
      </div>
    </section>
  );
}
