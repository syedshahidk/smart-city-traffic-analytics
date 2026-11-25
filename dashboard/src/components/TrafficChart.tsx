import {
  Area,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Activity, AlertCircle } from 'lucide-react';

import { compactTimestamp, readableTimestamp } from '../lib/dates';
import type { CongestionTrendPoint } from '../types/api';

interface TrafficChartProps {
  data: CongestionTrendPoint[];
  title: string;
  description: string;
  loading?: boolean;
  error?: string | null;
  emptyLabel?: string;
  variant?: 'combined' | 'speed' | 'congestion';
}

function ChartStateMessage({ label, isError }: { label: string; isError?: boolean }) {
  return (
    <div className={`chart-message ${isError ? 'text-rose-400' : 'text-slate-400'}`}>
      {isError ? <AlertCircle size={20} className="text-rose-400" /> : <Activity size={20} className="animate-spin text-cyan-400" />}
      <span className="text-sm font-medium">{label}</span>
    </div>
  );
}

export function TrafficChart({
  data,
  title,
  description,
  loading = false,
  error,
  emptyLabel = 'No metric windows recorded in this time range.',
  variant = 'combined',
}: TrafficChartProps) {
  const hasSpeed = variant !== 'congestion';
  const hasCongestion = variant !== 'speed';

  // Compute summary stats for the current slice
  const validSpeeds = data
    .map((d) => d.weighted_average_speed)
    .filter((s): s is number => s !== null && Number.isFinite(s));
  const validCongestion = data
    .map((d) => d.weighted_congestion_percentage)
    .filter((c): c is number => c !== null && Number.isFinite(c));

  const minSpeed = validSpeeds.length > 0 ? Math.min(...validSpeeds) : null;
  const maxSpeed = validSpeeds.length > 0 ? Math.max(...validSpeeds) : null;
  const avgSpeed = validSpeeds.length > 0 ? validSpeeds.reduce((a, b) => a + b, 0) / validSpeeds.length : null;
  const peakCongestion = validCongestion.length > 0 ? Math.max(...validCongestion) : null;

  return (
    <section className="panel chart-panel">
      <div className="panel-heading">
        <div>
          <div className="section-tag-inline">
            <Activity size={12} className="text-cyan-400" />
            <span>TIME-SERIES TELEMETRY</span>
          </div>
          <h2 className="mt-0.5 text-base font-semibold text-slate-100">{title}</h2>
          <p className="text-xs text-slate-400">{description}</p>
        </div>

        {/* Real-time stats badges */}
        {data.length > 0 && (
          <div className="flex flex-wrap items-center gap-2 text-xs">
            {avgSpeed !== null && (
              <span className="px-2 py-1 rounded bg-slate-800/80 border border-slate-700/60 font-mono text-cyan-300">
                Avg: <strong className="font-bold">{avgSpeed.toFixed(1)}</strong> mph
              </span>
            )}
            {minSpeed !== null && maxSpeed !== null && (
              <span className="hidden sm:inline-block px-2 py-1 rounded bg-slate-800/80 border border-slate-700/60 font-mono text-slate-300">
                Range: {minSpeed.toFixed(1)} – {maxSpeed.toFixed(1)} mph
              </span>
            )}
            {peakCongestion !== null && (
              <span className={`px-2 py-1 rounded border font-mono ${
                peakCongestion >= 50
                  ? 'bg-rose-950/40 border-rose-800/60 text-rose-300'
                  : 'bg-amber-950/40 border-amber-800/60 text-amber-300'
              }`}>
                Peak Congestion: <strong className="font-bold">{peakCongestion.toFixed(1)}%</strong>
              </span>
            )}
          </div>
        )}
      </div>

      {loading ? (
        <ChartStateMessage label="Aggregating metrics across time buckets…" />
      ) : error ? (
        <ChartStateMessage label={error} isError />
      ) : data.length === 0 ? (
        <ChartStateMessage label={emptyLabel} />
      ) : (
        <div className="chart-wrap" style={{ height: 320 }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={data} margin={{ top: 16, right: 16, left: -4, bottom: 4 }}>
              <defs>
                <linearGradient id="speedGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#22d3ee" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#22d3ee" stopOpacity={0.0} />
                </linearGradient>
                <linearGradient id="congestionFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#f59e0b" stopOpacity={0.35} />
                  <stop offset="85%" stopColor="#f59e0b" stopOpacity={0.02} />
                </linearGradient>
              </defs>

              <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" vertical={false} />

              <XAxis
                dataKey="bucket_start"
                tickFormatter={(value: string) => compactTimestamp(value)}
                tick={{ fill: '#94a3b8', fontSize: 11 }}
                axisLine={{ stroke: '#334155' }}
                tickLine={false}
                minTickGap={40}
              />

              {hasSpeed && (
                <YAxis
                  yAxisId="speed"
                  tick={{ fill: '#94a3b8', fontSize: 11 }}
                  axisLine={{ stroke: '#334155' }}
                  tickLine={false}
                  width={52}
                  domain={['auto', 'auto']}
                  tickFormatter={(val: number) => `${Math.round(val)} mph`}
                />
              )}

              {hasCongestion && (
                <YAxis
                  yAxisId="congestion"
                  orientation="right"
                  tick={{ fill: '#94a3b8', fontSize: 11 }}
                  axisLine={{ stroke: '#334155' }}
                  tickLine={false}
                  width={44}
                  domain={[0, 100]}
                  tickFormatter={(val: number) => `${val}%`}
                />
              )}

              {/* Congestion Threshold Line at 20.0 mph */}
              {hasSpeed && (
                <ReferenceLine
                  yAxisId="speed"
                  y={20.0}
                  stroke="#f43f5e"
                  strokeDasharray="4 4"
                  strokeWidth={1.5}
                  label={{
                    value: 'Congestion Threshold (20 mph)',
                    position: 'insideBottomRight',
                    fill: '#f43f5e',
                    fontSize: 10,
                    fontWeight: 600,
                  }}
                />
              )}

              <Tooltip
                labelFormatter={(value) => readableTimestamp(String(value))}
                formatter={(value: any, name: any) => {
                  if (typeof value !== 'number') return [String(value ?? '—'), name];
                  if (name === 'Average Speed' || name === 'Speed') return [`${value.toFixed(1)} mph`, 'Speed'];
                  if (name === 'Congestion Share' || name === 'Congestion') return [`${value.toFixed(1)}%`, 'Congestion'];
                  if (name === 'Total Readings') return [value.toLocaleString(), 'Readings Volume'];
                  return [value.toFixed(1), name];
                }}
                contentStyle={{
                  backgroundColor: '#0f172a',
                  borderColor: '#334155',
                  borderRadius: '8px',
                  boxShadow: '0 10px 25px -5px rgba(0, 0, 0, 0.6)',
                  color: '#f8fafc',
                  fontSize: '12px',
                  padding: '10px 14px',
                }}
                labelStyle={{ color: '#cbd5e1', fontWeight: 600, marginBottom: '6px' }}
              />

              <Legend
                wrapperStyle={{
                  color: '#94a3b8',
                  fontSize: '12px',
                  paddingTop: '10px',
                }}
              />

              {hasCongestion && (
                <Area
                  yAxisId="congestion"
                  type="monotone"
                  dataKey="weighted_congestion_percentage"
                  name="Congestion Share"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  fill="url(#congestionFill)"
                  isAnimationActive={false}
                />
              )}

              {hasSpeed && (
                <Line
                  yAxisId="speed"
                  type="monotone"
                  dataKey="weighted_average_speed"
                  name="Average Speed"
                  stroke="#22d3ee"
                  strokeWidth={2.5}
                  dot={false}
                  activeDot={{ r: 5, fill: '#22d3ee', stroke: '#020617', strokeWidth: 2 }}
                  isAnimationActive={false}
                />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Analytical Legend Footnote */}
      <div className="flex flex-wrap items-center justify-between mt-3 pt-2.5 border-t border-slate-800/80 text-[11px] text-slate-400">
        <div className="flex items-center gap-3">
          <span className="flex items-center gap-1.5">
            <span className="w-2.5 h-0.5 bg-cyan-400 rounded" />
            <span>Speed (mph)</span>
          </span>
          <span className="flex items-center gap-1.5">
            <span className="w-2.5 h-2 bg-amber-500/40 border border-amber-500 rounded-sm" />
            <span>Congestion Share (%)</span>
          </span>
          <span className="flex items-center gap-1.5 text-rose-400">
            <span className="w-2.5 h-0.5 border-b border-rose-500 border-dashed" />
            <span>&lt; 20 mph Threshold</span>
          </span>
        </div>
        <span className="text-slate-500 font-mono">
          Weighted by sensor observation counts
        </span>
      </div>
    </section>
  );
}
