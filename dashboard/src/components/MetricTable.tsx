import { useState, useMemo } from 'react';
import {
  ArrowDown,
  ArrowUp,
  ChevronDown,
  Download,
  Filter,
  Search,
} from 'lucide-react';

import { compactTimestamp, readableTimestamp } from '../lib/dates';
import type { MetricRecord } from '../types/api';

interface MetricTableProps {
  rows: MetricRecord[];
  loading: boolean;
  error: string | null;
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore?: () => void;
  compact?: boolean;
}

type SortField = 'window_start' | 'average_speed' | 'congestion_percentage' | 'total_sensor_readings';
type SortDirection = 'asc' | 'desc';
type FilterStatus = 'all' | 'congested' | 'normal';

export function MetricTable({
  rows,
  loading,
  error,
  hasMore,
  loadingMore,
  onLoadMore,
  compact = false,
}: MetricTableProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<FilterStatus>('all');
  const [sortField, setSortField] = useState<SortField>('window_start');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
  const [selectedRecord, setSelectedRecord] = useState<MetricRecord | null>(null);

  // Sorting and filtering
  const filteredRows = useMemo(() => {
    let result = rows;

    if (statusFilter === 'congested') {
      result = result.filter((r) => r.congestion_percentage >= 50);
    } else if (statusFilter === 'normal') {
      result = result.filter((r) => r.congestion_percentage < 50);
    }

    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      result = result.filter(
        (r) =>
          r.window_start.toLowerCase().includes(q) ||
          r.average_speed.toFixed(1).includes(q) ||
          r.dataset.toLowerCase().includes(q)
      );
    }

    return [...result].sort((a, b) => {
      let comparison = 0;
      if (sortField === 'window_start') {
        comparison = new Date(a.window_start).getTime() - new Date(b.window_start).getTime();
      } else {
        comparison = a[sortField] - b[sortField];
      }
      return sortDirection === 'asc' ? comparison : -comparison;
    });
  }, [rows, statusFilter, searchQuery, sortField, sortDirection]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection((prev) => (prev === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const exportCsv = () => {
    if (rows.length === 0) return;
    const headers = [
      'dataset',
      'window_start',
      'window_end',
      'average_speed_mph',
      'total_sensor_readings',
      'congested_sensor_count',
      'congestion_percentage',
      'processed_at',
    ];
    const csvRows = rows.map((r) => [
      r.dataset,
      r.window_start,
      r.window_end,
      r.average_speed.toFixed(2),
      r.total_sensor_readings,
      r.congested_sensor_count,
      r.congestion_percentage.toFixed(2),
      r.processed_at,
    ]);
    const csvContent = [headers.join(','), ...csvRows.map((e) => e.join(','))].join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.setAttribute('href', url);
    link.setAttribute('download', `traffic_metrics_${rows[0]?.dataset ?? 'export'}_${Date.now()}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const displayRows = compact ? rows.slice(0, 6) : filteredRows;

  if (loading && rows.length === 0) {
    return <div className="table-state">Loading metric windows from PostgreSQL store…</div>;
  }
  if (error && rows.length === 0) {
    return <div className="table-state error-text">{error}</div>;
  }
  if (rows.length === 0) {
    return <div className="table-state">No metric records found in the selected range.</div>;
  }

  return (
    <>
      {/* Table controls in full explorer mode */}
      {!compact && (
        <div className="flex flex-wrap items-center justify-between gap-3 mb-3 p-2 rounded-lg bg-slate-900/60 border border-slate-800">
          <div className="flex items-center gap-2 flex-1 min-w-[200px]">
            <Search size={14} className="text-slate-400 ml-1.5" />
            <input
              type="text"
              placeholder="Search by timestamp or speed…"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full bg-transparent border-0 text-xs text-slate-200 placeholder:text-slate-500 focus:outline-none"
            />
          </div>

          <div className="flex items-center gap-2 text-xs">
            <span className="text-slate-400 text-[11px] font-medium flex items-center gap-1">
              <Filter size={12} /> Status:
            </span>
            <div className="flex rounded-md bg-slate-950 p-0.5 border border-slate-800">
              <button
                type="button"
                onClick={() => setStatusFilter('all')}
                className={`px-2 py-0.5 rounded text-[11px] font-medium transition-colors ${
                  statusFilter === 'all' ? 'bg-cyan-500/20 text-cyan-300 font-semibold' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                All ({rows.length})
              </button>
              <button
                type="button"
                onClick={() => setStatusFilter('congested')}
                className={`px-2 py-0.5 rounded text-[11px] font-medium transition-colors ${
                  statusFilter === 'congested' ? 'bg-amber-500/20 text-amber-300 font-semibold' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                Congested (&ge;50%)
              </button>
              <button
                type="button"
                onClick={() => setStatusFilter('normal')}
                className={`px-2 py-0.5 rounded text-[11px] font-medium transition-colors ${
                  statusFilter === 'normal' ? 'bg-emerald-500/20 text-emerald-300 font-semibold' : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                Normal Flow
              </button>
            </div>

            <button
              type="button"
              onClick={exportCsv}
              className="flex items-center gap-1 px-2.5 py-1 rounded bg-slate-800/80 hover:bg-slate-700/80 text-cyan-300 border border-slate-700/60 font-medium transition-colors text-[11px]"
              title="Download loaded records as CSV"
            >
              <Download size={13} /> Export CSV
            </button>
          </div>
        </div>
      )}

      {/* Main Table */}
      <div className="table-scroll">
        <table className="w-full text-left text-xs">
          <thead>
            <tr>
              <th
                onClick={() => !compact && handleSort('window_start')}
                className={!compact ? 'cursor-pointer select-none hover:text-cyan-300' : ''}
              >
                <div className="flex items-center gap-1">
                  <span>Window Start (UTC)</span>
                  {!compact && sortField === 'window_start' && (
                    sortDirection === 'asc' ? <ArrowUp size={12} /> : <ArrowDown size={12} />
                  )}
                </div>
              </th>
              <th
                onClick={() => !compact && handleSort('average_speed')}
                className={!compact ? 'cursor-pointer select-none hover:text-cyan-300' : ''}
              >
                <div className="flex items-center gap-1">
                  <span>Average Speed</span>
                  {!compact && sortField === 'average_speed' && (
                    sortDirection === 'asc' ? <ArrowUp size={12} /> : <ArrowDown size={12} />
                  )}
                </div>
              </th>
              <th
                onClick={() => !compact && handleSort('total_sensor_readings')}
                className={!compact ? 'cursor-pointer select-none hover:text-cyan-300' : ''}
              >
                <div className="flex items-center gap-1">
                  <span>Sensor Readings</span>
                  {!compact && sortField === 'total_sensor_readings' && (
                    sortDirection === 'asc' ? <ArrowUp size={12} /> : <ArrowDown size={12} />
                  )}
                </div>
              </th>
              <th>Congested Readings</th>
              <th
                onClick={() => !compact && handleSort('congestion_percentage')}
                className={!compact ? 'cursor-pointer select-none hover:text-cyan-300' : ''}
              >
                <div className="flex items-center gap-1">
                  <span>Congestion Rate</span>
                  {!compact && sortField === 'congestion_percentage' && (
                    sortDirection === 'asc' ? <ArrowUp size={12} /> : <ArrowDown size={12} />
                  )}
                </div>
              </th>
              {!compact && <th>Window End</th>}
              {!compact && <th className="text-right">Actions</th>}
            </tr>
          </thead>
          <tbody>
            {displayRows.map((row) => {
              const isCongested = row.congestion_percentage >= 50;
              const isSevere = row.average_speed < 20;

              return (
                <tr
                  key={`${row.dataset}-${row.window_start}-${row.window_end}`}
                  className="hover:bg-slate-800/40 transition-colors"
                >
                  <td className="table-time font-mono font-medium text-slate-200">
                    {compactTimestamp(row.window_start)}
                  </td>
                  <td>
                    <span className={`font-mono font-bold ${isSevere ? 'text-rose-400' : 'text-slate-100'}`}>
                      {row.average_speed.toFixed(1)}
                    </span>{' '}
                    <span className="unit text-slate-500">mph</span>
                  </td>
                  <td className="font-mono text-slate-300">
                    {row.total_sensor_readings.toLocaleString()}
                  </td>
                  <td className="font-mono">
                    <span className={row.congested_sensor_count > 0 ? 'text-amber-400 font-semibold' : 'text-slate-400'}>
                      {row.congested_sensor_count.toLocaleString()} {row.congested_sensor_count === 1 ? 'reading' : 'readings'}
                    </span>
                  </td>
                  <td>
                    <span
                      className={`percent-pill font-mono font-bold ${
                        isCongested
                          ? 'bg-rose-950/60 text-rose-300 border border-rose-800/80'
                          : row.congestion_percentage > 20
                          ? 'bg-amber-950/50 text-amber-300 border border-amber-800/60'
                          : 'bg-emerald-950/40 text-emerald-300 border border-emerald-800/50'
                      }`}
                    >
                      {row.congestion_percentage.toFixed(1)}%
                    </span>
                  </td>
                  {!compact && (
                    <td className="font-mono text-slate-400">
                      {compactTimestamp(row.window_end)}
                    </td>
                  )}
                  {!compact && (
                    <td className="text-right">
                      <button
                        type="button"
                        onClick={() => setSelectedRecord(row)}
                        className="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300 text-[11px] font-medium transition-colors"
                      >
                        Inspect
                      </button>
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Row detail drawer / modal */}
      {selectedRecord && (
        <div className="fixed inset-0 z-50 bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4">
          <div className="bg-slate-900 border border-slate-700 rounded-xl max-w-lg w-full p-5 shadow-2xl space-y-4">
            <div className="flex items-center justify-between border-b border-slate-800 pb-3">
              <div>
                <span className="text-[10px] font-bold text-cyan-400 tracking-wider uppercase">
                  5-Minute Aggregated Window Detail
                </span>
                <h3 className="text-base font-semibold text-slate-100 mt-0.5">
                  {selectedRecord.dataset.toUpperCase()} Sensor Aggregate
                </h3>
              </div>
              <button
                type="button"
                onClick={() => setSelectedRecord(null)}
                className="text-slate-400 hover:text-slate-200 text-sm font-bold p-1"
              >
                ✕
              </button>
            </div>

            <div className="grid grid-cols-2 gap-3 text-xs">
              <div className="p-2.5 rounded bg-slate-950/60 border border-slate-800">
                <span className="text-slate-400">Window Start</span>
                <div className="font-mono font-medium text-slate-200 mt-1">
                  {readableTimestamp(selectedRecord.window_start)}
                </div>
              </div>
              <div className="p-2.5 rounded bg-slate-950/60 border border-slate-800">
                <span className="text-slate-400">Window End</span>
                <div className="font-mono font-medium text-slate-200 mt-1">
                  {readableTimestamp(selectedRecord.window_end)}
                </div>
              </div>
              <div className="p-2.5 rounded bg-slate-950/60 border border-slate-800">
                <span className="text-slate-400">Average Network Speed</span>
                <div className="font-mono font-bold text-cyan-400 text-base mt-1">
                  {selectedRecord.average_speed.toFixed(2)} mph
                </div>
              </div>
              <div className="p-2.5 rounded bg-slate-950/60 border border-slate-800">
                <span className="text-slate-400">Congestion Rate</span>
                <div className={`font-mono font-bold text-base mt-1 ${
                  selectedRecord.congestion_percentage >= 50 ? 'text-rose-400' : 'text-amber-400'
                }`}>
                  {selectedRecord.congestion_percentage.toFixed(2)}%
                </div>
              </div>
              <div className="p-2.5 rounded bg-slate-950/60 border border-slate-800">
                <span className="text-slate-400">Total Valid Readings</span>
                <div className="font-mono font-medium text-slate-200 mt-1">
                  {selectedRecord.total_sensor_readings.toLocaleString()} readings
                </div>
              </div>
              <div className="p-2.5 rounded bg-slate-950/60 border border-slate-800">
                <span className="text-slate-400">Congested Readings (&lt; 20 mph)</span>
                <div className="font-mono font-medium text-slate-200 mt-1">
                  {selectedRecord.congested_sensor_count.toLocaleString()} {selectedRecord.congested_sensor_count === 1 ? 'reading' : 'readings'}
                </div>
              </div>
            </div>

            <div className="text-[11px] text-slate-400 pt-2 border-t border-slate-800 flex justify-between items-center">
              <span>Database processed timestamp: {readableTimestamp(selectedRecord.processed_at)}</span>
              <button
                type="button"
                onClick={() => setSelectedRecord(null)}
                className="px-3 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 font-medium text-xs"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {error && rows.length > 0 && <p className="inline-error mt-2">{error}</p>}

      {/* Footer controls */}
      <div className="flex flex-wrap items-center justify-between gap-3 mt-3 pt-2 border-t border-slate-800/80 text-xs">
        <div className="text-slate-400 text-[11px]">
          Showing {displayRows.length.toLocaleString()} of {rows.length.toLocaleString()} windows loaded
          {compact && ' (Previewing recent 6 rows)'}
        </div>

        {!compact && onLoadMore && hasMore && (
          <button
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-cyan-950/40 hover:bg-cyan-900/50 text-cyan-300 border border-cyan-800/60 font-medium transition-colors disabled:opacity-50"
            type="button"
            disabled={loadingMore}
            onClick={onLoadMore}
          >
            {loadingMore ? 'Loading next batch from cursor…' : 'Load more windows'}
            <ChevronDown size={14} />
          </button>
        )}
      </div>
    </>
  );
}
