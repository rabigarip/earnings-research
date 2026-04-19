import { useCallback, useEffect, useMemo, useState } from "react";
import {
  addMonths,
  eachDayOfInterval,
  endOfMonth,
  endOfWeek,
  format,
  isSameDay,
  isSameMonth,
  parseISO,
  startOfMonth,
  startOfWeek,
  subMonths,
} from "date-fns";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "";
const WEEK_STARTS_ON = 1; // Monday

function iso(d) {
  return format(d, "yyyy-MM-dd");
}

function formatNumber(v) {
  if (v == null || Number.isNaN(v)) return "—";
  if (Math.abs(v) >= 1e9) return (v / 1e9).toFixed(2) + "B";
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + "K";
  return String(v);
}

function EventChip({ event, onClick }) {
  const confirmed = !!event.confirmed;
  return (
    <button
      type="button"
      onClick={onClick}
      className={`w-full text-left rounded-md px-2 py-1 text-[11px] leading-tight transition ${
        confirmed
          ? "bg-emerald-900/40 hover:bg-emerald-800/60 border border-emerald-700/60"
          : "bg-slate-800 hover:bg-slate-700 border border-slate-700"
      }`}
      title={`${event.ticker} — ${event.company_name || ""} (${confirmed ? "confirmed" : "estimated"})`}
    >
      <div className="flex items-center gap-1">
        <span
          className={`h-1.5 w-1.5 rounded-full ${
            confirmed ? "bg-emerald-400" : "bg-amber-400"
          }`}
        />
        <span className="font-mono text-blue-300 truncate">{event.ticker}</span>
      </div>
    </button>
  );
}

function EventDetail({ event, onClose, onGeneratePreview, generating }) {
  if (!event) return null;
  return (
    <div className="fixed inset-0 z-50 flex">
      <div
        className="flex-1 bg-black/60"
        onClick={onClose}
        role="presentation"
      />
      <aside className="w-[400px] max-w-full bg-slate-900 border-l border-slate-800 p-6 overflow-auto">
        <div className="flex items-start justify-between mb-4">
          <div>
            <div className="text-xs uppercase tracking-wide text-slate-500">
              {event.country || ""} · {event.sector || ""}
            </div>
            <div className="text-xl font-semibold">
              <span className="font-mono text-blue-300">{event.ticker}</span>
            </div>
            <div className="text-sm text-slate-300">{event.company_name}</div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-slate-400 hover:text-white"
            aria-label="Close"
          >
            ✕
          </button>
        </div>
        <dl className="text-sm grid grid-cols-2 gap-y-2">
          <dt className="text-slate-500">Event date</dt>
          <dd>{event.event_date}</dd>
          <dt className="text-slate-500">Status</dt>
          <dd className={event.confirmed ? "text-emerald-400" : "text-amber-400"}>
            {event.confirmed ? "Confirmed" : "Estimated"}
          </dd>
          <dt className="text-slate-500">Period</dt>
          <dd>{event.period_label || "—"}</dd>
          <dt className="text-slate-500">Consensus revenue</dt>
          <dd>{formatNumber(event.consensus_revenue)}</dd>
          <dt className="text-slate-500">Consensus EPS</dt>
          <dd>{formatNumber(event.consensus_eps)}</dd>
          <dt className="text-slate-500">Source</dt>
          <dd className="capitalize">{event.source || "—"}</dd>
          <dt className="text-slate-500">Last checked</dt>
          <dd className="text-xs text-slate-400">{event.last_checked || "—"}</dd>
        </dl>
        <button
          type="button"
          onClick={() => onGeneratePreview(event.ticker)}
          disabled={generating}
          className="mt-6 w-full rounded-lg bg-blue-600 hover:bg-blue-500 disabled:bg-slate-700 disabled:cursor-not-allowed px-4 py-2 text-sm font-medium"
        >
          {generating ? "Generating…" : "Generate preview & download"}
        </button>
      </aside>
    </div>
  );
}

export default function CalendarPage() {
  const [anchor, setAnchor] = useState(() => startOfMonth(new Date()));
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [selected, setSelected] = useState(null);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshStatus, setRefreshStatus] = useState("");
  const [generating, setGenerating] = useState(false);
  const [confirmedOnly, setConfirmedOnly] = useState(false);
  const [countryFilter, setCountryFilter] = useState("");

  const days = useMemo(() => {
    const start = startOfWeek(startOfMonth(anchor), { weekStartsOn: WEEK_STARTS_ON });
    const end = endOfWeek(endOfMonth(anchor), { weekStartsOn: WEEK_STARTS_ON });
    return eachDayOfInterval({ start, end });
  }, [anchor]);

  const rangeStart = days[0];
  const rangeEnd = days[days.length - 1];

  const fetchEvents = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams({
        start: iso(rangeStart),
        end: iso(rangeEnd),
      });
      if (confirmedOnly) params.set("confirmed", "1");
      if (countryFilter.trim()) params.set("countries", countryFilter.trim());
      const res = await fetch(`${API_BASE}/api/calendar?${params.toString()}`);
      if (!res.ok) throw new Error(`Calendar fetch failed (${res.status})`);
      const data = await res.json();
      setEvents(Array.isArray(data.events) ? data.events : []);
    } catch (e) {
      setError(e?.message || String(e));
      setEvents([]);
    } finally {
      setLoading(false);
    }
  }, [rangeStart, rangeEnd, confirmedOnly, countryFilter]);

  useEffect(() => {
    fetchEvents();
  }, [fetchEvents]);

  const eventsByDay = useMemo(() => {
    const map = new Map();
    for (const e of events) {
      const d = e.event_date;
      if (!map.has(d)) map.set(d, []);
      map.get(d).push(e);
    }
    return map;
  }, [events]);

  const triggerRefresh = async () => {
    setRefreshing(true);
    setRefreshStatus("Queued…");
    try {
      const res = await fetch(`${API_BASE}/api/calendar/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ force: false }),
      });
      if (!res.ok) throw new Error(`Refresh failed (${res.status})`);
      const { job_id } = await res.json();
      // Poll every 2s until completion (or 2 min timeout)
      const started = Date.now();
      while (Date.now() - started < 120_000) {
        await new Promise((r) => setTimeout(r, 2000));
        const s = await fetch(`${API_BASE}/api/calendar/refresh/${job_id}`);
        const job = await s.json();
        setRefreshStatus(
          job.status === "running"
            ? "Running…"
            : job.status === "completed"
            ? `Done: updated ${job.summary?.updated ?? 0}/${job.summary?.total ?? 0}`
            : job.status,
        );
        if (job.status === "completed" || job.status === "failed") {
          await fetchEvents();
          break;
        }
      }
    } catch (e) {
      setRefreshStatus(`Error: ${e?.message || e}`);
    } finally {
      setRefreshing(false);
    }
  };

  const generatePreview = async (ticker) => {
    setGenerating(true);
    try {
      const res = await fetch(`${API_BASE}/api/reports`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ticker, skip_llm: true }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err?.detail?.summary || err?.detail || "Generation failed");
      }
      const created = await res.json();
      const runId = created?.report?.id;
      if (!runId) throw new Error("No run id");
      const dl = await fetch(`${API_BASE}/api/reports/${runId}/download?t=${Date.now()}`);
      if (!dl.ok) throw new Error("Download failed");
      const blob = await dl.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${ticker}_earnings_preview.pptx`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 100);
    } catch (e) {
      alert(`Could not generate: ${e?.message || e}`);
    } finally {
      setGenerating(false);
    }
  };

  const weekdayLabels = useMemo(
    () => ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
    [],
  );

  return (
    <div className="max-w-7xl mx-auto px-6 py-8">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <div>
          <h1 className="text-2xl font-semibold">Earnings Calendar</h1>
          <p className="text-sm text-slate-400">
            {format(anchor, "MMMM yyyy")} · {events.length} event
            {events.length === 1 ? "" : "s"} in range
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setAnchor((d) => subMonths(d, 1))}
            className="rounded-md bg-slate-800 hover:bg-slate-700 px-3 py-1 text-sm"
          >
            ◄
          </button>
          <button
            type="button"
            onClick={() => setAnchor(startOfMonth(new Date()))}
            className="rounded-md bg-slate-800 hover:bg-slate-700 px-3 py-1 text-sm"
          >
            Today
          </button>
          <button
            type="button"
            onClick={() => setAnchor((d) => addMonths(d, 1))}
            className="rounded-md bg-slate-800 hover:bg-slate-700 px-3 py-1 text-sm"
          >
            ►
          </button>
          <button
            type="button"
            onClick={triggerRefresh}
            disabled={refreshing}
            className="ml-2 rounded-md bg-blue-600 hover:bg-blue-500 disabled:bg-slate-700 px-3 py-1 text-sm"
          >
            {refreshing ? "Refreshing…" : "Refresh data"}
          </button>
        </div>
      </div>

      <div className="flex flex-wrap gap-3 mb-4 text-sm">
        <label className="flex items-center gap-2 text-slate-300">
          <input
            type="checkbox"
            checked={confirmedOnly}
            onChange={(e) => setConfirmedOnly(e.target.checked)}
          />
          Confirmed only
        </label>
        <input
          type="text"
          placeholder="Countries (e.g. SA,AE)"
          value={countryFilter}
          onChange={(e) => setCountryFilter(e.target.value)}
          className="rounded-md bg-slate-900 border border-slate-700 px-3 py-1 text-sm w-48"
        />
        {refreshStatus ? (
          <span className="text-xs text-slate-400 self-center">{refreshStatus}</span>
        ) : null}
        {loading ? (
          <span className="text-xs text-slate-400 self-center">Loading…</span>
        ) : null}
        {error ? (
          <span className="text-xs text-rose-400 self-center">{error}</span>
        ) : null}
      </div>

      <div className="grid grid-cols-7 gap-[1px] bg-slate-800 border border-slate-800 rounded-lg overflow-hidden">
        {weekdayLabels.map((w) => (
          <div
            key={w}
            className="bg-slate-900 text-xs uppercase tracking-wide text-slate-500 px-2 py-2 text-center"
          >
            {w}
          </div>
        ))}
        {days.map((day) => {
          const key = iso(day);
          const dayEvents = eventsByDay.get(key) || [];
          const inMonth = isSameMonth(day, anchor);
          const today = isSameDay(day, new Date());
          return (
            <div
              key={key}
              className={`min-h-[110px] p-1.5 bg-slate-950 ${
                inMonth ? "" : "opacity-50"
              }`}
            >
              <div
                className={`text-xs mb-1 ${
                  today
                    ? "inline-block rounded bg-blue-600 text-white px-1.5"
                    : "text-slate-400"
                }`}
              >
                {format(day, "d")}
              </div>
              <div className="flex flex-col gap-1">
                {dayEvents.slice(0, 4).map((e) => (
                  <EventChip
                    key={`${e.ticker}-${e.event_date}`}
                    event={e}
                    onClick={() => setSelected(e)}
                  />
                ))}
                {dayEvents.length > 4 ? (
                  <div className="text-[10px] text-slate-500 pl-1">
                    +{dayEvents.length - 4} more
                  </div>
                ) : null}
              </div>
            </div>
          );
        })}
      </div>

      <EventDetail
        event={selected}
        onClose={() => setSelected(null)}
        onGeneratePreview={generatePreview}
        generating={generating}
      />

      <div className="mt-6 text-xs text-slate-500">
        <span className="inline-flex items-center gap-1 mr-4">
          <span className="h-2 w-2 rounded-full bg-emerald-400 inline-block" />
          confirmed
        </span>
        <span className="inline-flex items-center gap-1">
          <span className="h-2 w-2 rounded-full bg-amber-400 inline-block" />
          estimated
        </span>
      </div>
    </div>
  );
}
