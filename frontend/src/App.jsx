import { useCallback, useEffect, useMemo, useRef, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "";

function extractFilename(contentDisposition, fallback) {
  if (!contentDisposition) return fallback;
  const m = /filename\*?=(?:UTF-8''|")?([^\";]+)/i.exec(contentDisposition);
  if (!m || !m[1]) return fallback;
  return decodeURIComponent(m[1].replace(/"/g, "").trim());
}

export default function App() {
  const [ticker, setTicker] = useState("2222.SR");
  const [skipLlm, setSkipLlm] = useState(false);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const [suggestions, setSuggestions] = useState([]);
  const [suggestOpen, setSuggestOpen] = useState(false);
  const [highlight, setHighlight] = useState(-1);
  const [suggestLoading, setSuggestLoading] = useState(false);
  const blurCloseTimer = useRef(null);
  const inputRef = useRef(null);

  useEffect(
    () => () => {
      if (blurCloseTimer.current) clearTimeout(blurCloseTimer.current);
    },
    [],
  );

  const disabled = useMemo(() => loading || !ticker.trim(), [loading, ticker]);

  useEffect(() => {
    const q = ticker;
    setHighlight(-1);
    const id = setTimeout(async () => {
      setSuggestLoading(true);
      try {
        const res = await fetch(
          `${API_BASE}/api/tickers/search?q=${encodeURIComponent(q.trim())}`,
        );
        const data = await res.json().catch(() => ({}));
        setSuggestions(Array.isArray(data.results) ? data.results : []);
      } catch {
        setSuggestions([]);
      } finally {
        setSuggestLoading(false);
      }
    }, 200);
    return () => clearTimeout(id);
  }, [ticker]);

  const pickSuggestion = useCallback((row) => {
    if (!row?.ticker) return;
    setTicker(row.ticker);
    setSuggestOpen(false);
    setHighlight(-1);
    inputRef.current?.focus();
  }, []);

  const openSuggestions = useCallback(() => {
    if (blurCloseTimer.current) clearTimeout(blurCloseTimer.current);
    setSuggestOpen(true);
  }, []);

  const scheduleCloseSuggestions = useCallback(() => {
    blurCloseTimer.current = setTimeout(() => {
      setSuggestOpen(false);
      setHighlight(-1);
    }, 150);
  }, []);

  const generateAndDownload = async () => {
    setError("");
    setStatus("Generating report...");
    setLoading(true);
    try {
      const tk = ticker.trim().toUpperCase();
      const createRes = await fetch(`${API_BASE}/api/reports`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ticker: tk, skip_llm: skipLlm }),
      });
      if (!createRes.ok) {
        const err = await createRes.json().catch(() => ({}));
        throw new Error(err?.detail || "Failed to generate report");
      }

      const created = await createRes.json();
      const runId = created?.report?.id;
      if (!runId) {
        throw new Error("Report generated but run id is missing");
      }

      setStatus("Downloading report...");
      const dlRes = await fetch(`${API_BASE}/api/reports/${runId}/download?t=${Date.now()}`);
      if (!dlRes.ok) {
        const err = await dlRes.json().catch(() => ({}));
        throw new Error(err?.detail || "Failed to download report");
      }

      const blob = await dlRes.blob();
      const filename = extractFilename(
        dlRes.headers.get("content-disposition"),
        `${tk}_preview.pptx`,
      );
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      setStatus(`Done. Downloaded ${filename}`);
    } catch (e) {
      setError(e?.message || "Unexpected error");
      setStatus("");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex items-center justify-center px-4">
      <div className="w-full max-w-xl bg-slate-900 border border-slate-800 rounded-xl p-6">
        <h1 className="text-2xl font-semibold mb-2">Download Earnings Report</h1>
        <p className="text-sm text-slate-400 mb-6">
          Enter a ticker, generate report, and download it directly.
        </p>

        <label className="block text-sm mb-2 text-slate-300" htmlFor="ticker-input">
          Ticker
        </label>
        <div className="relative mb-4">
          <input
            id="ticker-input"
            ref={inputRef}
            autoComplete="off"
            role="combobox"
            aria-expanded={suggestOpen}
            aria-controls="ticker-suggestions"
            aria-activedescendant={
              highlight >= 0 && suggestions[highlight]
                ? `ticker-opt-${highlight}`
                : undefined
            }
            value={ticker}
            onChange={(e) => {
              setTicker(e.target.value);
              openSuggestions();
            }}
            onFocus={openSuggestions}
            onBlur={scheduleCloseSuggestions}
            onKeyDown={(e) => {
              if (!suggestOpen && (e.key === "ArrowDown" || e.key === "ArrowUp")) {
                openSuggestions();
                return;
              }
              if (!suggestOpen) return;
              if (e.key === "Escape") {
                e.preventDefault();
                setSuggestOpen(false);
                setHighlight(-1);
                return;
              }
              if (e.key === "ArrowDown") {
                e.preventDefault();
                setHighlight((h) =>
                  suggestions.length ? Math.min(h + 1, suggestions.length - 1) : -1,
                );
                return;
              }
              if (e.key === "ArrowUp") {
                e.preventDefault();
                setHighlight((h) => (h <= 0 ? -1 : h - 1));
                return;
              }
              if (e.key === "Enter" && highlight >= 0 && suggestions[highlight]) {
                e.preventDefault();
                pickSuggestion(suggestions[highlight]);
              }
            }}
            placeholder="Type ticker or company name…"
            className="w-full rounded-lg bg-slate-950 border border-slate-700 px-3 py-2 outline-none focus:border-blue-500"
          />
          {suggestOpen && (suggestions.length > 0 || suggestLoading || ticker.trim()) ? (
            <ul
              id="ticker-suggestions"
              role="listbox"
              className="absolute z-50 mt-1 max-h-52 w-full overflow-auto rounded-lg border border-slate-700 bg-slate-900 py-1 shadow-lg"
            >
              {suggestLoading && suggestions.length === 0 ? (
                <li className="px-3 py-2 text-sm text-slate-500">Loading…</li>
              ) : null}
              {!suggestLoading && suggestions.length === 0 && ticker.trim() ? (
                <li className="px-3 py-2 text-sm text-slate-500">No matching companies</li>
              ) : null}
              {suggestions.map((row, i) => (
                <li
                  key={`${row.ticker}-${i}`}
                  id={`ticker-opt-${i}`}
                  role="option"
                  aria-selected={i === highlight}
                  className={`cursor-pointer px-3 py-2 text-sm ${
                    i === highlight ? "bg-slate-800 text-white" : "text-slate-200 hover:bg-slate-800/80"
                  }`}
                  onMouseDown={(e) => e.preventDefault()}
                  onMouseEnter={() => setHighlight(i)}
                  onClick={() => pickSuggestion(row)}
                >
                  <span className="font-mono text-blue-300">{row.ticker}</span>
                  <span className="mx-2 text-slate-500">·</span>
                  <span>{row.company}</span>
                  {row.country ? (
                    <span className="ml-2 text-xs text-slate-500">{row.country}</span>
                  ) : null}
                </li>
              ))}
            </ul>
          ) : null}
        </div>

        <label className="flex items-center gap-2 text-sm text-slate-300 mb-6">
          <input
            type="checkbox"
            checked={skipLlm}
            onChange={(e) => setSkipLlm(e.target.checked)}
          />
          Skip LLM (faster)
        </label>

        <button
          onClick={generateAndDownload}
          disabled={disabled}
          className="w-full rounded-lg bg-blue-600 hover:bg-blue-500 disabled:bg-slate-700 disabled:cursor-not-allowed px-4 py-2 font-medium"
        >
          {loading ? "Working..." : "Generate & Download"}
        </button>

        {status ? <p className="mt-4 text-sm text-emerald-400">{status}</p> : null}
        {error ? <p className="mt-2 text-sm text-rose-400">{error}</p> : null}
      </div>
    </div>
  );
}
