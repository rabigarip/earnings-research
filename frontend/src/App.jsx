import { useMemo, useState } from "react";

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

  const disabled = useMemo(() => loading || !ticker.trim(), [loading, ticker]);

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

        <label className="block text-sm mb-2 text-slate-300">Ticker</label>
        <input
          value={ticker}
          onChange={(e) => setTicker(e.target.value)}
          placeholder="e.g. 2222.SR"
          className="w-full rounded-lg bg-slate-950 border border-slate-700 px-3 py-2 mb-4 outline-none focus:border-blue-500"
        />

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
