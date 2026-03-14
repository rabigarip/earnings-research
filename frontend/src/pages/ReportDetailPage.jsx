import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { ArrowLeft, FileText, CheckCircle, XCircle } from "lucide-react";
import { useReportContext } from "../context/ReportContext";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "";

export default function ReportDetailPage() {
  const { id } = useParams();
  const navigate = useNavigate();
  const { getPayload } = useReportContext();
  const [run, setRun] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const payload = getPayload(id);

  useEffect(() => {
    if (!id) return;
    let cancelled = false;
    setLoading(true);
    setError("");
    fetch(`${API_BASE}/api/reports/${id}`)
      .then((res) => {
        if (!res.ok) throw new Error("Report not found");
        return res.json();
      })
      .then((data) => { if (!cancelled) setRun(data); })
      .catch((err) => { if (!cancelled) setError(err.message || "Failed to load report"); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [id]);

  if (loading && !run) {
    return (
      <div className="min-h-screen bg-[#0a0e1a] flex items-center justify-center">
        <p className="text-[#5a6a8a]">Loading report...</p>
      </div>
    );
  }
  if (error || !run) {
    return (
      <div className="min-h-screen bg-[#0a0e1a] p-6">
        <button onClick={() => navigate("/")} className="flex items-center gap-2 text-[#5a6a8a] hover:text-white mb-6">
          <ArrowLeft className="w-4 h-4" /> Back
        </button>
        <p className="text-red-400">{error || "Report not found"}</p>
      </div>
    );
  }

  const steps = run.steps || [];
  return (
    <div className="min-h-screen bg-[#0a0e1a]">
      <header className="border-b border-[#1a2035] px-6 py-4">
        <button onClick={() => navigate("/")} className="flex items-center gap-2 text-[#5a6a8a] hover:text-white transition-colors text-sm">
          <ArrowLeft className="w-4 h-4" /> Back to Reports
        </button>
      </header>
      <main className="max-w-[900px] mx-auto px-6 py-8">
        <div className="mb-8">
          <h1 className="text-2xl font-bold text-white mb-1">{run.company || run.ticker}</h1>
          <p className="text-[#8ab4e8] font-mono text-sm">{run.ticker}</p>
          <p className="text-[#5a6a8a] text-sm mt-1">{run.country} · {run.status} · {run.created ? new Date(run.created).toLocaleString() : ""}</p>
        </div>
        {payload && (
          <section className="mb-8 p-5 rounded-xl bg-[#111827]/60 border border-[#1a2035]">
            <h2 className="text-xs font-semibold tracking-wider text-[#5a6a8a] uppercase mb-4 flex items-center gap-2">
              <FileText className="w-4 h-4" /> Report payload (preview)
            </h2>
            <div className="text-sm text-[#8a9ab0] space-y-2">
              {payload.company && <p><span className="text-[#5a6a8a]">Company:</span> {payload.company.company_name || payload.company.ticker}</p>}
              {payload.quote && <p><span className="text-[#5a6a8a]">Price:</span> {payload.quote.currency} {payload.quote.last_close}</p>}
              {payload.news_summary?.summary && <p className="mt-2"><span className="text-[#5a6a8a]">News summary:</span> {payload.news_summary.summary.slice(0, 200)}…</p>}
            </div>
          </section>
        )}
        {!payload && <p className="text-[#5a6a8a] text-sm mb-6">Full payload not stored for this run. Rerun from the reports list to regenerate and view.</p>}
        <section>
          <h2 className="text-xs font-semibold tracking-wider text-[#5a6a8a] uppercase mb-4">Pipeline steps</h2>
          <ul className="space-y-2">
            {steps.map((s, i) => (
              <li key={i} className="flex items-center gap-3 px-4 py-3 rounded-lg bg-[#111827]/60 border border-[#1a2035]">
                {s.status === "success" ? <CheckCircle className="w-4 h-4 text-emerald-400 shrink-0" /> : s.status === "failed" ? <XCircle className="w-4 h-4 text-red-400 shrink-0" /> : <span className="w-4 h-4 shrink-0 rounded-full bg-[#2a3a5a]" />}
                <div className="min-w-0">
                  <p className="text-sm font-medium text-white">{s.step_name}</p>
                  <p className="text-xs text-[#5a6a8a]">{s.message || s.status}</p>
                </div>
                <span className="ml-auto text-[10px] text-[#3a4a6a] uppercase">{s.status}</span>
              </li>
            ))}
          </ul>
        </section>
      </main>
    </div>
  );
}
