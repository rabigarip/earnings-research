import { useState, useEffect, useRef } from "react";
import { X, Search } from "lucide-react";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "";

export default function NewReportModal({ open, onClose, onGenerate }) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [selected, setSelected] = useState(null);
  const [searching, setSearching] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState("");
  const inputRef = useRef(null);
  const debounceRef = useRef(null);

  useEffect(() => {
    if (open) {
      setQuery("");
      setResults([]);
      setSelected(null);
      setError("");
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [open]);

  useEffect(() => {
    if (!query.trim()) { setResults([]); return; }
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      setSearching(true);
      try {
        const res = await fetch(`${API_BASE}/api/tickers/search?q=${encodeURIComponent(query)}`);
        if (!res.ok) throw new Error("Search failed");
        const data = await res.json();
        setResults(data.results || data || []);
      } catch (err) {
        console.error("Ticker search failed:", err);
        setResults([]);
      } finally {
        setSearching(false);
      }
    }, 300);
    return () => clearTimeout(debounceRef.current);
  }, [query]);

  const handleGenerate = async () => {
    const ticker = selected?.ticker || query.trim();
    if (!ticker) return;
    setGenerating(true);
    setError("");
    try {
      await onGenerate(ticker);
    } catch (err) {
      setError(err.message || "Failed to generate report");
    } finally {
      setGenerating(false);
    }
  };

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center" onClick={onClose}>
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />
      <div onClick={(e) => e.stopPropagation()} className="relative bg-[#151b2e] border border-[#1e2a4a] rounded-2xl w-full max-w-md mx-4 p-6 shadow-2xl">
        <button onClick={onClose} className="absolute top-4 right-4 text-[#5a6a8a] hover:text-white transition-colors">
          <X className="w-5 h-5" />
        </button>
        <h2 className="text-xl font-bold text-white mb-6">New Report</h2>
        <label className="block text-[10px] font-semibold tracking-[0.15em] text-[#5a6a8a] uppercase mb-3">Ticker Symbol</label>
        <div className="relative mb-2">
          <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-[#3a4a6a]" />
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => { setQuery(e.target.value); setSelected(null); }}
            onKeyDown={(e) => e.key === "Enter" && handleGenerate()}
            placeholder="Search by ticker or company name..."
            className="w-full bg-[#0d1221] border border-[#1e2a4a] focus:border-[#3b6cb5] rounded-lg pl-11 pr-4 py-3 text-sm text-white placeholder-[#3a4a6a] outline-none transition-colors font-mono"
          />
        </div>
        <p className="text-[#5a6a8a] text-xs mb-4">Search across 300+ tickers from global markets. Report generation can take 1–2 minutes; on free hosting it may time out—retry if needed.</p>
        {results.length > 0 && !selected && (
          <div className="max-h-48 overflow-y-auto mb-4 border border-[#1e2a4a] rounded-lg">
            {results.map((item) => (
              <button
                key={item.ticker}
                onClick={() => { setSelected(item); setQuery(item.ticker); }}
                className="w-full flex items-center justify-between px-4 py-3 hover:bg-white/5 transition-colors border-b border-[#1a2035] last:border-b-0"
              >
                <div className="text-left">
                  <code className="text-sm text-[#8ab4e8] font-mono">{item.ticker}</code>
                  <p className="text-xs text-[#5a6a8a] mt-0.5">{item.company}</p>
                </div>
                <span className="text-[10px] text-[#5a6a8a]">{item.country}</span>
              </button>
            ))}
          </div>
        )}
        {searching && <p className="text-[#5a6a8a] text-xs mb-4">Searching...</p>}
        {error && <p className="text-red-400 text-xs mb-4">{error}</p>}
        <div className="flex justify-end gap-3 mt-2">
          <button onClick={onClose} className="px-5 py-2.5 text-sm text-[#8a9ab0] hover:text-white transition-colors">Cancel</button>
          <button onClick={handleGenerate} disabled={generating || (!selected && !query.trim())} className="px-5 py-2.5 bg-[#2a4a7a] hover:bg-[#335a8f] disabled:opacity-40 text-white text-sm font-medium rounded-lg transition-colors">
            {generating ? "Generating..." : "Generate Report"}
          </button>
        </div>
      </div>
    </div>
  );
}
