import { useState, useEffect, useMemo } from "react";
import {
  FileText,
  TrendingUp,
  Clock,
  Plus,
  Settings,
  LogOut,
  Shield,
  Eye,
  Download,
  RefreshCw,
  ChevronDown,
  ChevronsUpDown,
  ArrowDown,
  ArrowUp,
} from "lucide-react";
import { useNavigate } from "react-router-dom";
import NewReportModal from "../components/NewReportModal";
import StatusBadge from "../components/StatusBadge";
import { useReportContext } from "../context/ReportContext";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "";

export default function ReportsPage({ onSignOut }) {
  const navigate = useNavigate();
  const { setPayload } = useReportContext();
  const [reports, setReports] = useState([]);
  const [loading, setLoading] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);
  const [tickerFilter, setTickerFilter] = useState("all");
  const [countryFilter, setCountryFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [sortField, setSortField] = useState("created");
  const [sortDir, setSortDir] = useState("desc");

  const fetchReports = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/reports`);
      if (!res.ok) throw new Error("Failed to fetch");
      const data = await res.json();
      setReports(data.reports || data || []);
    } catch (err) {
      console.error("Failed to fetch reports:", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchReports();
    const interval = setInterval(fetchReports, 30000);
    return () => clearInterval(interval);
  }, []);

  const stats = useMemo(() => {
    const total = reports.length;
    const completed = reports.filter((r) => r.status === "COMPLETED").length;
    const inProgress = reports.filter((r) => ["RUNNING", "QUEUED"].includes(r.status)).length;
    return { total, completed, inProgress };
  }, [reports]);

  const tickers = useMemo(() => [...new Set(reports.map((r) => r.ticker))].sort(), [reports]);
  const countries = useMemo(() => [...new Set(reports.map((r) => r.country))].sort(), [reports]);
  const statuses = useMemo(() => [...new Set(reports.map((r) => r.status))].sort(), [reports]);

  const filteredReports = useMemo(() => {
    let result = [...reports];
    if (tickerFilter !== "all") result = result.filter((r) => r.ticker === tickerFilter);
    if (countryFilter !== "all") result = result.filter((r) => r.country === countryFilter);
    if (statusFilter !== "all") result = result.filter((r) => r.status === statusFilter);
    result.sort((a, b) => {
      let aVal = a[sortField];
      let bVal = b[sortField];
      if (sortField === "created") {
        aVal = new Date(aVal).getTime();
        bVal = new Date(bVal).getTime();
      }
      if (typeof aVal === "string") {
        aVal = aVal.toLowerCase();
        bVal = bVal.toLowerCase();
      }
      if (aVal < bVal) return sortDir === "asc" ? -1 : 1;
      if (aVal > bVal) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
    return result;
  }, [reports, tickerFilter, countryFilter, statusFilter, sortField, sortDir]);

  const handleSort = (field) => {
    if (sortField === field) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else {
      setSortField(field);
      setSortDir("desc");
    }
  };

  const handleGenerateReport = async (ticker) => {
    try {
      const res = await fetch(`${API_BASE}/api/reports`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ticker, skip_llm: true }),
      });
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        const detail = errData.detail;
        let msg =
          typeof detail === "string"
            ? detail
            : Array.isArray(detail)
              ? detail.map((d) => d.msg ?? d).join(", ")
              : "";
        if (!msg) {
          if (res.status === 504 || res.status === 502)
            msg =
              "Request timed out or server unavailable. Report generation can take 1–2 minutes; try again or use a faster connection.";
          else msg = "Failed to create report.";
        }
        throw new Error(msg);
      }
      const data = await res.json();
      if (data.payload && data.report?.id) setPayload(data.report.id, data.payload);
      setModalOpen(false);
      fetchReports();
    } catch (err) {
      console.error("Failed to generate report:", err);
      throw err;
    }
  };

  const handleAction = async (action, reportId) => {
    try {
      switch (action) {
        case "view":
          navigate(`/reports/${reportId}`);
          break;
        case "memo":
        case "qa":
          navigate(`/reports/${reportId}`);
          break;
        case "download":
          window.open(`${API_BASE}/api/reports/${reportId}/download?t=${Date.now()}`, "_blank");
          break;
        case "rerun": {
          const res = await fetch(`${API_BASE}/api/reports/${reportId}/rerun`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
          });
          if (!res.ok) throw new Error("Rerun failed");
          const data = await res.json();
          if (data.payload && data.report?.id) setPayload(data.report.id, data.payload);
          fetchReports();
          break;
        }
      }
    } catch (err) {
      console.error(`Action ${action} failed:`, err);
    }
  };

  const handleSignOut = () => {
    localStorage.removeItem("ep_token");
    onSignOut();
  };

  const formatDate = (dateStr) => {
    const d = new Date(dateStr);
    const day = String(d.getDate()).padStart(2, "0");
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    return `${day} ${months[d.getMonth()]} ${d.getFullYear()} · ${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
  };

  const SortIcon = ({ field }) => {
    if (sortField !== field) return <ChevronsUpDown className="w-3 h-3 ml-1 opacity-40" />;
    return sortDir === "asc" ? <ArrowUp className="w-3 h-3 ml-1 text-blue-400" /> : <ArrowDown className="w-3 h-3 ml-1 text-blue-400" />;
  };

  return (
    <div className="min-h-screen bg-[#0a0e1a]">
      <header className="border-b border-[#1a2035] px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-[#141a2e] border border-[#1e2a4a] flex items-center justify-center">
            <Shield className="w-4 h-4 text-[#4a6fa5]" />
          </div>
          <span className="text-white font-medium text-sm">Earnings Preview</span>
        </div>
        <div className="flex items-center gap-4">
          <button className="text-[#5a6a8a] hover:text-white transition-colors">
            <Settings className="w-5 h-5" />
          </button>
          <button onClick={handleSignOut} className="flex items-center gap-2 text-[#5a6a8a] hover:text-white transition-colors text-sm">
            <LogOut className="w-4 h-4" /> Sign out
          </button>
        </div>
      </header>
      <main className="max-w-[1400px] mx-auto px-6 py-8">
        <div className="flex items-start justify-between mb-8">
          <div>
            <h1 className="text-3xl font-bold text-white mb-1">Reports</h1>
            <p className="text-[#5a6a8a] text-sm">Generate and manage earnings preview reports</p>
          </div>
          <button onClick={() => setModalOpen(true)} className="flex items-center gap-2 bg-[#2a4a7a] hover:bg-[#335a8f] text-white text-sm font-medium px-5 py-2.5 rounded-lg transition-colors">
            <Plus className="w-4 h-4" /> New Report
          </button>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-10">
          <StatCard label="Total Reports" value={stats.total} sub="All time" icon={<FileText className="w-5 h-5" />} color="emerald" />
          <StatCard label="Completed" value={stats.completed} sub="Ready for review" icon={<TrendingUp className="w-5 h-5" />} color="green" />
          <StatCard label="In Progress" value={stats.inProgress} sub="Processing now" icon={<Clock className="w-5 h-5" />} color="blue" />
        </div>
        <div className="mb-6">
          <h2 className="text-[10px] font-semibold tracking-[0.15em] text-[#5a6a8a] uppercase mb-4">Recent Reports</h2>
          <div className="flex items-center justify-between">
            <div className="flex gap-3">
              <FilterSelect value={tickerFilter} onChange={setTickerFilter} options={tickers} allLabel="All Tickers" />
              <FilterSelect value={countryFilter} onChange={setCountryFilter} options={countries} allLabel="All Countries" />
              <FilterSelect value={statusFilter} onChange={setStatusFilter} options={statuses} allLabel="All Statuses" />
            </div>
            <span className="text-[#5a6a8a] text-sm">{filteredReports.length} of {reports.length} reports</span>
          </div>
        </div>
        <div className="border border-[#1a2035] rounded-xl overflow-hidden">
          <table className="w-full">
            <thead>
              <tr className="border-b border-[#1a2035]">
                <Th onClick={() => handleSort("ticker")}>Ticker <SortIcon field="ticker" /></Th>
                <Th onClick={() => handleSort("company")}>Company <SortIcon field="company" /></Th>
                <Th onClick={() => handleSort("country")}>Country <SortIcon field="country" /></Th>
                <Th onClick={() => handleSort("status")}>Status <SortIcon field="status" /></Th>
                <Th onClick={() => handleSort("created")}>Created <SortIcon field="created" /></Th>
                <Th>Warnings</Th>
                <Th align="right">Actions</Th>
              </tr>
            </thead>
            <tbody>
              {loading && reports.length === 0 ? (
                <tr><td colSpan={7} className="text-center py-16 text-[#5a6a8a]">Loading reports...</td></tr>
              ) : filteredReports.length === 0 ? (
                <tr><td colSpan={7} className="text-center py-16 text-[#5a6a8a]">No reports found</td></tr>
              ) : (
                filteredReports.map((report) => (
                  <tr key={report.id} className="border-b border-[#1a2035] last:border-b-0 hover:bg-[#0d1225] transition-colors">
                    <td className="px-5 py-4"><code className="text-sm text-[#8ab4e8] font-mono">{report.ticker}</code></td>
                    <td className="px-5 py-4 text-sm text-white">{report.company}</td>
                    <td className="px-5 py-4 text-sm text-[#8a9ab0]">{report.country}</td>
                    <td className="px-5 py-4"><StatusBadge status={report.status} /></td>
                    <td className="px-5 py-4 text-sm text-[#8a9ab0]">{formatDate(report.created)}</td>
                    <td className="px-5 py-4 text-sm">{report.warnings > 0 ? <span className="text-amber-400 font-medium">{report.warnings}</span> : <span className="text-[#3a4a6a]">—</span>}</td>
                    <td className="px-5 py-4">
                      <div className="flex items-center justify-end gap-1">
                        <ActionBtn icon={<Eye className="w-3.5 h-3.5" />} label="View" onClick={() => handleAction("view", report.id)} />
                        {report.status === "COMPLETED" && (
                          <>
                            <ActionBtn label="Memo" onClick={() => handleAction("memo", report.id)} />
                            <ActionBtn icon={<Download className="w-3.5 h-3.5" />} onClick={() => handleAction("download", report.id)} />
                            <ActionBtn label="QA" onClick={() => handleAction("qa", report.id)} />
                          </>
                        )}
                        {report.status === "FAILED" && (
                          <ActionBtn icon={<RefreshCw className="w-3.5 h-3.5" />} label="Rerun" onClick={() => handleAction("rerun", report.id)} />
                        )}
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </main>
      <NewReportModal open={modalOpen} onClose={() => setModalOpen(false)} onGenerate={handleGenerateReport} />
    </div>
  );
}

function StatCard({ label, value, sub, icon, color }) {
  const iconColors = { emerald: "text-emerald-400 bg-emerald-400/10", green: "text-green-400 bg-green-400/10", blue: "text-blue-400 bg-blue-400/10" };
  const subColors = { emerald: "text-[#5a6a8a]", green: "text-green-400/80", blue: "text-blue-400/80" };
  return (
    <div className="bg-[#111827]/60 border border-[#1a2035] rounded-xl px-6 py-5 flex items-start justify-between">
      <div>
        <p className="text-[10px] font-semibold tracking-[0.15em] text-[#5a6a8a] uppercase mb-2">{label}</p>
        <p className="text-4xl font-bold text-white mb-1">{value}</p>
        <p className={`text-xs ${subColors[color]}`}>{sub}</p>
      </div>
      <div className={`p-2 rounded-lg ${iconColors[color]}`}>{icon}</div>
    </div>
  );
}

function FilterSelect({ value, onChange, options, allLabel }) {
  return (
    <div className="relative">
      <select value={value} onChange={(e) => onChange(e.target.value)} className="appearance-none bg-[#111827]/60 border border-[#1a2035] text-sm text-[#8a9ab0] rounded-lg pl-4 pr-9 py-2.5 outline-none focus:border-[#3b6cb5] transition-colors cursor-pointer">
        <option value="all">{allLabel}</option>
        {options.map((opt) => <option key={opt} value={opt}>{opt}</option>)}
      </select>
      <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[#5a6a8a] pointer-events-none" />
    </div>
  );
}

function Th({ children, onClick, align = "left" }) {
  return (
    <th onClick={onClick} className={`px-5 py-3 text-[10px] font-semibold tracking-[0.15em] text-[#5a6a8a] uppercase ${align === "right" ? "text-right" : "text-left"} ${onClick ? "cursor-pointer hover:text-[#8a9ab0] select-none" : ""}`}>
      <span className="inline-flex items-center">{children}</span>
    </th>
  );
}

function ActionBtn({ icon, label, onClick }) {
  return (
    <button onClick={onClick} className="flex items-center gap-1.5 text-[#5a6a8a] hover:text-white text-xs px-2 py-1.5 rounded hover:bg-white/5 transition-colors">
      {icon}
      {label && <span>{label}</span>}
    </button>
  );
}
