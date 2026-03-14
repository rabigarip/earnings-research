export default function StatusBadge({ status }) {
  const config = {
    QUEUED: { bg: "bg-[#2a2a3a]", text: "text-[#8a9ab0]", dot: false, label: "QUEUED" },
    RUNNING: { bg: "bg-[#1a2a3a]", text: "text-blue-400", dot: true, dotColor: "bg-blue-400", label: "RUNNING" },
    COMPLETED: { bg: "bg-emerald-500/20", text: "text-emerald-400", dot: false, label: "COMPLETED" },
    FAILED: { bg: "bg-red-500/20", text: "text-red-400", dot: false, label: "FAILED" },
  };
  const c = config[status] || config.QUEUED;
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11px] font-semibold tracking-wide ${c.bg} ${c.text}`}>
      {c.dot && <span className={`w-1.5 h-1.5 rounded-full ${c.dotColor} animate-pulse`} />}
      {c.label}
    </span>
  );
}
