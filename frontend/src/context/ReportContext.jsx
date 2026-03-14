import { createContext, useContext, useState, useCallback } from "react";

const ReportContext = createContext(null);

export function ReportProvider({ children }) {
  const [payloads, setPayloads] = useState({});

  const setPayload = useCallback((runId, payload) => {
    if (!runId) return;
    setPayloads((prev) => ({ ...prev, [runId]: payload }));
  }, []);

  const getPayload = useCallback((runId) => payloads[runId] ?? null, [payloads]);

  return (
    <ReportContext.Provider value={{ setPayload, getPayload }}>
      {children}
    </ReportContext.Provider>
  );
}

export function useReportContext() {
  const ctx = useContext(ReportContext);
  if (!ctx) throw new Error("useReportContext must be used within ReportProvider");
  return ctx;
}
