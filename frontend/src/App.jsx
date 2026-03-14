import { useState } from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { ReportProvider } from "./context/ReportContext";
import LoginPage from "./pages/LoginPage";
import ReportsPage from "./pages/ReportsPage";
import ReportDetailPage from "./pages/ReportDetailPage";

export default function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  if (!isAuthenticated) {
    return <LoginPage onLogin={() => setIsAuthenticated(true)} />;
  }

  return (
    <BrowserRouter>
      <ReportProvider>
        <Routes>
          <Route path="/" element={<ReportsPage onSignOut={() => setIsAuthenticated(false)} />} />
          <Route path="/reports/:id" element={<ReportDetailPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </ReportProvider>
    </BrowserRouter>
  );
}
