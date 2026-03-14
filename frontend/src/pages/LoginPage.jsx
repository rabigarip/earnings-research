import { useState } from "react";
import { Shield, Lock } from "lucide-react";

export default function LoginPage({ onLogin }) {
  const [accessCode, setAccessCode] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!accessCode.trim()) {
      setError("Please enter an access code");
      return;
    }
    setLoading(true);
    setError("");

    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ accessCode }),
      });

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.message || "Invalid access code");
      }

      const data = await res.json();
      if (data.token) {
        localStorage.setItem("ep_token", data.token);
      }
      onLogin();
    } catch (err) {
      setError(err.message || "Authentication failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#0a0e1a] flex items-center justify-center px-4">
      <div className="w-full max-w-sm flex flex-col items-center">
        <div className="w-14 h-14 rounded-xl bg-[#141a2e] border border-[#1e2a4a] flex items-center justify-center mb-6">
          <Shield className="w-7 h-7 text-[#4a6fa5]" />
        </div>
        <h1 className="text-3xl font-serif font-bold text-white mb-2 italic">
          Earnings Preview
        </h1>
        <p className="text-[#5a6a8a] text-sm mb-10">
          Institutional Research Platform
        </p>
        <div className="w-full bg-[#111827]/60 border border-[#1e2a4a] rounded-2xl p-8">
          <label className="block text-[10px] font-semibold tracking-[0.15em] text-[#5a6a8a] uppercase mb-3">
            Access Code
          </label>
          <div className="relative mb-4">
            <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-[#3a4a6a]" />
            <input
              type="password"
              value={accessCode}
              onChange={(e) => {
                setAccessCode(e.target.value);
                setError("");
              }}
              onKeyDown={(e) => e.key === "Enter" && handleSubmit(e)}
              placeholder="Enter site password"
              className="w-full bg-[#0d1221] border border-[#1e2a4a] focus:border-[#3b6cb5] rounded-lg pl-11 pr-4 py-3 text-sm text-white placeholder-[#3a4a6a] outline-none transition-colors"
            />
          </div>
          {error && (
            <p className="text-red-400 text-xs mb-3">{error}</p>
          )}
          <button
            onClick={handleSubmit}
            disabled={loading}
            className="w-full bg-[#2a4a7a] hover:bg-[#335a8f] disabled:opacity-50 text-[#8ab4e8] font-medium py-3 rounded-lg transition-colors text-sm"
          >
            {loading ? "Signing in..." : "Sign In"}
          </button>
        </div>
        <p className="text-[#3a4a6a] text-xs mt-8">
          Authorized personnel only · Contact administrator for access
        </p>
      </div>
    </div>
  );
}
