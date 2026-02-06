"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

type SourceStat = {
  id: string;
  name: string;
  level: "federal" | "cantonal";
  canton?: string | null;
  connector: string;
  count: number;
  status: "indexed" | "pending";
};

type StatsResponse = {
  total_decisions: number;
  sources: SourceStat[];
};

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/+$/, "") ?? "http://localhost:8000";

export default function DashboardPage() {
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);

  async function fetchStats() {
    try {
      const res = await fetch(`${API_BASE}/api/stats`);
      const data: StatsResponse = await res.json();
      setStats(data);
      setLastUpdated(new Date());
    } catch (e) {
      console.error("Failed to fetch stats", e);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchStats();
  }, []);

  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(fetchStats, 3000);
    return () => clearInterval(interval);
  }, [autoRefresh]);

  const federalSources = stats?.sources.filter((s) => s.level === "federal") ?? [];
  const cantonalSources = stats?.sources.filter((s) => s.level === "cantonal") ?? [];
  const indexedCount = stats?.sources.filter((s) => s.count > 0).length ?? 0;
  const totalSources = stats?.sources.length ?? 0;

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="border-b border-line">
        <div className="mx-auto max-w-6xl px-4 py-6 md:px-6">
          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <div>
              <h1 className="text-base font-medium text-fg">Statistics</h1>
              <p className="mt-0.5 text-xs text-dim">
                Ingestion status across all sources
              </p>
            </div>
            <nav className="flex items-center gap-4 text-xs">
              <label className="flex items-center gap-1.5 cursor-pointer text-dim hover:text-fg">
                <input
                  type="checkbox"
                  checked={autoRefresh}
                  onChange={(e) => setAutoRefresh(e.target.checked)}
                />
                <span>Auto-refresh</span>
              </label>
              <Link href="/" className="text-dim hover:text-fg">
                Search
              </Link>
            </nav>
          </div>
        </div>
      </header>

      {/* Main */}
      <main className="mx-auto max-w-6xl px-4 py-6 md:px-6">
        {/* Summary */}
        <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
          <StatCard
            label="Total"
            value={loading ? "—" : stats?.total_decisions.toLocaleString() ?? "0"}
          />
          <StatCard
            label="Sources"
            value={loading ? "—" : `${indexedCount}/${totalSources}`}
          />
          <StatCard
            label="Federal"
            value={loading ? "—" : `${federalSources.filter((s) => s.count > 0).length}/${federalSources.length}`}
          />
          <StatCard
            label="Cantonal"
            value={loading ? "—" : `${cantonalSources.filter((s) => s.count > 0).length}/${cantonalSources.length}`}
          />
        </div>

        {lastUpdated && (
          <div className="mt-3 text-xs text-faint">
            Updated {lastUpdated.toLocaleTimeString()}
            {autoRefresh && " · auto"}
          </div>
        )}

        {/* Federal */}
        <section className="mt-8">
          <h2 className="text-xs font-medium text-dim mb-3">Federal Courts</h2>
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
            {federalSources.map((source) => (
              <SourceCard key={source.id} source={source} />
            ))}
          </div>
        </section>

        {/* Cantonal */}
        <section className="mt-8">
          <h2 className="text-xs font-medium text-dim mb-3">Cantonal Courts</h2>
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
            {cantonalSources.map((source) => (
              <SourceCard key={source.id} source={source} />
            ))}
          </div>
        </section>
      </main>
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="border border-line p-4">
      <div className="text-xs text-dim">{label}</div>
      <div className="mt-1 text-2xl font-medium text-fg tabular-nums">{value}</div>
    </div>
  );
}

function SourceCard({ source }: { source: SourceStat }) {
  const hasData = source.count > 0;

  return (
    <div className="border border-line p-3">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-1.5">
            <span
              className={`h-1.5 w-1.5 rounded-full ${
                hasData ? "bg-accent" : "bg-line"
              }`}
            />
            <span className="text-xs font-medium text-fg uppercase">
              {source.id}
            </span>
          </div>
          <div className="mt-0.5 text-xs text-dim truncate" title={source.name}>
            {source.name}
          </div>
        </div>
        <span className={`text-xs tabular-nums ${hasData ? "text-fg" : "text-faint"}`}>
          {source.count.toLocaleString()}
        </span>
      </div>
      <div className="mt-2 flex items-center justify-between text-xs text-faint">
        <span>{source.canton || source.level}</span>
      </div>
    </div>
  );
}
