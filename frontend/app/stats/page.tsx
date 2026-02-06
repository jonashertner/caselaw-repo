"use client";

import { useEffect, useState, useMemo } from "react";
import Link from "next/link";

type StatsResponse = {
  total_decisions: number;
  federal_decisions: number;
  cantonal_decisions: number;
  decisions_by_canton: Record<string, number>;
  decisions_by_year: Record<string, number>;
  decisions_by_language: Record<string, number>;
  recent_decisions: { last_24h: number; last_7d: number; last_30d: number };
  coverage: { total_sources: number; indexed_sources: number; cantons_covered: number };
  sources: Array<{ id: string; name: string; level: string; canton?: string; count: number }>;
};

const API = process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/+$/, "") ?? "http://localhost:8000";

// Language configuration
type LangCode = "de" | "fr" | "it" | "rm" | "en";

const LANGUAGES: { code: LangCode; label: string }[] = [
  { code: "de", label: "DE" },
  { code: "fr", label: "FR" },
  { code: "it", label: "IT" },
  { code: "rm", label: "RM" },
  { code: "en", label: "EN" },
];

const translations: Record<LangCode, {
  title: string;
  search: string;
  sources: string;
  totalDecisions: string;
  federal: string;
  cantonal: string;
  coverage: string;
  cantons: string;
  recentActivity: string;
  hours24: string;
  days7: string;
  days30: string;
  decisionsByYear: string;
  topCantons: string;
  allCantons: string;
  allSources: string;
  byLanguage: string;
  german: string;
  french: string;
  italian: string;
  romansh: string;
  loading: string;
  federalCourts: string;
  cantonalCourts: string;
}> = {
  de: {
    title: "Statistiken",
    search: "Suche",
    sources: "Quellen",
    totalDecisions: "Entscheide Total",
    federal: "Bund",
    cantonal: "Kantone",
    coverage: "Abdeckung",
    cantons: "Kantone",
    recentActivity: "Letzte Aktivität",
    hours24: "24 Stunden",
    days7: "7 Tage",
    days30: "30 Tage",
    decisionsByYear: "Entscheide nach Jahr",
    topCantons: "Top Kantone",
    allCantons: "Alle Kantone",
    allSources: "Alle Quellen",
    byLanguage: "Nach Sprache",
    german: "Deutsch",
    french: "Französisch",
    italian: "Italienisch",
    romansh: "Rätoromanisch",
    loading: "Laden...",
    federalCourts: "Bundesgerichte",
    cantonalCourts: "Kantonsgerichte",
  },
  fr: {
    title: "Statistiques",
    search: "Recherche",
    sources: "Sources",
    totalDecisions: "Décisions Totales",
    federal: "Fédéral",
    cantonal: "Cantonal",
    coverage: "Couverture",
    cantons: "cantons",
    recentActivity: "Activité Récente",
    hours24: "24 heures",
    days7: "7 jours",
    days30: "30 jours",
    decisionsByYear: "Décisions par Année",
    topCantons: "Top Cantons",
    allCantons: "Tous les Cantons",
    allSources: "Toutes les Sources",
    byLanguage: "Par Langue",
    german: "Allemand",
    french: "Français",
    italian: "Italien",
    romansh: "Romanche",
    loading: "Chargement...",
    federalCourts: "Tribunaux fédéraux",
    cantonalCourts: "Tribunaux cantonaux",
  },
  it: {
    title: "Statistiche",
    search: "Ricerca",
    sources: "Fonti",
    totalDecisions: "Decisioni Totali",
    federal: "Federale",
    cantonal: "Cantonale",
    coverage: "Copertura",
    cantons: "cantoni",
    recentActivity: "Attività Recente",
    hours24: "24 ore",
    days7: "7 giorni",
    days30: "30 giorni",
    decisionsByYear: "Decisioni per Anno",
    topCantons: "Top Cantoni",
    allCantons: "Tutti i Cantoni",
    allSources: "Tutte le Fonti",
    byLanguage: "Per Lingua",
    german: "Tedesco",
    french: "Francese",
    italian: "Italiano",
    romansh: "Romancio",
    loading: "Caricamento...",
    federalCourts: "Tribunali federali",
    cantonalCourts: "Tribunali cantonali",
  },
  rm: {
    title: "Statisticas",
    search: "Tschertga",
    sources: "Funtaunas",
    totalDecisions: "Decisiuns Totalas",
    federal: "Federal",
    cantonal: "Chantunal",
    coverage: "Cuvretgira",
    cantons: "chantuns",
    recentActivity: "Activitad Recenta",
    hours24: "24 uras",
    days7: "7 dis",
    days30: "30 dis",
    decisionsByYear: "Decisiuns per Onn",
    topCantons: "Top Chantuns",
    allCantons: "Tuts ils Chantuns",
    allSources: "Tuttas las Funtaunas",
    byLanguage: "Per Lingua",
    german: "Tudestg",
    french: "Franzos",
    italian: "Talian",
    romansh: "Rumantsch",
    loading: "Chargiar...",
    federalCourts: "Derschaders federals",
    cantonalCourts: "Derschaders chantunals",
  },
  en: {
    title: "Statistics",
    search: "Search",
    sources: "Sources",
    totalDecisions: "Total Decisions",
    federal: "Federal",
    cantonal: "Cantonal",
    coverage: "Coverage",
    cantons: "cantons",
    recentActivity: "Recent Activity",
    hours24: "24 hours",
    days7: "7 days",
    days30: "30 days",
    decisionsByYear: "Decisions by Year",
    topCantons: "Top Cantons",
    allCantons: "All Cantons",
    allSources: "All Sources",
    byLanguage: "By Language",
    german: "German",
    french: "French",
    italian: "Italian",
    romansh: "Romansh",
    loading: "Loading...",
    federalCourts: "Federal Courts",
    cantonalCourts: "Cantonal Courts",
  },
};

// Canton full names for display
const CANTON_NAMES: Record<string, Record<LangCode, string>> = {
  ZH: { de: "Zürich", fr: "Zurich", it: "Zurigo", rm: "Turitg", en: "Zurich" },
  BE: { de: "Bern", fr: "Berne", it: "Berna", rm: "Berna", en: "Bern" },
  LU: { de: "Luzern", fr: "Lucerne", it: "Lucerna", rm: "Lucerna", en: "Lucerne" },
  UR: { de: "Uri", fr: "Uri", it: "Uri", rm: "Uri", en: "Uri" },
  SZ: { de: "Schwyz", fr: "Schwytz", it: "Svitto", rm: "Sviz", en: "Schwyz" },
  OW: { de: "Obwalden", fr: "Obwald", it: "Obvaldo", rm: "Sursilvania", en: "Obwalden" },
  NW: { de: "Nidwalden", fr: "Nidwald", it: "Nidvaldo", rm: "Sutsilvania", en: "Nidwalden" },
  GL: { de: "Glarus", fr: "Glaris", it: "Glarona", rm: "Glaruna", en: "Glarus" },
  ZG: { de: "Zug", fr: "Zoug", it: "Zugo", rm: "Zug", en: "Zug" },
  FR: { de: "Freiburg", fr: "Fribourg", it: "Friburgo", rm: "Friburg", en: "Fribourg" },
  SO: { de: "Solothurn", fr: "Soleure", it: "Soletta", rm: "Soloturn", en: "Solothurn" },
  BS: { de: "Basel-Stadt", fr: "Bâle-Ville", it: "Basilea Città", rm: "Basilea-Citad", en: "Basel-City" },
  BL: { de: "Basel-Landschaft", fr: "Bâle-Campagne", it: "Basilea Campagna", rm: "Basilea-Champagna", en: "Basel-Country" },
  SH: { de: "Schaffhausen", fr: "Schaffhouse", it: "Sciaffusa", rm: "Schaffusa", en: "Schaffhausen" },
  AR: { de: "Appenzell A.Rh.", fr: "Appenzell R.-E.", it: "Appenzello Est.", rm: "Appenzell da Dadora", en: "Appenzell A.Rh." },
  AI: { de: "Appenzell I.Rh.", fr: "Appenzell R.-I.", it: "Appenzello Int.", rm: "Appenzell da Dadens", en: "Appenzell I.Rh." },
  SG: { de: "St. Gallen", fr: "Saint-Gall", it: "San Gallo", rm: "Son Gagl", en: "St. Gallen" },
  GR: { de: "Graubünden", fr: "Grisons", it: "Grigioni", rm: "Grischun", en: "Graubünden" },
  AG: { de: "Aargau", fr: "Argovie", it: "Argovia", rm: "Argovia", en: "Aargau" },
  TG: { de: "Thurgau", fr: "Thurgovie", it: "Turgovia", rm: "Turgovia", en: "Thurgau" },
  TI: { de: "Tessin", fr: "Tessin", it: "Ticino", rm: "Tessin", en: "Ticino" },
  VD: { de: "Waadt", fr: "Vaud", it: "Vaud", rm: "Vad", en: "Vaud" },
  VS: { de: "Wallis", fr: "Valais", it: "Vallese", rm: "Vallais", en: "Valais" },
  NE: { de: "Neuenburg", fr: "Neuchâtel", it: "Neuchâtel", rm: "Neuchâtel", en: "Neuchâtel" },
  GE: { de: "Genf", fr: "Genève", it: "Ginevra", rm: "Genevra", en: "Geneva" },
  JU: { de: "Jura", fr: "Jura", it: "Giura", rm: "Giura", en: "Jura" },
};

// Mini bar chart component
function MiniBar({ value, max, color = "accent" }: { value: number; max: number; color?: string }) {
  const pct = max > 0 ? (value / max) * 100 : 0;
  return (
    <div className="h-2 bg-line/50 rounded-full overflow-hidden">
      <div
        className={`h-full rounded-full transition-all duration-500 ${
          color === "accent" ? "bg-accent" : color === "red" ? "bg-red-500" : "bg-fg/20"
        }`}
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}

// Sparkline component for year trends
function Sparkline({ data, height = 40, width = 160 }: { data: number[]; height?: number; width?: number }) {
  if (data.length < 2) return null;
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * width;
    const y = height - ((v - min) / range) * (height - 8) - 4;
    return `${x},${y}`;
  }).join(" ");

  // Area fill
  const areaPoints = `0,${height} ${points} ${width},${height}`;

  return (
    <svg width={width} height={height} className="text-accent">
      <polygon
        points={areaPoints}
        fill="currentColor"
        fillOpacity="0.1"
      />
      <polyline
        points={points}
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

// Skeleton components
function StatSkeleton() {
  return (
    <div className="animate-pulse">
      <div className="h-3 w-20 bg-line/50 rounded mb-3" />
      <div className="h-10 w-32 bg-line/50 rounded mb-2" />
      <div className="h-3 w-16 bg-line/50 rounded" />
    </div>
  );
}

function BarSkeleton() {
  return (
    <div className="animate-pulse">
      <div className="flex justify-between mb-2">
        <div className="h-4 w-16 bg-line/50 rounded" />
        <div className="h-4 w-12 bg-line/50 rounded" />
      </div>
      <div className="h-2 bg-line/50 rounded-full" />
    </div>
  );
}

function CantonSkeleton() {
  return (
    <div className="animate-pulse aspect-square bg-line/30 rounded flex items-center justify-center">
      <div className="h-4 w-8 bg-line/50 rounded" />
    </div>
  );
}

export default function StatsPage() {
  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [lang, setLang] = useState<LangCode>("de");

  // Load saved language preference
  useEffect(() => {
    const saved = localStorage.getItem("swisslaw-lang") as LangCode;
    if (saved && LANGUAGES.some(l => l.code === saved)) {
      setLang(saved);
    }
  }, []);

  const t = translations[lang];

  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const res = await fetch(`${API}/api/stats`);
        if (!res.ok) {
          throw new Error(`Failed to load stats: ${res.status}`);
        }
        const data = await res.json();
        setStats(data);
        setError(null);
      } catch (err) {
        console.error("Stats error:", err);
        setError(err instanceof Error ? err.message : "Failed to load statistics");
      } finally {
        setLoading(false);
      }
    };
    fetchStats();
  }, []);

  // Derived data
  const yearData = useMemo(() => {
    if (!stats?.decisions_by_year) return { trend: [], years: [] };
    const entries = Object.entries(stats.decisions_by_year)
      .filter(([y]) => Number(y) >= 2000)
      .sort(([a], [b]) => Number(a) - Number(b));
    return {
      trend: entries.map(([, v]) => v),
      years: entries.map(([y]) => y),
    };
  }, [stats]);

  const topCantons = useMemo(() => {
    if (!stats?.decisions_by_canton) return [];
    return Object.entries(stats.decisions_by_canton)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 10);
  }, [stats]);

  const languageData = useMemo(() => {
    if (!stats?.decisions_by_language) return [];
    const langMap: Record<string, { key: string; label: string }> = {
      de: { key: "de", label: t.german },
      fr: { key: "fr", label: t.french },
      it: { key: "it", label: t.italian },
      rm: { key: "rm", label: t.romansh },
    };
    return Object.entries(stats.decisions_by_language)
      .filter(([k]) => langMap[k])
      .map(([k, v]) => ({ ...langMap[k], count: v }))
      .sort((a, b) => b.count - a.count);
  }, [stats, t]);

  const federalSources = useMemo(() => {
    if (!stats?.sources) return [];
    return stats.sources.filter(s => s.level === "federal").sort((a, b) => b.count - a.count);
  }, [stats]);

  const cantonalSources = useMemo(() => {
    if (!stats?.sources) return [];
    return stats.sources.filter(s => s.level === "cantonal").sort((a, b) => b.count - a.count);
  }, [stats]);

  const federalPct = stats ? ((stats.federal_decisions / stats.total_decisions) * 100).toFixed(0) : "0";
  const cantonalPct = stats ? ((stats.cantonal_decisions / stats.total_decisions) * 100).toFixed(0) : "0";

  const handleLangChange = (code: LangCode) => {
    setLang(code);
    localStorage.setItem("swisslaw-lang", code);
  };

  return (
    <div className="min-h-screen bg-white">
      {/* Header */}
      <header className="border-b border-line sticky top-0 bg-white/95 backdrop-blur-sm z-10">
        <div className="max-w-5xl mx-auto px-4 sm:px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <Link href="/" className="flex items-center gap-2 group">
                <span className="text-xl" aria-hidden>⚖️</span>
                <span className="font-medium text-fg group-hover:text-accent transition-colors hidden sm:inline">
                  Swiss Caselaw
                </span>
              </Link>
              <span className="text-dim">/</span>
              <h1 className="text-lg font-medium">{t.title}</h1>
            </div>

            <div className="flex items-center gap-4">
              <nav className="hidden sm:flex gap-4 text-sm text-dim">
                <Link href="/" className="hover:text-fg transition-colors">{t.search}</Link>
                <Link href="/dashboard" className="hover:text-fg transition-colors">{t.sources}</Link>
              </nav>

              {/* Language Switcher */}
              <div className="flex border border-line rounded-lg overflow-hidden">
                {LANGUAGES.map((l) => (
                  <button
                    key={l.code}
                    onClick={() => handleLangChange(l.code)}
                    className={`px-2 py-1 text-xs font-medium transition-colors ${
                      lang === l.code
                        ? "bg-fg text-white"
                        : "text-dim hover:text-fg hover:bg-bg"
                    }`}
                  >
                    {l.label}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-5xl mx-auto px-4 sm:px-6 py-6 sm:py-10">
        {/* Error Display */}
        {error && (
          <div className="mb-6 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl text-red-700 dark:text-red-400">
            <p className="font-medium">Error loading statistics</p>
            <p className="text-sm mt-1">{error}</p>
            <button
              onClick={() => window.location.reload()}
              className="mt-2 text-sm underline hover:no-underline"
            >
              Retry
            </button>
          </div>
        )}

        {/* Hero Stats */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 sm:gap-6">
          {loading ? (
            <>
              <div className="col-span-2 lg:col-span-1"><StatSkeleton /></div>
              <StatSkeleton />
              <StatSkeleton />
              <StatSkeleton />
            </>
          ) : (
            <>
              <div className="col-span-2 lg:col-span-1">
                <div className="text-dim text-xs uppercase tracking-wide mb-2">{t.totalDecisions}</div>
                <div className="text-4xl sm:text-5xl font-light tabular-nums">
                  {stats?.total_decisions.toLocaleString()}
                </div>
                <div className="mt-4">
                  <Sparkline data={yearData.trend} />
                </div>
                <div className="text-xs text-dim mt-2">
                  {yearData.years[0]} – {yearData.years[yearData.years.length - 1]}
                </div>
              </div>

              <div>
                <div className="text-dim text-xs uppercase tracking-wide mb-2">{t.federal}</div>
                <div className="text-3xl sm:text-4xl font-light tabular-nums text-red-600">
                  {stats?.federal_decisions.toLocaleString()}
                </div>
                <div className="text-sm text-dim mt-1">{federalPct}%</div>
              </div>

              <div>
                <div className="text-dim text-xs uppercase tracking-wide mb-2">{t.cantonal}</div>
                <div className="text-3xl sm:text-4xl font-light tabular-nums text-accent">
                  {stats?.cantonal_decisions.toLocaleString()}
                </div>
                <div className="text-sm text-dim mt-1">{cantonalPct}%</div>
              </div>

              <div>
                <div className="text-dim text-xs uppercase tracking-wide mb-2">{t.coverage}</div>
                <div className="text-3xl sm:text-4xl font-light tabular-nums">
                  {stats?.coverage.indexed_sources}
                  <span className="text-lg text-dim">/{stats?.coverage.total_sources}</span>
                </div>
                <div className="text-sm text-dim mt-1">{stats?.coverage.cantons_covered} {t.cantons}</div>
              </div>
            </>
          )}
        </div>

        {/* Recent Activity */}
        <section className="mt-10 pt-8 border-t border-line">
          <div className="text-dim text-xs uppercase tracking-wide mb-4">{t.recentActivity}</div>
          <div className="grid grid-cols-3 gap-4 sm:gap-6">
            {loading ? (
              <>
                <StatSkeleton />
                <StatSkeleton />
                <StatSkeleton />
              </>
            ) : (
              [
                { label: t.hours24, value: stats?.recent_decisions.last_24h || 0 },
                { label: t.days7, value: stats?.recent_decisions.last_7d || 0 },
                { label: t.days30, value: stats?.recent_decisions.last_30d || 0 },
              ].map((item) => (
                <div key={item.label}>
                  <div className="text-2xl sm:text-3xl font-light tabular-nums text-green-600">
                    +{item.value.toLocaleString()}
                  </div>
                  <div className="text-xs text-dim mt-1">{item.label}</div>
                </div>
              ))
            )}
          </div>
        </section>

        {/* Languages */}
        <section className="mt-10 pt-8 border-t border-line">
          <div className="text-dim text-xs uppercase tracking-wide mb-4">{t.byLanguage}</div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 sm:gap-6">
            {loading ? (
              Array(4).fill(0).map((_, i) => <BarSkeleton key={i} />)
            ) : (
              languageData.map((item) => {
                const max = languageData[0]?.count || 1;
                return (
                  <div key={item.key}>
                    <div className="flex items-baseline justify-between mb-2">
                      <span className="text-sm font-medium">{item.label}</span>
                      <span className="text-sm tabular-nums text-dim">{item.count.toLocaleString()}</span>
                    </div>
                    <MiniBar value={item.count} max={max} />
                  </div>
                );
              })
            )}
          </div>
        </section>

        {/* Top Cantons */}
        <section className="mt-10 pt-8 border-t border-line">
          <div className="text-dim text-xs uppercase tracking-wide mb-4">{t.topCantons}</div>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-4">
            {loading ? (
              Array(10).fill(0).map((_, i) => <BarSkeleton key={i} />)
            ) : (
              topCantons.map(([canton, count], idx) => {
                const cantonName = CANTON_NAMES[canton]?.[lang] || canton;
                return (
                  <div key={canton} className="flex items-center gap-3">
                    <span className="text-dim text-sm w-5 tabular-nums">{idx + 1}.</span>
                    <div className="flex-1">
                      <div className="flex items-baseline justify-between mb-1.5">
                        <div className="flex items-center gap-2">
                          <span className="text-xs font-bold bg-line/50 px-1.5 py-0.5 rounded">{canton}</span>
                          <span className="text-sm text-dim hidden sm:inline">{cantonName}</span>
                        </div>
                        <span className="text-sm tabular-nums">{count.toLocaleString()}</span>
                      </div>
                      <MiniBar value={count} max={topCantons[0]?.[1] || 1} />
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </section>

        {/* All Cantons Grid */}
        <section className="mt-10 pt-8 border-t border-line">
          <div className="text-dim text-xs uppercase tracking-wide mb-4">{t.allCantons}</div>
          <div className="grid grid-cols-4 sm:grid-cols-6 lg:grid-cols-9 gap-2 sm:gap-3">
            {loading ? (
              Array(26).fill(0).map((_, i) => <CantonSkeleton key={i} />)
            ) : (
              stats?.decisions_by_canton &&
              Object.entries(stats.decisions_by_canton)
                .sort(([a], [b]) => a.localeCompare(b))
                .map(([canton, count]) => {
                  const max = Math.max(...Object.values(stats.decisions_by_canton));
                  const intensity = count / max;
                  const cantonName = CANTON_NAMES[canton]?.[lang] || canton;
                  return (
                    <Link
                      key={canton}
                      href={`/?canton=${canton}`}
                      className="aspect-square flex flex-col items-center justify-center p-2 transition-all hover:scale-105 rounded-lg group cursor-pointer"
                      style={{
                        backgroundColor: `rgba(37, 99, 235, ${0.08 + intensity * 0.3})`,
                      }}
                      title={cantonName}
                    >
                      <div className="text-sm font-bold group-hover:text-accent transition-colors">{canton}</div>
                      <div className="text-xs text-dim tabular-nums mt-0.5">
                        {count >= 1000 ? `${(count / 1000).toFixed(0)}k` : count}
                      </div>
                    </Link>
                  );
                })
            )}
          </div>
        </section>

        {/* Federal Sources */}
        <section className="mt-10 pt-8 border-t border-line">
          <div className="text-dim text-xs uppercase tracking-wide mb-4">{t.federalCourts}</div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {loading ? (
              Array(6).fill(0).map((_, i) => <BarSkeleton key={i} />)
            ) : (
              federalSources.map((s) => (
                <Link
                  key={s.id}
                  href={`/?source=${s.id}`}
                  className="flex items-center justify-between p-3 border border-line rounded-lg hover:border-red-300 hover:bg-red-50/30 transition-all group"
                >
                  <div className="flex items-center gap-3 min-w-0">
                    <span className="w-1 h-8 bg-red-500 rounded-full shrink-0" />
                    <div className="min-w-0">
                      <div className="text-sm font-medium group-hover:text-red-600 transition-colors">{s.id}</div>
                      <div className="text-xs text-dim truncate">{s.name}</div>
                    </div>
                  </div>
                  <span className="text-sm tabular-nums font-medium ml-2 shrink-0">
                    {s.count.toLocaleString()}
                  </span>
                </Link>
              ))
            )}
          </div>
        </section>

        {/* Cantonal Sources */}
        <section className="mt-10 pt-8 border-t border-line">
          <div className="text-dim text-xs uppercase tracking-wide mb-4">{t.cantonalCourts}</div>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {loading ? (
              Array(12).fill(0).map((_, i) => <BarSkeleton key={i} />)
            ) : (
              cantonalSources.slice(0, 24).map((s) => (
                <Link
                  key={s.id}
                  href={`/?source=${s.id}`}
                  className="flex items-center justify-between p-3 border border-line rounded-lg hover:border-accent/50 hover:bg-accent/5 transition-all group"
                >
                  <div className="flex items-center gap-3 min-w-0">
                    <span className="w-1 h-8 bg-accent rounded-full shrink-0" />
                    <div className="min-w-0">
                      <div className="text-sm font-medium group-hover:text-accent transition-colors">{s.id}</div>
                      <div className="text-xs text-dim truncate">{s.name}</div>
                    </div>
                  </div>
                  <span className="text-sm tabular-nums font-medium ml-2 shrink-0">
                    {s.count.toLocaleString()}
                  </span>
                </Link>
              ))
            )}
          </div>
          {cantonalSources.length > 24 && (
            <div className="mt-4 text-center">
              <Link
                href="/dashboard"
                className="text-sm text-accent hover:underline"
              >
                {t.sources} ({cantonalSources.length - 24} more) →
              </Link>
            </div>
          )}
        </section>

        {/* Year Breakdown */}
        <section className="mt-10 pt-8 border-t border-line">
          <div className="text-dim text-xs uppercase tracking-wide mb-4">{t.decisionsByYear}</div>
          <div className="grid grid-cols-3 sm:grid-cols-5 lg:grid-cols-7 gap-2 sm:gap-3">
            {loading ? (
              Array(14).fill(0).map((_, i) => (
                <div key={i} className="animate-pulse">
                  <div className="h-4 w-12 bg-line/50 rounded mb-1" />
                  <div className="h-6 w-16 bg-line/50 rounded" />
                </div>
              ))
            ) : (
              stats?.decisions_by_year &&
              Object.entries(stats.decisions_by_year)
                .filter(([y]) => Number(y) >= 2010)
                .sort(([a], [b]) => Number(b) - Number(a))
                .map(([year, count]) => {
                  const max = Math.max(
                    ...Object.entries(stats.decisions_by_year)
                      .filter(([y]) => Number(y) >= 2010)
                      .map(([, v]) => v)
                  );
                  const pct = (count / max) * 100;
                  return (
                    <Link
                      key={year}
                      href={`/?year=${year}`}
                      className="p-2 rounded-lg hover:bg-bg transition-colors group"
                    >
                      <div className="text-xs text-dim mb-1">{year}</div>
                      <div className="text-sm font-medium tabular-nums group-hover:text-accent transition-colors">
                        {count.toLocaleString()}
                      </div>
                      <div className="mt-2 h-1 bg-line/50 rounded-full overflow-hidden">
                        <div
                          className="h-full bg-accent rounded-full transition-all"
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                    </Link>
                  );
                })
            )}
          </div>
        </section>
      </main>

      {/* Mobile Navigation */}
      <nav className="sm:hidden fixed bottom-0 inset-x-0 border-t border-line bg-white/95 backdrop-blur-sm">
        <div className="flex justify-around py-3">
          <Link href="/" className="flex flex-col items-center text-dim hover:text-fg">
            <span className="text-lg">🔍</span>
            <span className="text-xs mt-1">{t.search}</span>
          </Link>
          <Link href="/stats" className="flex flex-col items-center text-accent">
            <span className="text-lg">📊</span>
            <span className="text-xs mt-1">{t.title}</span>
          </Link>
          <Link href="/dashboard" className="flex flex-col items-center text-dim hover:text-fg">
            <span className="text-lg">⚖️</span>
            <span className="text-xs mt-1">{t.sources}</span>
          </Link>
        </div>
      </nav>
    </div>
  );
}
