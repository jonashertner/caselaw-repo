"use client";

import { useState, useCallback, useRef, ReactElement } from "react";
import { useToast } from "@/hooks/useToast";

export type Decision = {
  id: string;
  source_id: string;
  source_name: string;
  level: string;
  canton?: string | null;
  court?: string | null;
  docket?: string | null;
  decision_date?: string | null;
  title?: string | null;
  language?: string | null;
  url: string;
  pdf_url?: string | null;
};

type ResultCardProps = {
  decision: Decision;
  score: number;
  snippet: string;
  query?: string;
  onClick: () => void;
  onSave?: () => void;
  isSaved?: boolean;
  translations: {
    federal: string;
    cantonal: string;
    copy: string;
    copied: string;
    source: string;
    pdf: string;
    save: string;
    saved: string;
  };
};

// Highlight matching keywords in text
function highlightKeywords(text: string, query?: string): ReactElement {
  if (!query || !query.trim()) return <>{text}</>;

  const keywords = query
    .trim()
    .split(/\s+/)
    .filter((k) => k.length > 2)
    .map((k) => k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));

  if (keywords.length === 0) return <>{text}</>;

  const pattern = new RegExp(`(${keywords.join("|")})`, "gi");
  const parts = text.split(pattern);

  return (
    <>
      {parts.map((part, i) =>
        pattern.test(part) ? (
          <mark key={i} className="bg-warning-subtle text-fg px-0.5 rounded">
            {part}
          </mark>
        ) : (
          <span key={i}>{part}</span>
        )
      )}
    </>
  );
}

// Relevance bar with gradient
function RelevanceBar({ score }: { score: number }) {
  const percentage = Math.round(score * 100);

  // Color based on score
  const getColor = () => {
    if (percentage >= 80) return "bg-success";
    if (percentage >= 60) return "bg-accent";
    if (percentage >= 40) return "bg-warning";
    return "bg-fg-faint";
  };

  return (
    <div className="flex items-center gap-2">
      <div className="w-16 h-1.5 bg-bg-muted rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all ${getColor()}`}
          style={{ width: `${percentage}%` }}
        />
      </div>
      <span className="text-xs text-fg-subtle tabular-nums">{percentage}%</span>
    </div>
  );
}

export function ResultCard({
  decision,
  score,
  snippet,
  query,
  onClick,
  onSave,
  isSaved = false,
  translations,
}: ResultCardProps) {
  const [showActions, setShowActions] = useState(false);
  const [copied, setCopied] = useState(false);
  const { success } = useToast();

  const handleCopyCitation = useCallback(
    async (e: React.MouseEvent) => {
      e.stopPropagation();
      const citation = decision.docket || decision.title || decision.source_name;
      await navigator.clipboard.writeText(citation);
      setCopied(true);
      success(`${translations.copied} ${citation}`);
      setTimeout(() => setCopied(false), 2000);
    },
    [decision, success, translations.copied]
  );

  const handleOpenSource = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      window.open(decision.url, "_blank", "noopener,noreferrer");
    },
    [decision.url]
  );

  const handleOpenPdf = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      if (decision.pdf_url) {
        window.open(decision.pdf_url, "_blank", "noopener,noreferrer");
      }
    },
    [decision.pdf_url]
  );

  const handleSave = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      onSave?.();
    },
    [onSave]
  );

  return (
    <button
      onClick={onClick}
      onMouseEnter={() => setShowActions(true)}
      onMouseLeave={() => setShowActions(false)}
      className="w-full text-left px-4 py-4 lg:px-0 hover:bg-bg-elevated transition-colors group relative"
    >
      <div className="flex items-start gap-3">
        {/* Level indicator */}
        <div
          className={`shrink-0 w-1 h-14 rounded-full transition-all ${
            decision.level === "federal"
              ? "bg-federal group-hover:bg-federal/80"
              : "bg-accent group-hover:bg-accent/80"
          }`}
        />

        <div className="flex-1 min-w-0">
          {/* Header row */}
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              {/* Title / Docket */}
              <h3 className="font-medium text-fg group-hover:text-accent transition-colors truncate">
                {decision.docket || decision.title || decision.source_name}
              </h3>

              {/* Metadata */}
              <div className="mt-1 flex flex-wrap items-center gap-2 text-sm text-fg-subtle">
                {decision.decision_date && (
                  <time className="tabular-nums">{decision.decision_date}</time>
                )}
                {decision.canton && (
                  <span className="px-1.5 py-0.5 bg-bg-muted rounded text-xs font-medium">
                    {decision.canton}
                  </span>
                )}
                {decision.level === "federal" && (
                  <span className="px-1.5 py-0.5 bg-federal-subtle text-federal rounded text-xs font-medium">
                    {translations.federal}
                  </span>
                )}
                {decision.court && (
                  <span className="truncate max-w-[200px]">{decision.court}</span>
                )}
              </div>
            </div>

            {/* Right side: Score + Language */}
            <div className="shrink-0 flex flex-col items-end gap-1">
              <RelevanceBar score={score} />
              {decision.language && (
                <span className="uppercase text-xs text-fg-faint">{decision.language}</span>
              )}
            </div>
          </div>

          {/* Snippet */}
          <p className="mt-2 text-sm text-fg-muted line-clamp-2 leading-relaxed">
            {highlightKeywords(snippet, query)}
          </p>

          {/* Quick Actions - shown on hover */}
          <div
            className={`mt-3 flex items-center gap-2 transition-all duration-200 ${
              showActions ? "opacity-100 translate-y-0" : "opacity-0 -translate-y-1 pointer-events-none"
            } lg:opacity-0 lg:group-hover:opacity-100 lg:group-hover:translate-y-0`}
          >
            <button
              onClick={handleCopyCitation}
              className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-fg-subtle bg-bg-muted hover:bg-bg-subtle rounded-lg transition-colors"
              title={translations.copy}
            >
              {copied ? (
                <svg className="w-3.5 h-3.5 text-success" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              ) : (
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
                  />
                </svg>
              )}
              {copied ? translations.copied : translations.copy}
            </button>

            <button
              onClick={handleOpenSource}
              className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-fg-subtle bg-bg-muted hover:bg-bg-subtle rounded-lg transition-colors"
              title={translations.source}
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={1.5}
                  d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
                />
              </svg>
              {translations.source}
            </button>

            {decision.pdf_url && (
              <button
                onClick={handleOpenPdf}
                className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-fg-subtle bg-bg-muted hover:bg-bg-subtle rounded-lg transition-colors"
                title={translations.pdf}
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"
                  />
                </svg>
                {translations.pdf}
              </button>
            )}

            {onSave && (
              <button
                onClick={handleSave}
                className={`flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium rounded-lg transition-colors ${
                  isSaved
                    ? "text-warning bg-warning-subtle hover:bg-warning-muted"
                    : "text-fg-subtle bg-bg-muted hover:bg-bg-subtle"
                }`}
                title={isSaved ? translations.saved : translations.save}
              >
                <svg
                  className="w-3.5 h-3.5"
                  fill={isSaved ? "currentColor" : "none"}
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={1.5}
                    d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z"
                  />
                </svg>
                {isSaved ? translations.saved : translations.save}
              </button>
            )}
          </div>
        </div>
      </div>
    </button>
  );
}

// Skeleton loader for result cards
export function ResultCardSkeleton() {
  return (
    <div className="px-4 py-4 lg:px-0 animate-pulse">
      <div className="flex items-start gap-3">
        <div className="w-1 h-14 bg-bg-muted rounded-full" />
        <div className="flex-1">
          <div className="flex items-start justify-between gap-3">
            <div className="flex-1">
              <div className="h-5 bg-bg-muted rounded w-48 mb-2" />
              <div className="flex gap-2">
                <div className="h-4 bg-bg-subtle rounded w-24" />
                <div className="h-4 bg-bg-subtle rounded w-12" />
              </div>
            </div>
            <div className="h-4 bg-bg-subtle rounded w-16" />
          </div>
          <div className="mt-3 space-y-2">
            <div className="h-3 bg-bg-subtle rounded w-full" />
            <div className="h-3 bg-bg-subtle rounded w-3/4" />
          </div>
        </div>
      </div>
    </div>
  );
}
