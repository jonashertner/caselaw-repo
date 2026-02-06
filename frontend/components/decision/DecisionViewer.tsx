"use client";

import { useState, useEffect, useRef, useMemo, useCallback } from "react";

export type DecisionData = {
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
  content_text?: string | null;
  error?: string;
};

type TocItem = {
  id: string;
  level: number;
  title: string;
  position: number;
};

type DecisionViewerProps = {
  decision: DecisionData;
  onClose: () => void;
  onCitationClick?: (citation: string) => void;
  translations: {
    federal: string;
    cantonal: string;
    source: string;
    pdf: string;
    copy: string;
    copied: string;
    close: string;
    tableOfContents: string;
    loading: string;
    error: string;
  };
};

// Parse content for section headers
function parseTableOfContents(text: string): TocItem[] {
  const items: TocItem[] = [];
  const lines = text.split("\n");

  // Patterns for section detection
  const patterns = [
    // German
    { regex: /^(Sachverhalt|Fakten|Tatbestand)\s*:?$/i, level: 1 },
    { regex: /^(Erwägungen?|Gründe)\s*:?$/i, level: 1 },
    { regex: /^(Entscheid|Urteil|Dispositiv)\s*:?$/i, level: 1 },
    // French
    { regex: /^(Faits|En fait)\s*:?$/i, level: 1 },
    { regex: /^(Considérants?|En droit)\s*:?$/i, level: 1 },
    { regex: /^(Dispositif|Par ces motifs)\s*:?$/i, level: 1 },
    // Italian
    { regex: /^(Fatti|In fatto)\s*:?$/i, level: 1 },
    { regex: /^(Considerandi?|In diritto)\s*:?$/i, level: 1 },
    { regex: /^(Dispositivo|Per questi motivi)\s*:?$/i, level: 1 },
    // Roman numerals (I., II., III., etc.)
    { regex: /^([IVXLCDM]+)\.\s*$/i, level: 1 },
    // Numbered sections (1., 2., 3., etc. at start of line)
    { regex: /^(\d+)\.\s+[A-Z]/i, level: 2 },
    // Letter sections (A., B., C., etc.)
    { regex: /^([A-Z])\.\s+/i, level: 2 },
  ];

  let position = 0;
  let id = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) {
      position += lines[i].length + 1;
      continue;
    }

    for (const pattern of patterns) {
      const match = line.match(pattern.regex);
      if (match) {
        items.push({
          id: `toc-${id++}`,
          level: pattern.level,
          title: line.replace(/:$/, ""),
          position,
        });
        break;
      }
    }

    position += lines[i].length + 1;
  }

  return items;
}

// Reading progress hook
function useReadingProgress(contentRef: React.RefObject<HTMLDivElement | null>) {
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    const handleScroll = () => {
      if (!contentRef.current) return;

      const { scrollTop, scrollHeight, clientHeight } = contentRef.current;
      const scrollableHeight = scrollHeight - clientHeight;
      if (scrollableHeight > 0) {
        const currentProgress = (scrollTop / scrollableHeight) * 100;
        setProgress(Math.min(Math.max(currentProgress, 0), 100));
      }
    };

    const element = contentRef.current;
    if (element) {
      element.addEventListener("scroll", handleScroll);
      return () => element.removeEventListener("scroll", handleScroll);
    }
  }, [contentRef]);

  return progress;
}

export function DecisionViewer({
  decision,
  onClose,
  onCitationClick,
  translations,
}: DecisionViewerProps) {
  const [copied, setCopied] = useState(false);
  const [showToc, setShowToc] = useState(true);
  const [activeTocItem, setActiveTocItem] = useState<string | null>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const progress = useReadingProgress(contentRef);

  // Parse table of contents
  const tocItems = useMemo(() => {
    if (!decision.content_text) return [];
    return parseTableOfContents(decision.content_text);
  }, [decision.content_text]);

  // Copy content to clipboard
  const handleCopy = useCallback(async () => {
    if (!decision.content_text) return;
    await navigator.clipboard.writeText(decision.content_text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [decision.content_text]);

  // Scroll to TOC item
  const scrollToSection = useCallback((position: number) => {
    if (!contentRef.current || !decision.content_text) return;

    // Find the line index for this position
    const textBefore = decision.content_text.substring(0, position);
    const lineNumber = textBefore.split("\n").length - 1;

    // Find the corresponding element
    const lines = contentRef.current.querySelectorAll(".decision-line");
    const targetLine = lines[lineNumber];

    if (targetLine) {
      targetLine.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }, [decision.content_text]);

  // Track active TOC item based on scroll position
  useEffect(() => {
    if (!contentRef.current || tocItems.length === 0) return;

    const handleScroll = () => {
      const scrollTop = contentRef.current?.scrollTop || 0;
      const lineHeight = 24; // Approximate line height
      const currentPosition = scrollTop / lineHeight * 80; // Rough character position

      let activeItem = tocItems[0]?.id;
      for (const item of tocItems) {
        if (item.position <= currentPosition) {
          activeItem = item.id;
        } else {
          break;
        }
      }
      setActiveTocItem(activeItem);
    };

    const element = contentRef.current;
    element.addEventListener("scroll", handleScroll);
    handleScroll(); // Initial check

    return () => element.removeEventListener("scroll", handleScroll);
  }, [tocItems]);

  // Render content with line-by-line structure for TOC navigation
  const renderedContent = useMemo(() => {
    if (!decision.content_text) return null;

    const lines = decision.content_text.split("\n");
    return lines.map((line, i) => (
      <div key={i} className="decision-line whitespace-pre-wrap">
        {line || "\u00A0"}
      </div>
    ));
  }, [decision.content_text]);

  return (
    <div className="fixed inset-0 z-modal bg-overlay lg:p-8 overflow-hidden" onClick={onClose}>
      <div
        className="h-full lg:h-auto lg:max-h-full lg:mx-auto lg:max-w-5xl bg-bg-elevated lg:rounded-2xl flex flex-col animate-scale-in"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Reading Progress Bar */}
        <div
          className="h-1 bg-accent transition-all duration-100 ease-linear rounded-t-2xl"
          style={{ width: `${progress}%` }}
        />

        {/* Safe area for mobile */}
        <div className="safe-area-top lg:hidden" />

        {/* Header */}
        <header className="sticky top-0 bg-bg-elevated border-b border-border px-4 py-4 flex items-start justify-between gap-4 z-10 lg:rounded-t-2xl">
          <div className="min-w-0 flex-1">
            {/* Badges */}
            <div className="flex items-center gap-2 mb-1">
              {decision.level === "federal" && (
                <span className="px-2 py-0.5 bg-federal-subtle text-federal rounded text-xs font-medium">
                  {translations.federal}
                </span>
              )}
              {decision.canton && (
                <span className="px-2 py-0.5 bg-bg-muted rounded text-xs font-medium">
                  {decision.canton}
                </span>
              )}
            </div>

            {/* Title */}
            <h2 className="text-lg font-semibold text-fg">
              {decision.docket || decision.title || decision.source_name || "Decision"}
            </h2>

            {/* Metadata */}
            <div className="mt-1.5 flex flex-wrap items-center gap-3 text-sm text-fg-subtle">
              {decision.decision_date && <span>{decision.decision_date}</span>}
              {decision.court && <span>{decision.court}</span>}
              {decision.language && <span className="uppercase">{decision.language}</span>}
            </div>

            {/* Actions */}
            <div className="mt-3 flex flex-wrap gap-3">
              {decision.url && (
                <a
                  href={decision.url}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1.5 text-sm text-fg-subtle hover:text-fg transition-colors"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                  </svg>
                  {translations.source}
                </a>
              )}
              {decision.pdf_url && (
                <a
                  href={decision.pdf_url}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1.5 text-sm text-fg-subtle hover:text-fg transition-colors"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z" />
                  </svg>
                  {translations.pdf}
                </a>
              )}
              {decision.content_text && (
                <button
                  onClick={handleCopy}
                  className="inline-flex items-center gap-1.5 text-sm text-fg-subtle hover:text-fg transition-colors"
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                  </svg>
                  {copied ? translations.copied : translations.copy}
                </button>
              )}
              {tocItems.length > 0 && (
                <button
                  onClick={() => setShowToc(!showToc)}
                  className={`lg:hidden inline-flex items-center gap-1.5 text-sm transition-colors ${
                    showToc ? "text-accent" : "text-fg-subtle hover:text-fg"
                  }`}
                >
                  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 6h16M4 10h16M4 14h16M4 18h16" />
                  </svg>
                  TOC
                </button>
              )}
            </div>
          </div>

          {/* Close button */}
          <button
            onClick={onClose}
            className="shrink-0 w-10 h-10 flex items-center justify-center rounded-full bg-bg-muted text-fg-subtle hover:bg-bg-subtle hover:text-fg transition-colors"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </header>

        {/* Content area with TOC sidebar */}
        <div className="flex-1 flex overflow-hidden">
          {/* Table of Contents Sidebar */}
          {tocItems.length > 0 && showToc && (
            <aside className="hidden lg:block w-64 border-r border-border overflow-auto p-4">
              <h3 className="text-xs font-medium text-fg-subtle uppercase tracking-wide mb-3">
                {translations.tableOfContents}
              </h3>
              <nav className="space-y-1">
                {tocItems.map((item) => (
                  <button
                    key={item.id}
                    onClick={() => scrollToSection(item.position)}
                    className={`w-full text-left px-2 py-1.5 rounded text-sm transition-colors ${
                      activeTocItem === item.id
                        ? "bg-accent-subtle text-accent font-medium"
                        : "text-fg-muted hover:text-fg hover:bg-bg-subtle"
                    }`}
                    style={{ paddingLeft: `${(item.level - 1) * 12 + 8}px` }}
                  >
                    {item.title}
                  </button>
                ))}
              </nav>
            </aside>
          )}

          {/* Mobile TOC dropdown */}
          {tocItems.length > 0 && showToc && (
            <div className="lg:hidden absolute top-full left-0 right-0 bg-bg-elevated border-b border-border shadow-lg z-20 max-h-64 overflow-auto">
              <nav className="p-2">
                {tocItems.map((item) => (
                  <button
                    key={item.id}
                    onClick={() => {
                      scrollToSection(item.position);
                      setShowToc(false);
                    }}
                    className={`w-full text-left px-3 py-2 rounded text-sm transition-colors ${
                      activeTocItem === item.id
                        ? "bg-accent-subtle text-accent font-medium"
                        : "text-fg-muted hover:text-fg hover:bg-bg-subtle"
                    }`}
                    style={{ paddingLeft: `${(item.level - 1) * 12 + 12}px` }}
                  >
                    {item.title}
                  </button>
                ))}
              </nav>
            </div>
          )}

          {/* Main content */}
          <div ref={contentRef} className="flex-1 overflow-auto p-4 lg:p-6">
            {decision.error ? (
              <p className="text-sm text-fg-subtle text-center py-12">{translations.error}</p>
            ) : decision.content_text ? (
              <div className="prose prose-neutral prose-sm max-w-none">
                <div className="text-sm leading-relaxed text-fg-muted font-sans">
                  {renderedContent}
                </div>
              </div>
            ) : (
              <div className="flex items-center justify-center py-12">
                <span className="w-6 h-6 border-2 border-border border-t-accent rounded-full animate-spin" />
              </div>
            )}
          </div>
        </div>

        {/* Safe area for mobile */}
        <div className="safe-area-bottom lg:hidden" />
      </div>
    </div>
  );
}
