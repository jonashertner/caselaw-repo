"use client";

import { useState, useRef, useEffect, ReactNode } from "react";
import { extractCitations, CitationMatch, citationToSearchQuery } from "@/lib/citations";

type CitationLinkProps = {
  citation: CitationMatch;
  onClick: (query: string) => void;
  onHover?: (query: string) => void;
  children: ReactNode;
};

function CitationLink({ citation, onClick, onHover, children }: CitationLinkProps) {
  const [showPreview, setShowPreview] = useState(false);
  const [previewPosition, setPreviewPosition] = useState({ top: 0, left: 0 });
  const linkRef = useRef<HTMLButtonElement>(null);
  const timeoutRef = useRef<NodeJS.Timeout | undefined>(undefined);

  const handleMouseEnter = () => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);

    timeoutRef.current = setTimeout(() => {
      if (linkRef.current) {
        const rect = linkRef.current.getBoundingClientRect();
        setPreviewPosition({
          top: rect.bottom + 8,
          left: Math.max(16, Math.min(rect.left, window.innerWidth - 300)),
        });
        setShowPreview(true);
        onHover?.(citationToSearchQuery(citation));
      }
    }, 500);
  };

  const handleMouseLeave = () => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    setShowPreview(false);
  };

  const handleClick = () => {
    onClick(citationToSearchQuery(citation));
  };

  useEffect(() => {
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, []);

  return (
    <>
      <button
        ref={linkRef}
        onClick={handleClick}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        className="inline text-accent hover:text-accent-hover underline underline-offset-2 decoration-accent/40 hover:decoration-accent transition-colors cursor-pointer"
      >
        {children}
      </button>

      {/* Preview tooltip */}
      {showPreview && (
        <div
          className="fixed z-popover bg-bg-elevated border border-border rounded-lg shadow-lg p-3 max-w-xs animate-fade-in pointer-events-none"
          style={{ top: previewPosition.top, left: previewPosition.left }}
        >
          <div className="flex items-center gap-2 text-sm">
            <svg className="w-4 h-4 text-accent shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            <span className="font-medium text-fg">{citation.text}</span>
          </div>
          <p className="mt-1 text-xs text-fg-subtle">Click to search for this citation</p>
          {citation.type === "bge" && (
            <p className="mt-1 text-xs text-fg-faint">Federal Supreme Court decision</p>
          )}
          {citation.type === "docket" && (
            <p className="mt-1 text-xs text-fg-faint">Federal court case number</p>
          )}
          {citation.type === "article" && (
            <p className="mt-1 text-xs text-fg-faint">Legal article reference</p>
          )}
        </div>
      )}
    </>
  );
}

type LinkedTextProps = {
  text: string;
  onCitationClick: (query: string) => void;
  onCitationHover?: (query: string) => void;
};

// Component that renders text with clickable citation links
export function LinkedText({ text, onCitationClick, onCitationHover }: LinkedTextProps) {
  const citations = extractCitations(text);

  if (citations.length === 0) {
    return <>{text}</>;
  }

  const parts: ReactNode[] = [];
  let lastIndex = 0;

  citations.forEach((citation, i) => {
    // Add text before citation
    if (citation.start > lastIndex) {
      parts.push(
        <span key={`text-${i}`}>{text.slice(lastIndex, citation.start)}</span>
      );
    }

    // Add citation link
    parts.push(
      <CitationLink
        key={`citation-${i}`}
        citation={citation}
        onClick={onCitationClick}
        onHover={onCitationHover}
      >
        {citation.text}
      </CitationLink>
    );

    lastIndex = citation.end;
  });

  // Add remaining text
  if (lastIndex < text.length) {
    parts.push(<span key="text-end">{text.slice(lastIndex)}</span>);
  }

  return <>{parts}</>;
}

// Hook for using citation links in a context
export function useCitationLinks() {
  const handleCitationClick = (query: string) => {
    // This could navigate to search or open a preview
    console.log("Citation clicked:", query);
  };

  const handleCitationHover = (query: string) => {
    // This could prefetch the citation data
    console.log("Citation hovered:", query);
  };

  return {
    onCitationClick: handleCitationClick,
    onCitationHover: handleCitationHover,
  };
}

export { extractCitations, type CitationMatch };
