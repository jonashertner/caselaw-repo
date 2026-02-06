"use client";

import { useState, useRef, useEffect, useCallback, KeyboardEvent } from "react";
import { useRecentSearches } from "@/hooks/useLocalStorage";

export type SearchSuggestion = {
  type: "recent" | "citation" | "suggestion";
  text: string;
  highlight?: string;
};

type SearchBarProps = {
  value: string;
  onChange: (value: string) => void;
  onSearch: (query: string) => void;
  placeholder?: string;
  loading?: boolean;
  suggestions?: SearchSuggestion[];
  disabled?: boolean;
  autoFocus?: boolean;
};

const POPULAR_CITATIONS = [
  "BGE 144 III 93",
  "BGE 143 II 202",
  "BGE 142 III 364",
  "ATF 144 III 93",
  "ATF 143 II 202",
];

export function SearchBar({
  value,
  onChange,
  onSearch,
  placeholder = "Search court decisions...",
  loading = false,
  suggestions: externalSuggestions,
  disabled = false,
  autoFocus = false,
}: SearchBarProps) {
  const [isFocused, setIsFocused] = useState(false);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const { searches: recentSearches, addSearch, removeSearch } = useRecentSearches();

  // Combine suggestions
  const suggestions: SearchSuggestion[] = (() => {
    if (externalSuggestions && externalSuggestions.length > 0) {
      return externalSuggestions;
    }

    const query = value.trim().toLowerCase();
    const result: SearchSuggestion[] = [];

    // Recent searches matching query
    const matchingRecent = recentSearches
      .filter((s) => !query || s.toLowerCase().includes(query))
      .slice(0, 5)
      .map((text) => ({ type: "recent" as const, text }));

    result.push(...matchingRecent);

    // Popular citations matching query
    if (query.length > 0) {
      const matchingCitations = POPULAR_CITATIONS.filter((c) =>
        c.toLowerCase().includes(query)
      )
        .slice(0, 3)
        .map((text) => ({ type: "citation" as const, text }));
      result.push(...matchingCitations);
    }

    return result.slice(0, 8);
  })();

  // Handle click outside to close suggestions
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  // Reset selected index when suggestions change
  useEffect(() => {
    setSelectedIndex(-1);
  }, [suggestions.length, value]);

  const handleFocus = useCallback(() => {
    setIsFocused(true);
    setShowSuggestions(true);
  }, []);

  const handleBlur = useCallback(() => {
    setIsFocused(false);
    // Delay hiding suggestions to allow click events
    setTimeout(() => setShowSuggestions(false), 150);
  }, []);

  const handleSearch = useCallback(
    (query: string) => {
      const trimmed = query.trim();
      if (!trimmed) return;

      addSearch(trimmed);
      onSearch(trimmed);
      setShowSuggestions(false);
      inputRef.current?.blur();
    },
    [addSearch, onSearch]
  );

  const handleSuggestionClick = useCallback(
    (suggestion: SearchSuggestion) => {
      onChange(suggestion.text);
      handleSearch(suggestion.text);
    },
    [onChange, handleSearch]
  );

  const handleKeyDown = useCallback(
    (e: KeyboardEvent<HTMLInputElement>) => {
      if (!showSuggestions || suggestions.length === 0) {
        if (e.key === "Enter") {
          e.preventDefault();
          handleSearch(value);
        }
        return;
      }

      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          setSelectedIndex((i) => (i < suggestions.length - 1 ? i + 1 : 0));
          break;
        case "ArrowUp":
          e.preventDefault();
          setSelectedIndex((i) => (i > 0 ? i - 1 : suggestions.length - 1));
          break;
        case "Enter":
          e.preventDefault();
          if (selectedIndex >= 0 && suggestions[selectedIndex]) {
            handleSuggestionClick(suggestions[selectedIndex]);
          } else {
            handleSearch(value);
          }
          break;
        case "Escape":
          e.preventDefault();
          setShowSuggestions(false);
          setSelectedIndex(-1);
          break;
        case "Tab":
          if (selectedIndex >= 0 && suggestions[selectedIndex]) {
            e.preventDefault();
            onChange(suggestions[selectedIndex].text);
            setSelectedIndex(-1);
          }
          break;
      }
    },
    [showSuggestions, suggestions, selectedIndex, value, handleSearch, handleSuggestionClick, onChange]
  );

  const handleRemoveRecent = useCallback(
    (e: React.MouseEvent, text: string) => {
      e.stopPropagation();
      removeSearch(text);
    },
    [removeSearch]
  );

  const suggestionIcons = {
    recent: (
      <svg className="w-4 h-4 text-fg-subtle" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
    ),
    citation: (
      <svg className="w-4 h-4 text-fg-subtle" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
    ),
    suggestion: (
      <svg className="w-4 h-4 text-fg-subtle" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
      </svg>
    ),
  };

  return (
    <div ref={containerRef} className="relative flex-1">
      {/* Input */}
      <div
        className={`flex items-center gap-2 px-4 py-3 rounded-xl border bg-bg-elevated transition-all ${
          isFocused
            ? "border-accent shadow-md ring-2 ring-accent/20"
            : "border-border hover:border-border-strong"
        }`}
      >
        {loading ? (
          <span className="w-5 h-5 border-2 border-fg-faint border-t-accent rounded-full animate-spin shrink-0" />
        ) : (
          <svg className="w-5 h-5 text-fg-subtle shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        )}
        <input
          ref={inputRef}
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onFocus={handleFocus}
          onBlur={handleBlur}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          disabled={disabled}
          autoFocus={autoFocus}
          className="flex-1 bg-transparent border-0 p-0 text-base text-fg placeholder:text-fg-faint focus:outline-none focus:ring-0 disabled:opacity-50"
        />
        {value && (
          <button
            onClick={() => onChange("")}
            className="p-1 rounded hover:bg-bg-muted text-fg-subtle hover:text-fg transition-colors"
            type="button"
            tabIndex={-1}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}
        {/* Keyboard shortcut hint */}
        <kbd className="hidden lg:flex items-center gap-0.5 px-1.5 py-0.5 text-xs font-medium text-fg-faint bg-bg-subtle rounded border border-border">
          /
        </kbd>
      </div>

      {/* Suggestions Dropdown */}
      {showSuggestions && suggestions.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-2 py-2 bg-bg-elevated rounded-xl border border-border shadow-lg z-dropdown animate-slide-up">
          {suggestions.map((suggestion, index) => (
            <button
              key={`${suggestion.type}-${suggestion.text}`}
              onClick={() => handleSuggestionClick(suggestion)}
              onMouseEnter={() => setSelectedIndex(index)}
              className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors ${
                index === selectedIndex ? "bg-accent-subtle" : "hover:bg-bg-subtle"
              }`}
            >
              {suggestionIcons[suggestion.type]}
              <span
                className={`flex-1 text-sm ${
                  index === selectedIndex ? "text-accent font-medium" : "text-fg"
                }`}
              >
                {suggestion.text}
              </span>
              {suggestion.type === "recent" && (
                <button
                  onClick={(e) => handleRemoveRecent(e, suggestion.text)}
                  className="p-1 rounded hover:bg-bg-muted text-fg-faint hover:text-fg transition-colors"
                  title="Remove from recent"
                >
                  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              )}
              {suggestion.type === "citation" && (
                <span className="text-xs text-fg-faint">Citation</span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// Compact search button for mobile
export function SearchButton({
  onClick,
  loading = false,
}: {
  onClick: () => void;
  loading?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      disabled={loading}
      className="px-6 py-3 rounded-xl bg-fg text-bg font-medium hover:bg-fg/90 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
    >
      {loading ? (
        <span className="w-5 h-5 border-2 border-bg/30 border-t-bg rounded-full animate-spin inline-block" />
      ) : (
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
      )}
    </button>
  );
}
