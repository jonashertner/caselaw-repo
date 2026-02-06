"use client";

import { useState, useEffect, useCallback, useRef, useMemo, ReactElement } from "react";
import { useKeyboardShortcut, useEscapeKey, formatShortcut } from "@/hooks/useKeyboardShortcuts";

export type CommandItem = {
  id: string;
  label: string;
  description?: string;
  icon?: ReactElement;
  shortcut?: { key: string; meta?: boolean; ctrl?: boolean; alt?: boolean; shift?: boolean };
  category: "recent" | "navigation" | "actions" | "saved" | "search";
  onSelect: () => void;
  keywords?: string[];
};

type CommandPaletteProps = {
  isOpen: boolean;
  onClose: () => void;
  commands: CommandItem[];
  recentSearches?: string[];
  onSearch?: (query: string) => void;
  placeholder?: string;
};

function fuzzyMatch(text: string, query: string): boolean {
  const textLower = text.toLowerCase();
  const queryLower = query.toLowerCase();

  let textIndex = 0;
  for (let queryIndex = 0; queryIndex < queryLower.length; queryIndex++) {
    const char = queryLower[queryIndex];
    const foundIndex = textLower.indexOf(char, textIndex);
    if (foundIndex === -1) return false;
    textIndex = foundIndex + 1;
  }
  return true;
}

function highlightMatch(text: string, query: string): ReactElement {
  if (!query) return <>{text}</>;

  const textLower = text.toLowerCase();
  const queryLower = query.toLowerCase();
  const parts: ReactElement[] = [];
  let lastIndex = 0;
  let textIndex = 0;

  for (const char of queryLower) {
    const foundIndex = textLower.indexOf(char, textIndex);
    if (foundIndex === -1) break;

    if (foundIndex > lastIndex) {
      parts.push(<span key={`text-${lastIndex}`}>{text.slice(lastIndex, foundIndex)}</span>);
    }
    parts.push(
      <span key={`match-${foundIndex}`} className="text-accent font-semibold">
        {text[foundIndex]}
      </span>
    );
    lastIndex = foundIndex + 1;
    textIndex = foundIndex + 1;
  }

  if (lastIndex < text.length) {
    parts.push(<span key={`text-${lastIndex}`}>{text.slice(lastIndex)}</span>);
  }

  return <>{parts}</>;
}

const categoryLabels: Record<string, string> = {
  recent: "Recent Searches",
  navigation: "Navigation",
  actions: "Actions",
  saved: "Saved Searches",
  search: "Search",
};

const categoryIcons: Record<string, ReactElement> = {
  recent: (
    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  ),
  navigation: (
    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
    </svg>
  ),
  actions: (
    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M13 10V3L4 14h7v7l9-11h-7z" />
    </svg>
  ),
  saved: (
    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" />
    </svg>
  ),
  search: (
    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
    </svg>
  ),
};

export function CommandPalette({
  isOpen,
  onClose,
  commands,
  recentSearches = [],
  onSearch,
  placeholder = "Search commands...",
}: CommandPaletteProps) {
  const [query, setQuery] = useState("");
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // Filter commands based on query
  const filteredCommands = useMemo(() => {
    if (!query.trim()) {
      // Show all commands grouped by category
      return commands;
    }

    return commands.filter((cmd) => {
      if (fuzzyMatch(cmd.label, query)) return true;
      if (cmd.description && fuzzyMatch(cmd.description, query)) return true;
      if (cmd.keywords?.some((kw) => fuzzyMatch(kw, query))) return true;
      return false;
    });
  }, [commands, query]);

  // Group commands by category
  const groupedCommands = useMemo(() => {
    const groups: Record<string, CommandItem[]> = {};
    for (const cmd of filteredCommands) {
      if (!groups[cmd.category]) {
        groups[cmd.category] = [];
      }
      groups[cmd.category].push(cmd);
    }
    return groups;
  }, [filteredCommands]);

  // Flat list for navigation
  const flatCommands = useMemo(() => {
    const categories = ["recent", "navigation", "actions", "saved", "search"];
    const flat: CommandItem[] = [];
    for (const cat of categories) {
      if (groupedCommands[cat]) {
        flat.push(...groupedCommands[cat]);
      }
    }
    return flat;
  }, [groupedCommands]);

  // Reset state when opened
  useEffect(() => {
    if (isOpen) {
      setQuery("");
      setSelectedIndex(0);
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [isOpen]);

  // Keyboard navigation
  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          setSelectedIndex((i) => Math.min(i + 1, flatCommands.length - 1));
          break;
        case "ArrowUp":
          e.preventDefault();
          setSelectedIndex((i) => Math.max(i - 1, 0));
          break;
        case "Enter":
          e.preventDefault();
          if (flatCommands[selectedIndex]) {
            flatCommands[selectedIndex].onSelect();
            onClose();
          } else if (query.trim() && onSearch) {
            onSearch(query.trim());
            onClose();
          }
          break;
        case "Escape":
          e.preventDefault();
          onClose();
          break;
      }
    },
    [flatCommands, selectedIndex, query, onSearch, onClose]
  );

  // Scroll selected item into view
  useEffect(() => {
    const selectedElement = listRef.current?.querySelector(`[data-index="${selectedIndex}"]`);
    if (selectedElement) {
      selectedElement.scrollIntoView({ block: "nearest" });
    }
  }, [selectedIndex]);

  // Close on escape
  useEscapeKey(onClose, isOpen);

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-command-palette flex items-start justify-center pt-[15vh]"
      onClick={onClose}
    >
      {/* Backdrop */}
      <div className="absolute inset-0 bg-overlay overlay-animate" />

      {/* Dialog */}
      <div
        className="relative w-full max-w-xl bg-bg-elevated rounded-2xl shadow-xl border border-border overflow-hidden animate-scale-in"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Search Input */}
        <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
          <svg className="w-5 h-5 text-fg-subtle shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setSelectedIndex(0);
            }}
            onKeyDown={handleKeyDown}
            placeholder={placeholder}
            className="flex-1 bg-transparent border-0 p-0 text-base text-fg placeholder:text-fg-faint focus:outline-none focus:ring-0"
          />
          <kbd className="hidden sm:flex items-center gap-0.5 px-1.5 py-0.5 text-xs font-medium text-fg-subtle bg-bg-muted rounded border border-border">
            Esc
          </kbd>
        </div>

        {/* Results */}
        <div ref={listRef} className="max-h-[50vh] overflow-auto py-2">
          {flatCommands.length === 0 && query.trim() ? (
            <div className="px-4 py-8 text-center text-fg-subtle">
              <p className="text-sm">No results found for "{query}"</p>
              {onSearch && (
                <button
                  onClick={() => {
                    onSearch(query.trim());
                    onClose();
                  }}
                  className="mt-2 text-sm text-accent hover:underline"
                >
                  Search for "{query}"
                </button>
              )}
            </div>
          ) : (
            Object.entries(groupedCommands).map(([category, items]) => (
              <div key={category}>
                <div className="px-4 py-2 flex items-center gap-2 text-xs font-medium text-fg-subtle uppercase tracking-wide">
                  {categoryIcons[category]}
                  {categoryLabels[category] || category}
                </div>
                {items.map((cmd) => {
                  const globalIndex = flatCommands.indexOf(cmd);
                  const isSelected = globalIndex === selectedIndex;

                  return (
                    <button
                      key={cmd.id}
                      data-index={globalIndex}
                      onClick={() => {
                        cmd.onSelect();
                        onClose();
                      }}
                      onMouseEnter={() => setSelectedIndex(globalIndex)}
                      className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors ${
                        isSelected ? "bg-accent-subtle" : "hover:bg-bg-subtle"
                      }`}
                    >
                      {cmd.icon && (
                        <span className={`shrink-0 ${isSelected ? "text-accent" : "text-fg-subtle"}`}>
                          {cmd.icon}
                        </span>
                      )}
                      <div className="flex-1 min-w-0">
                        <div className={`text-sm font-medium ${isSelected ? "text-accent" : "text-fg"}`}>
                          {highlightMatch(cmd.label, query)}
                        </div>
                        {cmd.description && (
                          <div className="text-xs text-fg-subtle truncate">{cmd.description}</div>
                        )}
                      </div>
                      {cmd.shortcut && (
                        <kbd className="shrink-0 px-1.5 py-0.5 text-xs font-medium text-fg-subtle bg-bg-muted rounded border border-border">
                          {formatShortcut(cmd.shortcut)}
                        </kbd>
                      )}
                    </button>
                  );
                })}
              </div>
            ))
          )}
        </div>

        {/* Footer */}
        <div className="px-4 py-2 border-t border-border flex items-center gap-4 text-xs text-fg-subtle">
          <span className="flex items-center gap-1">
            <kbd className="px-1 py-0.5 bg-bg-muted rounded border border-border">↑↓</kbd>
            navigate
          </span>
          <span className="flex items-center gap-1">
            <kbd className="px-1 py-0.5 bg-bg-muted rounded border border-border">↵</kbd>
            select
          </span>
          <span className="flex items-center gap-1">
            <kbd className="px-1 py-0.5 bg-bg-muted rounded border border-border">esc</kbd>
            close
          </span>
        </div>
      </div>
    </div>
  );
}

// Hook to manage command palette state
export function useCommandPaletteState() {
  const [isOpen, setIsOpen] = useState(false);

  const open = useCallback(() => setIsOpen(true), []);
  const close = useCallback(() => setIsOpen(false), []);
  const toggle = useCallback(() => setIsOpen((prev) => !prev), []);

  // Global Cmd/Ctrl+K shortcut
  useKeyboardShortcut({
    key: "k",
    meta: true,
    handler: toggle,
    description: "Open command palette",
  });

  return { isOpen, open, close, toggle };
}
