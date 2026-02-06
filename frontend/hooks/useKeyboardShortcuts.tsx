"use client";

import { useEffect, useCallback, useRef } from "react";

export type KeyboardShortcut = {
  key: string;
  ctrl?: boolean;
  meta?: boolean;
  alt?: boolean;
  shift?: boolean;
  handler: (e: KeyboardEvent) => void;
  description?: string;
  enabled?: boolean;
};

type ShortcutOptions = {
  enabled?: boolean;
  preventDefault?: boolean;
  stopPropagation?: boolean;
  ignoreInputs?: boolean;
};

const defaultOptions: ShortcutOptions = {
  enabled: true,
  preventDefault: true,
  stopPropagation: false,
  ignoreInputs: true,
};

function isInputElement(element: EventTarget | null): boolean {
  if (!element || !(element instanceof HTMLElement)) return false;
  const tagName = element.tagName.toLowerCase();
  return (
    tagName === "input" ||
    tagName === "textarea" ||
    tagName === "select" ||
    element.isContentEditable
  );
}

function matchesShortcut(e: KeyboardEvent, shortcut: KeyboardShortcut): boolean {
  const key = shortcut.key.toLowerCase();
  const eventKey = e.key.toLowerCase();

  // Handle special keys
  if (key === "escape" && eventKey === "escape") {
    return true;
  }

  if (eventKey !== key) return false;

  const metaOrCtrl = shortcut.meta || shortcut.ctrl;
  const eventMetaOrCtrl = e.metaKey || e.ctrlKey;

  if (metaOrCtrl && !eventMetaOrCtrl) return false;
  if (!metaOrCtrl && eventMetaOrCtrl) return false;

  if (shortcut.alt && !e.altKey) return false;
  if (!shortcut.alt && e.altKey) return false;

  if (shortcut.shift && !e.shiftKey) return false;
  if (!shortcut.shift && e.shiftKey) return false;

  return true;
}

export function useKeyboardShortcut(
  shortcut: KeyboardShortcut,
  options: ShortcutOptions = {}
) {
  const opts = { ...defaultOptions, ...options };
  const handlerRef = useRef(shortcut.handler);

  // Keep handler ref updated
  useEffect(() => {
    handlerRef.current = shortcut.handler;
  }, [shortcut.handler]);

  useEffect(() => {
    if (!opts.enabled || shortcut.enabled === false) return;

    const handleKeyDown = (e: KeyboardEvent) => {
      // Skip if in input field and ignoreInputs is true
      if (opts.ignoreInputs && isInputElement(e.target)) {
        // Allow Escape to work even in inputs
        if (shortcut.key.toLowerCase() !== "escape") {
          return;
        }
      }

      if (matchesShortcut(e, shortcut)) {
        if (opts.preventDefault) {
          e.preventDefault();
        }
        if (opts.stopPropagation) {
          e.stopPropagation();
        }
        handlerRef.current(e);
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [shortcut.key, shortcut.ctrl, shortcut.meta, shortcut.alt, shortcut.shift, shortcut.enabled, opts]);
}

export function useKeyboardShortcuts(
  shortcuts: KeyboardShortcut[],
  options: ShortcutOptions = {}
) {
  const opts = { ...defaultOptions, ...options };
  const shortcutsRef = useRef(shortcuts);

  // Keep shortcuts ref updated
  useEffect(() => {
    shortcutsRef.current = shortcuts;
  }, [shortcuts]);

  useEffect(() => {
    if (!opts.enabled) return;

    const handleKeyDown = (e: KeyboardEvent) => {
      for (const shortcut of shortcutsRef.current) {
        if (shortcut.enabled === false) continue;

        // Skip if in input field and ignoreInputs is true
        if (opts.ignoreInputs && isInputElement(e.target)) {
          if (shortcut.key.toLowerCase() !== "escape") {
            continue;
          }
        }

        if (matchesShortcut(e, shortcut)) {
          if (opts.preventDefault) {
            e.preventDefault();
          }
          if (opts.stopPropagation) {
            e.stopPropagation();
          }
          shortcut.handler(e);
          return; // Only trigger first matching shortcut
        }
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [opts]);
}

// Hook for command palette (Cmd/Ctrl+K)
export function useCommandPalette(onOpen: () => void) {
  useKeyboardShortcut({
    key: "k",
    meta: true,
    handler: onOpen,
    description: "Open command palette",
  });
}

// Hook for escape key
export function useEscapeKey(onEscape: () => void, enabled: boolean = true) {
  useKeyboardShortcut(
    {
      key: "Escape",
      handler: onEscape,
      description: "Close",
    },
    { enabled, ignoreInputs: false }
  );
}

// Hook for arrow key navigation
export function useArrowNavigation(
  onUp: () => void,
  onDown: () => void,
  onEnter: () => void,
  enabled: boolean = true
) {
  useKeyboardShortcuts(
    [
      { key: "ArrowUp", handler: onUp, description: "Previous item" },
      { key: "ArrowDown", handler: onDown, description: "Next item" },
      { key: "Enter", handler: onEnter, description: "Select item" },
    ],
    { enabled, ignoreInputs: false }
  );
}

// Format shortcut for display
export function formatShortcut(shortcut: Pick<KeyboardShortcut, "key" | "ctrl" | "meta" | "alt" | "shift">): string {
  const isMac = typeof navigator !== "undefined" && navigator.platform.toLowerCase().includes("mac");

  const parts: string[] = [];

  if (shortcut.ctrl || shortcut.meta) {
    parts.push(isMac ? "⌘" : "Ctrl");
  }
  if (shortcut.alt) {
    parts.push(isMac ? "⌥" : "Alt");
  }
  if (shortcut.shift) {
    parts.push(isMac ? "⇧" : "Shift");
  }

  // Format key
  let key = shortcut.key;
  if (key === "Escape") key = "Esc";
  if (key === "ArrowUp") key = "↑";
  if (key === "ArrowDown") key = "↓";
  if (key === "ArrowLeft") key = "←";
  if (key === "ArrowRight") key = "→";
  if (key === "Enter") key = "↵";
  if (key.length === 1) key = key.toUpperCase();

  parts.push(key);

  return parts.join(isMac ? "" : "+");
}

// Keyboard shortcut display component
export function ShortcutKey({ shortcut }: { shortcut: Pick<KeyboardShortcut, "key" | "ctrl" | "meta" | "alt" | "shift"> }) {
  const formatted = formatShortcut(shortcut);

  return (
    <kbd className="inline-flex items-center gap-0.5 px-1.5 py-0.5 text-xs font-medium text-fg-subtle bg-bg-muted rounded border border-border">
      {formatted}
    </kbd>
  );
}
