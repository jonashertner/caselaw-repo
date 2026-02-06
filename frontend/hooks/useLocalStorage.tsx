"use client";

import { useState, useEffect, useCallback } from "react";

export function useLocalStorage<T>(
  key: string,
  initialValue: T
): [T, (value: T | ((prev: T) => T)) => void, () => void] {
  // State to store our value
  const [storedValue, setStoredValue] = useState<T>(initialValue);
  const [isInitialized, setIsInitialized] = useState(false);

  // Initialize from localStorage
  useEffect(() => {
    try {
      const item = window.localStorage.getItem(key);
      if (item) {
        setStoredValue(JSON.parse(item));
      }
    } catch (error) {
      console.warn(`Error reading localStorage key "${key}":`, error);
    }
    setIsInitialized(true);
  }, [key]);

  // Return a wrapped version of useState's setter function that persists to localStorage
  const setValue = useCallback(
    (value: T | ((prev: T) => T)) => {
      try {
        // Allow value to be a function so we have same API as useState
        setStoredValue((prev) => {
          const valueToStore = value instanceof Function ? value(prev) : value;
          // Save to local storage
          window.localStorage.setItem(key, JSON.stringify(valueToStore));
          return valueToStore;
        });
      } catch (error) {
        console.warn(`Error setting localStorage key "${key}":`, error);
      }
    },
    [key]
  );

  // Remove from localStorage
  const removeValue = useCallback(() => {
    try {
      window.localStorage.removeItem(key);
      setStoredValue(initialValue);
    } catch (error) {
      console.warn(`Error removing localStorage key "${key}":`, error);
    }
  }, [key, initialValue]);

  return [storedValue, setValue, removeValue];
}

// Hook for managing recent searches
export function useRecentSearches(maxItems: number = 20) {
  const [searches, setSearches] = useLocalStorage<string[]>("swisslaw-recent-searches", []);

  const addSearch = useCallback(
    (query: string) => {
      const trimmed = query.trim();
      if (!trimmed) return;

      setSearches((prev) => {
        // Remove if exists, then add to front
        const filtered = prev.filter((s) => s.toLowerCase() !== trimmed.toLowerCase());
        return [trimmed, ...filtered].slice(0, maxItems);
      });
    },
    [setSearches, maxItems]
  );

  const removeSearch = useCallback(
    (query: string) => {
      setSearches((prev) => prev.filter((s) => s !== query));
    },
    [setSearches]
  );

  const clearSearches = useCallback(() => {
    setSearches([]);
  }, [setSearches]);

  return {
    searches,
    addSearch,
    removeSearch,
    clearSearches,
  };
}
