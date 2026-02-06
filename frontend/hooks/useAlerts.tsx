"use client";

import { useCallback, useEffect, useState } from "react";
import { useLocalStorage } from "./useLocalStorage";

export type AlertFrequency = "daily" | "weekly";

export type AlertFilters = {
  level?: string;
  canton?: string;
  language?: string;
  dateFrom?: string;
  dateTo?: string;
};

export type Alert = {
  id: string;
  name: string;
  query: string;
  filters: AlertFilters;
  enabled: boolean;
  frequency: AlertFrequency;
  lastChecked?: string;
  lastResultCount?: number;
  newResults?: number;
  createdAt: string;
};

type AlertsState = {
  alerts: Alert[];
  lastGlobalCheck?: string;
};

const STORAGE_KEY = "swisslaw-alerts";
const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/+$/, "") ?? "http://localhost:8000";

function generateId(): string {
  return `alert-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

export function useAlerts() {
  const [state, setState] = useLocalStorage<AlertsState>(STORAGE_KEY, {
    alerts: [],
  });
  const [checking, setChecking] = useState(false);
  const [hasNewResults, setHasNewResults] = useState(false);

  const alerts = state.alerts;
  const enabledAlerts = alerts.filter((a) => a.enabled);

  // Calculate total new results
  useEffect(() => {
    const totalNew = alerts.reduce((sum, a) => sum + (a.newResults || 0), 0);
    setHasNewResults(totalNew > 0);
  }, [alerts]);

  // Create a new alert
  const createAlert = useCallback(
    (name: string, query: string, filters: AlertFilters = {}, frequency: AlertFrequency = "daily"): Alert => {
      const newAlert: Alert = {
        id: generateId(),
        name,
        query,
        filters,
        enabled: true,
        frequency,
        createdAt: new Date().toISOString(),
      };

      setState((prev) => ({
        ...prev,
        alerts: [...prev.alerts, newAlert],
      }));

      return newAlert;
    },
    [setState]
  );

  // Update an alert
  const updateAlert = useCallback(
    (id: string, updates: Partial<Omit<Alert, "id" | "createdAt">>) => {
      setState((prev) => ({
        ...prev,
        alerts: prev.alerts.map((a) => (a.id === id ? { ...a, ...updates } : a)),
      }));
    },
    [setState]
  );

  // Delete an alert
  const deleteAlert = useCallback(
    (id: string) => {
      setState((prev) => ({
        ...prev,
        alerts: prev.alerts.filter((a) => a.id !== id),
      }));
    },
    [setState]
  );

  // Toggle alert enabled status
  const toggleAlert = useCallback(
    (id: string) => {
      setState((prev) => ({
        ...prev,
        alerts: prev.alerts.map((a) =>
          a.id === id ? { ...a, enabled: !a.enabled } : a
        ),
      }));
    },
    [setState]
  );

  // Check a single alert for new results
  const checkAlert = useCallback(
    async (alert: Alert): Promise<number> => {
      try {
        const body: Record<string, unknown> = {
          query: alert.query,
          limit: 1,
          offset: 0,
        };

        if (alert.filters.level) body.level = alert.filters.level;
        if (alert.filters.canton) body.canton = alert.filters.canton;
        if (alert.filters.language) body.language = alert.filters.language;
        if (alert.filters.dateFrom) body.date_from = alert.filters.dateFrom;
        if (alert.filters.dateTo) body.date_to = alert.filters.dateTo;

        const response = await fetch(`${API_BASE}/api/search`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });

        if (!response.ok) throw new Error("Search failed");

        const data = await response.json();
        const currentCount = data.total || 0;
        const previousCount = alert.lastResultCount || 0;
        const newResults = Math.max(0, currentCount - previousCount);

        // Update alert state
        setState((prev) => ({
          ...prev,
          alerts: prev.alerts.map((a) =>
            a.id === alert.id
              ? {
                  ...a,
                  lastChecked: new Date().toISOString(),
                  lastResultCount: currentCount,
                  newResults: alert.lastResultCount !== undefined ? newResults : 0,
                }
              : a
          ),
        }));

        return newResults;
      } catch (error) {
        console.error(`Failed to check alert ${alert.id}:`, error);
        return 0;
      }
    },
    [setState]
  );

  // Check all enabled alerts
  const checkAllAlerts = useCallback(async (): Promise<number> => {
    setChecking(true);
    let totalNew = 0;

    for (const alert of enabledAlerts) {
      const newResults = await checkAlert(alert);
      totalNew += newResults;
    }

    setState((prev) => ({
      ...prev,
      lastGlobalCheck: new Date().toISOString(),
    }));

    setChecking(false);
    return totalNew;
  }, [enabledAlerts, checkAlert, setState]);

  // Clear new results indicator for an alert
  const clearNewResults = useCallback(
    (id: string) => {
      setState((prev) => ({
        ...prev,
        alerts: prev.alerts.map((a) =>
          a.id === id ? { ...a, newResults: 0 } : a
        ),
      }));
    },
    [setState]
  );

  // Clear all new results indicators
  const clearAllNewResults = useCallback(() => {
    setState((prev) => ({
      ...prev,
      alerts: prev.alerts.map((a) => ({ ...a, newResults: 0 })),
    }));
  }, [setState]);

  // Check if we should auto-check (based on last check time and frequency)
  const shouldAutoCheck = useCallback(() => {
    if (!state.lastGlobalCheck) return true;

    const lastCheck = new Date(state.lastGlobalCheck);
    const now = new Date();
    const hoursSinceLastCheck = (now.getTime() - lastCheck.getTime()) / (1000 * 60 * 60);

    // Check at least once per day for daily alerts, once per week for weekly
    const hasDaily = enabledAlerts.some((a) => a.frequency === "daily");
    const hasWeekly = enabledAlerts.some((a) => a.frequency === "weekly");

    if (hasDaily && hoursSinceLastCheck >= 24) return true;
    if (hasWeekly && hoursSinceLastCheck >= 168) return true;

    return false;
  }, [state.lastGlobalCheck, enabledAlerts]);

  // Auto-check on mount if needed
  useEffect(() => {
    if (enabledAlerts.length > 0 && shouldAutoCheck()) {
      // Delay auto-check to not block initial render
      const timer = setTimeout(() => {
        checkAllAlerts();
      }, 2000);

      return () => clearTimeout(timer);
    }
  }, [enabledAlerts.length, shouldAutoCheck, checkAllAlerts]);

  return {
    alerts,
    enabledAlerts,
    hasNewResults,
    checking,
    createAlert,
    updateAlert,
    deleteAlert,
    toggleAlert,
    checkAlert,
    checkAllAlerts,
    clearNewResults,
    clearAllNewResults,
    lastGlobalCheck: state.lastGlobalCheck,
  };
}
