"use client";

import { useState, useCallback } from "react";
import { useAlerts, Alert, AlertFrequency, AlertFilters } from "@/hooks/useAlerts";
import { useToast } from "@/hooks/useToast";

type AlertManagerProps = {
  isOpen: boolean;
  onClose: () => void;
  onRunSearch?: (query: string, filters: AlertFilters) => void;
  currentQuery?: string;
  currentFilters?: AlertFilters;
  translations: {
    title: string;
    newAlert: string;
    createFromSearch: string;
    name: string;
    query: string;
    frequency: string;
    daily: string;
    weekly: string;
    create: string;
    cancel: string;
    delete: string;
    enable: string;
    disable: string;
    check: string;
    checking: string;
    noAlerts: string;
    newResults: string;
    lastChecked: string;
    viewResults: string;
    close: string;
  };
};

function AlertForm({
  onSubmit,
  onCancel,
  initialQuery = "",
  initialFilters = {},
  translations,
}: {
  onSubmit: (name: string, query: string, filters: AlertFilters, frequency: AlertFrequency) => void;
  onCancel: () => void;
  initialQuery?: string;
  initialFilters?: AlertFilters;
  translations: AlertManagerProps["translations"];
}) {
  const [name, setName] = useState("");
  const [query, setQuery] = useState(initialQuery);
  const [frequency, setFrequency] = useState<AlertFrequency>("daily");
  const [filters, setFilters] = useState<AlertFilters>(initialFilters);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim() || !query.trim()) return;
    onSubmit(name.trim(), query.trim(), filters, frequency);
  };

  return (
    <form onSubmit={handleSubmit} className="p-4 border-b border-border bg-bg-subtle">
      <div className="space-y-3">
        <div>
          <label className="block text-xs font-medium text-fg-subtle mb-1">
            {translations.name}
          </label>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g., Contract law updates"
            className="w-full"
            autoFocus
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-fg-subtle mb-1">
            {translations.query}
          </label>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="e.g., Arbeitsvertrag Kündigung"
            className="w-full"
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-fg-subtle mb-1">
            {translations.frequency}
          </label>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={() => setFrequency("daily")}
              className={`flex-1 px-3 py-2 text-sm rounded-lg border transition-colors ${
                frequency === "daily"
                  ? "border-accent bg-accent-subtle text-accent"
                  : "border-border hover:border-border-strong"
              }`}
            >
              {translations.daily}
            </button>
            <button
              type="button"
              onClick={() => setFrequency("weekly")}
              className={`flex-1 px-3 py-2 text-sm rounded-lg border transition-colors ${
                frequency === "weekly"
                  ? "border-accent bg-accent-subtle text-accent"
                  : "border-border hover:border-border-strong"
              }`}
            >
              {translations.weekly}
            </button>
          </div>
        </div>
      </div>

      <div className="flex gap-2 mt-4">
        <button
          type="submit"
          disabled={!name.trim() || !query.trim()}
          className="flex-1 px-3 py-2 text-sm font-medium text-bg bg-accent rounded-lg hover:bg-accent-hover disabled:opacity-50 transition-colors"
        >
          {translations.create}
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="px-3 py-2 text-sm text-fg-subtle hover:text-fg transition-colors"
        >
          {translations.cancel}
        </button>
      </div>
    </form>
  );
}

function AlertCard({
  alert,
  onToggle,
  onDelete,
  onCheck,
  onViewResults,
  checking,
  translations,
}: {
  alert: Alert;
  onToggle: () => void;
  onDelete: () => void;
  onCheck: () => void;
  onViewResults: () => void;
  checking: boolean;
  translations: AlertManagerProps["translations"];
}) {
  const hasNewResults = (alert.newResults || 0) > 0;

  return (
    <div className={`p-4 border-b border-border ${!alert.enabled ? "opacity-60" : ""}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="font-medium text-fg truncate">{alert.name}</h3>
            {hasNewResults && (
              <span className="px-1.5 py-0.5 text-xs font-medium bg-accent text-white rounded-full">
                {alert.newResults} {translations.newResults}
              </span>
            )}
          </div>
          <p className="text-sm text-fg-subtle truncate mt-0.5">{alert.query}</p>
          <div className="flex items-center gap-3 mt-2 text-xs text-fg-faint">
            <span className="capitalize">{alert.frequency}</span>
            {alert.lastChecked && (
              <span>
                {translations.lastChecked}: {new Date(alert.lastChecked).toLocaleDateString()}
              </span>
            )}
            {alert.lastResultCount !== undefined && (
              <span>{alert.lastResultCount} results</span>
            )}
          </div>
        </div>

        <div className="flex items-center gap-1">
          {/* Toggle enabled */}
          <button
            onClick={onToggle}
            className={`p-2 rounded-lg transition-colors ${
              alert.enabled
                ? "text-success hover:bg-success-subtle"
                : "text-fg-faint hover:bg-bg-muted"
            }`}
            title={alert.enabled ? translations.disable : translations.enable}
          >
            {alert.enabled ? (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
              </svg>
            ) : (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
              </svg>
            )}
          </button>

          {/* Check now */}
          <button
            onClick={onCheck}
            disabled={checking || !alert.enabled}
            className="p-2 rounded-lg text-fg-subtle hover:text-fg hover:bg-bg-muted transition-colors disabled:opacity-50"
            title={translations.check}
          >
            {checking ? (
              <span className="w-5 h-5 border-2 border-fg-faint border-t-accent rounded-full animate-spin inline-block" />
            ) : (
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
              </svg>
            )}
          </button>

          {/* View results */}
          {hasNewResults && (
            <button
              onClick={onViewResults}
              className="p-2 rounded-lg text-accent hover:bg-accent-subtle transition-colors"
              title={translations.viewResults}
            >
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
            </button>
          )}

          {/* Delete */}
          <button
            onClick={onDelete}
            className="p-2 rounded-lg text-fg-subtle hover:text-error hover:bg-error-subtle transition-colors"
            title={translations.delete}
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
            </svg>
          </button>
        </div>
      </div>
    </div>
  );
}

export function AlertManager({
  isOpen,
  onClose,
  onRunSearch,
  currentQuery,
  currentFilters,
  translations,
}: AlertManagerProps) {
  const {
    alerts,
    checking,
    createAlert,
    deleteAlert,
    toggleAlert,
    checkAlert,
    checkAllAlerts,
    clearNewResults,
  } = useAlerts();
  const { success } = useToast();

  const [showForm, setShowForm] = useState(false);
  const [checkingAlertId, setCheckingAlertId] = useState<string | null>(null);

  const handleCreate = useCallback(
    (name: string, query: string, filters: AlertFilters, frequency: AlertFrequency) => {
      createAlert(name, query, filters, frequency);
      setShowForm(false);
      success("Alert created");
    },
    [createAlert, success]
  );

  const handleCheck = useCallback(
    async (alert: Alert) => {
      setCheckingAlertId(alert.id);
      const newResults = await checkAlert(alert);
      setCheckingAlertId(null);
      if (newResults > 0) {
        success(`${newResults} new results found`);
      }
    },
    [checkAlert, success]
  );

  const handleViewResults = useCallback(
    (alert: Alert) => {
      clearNewResults(alert.id);
      onRunSearch?.(alert.query, alert.filters);
      onClose();
    },
    [clearNewResults, onRunSearch, onClose]
  );

  const handleCheckAll = useCallback(async () => {
    const totalNew = await checkAllAlerts();
    if (totalNew > 0) {
      success(`${totalNew} new results found across all alerts`);
    } else {
      success("No new results");
    }
  }, [checkAllAlerts, success]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-modal bg-overlay" onClick={onClose}>
      <div
        className="absolute right-0 top-0 bottom-0 w-full max-w-md bg-bg-elevated border-l border-border shadow-xl animate-slide-in-right overflow-hidden flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="px-4 py-3 border-b border-border flex items-center justify-between">
          <h2 className="text-lg font-semibold text-fg">{translations.title}</h2>
          <button
            onClick={onClose}
            className="p-2 rounded-lg text-fg-subtle hover:text-fg hover:bg-bg-muted transition-colors"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Actions */}
        <div className="px-4 py-3 border-b border-border flex items-center gap-2">
          <button
            onClick={() => setShowForm(true)}
            className="flex-1 flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium text-bg bg-fg rounded-lg hover:bg-fg/90 transition-colors"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            {translations.newAlert}
          </button>

          {currentQuery && (
            <button
              onClick={() => {
                setShowForm(true);
              }}
              className="px-3 py-2 text-sm text-accent hover:bg-accent-subtle rounded-lg transition-colors"
              title={translations.createFromSearch}
            >
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" />
              </svg>
            </button>
          )}

          {alerts.length > 0 && (
            <button
              onClick={handleCheckAll}
              disabled={checking}
              className="px-3 py-2 text-sm text-fg-subtle hover:text-fg hover:bg-bg-muted rounded-lg transition-colors disabled:opacity-50"
              title={translations.check}
            >
              {checking ? (
                <span className="w-5 h-5 border-2 border-fg-faint border-t-accent rounded-full animate-spin inline-block" />
              ) : (
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
              )}
            </button>
          )}
        </div>

        {/* New alert form */}
        {showForm && (
          <AlertForm
            onSubmit={handleCreate}
            onCancel={() => setShowForm(false)}
            initialQuery={currentQuery}
            initialFilters={currentFilters}
            translations={translations}
          />
        )}

        {/* Alerts list */}
        <div className="flex-1 overflow-auto">
          {alerts.length === 0 ? (
            <div className="text-center py-12 text-fg-subtle">
              <svg className="w-12 h-12 mx-auto mb-3 text-fg-faint" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
              </svg>
              <p className="text-sm">{translations.noAlerts}</p>
            </div>
          ) : (
            alerts.map((alert) => (
              <AlertCard
                key={alert.id}
                alert={alert}
                onToggle={() => toggleAlert(alert.id)}
                onDelete={() => deleteAlert(alert.id)}
                onCheck={() => handleCheck(alert)}
                onViewResults={() => handleViewResults(alert)}
                checking={checkingAlertId === alert.id}
                translations={translations}
              />
            ))
          )}
        </div>
      </div>
    </div>
  );
}

// Notification badge for alerts
export function AlertBadge({
  onClick,
  className = "",
}: {
  onClick?: () => void;
  className?: string;
}) {
  const { hasNewResults, alerts } = useAlerts();
  const totalNew = alerts.reduce((sum, a) => sum + (a.newResults || 0), 0);

  return (
    <button
      onClick={onClick}
      className={`relative p-2 rounded-lg transition-colors ${
        hasNewResults
          ? "text-accent hover:bg-accent-subtle"
          : "text-fg-subtle hover:text-fg hover:bg-bg-muted"
      } ${className}`}
    >
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
      </svg>
      {hasNewResults && (
        <span className="absolute -top-1 -right-1 w-5 h-5 flex items-center justify-center text-xs font-bold bg-accent text-white rounded-full">
          {totalNew > 9 ? "9+" : totalNew}
        </span>
      )}
    </button>
  );
}
