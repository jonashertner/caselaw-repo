"use client";

import { createContext, useContext, useCallback, useState, ReactNode } from "react";

export type ToastType = "success" | "error" | "warning" | "info";

export type Toast = {
  id: string;
  type: ToastType;
  message: string;
  duration?: number;
  action?: {
    label: string;
    onClick: () => void;
  };
};

type ToastContextType = {
  toasts: Toast[];
  addToast: (toast: Omit<Toast, "id">) => string;
  removeToast: (id: string) => void;
  clearToasts: () => void;
  success: (message: string, options?: Partial<Omit<Toast, "id" | "type" | "message">>) => string;
  error: (message: string, options?: Partial<Omit<Toast, "id" | "type" | "message">>) => string;
  warning: (message: string, options?: Partial<Omit<Toast, "id" | "type" | "message">>) => string;
  info: (message: string, options?: Partial<Omit<Toast, "id" | "type" | "message">>) => string;
};

const ToastContext = createContext<ToastContextType | undefined>(undefined);

const DEFAULT_DURATION = 5000;

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const addToast = useCallback((toast: Omit<Toast, "id">) => {
    const id = `toast-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const newToast: Toast = {
      ...toast,
      id,
      duration: toast.duration ?? DEFAULT_DURATION,
    };

    setToasts((prev) => [...prev, newToast]);
    return id;
  }, []);

  const removeToast = useCallback((id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const clearToasts = useCallback(() => {
    setToasts([]);
  }, []);

  const success = useCallback(
    (message: string, options?: Partial<Omit<Toast, "id" | "type" | "message">>) => {
      return addToast({ type: "success", message, ...options });
    },
    [addToast]
  );

  const error = useCallback(
    (message: string, options?: Partial<Omit<Toast, "id" | "type" | "message">>) => {
      return addToast({ type: "error", message, duration: 7000, ...options });
    },
    [addToast]
  );

  const warning = useCallback(
    (message: string, options?: Partial<Omit<Toast, "id" | "type" | "message">>) => {
      return addToast({ type: "warning", message, ...options });
    },
    [addToast]
  );

  const info = useCallback(
    (message: string, options?: Partial<Omit<Toast, "id" | "type" | "message">>) => {
      return addToast({ type: "info", message, ...options });
    },
    [addToast]
  );

  return (
    <ToastContext.Provider
      value={{ toasts, addToast, removeToast, clearToasts, success, error, warning, info }}
    >
      {children}
    </ToastContext.Provider>
  );
}

export function useToast() {
  const context = useContext(ToastContext);
  if (context === undefined) {
    throw new Error("useToast must be used within a ToastProvider");
  }
  return context;
}
