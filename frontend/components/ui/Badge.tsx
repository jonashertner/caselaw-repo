"use client";

import { ReactNode } from "react";

type BadgeVariant = "default" | "accent" | "federal" | "success" | "warning" | "error" | "info";
type BadgeSize = "sm" | "md";

const variantStyles: Record<BadgeVariant, string> = {
  default: "bg-bg-muted text-fg",
  accent: "bg-accent-subtle text-accent",
  federal: "bg-federal-subtle text-federal",
  success: "bg-success-subtle text-success",
  warning: "bg-warning-subtle text-warning",
  error: "bg-error-subtle text-error",
  info: "bg-info-subtle text-info",
};

const sizeStyles: Record<BadgeSize, string> = {
  sm: "px-1.5 py-0.5 text-xs",
  md: "px-2 py-1 text-sm",
};

type BadgeProps = {
  children: ReactNode;
  variant?: BadgeVariant;
  size?: BadgeSize;
  className?: string;
};

export function Badge({
  children,
  variant = "default",
  size = "sm",
  className = "",
}: BadgeProps) {
  return (
    <span
      className={`inline-flex items-center font-medium rounded ${variantStyles[variant]} ${sizeStyles[size]} ${className}`}
    >
      {children}
    </span>
  );
}
