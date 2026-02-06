"use client";

type SkeletonProps = {
  className?: string;
  variant?: "text" | "circular" | "rectangular";
  width?: string | number;
  height?: string | number;
};

export function Skeleton({
  className = "",
  variant = "text",
  width,
  height,
}: SkeletonProps) {
  const baseStyles = "animate-pulse bg-bg-muted";

  const variantStyles = {
    text: "rounded h-4",
    circular: "rounded-full",
    rectangular: "rounded-lg",
  };

  const style: React.CSSProperties = {};
  if (width) style.width = typeof width === "number" ? `${width}px` : width;
  if (height) style.height = typeof height === "number" ? `${height}px` : height;

  return (
    <div
      className={`${baseStyles} ${variantStyles[variant]} ${className}`}
      style={style}
    />
  );
}

// Pre-built skeleton patterns
export function TextSkeleton({ lines = 3, className = "" }: { lines?: number; className?: string }) {
  return (
    <div className={`space-y-2 ${className}`}>
      {Array.from({ length: lines }).map((_, i) => (
        <Skeleton
          key={i}
          variant="text"
          width={i === lines - 1 ? "75%" : "100%"}
        />
      ))}
    </div>
  );
}

export function CardSkeleton({ className = "" }: { className?: string }) {
  return (
    <div className={`space-y-3 ${className}`}>
      <Skeleton variant="rectangular" height={120} />
      <Skeleton variant="text" width="60%" />
      <Skeleton variant="text" width="100%" />
      <Skeleton variant="text" width="80%" />
    </div>
  );
}

export function AvatarSkeleton({ size = 40, className = "" }: { size?: number; className?: string }) {
  return (
    <Skeleton
      variant="circular"
      width={size}
      height={size}
      className={className}
    />
  );
}

export function StatSkeleton({ className = "" }: { className?: string }) {
  return (
    <div className={`${className}`}>
      <Skeleton variant="text" width={80} height={12} className="mb-3" />
      <Skeleton variant="text" width={120} height={40} className="mb-2" />
      <Skeleton variant="text" width={60} height={12} />
    </div>
  );
}

export function BarSkeleton({ className = "" }: { className?: string }) {
  return (
    <div className={`${className}`}>
      <div className="flex justify-between mb-2">
        <Skeleton variant="text" width={64} height={16} />
        <Skeleton variant="text" width={48} height={16} />
      </div>
      <Skeleton variant="rectangular" height={8} className="rounded-full" />
    </div>
  );
}
