"use client";

import { useRef, useCallback, useState, useEffect } from "react";

type SwipeDirection = "left" | "right" | "up" | "down" | null;

type SwipeConfig = {
  threshold?: number;
  onSwipeLeft?: () => void;
  onSwipeRight?: () => void;
  onSwipeUp?: () => void;
  onSwipeDown?: () => void;
  enabled?: boolean;
};

type SwipeState = {
  direction: SwipeDirection;
  distance: number;
  velocity: number;
  isSwiping: boolean;
};

export function useSwipeGesture(config: SwipeConfig = {}) {
  const {
    threshold = 50,
    onSwipeLeft,
    onSwipeRight,
    onSwipeUp,
    onSwipeDown,
    enabled = true,
  } = config;

  const [swipeState, setSwipeState] = useState<SwipeState>({
    direction: null,
    distance: 0,
    velocity: 0,
    isSwiping: false,
  });

  const startRef = useRef<{ x: number; y: number; time: number } | null>(null);
  const elementRef = useRef<HTMLDivElement>(null);

  const handleTouchStart = useCallback(
    (e: TouchEvent) => {
      if (!enabled) return;

      const touch = e.touches[0];
      startRef.current = {
        x: touch.clientX,
        y: touch.clientY,
        time: Date.now(),
      };

      setSwipeState({
        direction: null,
        distance: 0,
        velocity: 0,
        isSwiping: true,
      });
    },
    [enabled]
  );

  const handleTouchMove = useCallback(
    (e: TouchEvent) => {
      if (!enabled || !startRef.current) return;

      const touch = e.touches[0];
      const deltaX = touch.clientX - startRef.current.x;
      const deltaY = touch.clientY - startRef.current.y;
      const absX = Math.abs(deltaX);
      const absY = Math.abs(deltaY);

      let direction: SwipeDirection = null;
      let distance = 0;

      if (absX > absY) {
        direction = deltaX > 0 ? "right" : "left";
        distance = absX;
      } else {
        direction = deltaY > 0 ? "down" : "up";
        distance = absY;
      }

      const elapsed = Date.now() - startRef.current.time;
      const velocity = distance / elapsed;

      setSwipeState({
        direction,
        distance,
        velocity,
        isSwiping: true,
      });
    },
    [enabled]
  );

  const handleTouchEnd = useCallback(() => {
    if (!enabled || !startRef.current) return;

    const { direction, distance, velocity } = swipeState;

    // Trigger callbacks if threshold met
    if (distance >= threshold || velocity > 0.5) {
      switch (direction) {
        case "left":
          onSwipeLeft?.();
          break;
        case "right":
          onSwipeRight?.();
          break;
        case "up":
          onSwipeUp?.();
          break;
        case "down":
          onSwipeDown?.();
          break;
      }
    }

    startRef.current = null;
    setSwipeState({
      direction: null,
      distance: 0,
      velocity: 0,
      isSwiping: false,
    });
  }, [enabled, swipeState, threshold, onSwipeLeft, onSwipeRight, onSwipeUp, onSwipeDown]);

  const handleTouchCancel = useCallback(() => {
    startRef.current = null;
    setSwipeState({
      direction: null,
      distance: 0,
      velocity: 0,
      isSwiping: false,
    });
  }, []);

  // Attach/detach event listeners
  useEffect(() => {
    const element = elementRef.current;
    if (!element || !enabled) return;

    element.addEventListener("touchstart", handleTouchStart, { passive: true });
    element.addEventListener("touchmove", handleTouchMove, { passive: true });
    element.addEventListener("touchend", handleTouchEnd);
    element.addEventListener("touchcancel", handleTouchCancel);

    return () => {
      element.removeEventListener("touchstart", handleTouchStart);
      element.removeEventListener("touchmove", handleTouchMove);
      element.removeEventListener("touchend", handleTouchEnd);
      element.removeEventListener("touchcancel", handleTouchCancel);
    };
  }, [enabled, handleTouchStart, handleTouchMove, handleTouchEnd, handleTouchCancel]);

  return {
    ref: elementRef,
    ...swipeState,
  };
}

// Pull to refresh hook
export function usePullToRefresh(onRefresh: () => Promise<void>, threshold = 80) {
  const [isPulling, setIsPulling] = useState(false);
  const [pullDistance, setPullDistance] = useState(0);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const startYRef = useRef<number | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const handleTouchStart = useCallback((e: TouchEvent) => {
    const container = containerRef.current;
    if (!container || container.scrollTop > 0) return;

    startYRef.current = e.touches[0].clientY;
    setIsPulling(true);
  }, []);

  const handleTouchMove = useCallback(
    (e: TouchEvent) => {
      if (startYRef.current === null || isRefreshing) return;

      const currentY = e.touches[0].clientY;
      const distance = Math.max(0, currentY - startYRef.current);

      if (distance > 0) {
        e.preventDefault();
        // Apply resistance (diminishing returns)
        setPullDistance(Math.min(distance * 0.5, threshold * 1.5));
      }
    },
    [isRefreshing, threshold]
  );

  const handleTouchEnd = useCallback(async () => {
    if (isRefreshing) return;

    if (pullDistance >= threshold) {
      setIsRefreshing(true);
      await onRefresh();
      setIsRefreshing(false);
    }

    startYRef.current = null;
    setIsPulling(false);
    setPullDistance(0);
  }, [pullDistance, threshold, isRefreshing, onRefresh]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    container.addEventListener("touchstart", handleTouchStart, { passive: true });
    container.addEventListener("touchmove", handleTouchMove, { passive: false });
    container.addEventListener("touchend", handleTouchEnd);

    return () => {
      container.removeEventListener("touchstart", handleTouchStart);
      container.removeEventListener("touchmove", handleTouchMove);
      container.removeEventListener("touchend", handleTouchEnd);
    };
  }, [handleTouchStart, handleTouchMove, handleTouchEnd]);

  return {
    containerRef,
    isPulling,
    pullDistance,
    isRefreshing,
    showIndicator: pullDistance > 20,
    isReady: pullDistance >= threshold,
  };
}
