"use client";

import { useCallback } from "react";
import { useLocalStorage } from "./useLocalStorage";

export type AnnotationType = "highlight" | "note";
export type HighlightColor = "yellow" | "green" | "blue" | "pink" | "purple";

export type Annotation = {
  id: string;
  decisionId: string;
  type: AnnotationType;
  color: HighlightColor;
  startOffset: number;
  endOffset: number;
  selectedText: string;
  note?: string;
  createdAt: string;
  updatedAt: string;
};

type AnnotationsState = {
  annotations: Annotation[];
};

const STORAGE_KEY = "swisslaw-annotations";

function generateId(): string {
  return `ann-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

export function useAnnotations(decisionId?: string) {
  const [state, setState] = useLocalStorage<AnnotationsState>(STORAGE_KEY, {
    annotations: [],
  });

  // Get all annotations, optionally filtered by decision
  const annotations = decisionId
    ? state.annotations.filter((a) => a.decisionId === decisionId)
    : state.annotations;

  // Get annotations for a specific decision
  const getAnnotationsForDecision = useCallback(
    (id: string) => {
      return state.annotations.filter((a) => a.decisionId === id);
    },
    [state.annotations]
  );

  // Create a new annotation
  const createAnnotation = useCallback(
    (
      decisionId: string,
      type: AnnotationType,
      startOffset: number,
      endOffset: number,
      selectedText: string,
      color: HighlightColor = "yellow",
      note?: string
    ): Annotation => {
      const now = new Date().toISOString();
      const newAnnotation: Annotation = {
        id: generateId(),
        decisionId,
        type,
        color,
        startOffset,
        endOffset,
        selectedText,
        note,
        createdAt: now,
        updatedAt: now,
      };

      setState((prev) => ({
        ...prev,
        annotations: [...prev.annotations, newAnnotation],
      }));

      return newAnnotation;
    },
    [setState]
  );

  // Create a highlight
  const createHighlight = useCallback(
    (
      decisionId: string,
      startOffset: number,
      endOffset: number,
      selectedText: string,
      color: HighlightColor = "yellow"
    ): Annotation => {
      return createAnnotation(decisionId, "highlight", startOffset, endOffset, selectedText, color);
    },
    [createAnnotation]
  );

  // Create a note
  const createNote = useCallback(
    (
      decisionId: string,
      startOffset: number,
      endOffset: number,
      selectedText: string,
      noteText: string,
      color: HighlightColor = "blue"
    ): Annotation => {
      return createAnnotation(decisionId, "note", startOffset, endOffset, selectedText, color, noteText);
    },
    [createAnnotation]
  );

  // Update an annotation
  const updateAnnotation = useCallback(
    (id: string, updates: Partial<Pick<Annotation, "color" | "note">>) => {
      setState((prev) => ({
        ...prev,
        annotations: prev.annotations.map((a) =>
          a.id === id
            ? { ...a, ...updates, updatedAt: new Date().toISOString() }
            : a
        ),
      }));
    },
    [setState]
  );

  // Delete an annotation
  const deleteAnnotation = useCallback(
    (id: string) => {
      setState((prev) => ({
        ...prev,
        annotations: prev.annotations.filter((a) => a.id !== id),
      }));
    },
    [setState]
  );

  // Delete all annotations for a decision
  const deleteAnnotationsForDecision = useCallback(
    (decisionId: string) => {
      setState((prev) => ({
        ...prev,
        annotations: prev.annotations.filter((a) => a.decisionId !== decisionId),
      }));
    },
    [setState]
  );

  // Get annotation at a specific offset
  const getAnnotationAtOffset = useCallback(
    (decisionId: string, offset: number): Annotation | undefined => {
      return state.annotations.find(
        (a) =>
          a.decisionId === decisionId &&
          offset >= a.startOffset &&
          offset <= a.endOffset
      );
    },
    [state.annotations]
  );

  // Check if text overlaps with existing annotation
  const hasOverlappingAnnotation = useCallback(
    (decisionId: string, startOffset: number, endOffset: number): boolean => {
      return state.annotations.some(
        (a) =>
          a.decisionId === decisionId &&
          ((startOffset >= a.startOffset && startOffset <= a.endOffset) ||
            (endOffset >= a.startOffset && endOffset <= a.endOffset) ||
            (startOffset <= a.startOffset && endOffset >= a.endOffset))
      );
    },
    [state.annotations]
  );

  // Export annotations for a decision as text
  const exportAnnotations = useCallback(
    (decisionId: string): string => {
      const decisionAnnotations = state.annotations
        .filter((a) => a.decisionId === decisionId)
        .sort((a, b) => a.startOffset - b.startOffset);

      if (decisionAnnotations.length === 0) {
        return "No annotations";
      }

      const lines: string[] = [`Annotations for decision: ${decisionId}`, ""];

      decisionAnnotations.forEach((a, i) => {
        lines.push(`${i + 1}. [${a.type.toUpperCase()}] "${a.selectedText}"`);
        if (a.note) {
          lines.push(`   Note: ${a.note}`);
        }
        lines.push(`   Position: ${a.startOffset}-${a.endOffset}`);
        lines.push("");
      });

      return lines.join("\n");
    },
    [state.annotations]
  );

  // Export all annotations as JSON
  const exportAllAnnotationsJson = useCallback((): string => {
    return JSON.stringify(
      {
        annotations: state.annotations,
        exportedAt: new Date().toISOString(),
      },
      null,
      2
    );
  }, [state.annotations]);

  // Import annotations from JSON
  const importAnnotations = useCallback(
    (json: string): boolean => {
      try {
        const data = JSON.parse(json);
        if (data.annotations && Array.isArray(data.annotations)) {
          setState((prev) => ({
            ...prev,
            annotations: [...prev.annotations, ...data.annotations],
          }));
          return true;
        }
        return false;
      } catch {
        return false;
      }
    },
    [setState]
  );

  // Get count of annotations per decision
  const getAnnotationCounts = useCallback((): Record<string, number> => {
    const counts: Record<string, number> = {};
    state.annotations.forEach((a) => {
      counts[a.decisionId] = (counts[a.decisionId] || 0) + 1;
    });
    return counts;
  }, [state.annotations]);

  return {
    annotations,
    getAnnotationsForDecision,
    createAnnotation,
    createHighlight,
    createNote,
    updateAnnotation,
    deleteAnnotation,
    deleteAnnotationsForDecision,
    getAnnotationAtOffset,
    hasOverlappingAnnotation,
    exportAnnotations,
    exportAllAnnotationsJson,
    importAnnotations,
    getAnnotationCounts,
  };
}

// Color mapping for CSS classes
export const HIGHLIGHT_COLORS: Record<HighlightColor, { bg: string; border: string; label: string }> = {
  yellow: { bg: "highlight-yellow", border: "border-yellow-400", label: "Yellow" },
  green: { bg: "highlight-green", border: "border-green-400", label: "Green" },
  blue: { bg: "highlight-blue", border: "border-blue-400", label: "Blue" },
  pink: { bg: "highlight-pink", border: "border-pink-400", label: "Pink" },
  purple: { bg: "highlight-purple", border: "border-purple-400", label: "Purple" },
};
