"use client";

import { useState, useCallback, useEffect, useRef } from "react";
import {
  useAnnotations,
  Annotation,
  HighlightColor,
  HIGHLIGHT_COLORS,
} from "@/hooks/useAnnotations";

type AnnotationOverlayProps = {
  decisionId: string;
  text: string;
  onAnnotationClick?: (annotation: Annotation) => void;
};

type SelectionPopoverProps = {
  position: { x: number; y: number };
  onHighlight: (color: HighlightColor) => void;
  onNote: () => void;
  onClose: () => void;
};

function SelectionPopover({ position, onHighlight, onNote, onClose }: SelectionPopoverProps) {
  const popoverRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        onClose();
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [onClose]);

  return (
    <div
      ref={popoverRef}
      className="fixed z-popover bg-bg-elevated border border-border rounded-xl shadow-lg p-2 animate-scale-in"
      style={{
        left: Math.max(16, Math.min(position.x - 100, window.innerWidth - 220)),
        top: position.y + 8,
      }}
    >
      <div className="flex items-center gap-1 mb-2">
        {(Object.keys(HIGHLIGHT_COLORS) as HighlightColor[]).map((color) => (
          <button
            key={color}
            onClick={() => onHighlight(color)}
            className={`w-6 h-6 rounded-full transition-transform hover:scale-110 ${HIGHLIGHT_COLORS[color].bg}`}
            title={HIGHLIGHT_COLORS[color].label}
          />
        ))}
      </div>
      <button
        onClick={onNote}
        className="w-full flex items-center gap-2 px-2 py-1.5 text-sm text-fg-subtle hover:text-fg hover:bg-bg-subtle rounded-lg transition-colors"
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={1.5}
            d="M7 8h10M7 12h4m1 8l-4-4H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-3l-4 4z"
          />
        </svg>
        Add note
      </button>
    </div>
  );
}

type NotePopoverProps = {
  position: { x: number; y: number };
  selectedText: string;
  onSave: (note: string, color: HighlightColor) => void;
  onCancel: () => void;
};

function NotePopover({ position, selectedText, onSave, onCancel }: NotePopoverProps) {
  const [note, setNote] = useState("");
  const [color, setColor] = useState<HighlightColor>("blue");
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    textareaRef.current?.focus();
  }, []);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (note.trim()) {
      onSave(note.trim(), color);
    }
  };

  return (
    <div
      className="fixed z-popover bg-bg-elevated border border-border rounded-xl shadow-lg p-3 w-72 animate-scale-in"
      style={{
        left: Math.max(16, Math.min(position.x - 144, window.innerWidth - 300)),
        top: position.y + 8,
      }}
    >
      <div className="text-xs text-fg-subtle mb-2 line-clamp-2">"{selectedText}"</div>
      <form onSubmit={handleSubmit}>
        <textarea
          ref={textareaRef}
          value={note}
          onChange={(e) => setNote(e.target.value)}
          placeholder="Add your note..."
          className="w-full h-20 text-sm resize-none mb-2"
        />
        <div className="flex items-center gap-1 mb-2">
          {(Object.keys(HIGHLIGHT_COLORS) as HighlightColor[]).map((c) => (
            <button
              key={c}
              type="button"
              onClick={() => setColor(c)}
              className={`w-5 h-5 rounded-full transition-all ${HIGHLIGHT_COLORS[c].bg} ${
                color === c ? "ring-2 ring-offset-1 ring-fg" : ""
              }`}
            />
          ))}
        </div>
        <div className="flex gap-2">
          <button
            type="submit"
            disabled={!note.trim()}
            className="flex-1 px-3 py-1.5 text-sm font-medium text-bg bg-accent rounded-lg hover:bg-accent-hover disabled:opacity-50 transition-colors"
          >
            Save
          </button>
          <button
            type="button"
            onClick={onCancel}
            className="px-3 py-1.5 text-sm text-fg-subtle hover:text-fg transition-colors"
          >
            Cancel
          </button>
        </div>
      </form>
    </div>
  );
}

type AnnotationPopoverProps = {
  annotation: Annotation;
  position: { x: number; y: number };
  onUpdateColor: (color: HighlightColor) => void;
  onUpdateNote: (note: string) => void;
  onDelete: () => void;
  onClose: () => void;
};

function AnnotationPopover({
  annotation,
  position,
  onUpdateColor,
  onUpdateNote,
  onDelete,
  onClose,
}: AnnotationPopoverProps) {
  const [isEditing, setIsEditing] = useState(false);
  const [noteText, setNoteText] = useState(annotation.note || "");
  const popoverRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        onClose();
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [onClose]);

  const handleSaveNote = () => {
    onUpdateNote(noteText);
    setIsEditing(false);
  };

  return (
    <div
      ref={popoverRef}
      className="fixed z-popover bg-bg-elevated border border-border rounded-xl shadow-lg p-3 w-64 animate-scale-in"
      style={{
        left: Math.max(16, Math.min(position.x - 128, window.innerWidth - 280)),
        top: position.y + 8,
      }}
    >
      {/* Selected text preview */}
      <div className="text-xs text-fg-subtle mb-2 line-clamp-2">"{annotation.selectedText}"</div>

      {/* Color picker */}
      <div className="flex items-center gap-1 mb-2">
        {(Object.keys(HIGHLIGHT_COLORS) as HighlightColor[]).map((color) => (
          <button
            key={color}
            onClick={() => onUpdateColor(color)}
            className={`w-5 h-5 rounded-full transition-all ${HIGHLIGHT_COLORS[color].bg} ${
              annotation.color === color ? "ring-2 ring-offset-1 ring-fg" : ""
            }`}
          />
        ))}
      </div>

      {/* Note */}
      {annotation.type === "note" && (
        <div className="mb-2">
          {isEditing ? (
            <div>
              <textarea
                value={noteText}
                onChange={(e) => setNoteText(e.target.value)}
                className="w-full h-16 text-sm resize-none mb-2"
                autoFocus
              />
              <div className="flex gap-2">
                <button
                  onClick={handleSaveNote}
                  className="text-xs text-accent hover:underline"
                >
                  Save
                </button>
                <button
                  onClick={() => {
                    setNoteText(annotation.note || "");
                    setIsEditing(false);
                  }}
                  className="text-xs text-fg-subtle hover:text-fg"
                >
                  Cancel
                </button>
              </div>
            </div>
          ) : (
            <div
              className="text-sm text-fg p-2 bg-bg-subtle rounded cursor-pointer hover:bg-bg-muted"
              onClick={() => setIsEditing(true)}
            >
              {annotation.note || <span className="text-fg-faint italic">Click to add note</span>}
            </div>
          )}
        </div>
      )}

      {/* Actions */}
      <div className="flex items-center justify-between pt-2 border-t border-border">
        <span className="text-xs text-fg-faint">
          {new Date(annotation.createdAt).toLocaleDateString()}
        </span>
        <button
          onClick={onDelete}
          className="text-xs text-error hover:underline"
        >
          Delete
        </button>
      </div>
    </div>
  );
}

export function AnnotationOverlay({ decisionId, text, onAnnotationClick }: AnnotationOverlayProps) {
  const {
    annotations,
    createHighlight,
    createNote,
    updateAnnotation,
    deleteAnnotation,
  } = useAnnotations(decisionId);

  const [selection, setSelection] = useState<{
    startOffset: number;
    endOffset: number;
    text: string;
    position: { x: number; y: number };
  } | null>(null);

  const [showNotePopover, setShowNotePopover] = useState(false);
  const [activeAnnotation, setActiveAnnotation] = useState<{
    annotation: Annotation;
    position: { x: number; y: number };
  } | null>(null);

  // Handle text selection
  const handleMouseUp = useCallback(() => {
    const sel = window.getSelection();
    if (!sel || sel.isCollapsed || !sel.toString().trim()) {
      setSelection(null);
      return;
    }

    const selectedText = sel.toString();
    const range = sel.getRangeAt(0);
    const rect = range.getBoundingClientRect();

    // Calculate offsets (simplified - in production would need more robust calculation)
    const container = range.commonAncestorContainer;
    const textContent = container.textContent || "";
    const startOffset = textContent.indexOf(selectedText);
    const endOffset = startOffset + selectedText.length;

    setSelection({
      startOffset,
      endOffset,
      text: selectedText,
      position: { x: rect.left + rect.width / 2, y: rect.bottom },
    });
    setShowNotePopover(false);
  }, []);

  // Handle highlight creation
  const handleHighlight = useCallback(
    (color: HighlightColor) => {
      if (!selection) return;

      createHighlight(decisionId, selection.startOffset, selection.endOffset, selection.text, color);
      setSelection(null);
      window.getSelection()?.removeAllRanges();
    },
    [selection, decisionId, createHighlight]
  );

  // Handle note creation
  const handleAddNote = useCallback(
    (note: string, color: HighlightColor) => {
      if (!selection) return;

      createNote(decisionId, selection.startOffset, selection.endOffset, selection.text, note, color);
      setSelection(null);
      setShowNotePopover(false);
      window.getSelection()?.removeAllRanges();
    },
    [selection, decisionId, createNote]
  );

  // Handle annotation click
  const handleAnnotationClick = useCallback(
    (annotation: Annotation, e: React.MouseEvent) => {
      e.stopPropagation();
      setActiveAnnotation({
        annotation,
        position: { x: e.clientX, y: e.clientY },
      });
      onAnnotationClick?.(annotation);
    },
    [onAnnotationClick]
  );

  // Close selection popover
  const handleCloseSelection = useCallback(() => {
    setSelection(null);
    setShowNotePopover(false);
    window.getSelection()?.removeAllRanges();
  }, []);

  // Render text with annotations
  const renderAnnotatedText = () => {
    if (annotations.length === 0) {
      return <span>{text}</span>;
    }

    // Sort annotations by start offset
    const sortedAnnotations = [...annotations].sort((a, b) => a.startOffset - b.startOffset);

    const parts: React.ReactNode[] = [];
    let lastIndex = 0;

    sortedAnnotations.forEach((annotation, i) => {
      // Add text before annotation
      if (annotation.startOffset > lastIndex) {
        parts.push(
          <span key={`text-${i}`}>{text.slice(lastIndex, annotation.startOffset)}</span>
        );
      }

      // Add annotated text
      parts.push(
        <span
          key={`annotation-${annotation.id}`}
          onClick={(e) => handleAnnotationClick(annotation, e)}
          className={`cursor-pointer ${HIGHLIGHT_COLORS[annotation.color].bg} ${
            annotation.type === "note" ? "border-b-2 " + HIGHLIGHT_COLORS[annotation.color].border : ""
          }`}
        >
          {text.slice(annotation.startOffset, annotation.endOffset)}
          {annotation.type === "note" && (
            <sup className="text-xs text-accent ml-0.5">*</sup>
          )}
        </span>
      );

      lastIndex = annotation.endOffset;
    });

    // Add remaining text
    if (lastIndex < text.length) {
      parts.push(<span key="text-end">{text.slice(lastIndex)}</span>);
    }

    return <>{parts}</>;
  };

  return (
    <div onMouseUp={handleMouseUp} className="relative">
      {renderAnnotatedText()}

      {/* Selection popover */}
      {selection && !showNotePopover && (
        <SelectionPopover
          position={selection.position}
          onHighlight={handleHighlight}
          onNote={() => setShowNotePopover(true)}
          onClose={handleCloseSelection}
        />
      )}

      {/* Note creation popover */}
      {selection && showNotePopover && (
        <NotePopover
          position={selection.position}
          selectedText={selection.text}
          onSave={handleAddNote}
          onCancel={handleCloseSelection}
        />
      )}

      {/* Annotation edit popover */}
      {activeAnnotation && (
        <AnnotationPopover
          annotation={activeAnnotation.annotation}
          position={activeAnnotation.position}
          onUpdateColor={(color) => updateAnnotation(activeAnnotation.annotation.id, { color })}
          onUpdateNote={(note) => updateAnnotation(activeAnnotation.annotation.id, { note })}
          onDelete={() => {
            deleteAnnotation(activeAnnotation.annotation.id);
            setActiveAnnotation(null);
          }}
          onClose={() => setActiveAnnotation(null)}
        />
      )}
    </div>
  );
}

// Annotations sidebar listing all annotations for a decision
export function AnnotationsSidebar({
  decisionId,
  onAnnotationClick,
  onExport,
  translations,
}: {
  decisionId: string;
  onAnnotationClick?: (annotation: Annotation) => void;
  onExport?: () => void;
  translations: {
    title: string;
    noAnnotations: string;
    export: string;
    deleteAll: string;
    highlight: string;
    note: string;
  };
}) {
  const { annotations, deleteAnnotationsForDecision, exportAnnotations } = useAnnotations(decisionId);

  const handleExport = () => {
    const content = exportAnnotations(decisionId);
    const blob = new Blob([content], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `annotations-${decisionId}.txt`;
    a.click();
    URL.revokeObjectURL(url);
    onExport?.();
  };

  if (annotations.length === 0) {
    return (
      <div className="text-center py-8 text-fg-subtle">
        <svg className="w-10 h-10 mx-auto mb-2 text-fg-faint" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M7 8h10M7 12h4m1 8l-4-4H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-3l-4 4z" />
        </svg>
        <p className="text-sm">{translations.noAnnotations}</p>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-fg">{translations.title}</h3>
        <div className="flex gap-1">
          <button
            onClick={handleExport}
            className="p-1.5 text-fg-subtle hover:text-fg hover:bg-bg-muted rounded transition-colors"
            title={translations.export}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
            </svg>
          </button>
          <button
            onClick={() => deleteAnnotationsForDecision(decisionId)}
            className="p-1.5 text-fg-subtle hover:text-error hover:bg-error-subtle rounded transition-colors"
            title={translations.deleteAll}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
            </svg>
          </button>
        </div>
      </div>

      {annotations
        .sort((a, b) => a.startOffset - b.startOffset)
        .map((annotation) => (
          <button
            key={annotation.id}
            onClick={() => onAnnotationClick?.(annotation)}
            className="w-full text-left p-2 rounded-lg hover:bg-bg-subtle transition-colors group"
          >
            <div className="flex items-start gap-2">
              <span
                className={`shrink-0 w-3 h-3 rounded-full mt-1 ${HIGHLIGHT_COLORS[annotation.color].bg}`}
              />
              <div className="flex-1 min-w-0">
                <p className="text-sm text-fg line-clamp-2">"{annotation.selectedText}"</p>
                {annotation.note && (
                  <p className="text-xs text-fg-subtle mt-1 line-clamp-1">{annotation.note}</p>
                )}
                <div className="flex items-center gap-2 mt-1 text-xs text-fg-faint">
                  <span>{annotation.type === "note" ? translations.note : translations.highlight}</span>
                  <span>{new Date(annotation.createdAt).toLocaleDateString()}</span>
                </div>
              </div>
            </div>
          </button>
        ))}
    </div>
  );
}
