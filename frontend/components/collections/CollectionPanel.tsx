"use client";

import { useState, useCallback } from "react";
import { useCollections, Collection } from "@/hooks/useCollections";
import { useToast } from "@/hooks/useToast";

type CollectionPanelProps = {
  isOpen: boolean;
  onClose: () => void;
  onSelectCollection?: (collection: Collection) => void;
  selectedDecisionId?: string;
  translations: {
    title: string;
    newCollection: string;
    newFolder: string;
    name: string;
    description: string;
    create: string;
    cancel: string;
    delete: string;
    deleteConfirm: string;
    export: string;
    import: string;
    noCollections: string;
    decisions: string;
    addToCollection: string;
    removeFromCollection: string;
    close: string;
  };
};

function FolderTree({
  collection,
  level = 0,
  onSelect,
  onToggle,
  isExpanded,
  getChildren,
  selectedDecisionId,
  addDecision,
  removeDecision,
  isDecisionInCollection,
  onDelete,
  translations,
}: {
  collection: Collection;
  level?: number;
  onSelect?: (collection: Collection) => void;
  onToggle: (id: string) => void;
  isExpanded: (id: string) => boolean;
  getChildren: (id: string) => Collection[];
  selectedDecisionId?: string;
  addDecision: (collectionId: string, decisionId: string) => void;
  removeDecision: (collectionId: string, decisionId: string) => void;
  isDecisionInCollection: (collectionId: string, decisionId: string) => boolean;
  onDelete: (id: string) => void;
  translations: CollectionPanelProps["translations"];
}) {
  const children = getChildren(collection.id);
  const hasChildren = children.length > 0;
  const expanded = isExpanded(collection.id);
  const isInCollection = selectedDecisionId
    ? isDecisionInCollection(collection.id, selectedDecisionId)
    : false;

  const handleToggleDecision = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (!selectedDecisionId) return;

    if (isInCollection) {
      removeDecision(collection.id, selectedDecisionId);
    } else {
      addDecision(collection.id, selectedDecisionId);
    }
  };

  return (
    <div className="select-none">
      <div
        className={`group flex items-center gap-2 px-2 py-1.5 rounded-lg cursor-pointer transition-colors hover:bg-bg-subtle ${
          isInCollection ? "bg-accent-subtle" : ""
        }`}
        style={{ paddingLeft: `${level * 16 + 8}px` }}
        onClick={() => onSelect?.(collection)}
      >
        {/* Expand toggle */}
        <button
          onClick={(e) => {
            e.stopPropagation();
            onToggle(collection.id);
          }}
          className={`w-4 h-4 flex items-center justify-center text-fg-subtle transition-transform ${
            hasChildren ? "" : "invisible"
          } ${expanded ? "rotate-90" : ""}`}
        >
          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
        </button>

        {/* Folder icon */}
        <div
          className="w-4 h-4 rounded"
          style={{ backgroundColor: collection.color || "#3b82f6" }}
        />

        {/* Name */}
        <span className="flex-1 text-sm text-fg truncate">{collection.name}</span>

        {/* Decision count */}
        <span className="text-xs text-fg-subtle tabular-nums">
          {collection.decisions.length}
        </span>

        {/* Add/Remove button for decision mode */}
        {selectedDecisionId && (
          <button
            onClick={handleToggleDecision}
            className={`p-1 rounded transition-colors ${
              isInCollection
                ? "text-accent hover:text-accent-hover"
                : "text-fg-subtle hover:text-fg"
            }`}
            title={isInCollection ? translations.removeFromCollection : translations.addToCollection}
          >
            {isInCollection ? (
              <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
                <path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41L9 16.17z" />
              </svg>
            ) : (
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
              </svg>
            )}
          </button>
        )}

        {/* Delete button (shown on hover) */}
        <button
          onClick={(e) => {
            e.stopPropagation();
            onDelete(collection.id);
          }}
          className="opacity-0 group-hover:opacity-100 p-1 rounded text-fg-subtle hover:text-error transition-all"
          title={translations.delete}
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={1.5}
              d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
            />
          </svg>
        </button>
      </div>

      {/* Children */}
      {expanded && children.length > 0 && (
        <div>
          {children.map((child) => (
            <FolderTree
              key={child.id}
              collection={child}
              level={level + 1}
              onSelect={onSelect}
              onToggle={onToggle}
              isExpanded={isExpanded}
              getChildren={getChildren}
              selectedDecisionId={selectedDecisionId}
              addDecision={addDecision}
              removeDecision={removeDecision}
              isDecisionInCollection={isDecisionInCollection}
              onDelete={onDelete}
              translations={translations}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export function CollectionPanel({
  isOpen,
  onClose,
  onSelectCollection,
  selectedDecisionId,
  translations,
}: CollectionPanelProps) {
  const {
    rootCollections,
    getChildren,
    isExpanded,
    toggleExpanded,
    createCollection,
    deleteCollection,
    addDecision,
    removeDecision,
    exportToJson,
    exportToCsv,
  } = useCollections();
  const { success, error } = useToast();

  const [showNewForm, setShowNewForm] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDescription, setNewDescription] = useState("");
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);

  const handleCreate = useCallback(() => {
    if (!newName.trim()) return;

    createCollection(newName.trim(), undefined, undefined, newDescription.trim() || undefined);
    setNewName("");
    setNewDescription("");
    setShowNewForm(false);
    success("Collection created");
  }, [newName, newDescription, createCollection, success]);

  const handleDelete = useCallback(
    (id: string) => {
      if (confirmDelete === id) {
        deleteCollection(id, true);
        setConfirmDelete(null);
        success("Collection deleted");
      } else {
        setConfirmDelete(id);
      }
    },
    [confirmDelete, deleteCollection, success]
  );

  const handleExport = useCallback(
    (format: "json" | "csv") => {
      const content = format === "json" ? exportToJson() : exportToCsv();
      const blob = new Blob([content], {
        type: format === "json" ? "application/json" : "text/csv",
      });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `collections.${format}`;
      a.click();
      URL.revokeObjectURL(url);
      success(`Exported as ${format.toUpperCase()}`);
    },
    [exportToJson, exportToCsv, success]
  );

  const isDecisionInCollection = useCallback(
    (collectionId: string, decisionId: string) => {
      const collection = rootCollections.find((c) => c.id === collectionId) ||
        rootCollections.flatMap((c) => getChildren(c.id)).find((c) => c.id === collectionId);
      return collection?.decisions.includes(decisionId) || false;
    },
    [rootCollections, getChildren]
  );

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-modal bg-overlay" onClick={onClose}>
      <div
        className="absolute right-0 top-0 bottom-0 w-full max-w-sm bg-bg-elevated border-l border-border shadow-xl animate-slide-in-right overflow-hidden flex flex-col"
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
            onClick={() => setShowNewForm(true)}
            className="flex-1 flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium text-bg bg-fg rounded-lg hover:bg-fg/90 transition-colors"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            {translations.newCollection}
          </button>
          <button
            onClick={() => handleExport("json")}
            className="p-2 text-sm text-fg-subtle hover:text-fg hover:bg-bg-muted rounded-lg transition-colors"
            title={translations.export}
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
            </svg>
          </button>
        </div>

        {/* New collection form */}
        {showNewForm && (
          <div className="px-4 py-3 border-b border-border bg-bg-subtle">
            <input
              type="text"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder={translations.name}
              className="w-full mb-2"
              autoFocus
              onKeyDown={(e) => e.key === "Enter" && handleCreate()}
            />
            <input
              type="text"
              value={newDescription}
              onChange={(e) => setNewDescription(e.target.value)}
              placeholder={translations.description}
              className="w-full mb-3"
            />
            <div className="flex gap-2">
              <button
                onClick={handleCreate}
                disabled={!newName.trim()}
                className="flex-1 px-3 py-2 text-sm font-medium text-bg bg-accent rounded-lg hover:bg-accent-hover disabled:opacity-50 transition-colors"
              >
                {translations.create}
              </button>
              <button
                onClick={() => {
                  setShowNewForm(false);
                  setNewName("");
                  setNewDescription("");
                }}
                className="px-3 py-2 text-sm text-fg-subtle hover:text-fg transition-colors"
              >
                {translations.cancel}
              </button>
            </div>
          </div>
        )}

        {/* Collections list */}
        <div className="flex-1 overflow-auto p-2">
          {rootCollections.length === 0 ? (
            <div className="text-center py-8 text-fg-subtle">
              <svg className="w-12 h-12 mx-auto mb-3 text-fg-faint" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
              </svg>
              <p className="text-sm">{translations.noCollections}</p>
            </div>
          ) : (
            rootCollections.map((collection) => (
              <FolderTree
                key={collection.id}
                collection={collection}
                onSelect={onSelectCollection}
                onToggle={toggleExpanded}
                isExpanded={isExpanded}
                getChildren={getChildren}
                selectedDecisionId={selectedDecisionId}
                addDecision={addDecision}
                removeDecision={removeDecision}
                isDecisionInCollection={isDecisionInCollection}
                onDelete={handleDelete}
                translations={translations}
              />
            ))
          )}
        </div>

        {/* Delete confirmation */}
        {confirmDelete && (
          <div className="absolute inset-0 bg-overlay flex items-center justify-center p-4">
            <div className="bg-bg-elevated rounded-xl p-4 max-w-xs w-full shadow-xl">
              <p className="text-sm text-fg mb-4">{translations.deleteConfirm}</p>
              <div className="flex gap-2">
                <button
                  onClick={() => handleDelete(confirmDelete)}
                  className="flex-1 px-3 py-2 text-sm font-medium text-white bg-error rounded-lg hover:bg-error/90 transition-colors"
                >
                  {translations.delete}
                </button>
                <button
                  onClick={() => setConfirmDelete(null)}
                  className="flex-1 px-3 py-2 text-sm text-fg-subtle hover:text-fg transition-colors"
                >
                  {translations.cancel}
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
