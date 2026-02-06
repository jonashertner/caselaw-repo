"use client";

import { useCallback } from "react";
import { useLocalStorage } from "./useLocalStorage";

export type Collection = {
  id: string;
  name: string;
  description?: string;
  color?: string;
  parentId?: string | null;
  decisions: string[]; // Array of decision IDs
  createdAt: string;
  updatedAt: string;
};

type CollectionsState = {
  collections: Collection[];
  expandedFolders: string[];
};

const STORAGE_KEY = "swisslaw-collections";

const DEFAULT_COLORS = [
  "#ef4444", // red
  "#f97316", // orange
  "#eab308", // yellow
  "#22c55e", // green
  "#14b8a6", // teal
  "#3b82f6", // blue
  "#8b5cf6", // violet
  "#ec4899", // pink
];

function generateId(): string {
  return `col-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

export function useCollections() {
  const [state, setState] = useLocalStorage<CollectionsState>(STORAGE_KEY, {
    collections: [],
    expandedFolders: [],
  });

  // Get all collections
  const collections = state.collections;

  // Get root collections (no parent)
  const rootCollections = collections.filter((c) => !c.parentId);

  // Get children of a collection
  const getChildren = useCallback(
    (parentId: string) => {
      return collections.filter((c) => c.parentId === parentId);
    },
    [collections]
  );

  // Check if a collection is expanded
  const isExpanded = useCallback(
    (id: string) => state.expandedFolders.includes(id),
    [state.expandedFolders]
  );

  // Toggle folder expansion
  const toggleExpanded = useCallback(
    (id: string) => {
      setState((prev) => ({
        ...prev,
        expandedFolders: prev.expandedFolders.includes(id)
          ? prev.expandedFolders.filter((f) => f !== id)
          : [...prev.expandedFolders, id],
      }));
    },
    [setState]
  );

  // Create a new collection
  const createCollection = useCallback(
    (name: string, parentId?: string, color?: string, description?: string): Collection => {
      const now = new Date().toISOString();
      const newCollection: Collection = {
        id: generateId(),
        name,
        description,
        color: color || DEFAULT_COLORS[collections.length % DEFAULT_COLORS.length],
        parentId: parentId || null,
        decisions: [],
        createdAt: now,
        updatedAt: now,
      };

      setState((prev) => ({
        ...prev,
        collections: [...prev.collections, newCollection],
      }));

      return newCollection;
    },
    [collections.length, setState]
  );

  // Update a collection
  const updateCollection = useCallback(
    (id: string, updates: Partial<Pick<Collection, "name" | "description" | "color" | "parentId">>) => {
      setState((prev) => ({
        ...prev,
        collections: prev.collections.map((c) =>
          c.id === id
            ? { ...c, ...updates, updatedAt: new Date().toISOString() }
            : c
        ),
      }));
    },
    [setState]
  );

  // Delete a collection
  const deleteCollection = useCallback(
    (id: string, deleteChildren: boolean = false) => {
      setState((prev) => {
        let toDelete = new Set([id]);

        if (deleteChildren) {
          // Recursively find all children
          const findChildren = (parentId: string) => {
            prev.collections
              .filter((c) => c.parentId === parentId)
              .forEach((c) => {
                toDelete.add(c.id);
                findChildren(c.id);
              });
          };
          findChildren(id);
        } else {
          // Move children to root
          return {
            ...prev,
            collections: prev.collections
              .filter((c) => !toDelete.has(c.id))
              .map((c) => (c.parentId === id ? { ...c, parentId: null } : c)),
            expandedFolders: prev.expandedFolders.filter((f) => !toDelete.has(f)),
          };
        }

        return {
          ...prev,
          collections: prev.collections.filter((c) => !toDelete.has(c.id)),
          expandedFolders: prev.expandedFolders.filter((f) => !toDelete.has(f)),
        };
      });
    },
    [setState]
  );

  // Add a decision to a collection
  const addDecision = useCallback(
    (collectionId: string, decisionId: string) => {
      setState((prev) => ({
        ...prev,
        collections: prev.collections.map((c) =>
          c.id === collectionId && !c.decisions.includes(decisionId)
            ? {
                ...c,
                decisions: [...c.decisions, decisionId],
                updatedAt: new Date().toISOString(),
              }
            : c
        ),
      }));
    },
    [setState]
  );

  // Remove a decision from a collection
  const removeDecision = useCallback(
    (collectionId: string, decisionId: string) => {
      setState((prev) => ({
        ...prev,
        collections: prev.collections.map((c) =>
          c.id === collectionId
            ? {
                ...c,
                decisions: c.decisions.filter((d) => d !== decisionId),
                updatedAt: new Date().toISOString(),
              }
            : c
        ),
      }));
    },
    [setState]
  );

  // Move a decision between collections
  const moveDecision = useCallback(
    (decisionId: string, fromCollectionId: string, toCollectionId: string) => {
      setState((prev) => ({
        ...prev,
        collections: prev.collections.map((c) => {
          if (c.id === fromCollectionId) {
            return {
              ...c,
              decisions: c.decisions.filter((d) => d !== decisionId),
              updatedAt: new Date().toISOString(),
            };
          }
          if (c.id === toCollectionId && !c.decisions.includes(decisionId)) {
            return {
              ...c,
              decisions: [...c.decisions, decisionId],
              updatedAt: new Date().toISOString(),
            };
          }
          return c;
        }),
      }));
    },
    [setState]
  );

  // Check if a decision is in any collection
  const isDecisionSaved = useCallback(
    (decisionId: string): boolean => {
      return collections.some((c) => c.decisions.includes(decisionId));
    },
    [collections]
  );

  // Get collections containing a decision
  const getCollectionsForDecision = useCallback(
    (decisionId: string): Collection[] => {
      return collections.filter((c) => c.decisions.includes(decisionId));
    },
    [collections]
  );

  // Export collections as JSON
  const exportToJson = useCallback((): string => {
    return JSON.stringify({ collections, exportedAt: new Date().toISOString() }, null, 2);
  }, [collections]);

  // Export collections as CSV
  const exportToCsv = useCallback((): string => {
    const headers = ["Collection", "Parent", "Decision ID", "Created At"];
    const rows = collections.flatMap((c) =>
      c.decisions.map((d) => [
        c.name,
        collections.find((p) => p.id === c.parentId)?.name || "",
        d,
        c.createdAt,
      ])
    );

    return [
      headers.join(","),
      ...rows.map((r) => r.map((cell) => `"${cell}"`).join(",")),
    ].join("\n");
  }, [collections]);

  // Import collections from JSON
  const importFromJson = useCallback(
    (json: string): boolean => {
      try {
        const data = JSON.parse(json);
        if (data.collections && Array.isArray(data.collections)) {
          setState((prev) => ({
            ...prev,
            collections: [...prev.collections, ...data.collections],
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

  return {
    collections,
    rootCollections,
    getChildren,
    isExpanded,
    toggleExpanded,
    createCollection,
    updateCollection,
    deleteCollection,
    addDecision,
    removeDecision,
    moveDecision,
    isDecisionSaved,
    getCollectionsForDecision,
    exportToJson,
    exportToCsv,
    importFromJson,
  };
}
