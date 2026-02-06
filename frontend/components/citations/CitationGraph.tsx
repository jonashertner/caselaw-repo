"use client";

import { useEffect, useRef, useState, useCallback, useMemo } from "react";

type GraphNode = {
  id: string;
  label: string;
  level: "federal" | "cantonal";
  date?: string;
  isCurrent?: boolean;
};

type GraphLink = {
  source: string;
  target: string;
  type: "cites" | "cited_by";
};

type CitationGraphProps = {
  currentDecisionId: string;
  currentDecisionLabel: string;
  currentDecisionLevel: "federal" | "cantonal";
  citations: Array<{
    id: string;
    label: string;
    level: "federal" | "cantonal";
    date?: string;
    type: "cites" | "cited_by";
  }>;
  onNodeClick?: (id: string) => void;
  width?: number;
  height?: number;
};

// Simple force-directed graph implementation without D3
// Uses basic physics simulation
function useForceSimulation(
  nodes: GraphNode[],
  links: GraphLink[],
  width: number,
  height: number
) {
  const [positions, setPositions] = useState<Record<string, { x: number; y: number }>>({});
  const velocitiesRef = useRef<Record<string, { vx: number; vy: number }>>({});
  const animationRef = useRef<number | undefined>(undefined);

  useEffect(() => {
    // Initialize positions randomly around center
    const initialPositions: Record<string, { x: number; y: number }> = {};
    const initialVelocities: Record<string, { vx: number; vy: number }> = {};

    nodes.forEach((node, i) => {
      if (node.isCurrent) {
        // Place current node at center
        initialPositions[node.id] = { x: width / 2, y: height / 2 };
      } else {
        // Place other nodes in a circle around center
        const angle = (i / nodes.length) * 2 * Math.PI;
        const radius = Math.min(width, height) * 0.35;
        initialPositions[node.id] = {
          x: width / 2 + Math.cos(angle) * radius + (Math.random() - 0.5) * 50,
          y: height / 2 + Math.sin(angle) * radius + (Math.random() - 0.5) * 50,
        };
      }
      initialVelocities[node.id] = { vx: 0, vy: 0 };
    });

    setPositions(initialPositions);
    velocitiesRef.current = initialVelocities;

    // Run simulation
    const alpha = { value: 1 };
    const alphaDecay = 0.02;
    const alphaMin = 0.001;

    const tick = () => {
      if (alpha.value < alphaMin) {
        return;
      }

      const newPositions = { ...positions };
      const velocities = velocitiesRef.current;

      // Forces
      const centerStrength = 0.01;
      const chargeStrength = -300;
      const linkStrength = 0.05;
      const linkDistance = 120;

      nodes.forEach((node) => {
        if (!newPositions[node.id]) return;

        let fx = 0;
        let fy = 0;

        // Center force
        fx += (width / 2 - newPositions[node.id].x) * centerStrength;
        fy += (height / 2 - newPositions[node.id].y) * centerStrength;

        // Charge (repulsion) force
        nodes.forEach((other) => {
          if (node.id === other.id || !newPositions[other.id]) return;

          const dx = newPositions[node.id].x - newPositions[other.id].x;
          const dy = newPositions[node.id].y - newPositions[other.id].y;
          const dist = Math.sqrt(dx * dx + dy * dy) || 1;

          const force = (chargeStrength * alpha.value) / (dist * dist);
          fx += (dx / dist) * force;
          fy += (dy / dist) * force;
        });

        // Link force
        links.forEach((link) => {
          const sourceId = typeof link.source === "string" ? link.source : link.source;
          const targetId = typeof link.target === "string" ? link.target : link.target;

          if (node.id !== sourceId && node.id !== targetId) return;
          if (!newPositions[sourceId] || !newPositions[targetId]) return;

          const otherId = node.id === sourceId ? targetId : sourceId;
          const dx = newPositions[otherId].x - newPositions[node.id].x;
          const dy = newPositions[otherId].y - newPositions[node.id].y;
          const dist = Math.sqrt(dx * dx + dy * dy) || 1;

          const force = (dist - linkDistance) * linkStrength * alpha.value;
          fx += (dx / dist) * force;
          fy += (dy / dist) * force;
        });

        // Update velocity and position
        velocities[node.id] = {
          vx: (velocities[node.id]?.vx || 0) * 0.6 + fx,
          vy: (velocities[node.id]?.vy || 0) * 0.6 + fy,
        };

        // Keep current node more centered
        if (node.isCurrent) {
          velocities[node.id].vx *= 0.3;
          velocities[node.id].vy *= 0.3;
        }

        newPositions[node.id] = {
          x: Math.max(40, Math.min(width - 40, newPositions[node.id].x + velocities[node.id].vx)),
          y: Math.max(40, Math.min(height - 40, newPositions[node.id].y + velocities[node.id].vy)),
        };
      });

      setPositions(newPositions);
      alpha.value *= 1 - alphaDecay;

      animationRef.current = requestAnimationFrame(tick);
    };

    // Start simulation after a small delay to allow initial render
    const startTimer = setTimeout(() => {
      setPositions(initialPositions);
      animationRef.current = requestAnimationFrame(tick);
    }, 100);

    return () => {
      clearTimeout(startTimer);
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, [nodes, links, width, height]);

  return positions;
}

export function CitationGraph({
  currentDecisionId,
  currentDecisionLabel,
  currentDecisionLevel,
  citations,
  onNodeClick,
  width = 600,
  height = 400,
}: CitationGraphProps) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);

  // Build nodes and links
  const { nodes, links } = useMemo(() => {
    const nodeMap = new Map<string, GraphNode>();

    // Add current node
    nodeMap.set(currentDecisionId, {
      id: currentDecisionId,
      label: currentDecisionLabel,
      level: currentDecisionLevel,
      isCurrent: true,
    });

    // Add citation nodes
    citations.forEach((citation) => {
      if (!nodeMap.has(citation.id)) {
        nodeMap.set(citation.id, {
          id: citation.id,
          label: citation.label,
          level: citation.level,
          date: citation.date,
        });
      }
    });

    // Build links
    const linkList: GraphLink[] = citations.map((citation) => ({
      source: citation.type === "cites" ? currentDecisionId : citation.id,
      target: citation.type === "cites" ? citation.id : currentDecisionId,
      type: citation.type,
    }));

    return {
      nodes: Array.from(nodeMap.values()),
      links: linkList,
    };
  }, [currentDecisionId, currentDecisionLabel, currentDecisionLevel, citations]);

  const positions = useForceSimulation(nodes, links, width, height);

  const handleNodeClick = useCallback(
    (id: string) => {
      if (id !== currentDecisionId) {
        onNodeClick?.(id);
      }
    },
    [currentDecisionId, onNodeClick]
  );

  // Get color based on level
  const getNodeColor = (node: GraphNode) => {
    if (node.isCurrent) return "var(--color-accent)";
    return node.level === "federal" ? "var(--color-federal)" : "var(--color-cantonal)";
  };

  return (
    <div className="relative bg-bg-subtle rounded-xl overflow-hidden">
      <svg
        ref={svgRef}
        width={width}
        height={height}
        className="w-full h-auto"
        viewBox={`0 0 ${width} ${height}`}
      >
        {/* Arrow markers */}
        <defs>
          <marker
            id="arrow-cites"
            viewBox="0 0 10 10"
            refX="20"
            refY="5"
            markerWidth="6"
            markerHeight="6"
            orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--color-fg-faint)" />
          </marker>
          <marker
            id="arrow-cited"
            viewBox="0 0 10 10"
            refX="20"
            refY="5"
            markerWidth="6"
            markerHeight="6"
            orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--color-success)" />
          </marker>
        </defs>

        {/* Links */}
        {links.map((link, i) => {
          const sourcePos = positions[link.source];
          const targetPos = positions[link.target];
          if (!sourcePos || !targetPos) return null;

          return (
            <line
              key={`link-${i}`}
              x1={sourcePos.x}
              y1={sourcePos.y}
              x2={targetPos.x}
              y2={targetPos.y}
              stroke={link.type === "cites" ? "var(--color-fg-faint)" : "var(--color-success)"}
              strokeWidth={1.5}
              strokeOpacity={0.5}
              markerEnd={link.type === "cites" ? "url(#arrow-cites)" : "url(#arrow-cited)"}
            />
          );
        })}

        {/* Nodes */}
        {nodes.map((node) => {
          const pos = positions[node.id];
          if (!pos) return null;

          const isHovered = hoveredNode === node.id;
          const radius = node.isCurrent ? 20 : 14;

          return (
            <g
              key={node.id}
              transform={`translate(${pos.x}, ${pos.y})`}
              className={node.isCurrent ? "" : "cursor-pointer"}
              onMouseEnter={() => setHoveredNode(node.id)}
              onMouseLeave={() => setHoveredNode(null)}
              onClick={() => handleNodeClick(node.id)}
            >
              {/* Node circle */}
              <circle
                r={radius}
                fill={getNodeColor(node)}
                stroke={isHovered ? "var(--color-fg)" : "transparent"}
                strokeWidth={2}
                className="transition-all duration-150"
              />

              {/* Node label for current node */}
              {node.isCurrent && (
                <text
                  textAnchor="middle"
                  dy="0.35em"
                  className="text-[9px] font-bold fill-white pointer-events-none"
                >
                  {node.label.slice(0, 8)}
                </text>
              )}
            </g>
          );
        })}
      </svg>

      {/* Tooltip */}
      {hoveredNode && (
        <div className="absolute bottom-4 left-4 right-4 bg-bg-elevated border border-border rounded-lg p-3 shadow-lg">
          {(() => {
            const node = nodes.find((n) => n.id === hoveredNode);
            if (!node) return null;

            const citation = citations.find((c) => c.id === node.id);

            return (
              <>
                <div className="flex items-center gap-2">
                  <div
                    className="w-3 h-3 rounded-full"
                    style={{ backgroundColor: getNodeColor(node) }}
                  />
                  <span className="font-medium text-fg">{node.label}</span>
                  {node.isCurrent && (
                    <span className="text-xs text-fg-subtle">(current)</span>
                  )}
                </div>
                <div className="flex items-center gap-3 mt-1 text-xs text-fg-subtle">
                  <span className="capitalize">{node.level}</span>
                  {node.date && <span>{node.date}</span>}
                  {citation && (
                    <span className={citation.type === "cites" ? "text-fg-faint" : "text-success"}>
                      {citation.type === "cites" ? "Cited by this decision" : "Cites this decision"}
                    </span>
                  )}
                </div>
              </>
            );
          })()}
        </div>
      )}

      {/* Legend */}
      <div className="absolute top-3 right-3 flex flex-col gap-1 text-xs">
        <div className="flex items-center gap-2 bg-bg-elevated/90 px-2 py-1 rounded">
          <div className="w-2 h-2 rounded-full" style={{ backgroundColor: "var(--color-federal)" }} />
          <span className="text-fg-subtle">Federal</span>
        </div>
        <div className="flex items-center gap-2 bg-bg-elevated/90 px-2 py-1 rounded">
          <div className="w-2 h-2 rounded-full" style={{ backgroundColor: "var(--color-cantonal)" }} />
          <span className="text-fg-subtle">Cantonal</span>
        </div>
        <div className="flex items-center gap-2 bg-bg-elevated/90 px-2 py-1 rounded mt-1">
          <svg className="w-3 h-3" viewBox="0 0 10 10">
            <line x1="0" y1="5" x2="7" y2="5" stroke="var(--color-fg-faint)" strokeWidth="2" />
            <path d="M 5 2 L 10 5 L 5 8 z" fill="var(--color-fg-faint)" />
          </svg>
          <span className="text-fg-subtle">Cites</span>
        </div>
        <div className="flex items-center gap-2 bg-bg-elevated/90 px-2 py-1 rounded">
          <svg className="w-3 h-3" viewBox="0 0 10 10">
            <line x1="0" y1="5" x2="7" y2="5" stroke="var(--color-success)" strokeWidth="2" />
            <path d="M 5 2 L 10 5 L 5 8 z" fill="var(--color-success)" />
          </svg>
          <span className="text-fg-subtle">Cited by</span>
        </div>
      </div>
    </div>
  );
}

// Loading placeholder
export function CitationGraphSkeleton({ width = 600, height = 400 }: { width?: number; height?: number }) {
  return (
    <div
      className="bg-bg-subtle rounded-xl animate-pulse flex items-center justify-center"
      style={{ width, height }}
    >
      <div className="text-fg-subtle text-sm">Loading citation graph...</div>
    </div>
  );
}
