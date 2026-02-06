"use client";

import { useState, useMemo, useCallback } from "react";

// Canton codes and their SVG path data
// Simplified paths for Switzerland map
const CANTON_PATHS: Record<string, string> = {
  ZH: "M 305 95 L 325 85 L 345 90 L 355 105 L 350 120 L 335 130 L 315 125 L 300 115 Z",
  BE: "M 175 140 L 200 125 L 225 130 L 250 145 L 265 170 L 260 200 L 235 220 L 200 215 L 175 195 L 160 165 Z",
  LU: "M 255 135 L 275 125 L 290 135 L 295 155 L 280 170 L 260 165 L 250 150 Z",
  UR: "M 285 175 L 305 165 L 320 180 L 315 205 L 295 215 L 280 200 Z",
  SZ: "M 305 150 L 325 140 L 345 155 L 340 175 L 320 180 L 305 170 Z",
  OW: "M 260 165 L 280 155 L 290 170 L 280 185 L 260 180 Z",
  NW: "M 265 180 L 285 175 L 295 190 L 285 205 L 265 195 Z",
  GL: "M 345 155 L 365 145 L 380 160 L 375 180 L 355 185 L 345 170 Z",
  ZG: "M 290 135 L 310 125 L 320 140 L 315 155 L 300 155 L 290 145 Z",
  FR: "M 140 180 L 165 165 L 185 175 L 195 200 L 180 220 L 155 225 L 135 205 Z",
  SO: "M 200 100 L 225 90 L 245 100 L 250 120 L 235 135 L 210 130 L 195 115 Z",
  BS: "M 185 65 L 200 55 L 210 65 L 205 80 L 190 85 Z",
  BL: "M 175 80 L 195 70 L 215 80 L 220 100 L 200 110 L 180 105 L 170 90 Z",
  SH: "M 285 45 L 310 35 L 330 50 L 325 70 L 300 75 L 285 60 Z",
  AR: "M 365 95 L 380 85 L 395 95 L 390 110 L 375 115 L 365 105 Z",
  AI: "M 375 105 L 390 100 L 400 110 L 395 120 L 380 120 Z",
  SG: "M 350 90 L 375 80 L 400 90 L 415 115 L 400 140 L 375 150 L 355 140 L 345 115 Z",
  GR: "M 340 175 L 375 155 L 420 170 L 445 210 L 425 250 L 380 265 L 340 245 L 330 210 Z",
  AG: "M 235 85 L 265 75 L 285 90 L 285 115 L 265 125 L 240 120 L 230 100 Z",
  TG: "M 330 65 L 360 55 L 385 70 L 380 90 L 355 100 L 335 95 L 325 80 Z",
  TI: "M 280 235 L 315 220 L 345 240 L 355 280 L 330 310 L 295 300 L 270 270 Z",
  VD: "M 85 195 L 120 175 L 150 185 L 160 220 L 145 255 L 110 265 L 75 245 L 70 215 Z",
  VS: "M 150 225 L 185 210 L 230 220 L 265 250 L 250 290 L 200 305 L 155 290 L 135 255 Z",
  NE: "M 120 145 L 145 135 L 165 150 L 160 175 L 135 185 L 115 170 Z",
  GE: "M 50 235 L 75 225 L 90 245 L 80 270 L 55 275 L 40 255 Z",
  JU: "M 135 95 L 165 85 L 185 100 L 180 125 L 155 135 L 130 120 Z",
};

// Canton name translations
const CANTON_NAMES: Record<string, Record<string, string>> = {
  ZH: { de: "Zürich", fr: "Zurich", it: "Zurigo", en: "Zurich" },
  BE: { de: "Bern", fr: "Berne", it: "Berna", en: "Bern" },
  LU: { de: "Luzern", fr: "Lucerne", it: "Lucerna", en: "Lucerne" },
  UR: { de: "Uri", fr: "Uri", it: "Uri", en: "Uri" },
  SZ: { de: "Schwyz", fr: "Schwytz", it: "Svitto", en: "Schwyz" },
  OW: { de: "Obwalden", fr: "Obwald", it: "Obvaldo", en: "Obwalden" },
  NW: { de: "Nidwalden", fr: "Nidwald", it: "Nidvaldo", en: "Nidwalden" },
  GL: { de: "Glarus", fr: "Glaris", it: "Glarona", en: "Glarus" },
  ZG: { de: "Zug", fr: "Zoug", it: "Zugo", en: "Zug" },
  FR: { de: "Freiburg", fr: "Fribourg", it: "Friburgo", en: "Fribourg" },
  SO: { de: "Solothurn", fr: "Soleure", it: "Soletta", en: "Solothurn" },
  BS: { de: "Basel-Stadt", fr: "Bâle-Ville", it: "Basilea Città", en: "Basel-City" },
  BL: { de: "Basel-Landschaft", fr: "Bâle-Campagne", it: "Basilea Campagna", en: "Basel-Country" },
  SH: { de: "Schaffhausen", fr: "Schaffhouse", it: "Sciaffusa", en: "Schaffhausen" },
  AR: { de: "Appenzell A.Rh.", fr: "Appenzell R.-E.", it: "Appenzello Est.", en: "Appenzell A.Rh." },
  AI: { de: "Appenzell I.Rh.", fr: "Appenzell R.-I.", it: "Appenzello Int.", en: "Appenzell I.Rh." },
  SG: { de: "St. Gallen", fr: "Saint-Gall", it: "San Gallo", en: "St. Gallen" },
  GR: { de: "Graubünden", fr: "Grisons", it: "Grigioni", en: "Graubünden" },
  AG: { de: "Aargau", fr: "Argovie", it: "Argovia", en: "Aargau" },
  TG: { de: "Thurgau", fr: "Thurgovie", it: "Turgovia", en: "Thurgau" },
  TI: { de: "Tessin", fr: "Tessin", it: "Ticino", en: "Ticino" },
  VD: { de: "Waadt", fr: "Vaud", it: "Vaud", en: "Vaud" },
  VS: { de: "Wallis", fr: "Valais", it: "Vallese", en: "Valais" },
  NE: { de: "Neuenburg", fr: "Neuchâtel", it: "Neuchâtel", en: "Neuchâtel" },
  GE: { de: "Genf", fr: "Genève", it: "Ginevra", en: "Geneva" },
  JU: { de: "Jura", fr: "Jura", it: "Giura", en: "Jura" },
};

type SwissMapProps = {
  data: Record<string, number>;
  onCantonClick?: (canton: string) => void;
  selectedCanton?: string;
  language?: "de" | "fr" | "it" | "en";
  colorScheme?: "blue" | "federal";
};

export function SwissMap({
  data,
  onCantonClick,
  selectedCanton,
  language = "de",
  colorScheme = "blue",
}: SwissMapProps) {
  const [hoveredCanton, setHoveredCanton] = useState<string | null>(null);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });

  // Calculate color intensity based on value
  const { maxValue, getColor } = useMemo(() => {
    const values = Object.values(data);
    const max = Math.max(...values, 1);

    const getColor = (value: number) => {
      const intensity = value / max;
      if (colorScheme === "federal") {
        // Red gradient for federal theme
        const r = Math.round(220 + (255 - 220) * (1 - intensity));
        const g = Math.round(38 + (255 - 38) * (1 - intensity));
        const b = Math.round(38 + (255 - 38) * (1 - intensity));
        const a = 0.2 + intensity * 0.6;
        return `rgba(${r}, ${g}, ${b}, ${a})`;
      } else {
        // Blue gradient
        const a = 0.1 + intensity * 0.7;
        return `rgba(37, 99, 235, ${a})`;
      }
    };

    return { maxValue: max, getColor };
  }, [data, colorScheme]);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    setTooltipPosition({ x: e.clientX, y: e.clientY });
  }, []);

  const handleCantonClick = useCallback(
    (canton: string) => {
      onCantonClick?.(canton);
    },
    [onCantonClick]
  );

  const hoveredData = hoveredCanton
    ? {
        code: hoveredCanton,
        name: CANTON_NAMES[hoveredCanton]?.[language] || hoveredCanton,
        count: data[hoveredCanton] || 0,
      }
    : null;

  return (
    <div className="relative">
      <svg
        viewBox="0 0 460 340"
        className="w-full h-auto"
        onMouseMove={handleMouseMove}
      >
        {/* Map background */}
        <rect x="0" y="0" width="460" height="340" fill="transparent" />

        {/* Canton paths */}
        {Object.entries(CANTON_PATHS).map(([canton, path]) => {
          const value = data[canton] || 0;
          const isHovered = hoveredCanton === canton;
          const isSelected = selectedCanton === canton;

          return (
            <path
              key={canton}
              d={path}
              fill={getColor(value)}
              stroke={isSelected ? "var(--color-accent)" : isHovered ? "var(--color-fg)" : "var(--color-border)"}
              strokeWidth={isSelected ? 2 : isHovered ? 1.5 : 1}
              className="cursor-pointer transition-all duration-150"
              onMouseEnter={() => setHoveredCanton(canton)}
              onMouseLeave={() => setHoveredCanton(null)}
              onClick={() => handleCantonClick(canton)}
            />
          );
        })}

        {/* Canton labels */}
        {Object.entries(CANTON_PATHS).map(([canton, path]) => {
          // Calculate center of path (simplified)
          const coords = path.match(/\d+/g)?.map(Number) || [];
          const xs = coords.filter((_, i) => i % 2 === 0);
          const ys = coords.filter((_, i) => i % 2 === 1);
          const centerX = xs.reduce((a, b) => a + b, 0) / xs.length;
          const centerY = ys.reduce((a, b) => a + b, 0) / ys.length;

          return (
            <text
              key={`label-${canton}`}
              x={centerX}
              y={centerY}
              textAnchor="middle"
              dominantBaseline="middle"
              className="text-[8px] font-medium fill-current text-fg pointer-events-none select-none"
              style={{ opacity: hoveredCanton === canton ? 0 : 0.7 }}
            >
              {canton}
            </text>
          );
        })}
      </svg>

      {/* Tooltip */}
      {hoveredData && (
        <div
          className="fixed z-popover bg-bg-elevated border border-border rounded-lg shadow-lg px-3 py-2 pointer-events-none animate-fade-in"
          style={{
            left: tooltipPosition.x + 12,
            top: tooltipPosition.y + 12,
          }}
        >
          <div className="text-sm font-medium text-fg">{hoveredData.name}</div>
          <div className="text-xs text-fg-subtle">
            {hoveredData.count.toLocaleString()} decisions
          </div>
        </div>
      )}

      {/* Legend */}
      <div className="flex items-center justify-center gap-4 mt-4 text-xs text-fg-subtle">
        <div className="flex items-center gap-2">
          <div
            className="w-4 h-3 rounded"
            style={{ backgroundColor: getColor(0) }}
          />
          <span>0</span>
        </div>
        <div className="w-24 h-3 rounded" style={{
          background: colorScheme === "federal"
            ? "linear-gradient(to right, rgba(220, 38, 38, 0.2), rgba(220, 38, 38, 0.8))"
            : "linear-gradient(to right, rgba(37, 99, 235, 0.1), rgba(37, 99, 235, 0.8))"
        }} />
        <div className="flex items-center gap-2">
          <div
            className="w-4 h-3 rounded"
            style={{ backgroundColor: getColor(maxValue) }}
          />
          <span>{maxValue.toLocaleString()}</span>
        </div>
      </div>
    </div>
  );
}

// Canton grid as alternative visualization
export function CantonGrid({
  data,
  onCantonClick,
  language = "de",
}: {
  data: Record<string, number>;
  onCantonClick?: (canton: string) => void;
  language?: "de" | "fr" | "it" | "en";
}) {
  const maxValue = Math.max(...Object.values(data), 1);

  return (
    <div className="grid grid-cols-4 sm:grid-cols-6 lg:grid-cols-9 gap-2 sm:gap-3">
      {Object.entries(data)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([canton, count]) => {
          const intensity = count / maxValue;
          const cantonName = CANTON_NAMES[canton]?.[language] || canton;

          return (
            <button
              key={canton}
              onClick={() => onCantonClick?.(canton)}
              className="aspect-square flex flex-col items-center justify-center p-2 transition-all hover:scale-105 rounded-lg group cursor-pointer"
              style={{
                backgroundColor: `rgba(37, 99, 235, ${0.08 + intensity * 0.3})`,
              }}
              title={cantonName}
            >
              <div className="text-sm font-bold group-hover:text-accent transition-colors">
                {canton}
              </div>
              <div className="text-xs text-fg-subtle tabular-nums mt-0.5">
                {count >= 1000 ? `${(count / 1000).toFixed(0)}k` : count}
              </div>
            </button>
          );
        })}
    </div>
  );
}
