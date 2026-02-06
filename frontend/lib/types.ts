// Shared types for Swiss Case Law AI

export type SearchDecision = {
  id: string;
  source_id: string;
  source_name: string;
  level: string;
  canton?: string | null;
  court?: string | null;
  docket?: string | null;
  decision_date?: string | null;
  title?: string | null;
  language?: string | null;
  url: string;
  pdf_url?: string | null;
};

export type SearchHit = {
  decision: SearchDecision;
  score: number;
  snippet: string;
};

export type AnswerCitation = {
  marker: string;
  decision_id: string;
  chunk_id: string;
  source_name: string;
  docket?: string | null;
  decision_date?: string | null;
  url: string;
};

export type AnswerResponse = {
  answer: string;
  citations: AnswerCitation[];
  hits_count: number;
};

export type SavedSearch = {
  id: string;
  query: string;
  filters: {
    level?: string;
    canton?: string;
    language?: string;
    dateFrom?: string;
    dateTo?: string;
  };
  createdAt: string;
};

export type DecisionFull = SearchDecision & {
  content_text?: string | null;
  error?: string;
};

export type StatsResponse = {
  total_decisions: number;
  federal_decisions: number;
  cantonal_decisions: number;
  decisions_by_canton: Record<string, number>;
  decisions_by_year: Record<string, number>;
  decisions_by_language: Record<string, number>;
  recent_decisions: { last_24h: number; last_7d: number; last_30d: number };
  coverage: { total_sources: number; indexed_sources: number; cantons_covered: number };
  sources: Array<{ id: string; name: string; level: string; canton?: string; count: number }>;
};

export type Lang = "de" | "fr" | "it" | "rm" | "en";

export const CANTONS = [
  "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR",
  "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG",
  "TI", "UR", "VD", "VS", "ZG", "ZH"
] as const;

export const LANGUAGES = ["de", "fr", "it", "en"] as const;

export const PAGE_SIZES = [25, 50, 100, 200] as const;
