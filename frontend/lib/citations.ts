// Citation pattern matching for Swiss court decisions

export type CitationMatch = {
  text: string;
  type: "bge" | "atf" | "dtf" | "docket" | "article";
  start: number;
  end: number;
  normalized?: string;
};

// Regex patterns for Swiss legal citations
const patterns = {
  // BGE/ATF/DTF citations: BGE 144 III 93, ATF 143 II 202
  bge: /\b(BGE|ATF|DTF)\s+(\d+)\s+([IVX]+)\s+(\d+)(?:\s+E\.\s*(\d+(?:\.\d+)*))?/g,

  // Federal court docket numbers: 1C_123/2024, 2C_456/2023, 5A_789/2022
  docket: /\b(\d[A-Z])_(\d+)\/(\d{4})\b/g,

  // Cantonal court patterns: e.g., OGer ZH LA180001
  cantonalDocket: /\b(OGer|VGer|BGer|HGer|StGer)\s+([A-Z]{2})\s+([A-Z0-9]+)\b/g,

  // Swiss legal articles: Art. 41 OR, Art. 8 BV, § 123 StGB
  article: /\b(Art\.|§)\s*(\d+(?:\s*(?:Abs\.|al\.|para\.)\s*\d+)?(?:\s*(?:lit\.|Bst\.)\s*[a-z])?)\s+(OR|ZGB|StGB|BV|CO|CC|CP|Cst\.?|SchKG|VwVG|BGG|ZPO|StPO|ATSG)/gi,
};

// Extract all citations from text
export function extractCitations(text: string): CitationMatch[] {
  const matches: CitationMatch[] = [];

  // BGE/ATF/DTF
  let match: RegExpExecArray | null;

  // Reset regex lastIndex
  patterns.bge.lastIndex = 0;
  while ((match = patterns.bge.exec(text)) !== null) {
    matches.push({
      text: match[0],
      type: "bge",
      start: match.index,
      end: match.index + match[0].length,
      normalized: `${match[1]} ${match[2]} ${match[3]} ${match[4]}`,
    });
  }

  // Docket numbers
  patterns.docket.lastIndex = 0;
  while ((match = patterns.docket.exec(text)) !== null) {
    matches.push({
      text: match[0],
      type: "docket",
      start: match.index,
      end: match.index + match[0].length,
      normalized: `${match[1]}_${match[2]}/${match[3]}`,
    });
  }

  // Articles
  patterns.article.lastIndex = 0;
  while ((match = patterns.article.exec(text)) !== null) {
    matches.push({
      text: match[0],
      type: "article",
      start: match.index,
      end: match.index + match[0].length,
    });
  }

  // Sort by position
  matches.sort((a, b) => a.start - b.start);

  // Remove overlapping matches
  const filtered: CitationMatch[] = [];
  let lastEnd = 0;
  for (const m of matches) {
    if (m.start >= lastEnd) {
      filtered.push(m);
      lastEnd = m.end;
    }
  }

  return filtered;
}

// Check if text contains citations
export function hasCitations(text: string): boolean {
  patterns.bge.lastIndex = 0;
  patterns.docket.lastIndex = 0;

  return patterns.bge.test(text) || patterns.docket.test(text);
}

// Parse a BGE reference to extract components
export function parseBgeReference(text: string): {
  collection: string;
  volume: number;
  part: string;
  page: number;
  section?: string;
} | null {
  patterns.bge.lastIndex = 0;
  const match = patterns.bge.exec(text);
  if (!match) return null;

  return {
    collection: match[1],
    volume: parseInt(match[2], 10),
    part: match[3],
    page: parseInt(match[4], 10),
    section: match[5],
  };
}

// Parse a docket number
export function parseDocketNumber(text: string): {
  chamber: string;
  number: number;
  year: number;
} | null {
  patterns.docket.lastIndex = 0;
  const match = patterns.docket.exec(text);
  if (!match) return null;

  return {
    chamber: match[1],
    number: parseInt(match[2], 10),
    year: parseInt(match[3], 10),
  };
}

// Generate search query from citation
export function citationToSearchQuery(citation: CitationMatch): string {
  if (citation.normalized) {
    return citation.normalized;
  }
  return citation.text;
}

// Build URL for BGE reference
export function getBgeUrl(volume: number, part: string, page: number): string {
  return `https://www.bger.ch/ext/eurospider/live/de/php/clir/http/index.php?lang=de&type=show_document&highlight_docid=atf://BGE-${volume}-${part}-${page}`;
}

// Build URL for docket number
export function getDocketUrl(chamber: string, number: number, year: number): string {
  return `https://www.bger.ch/ext/eurospider/live/de/php/aza/http/index.php?highlight_docid=aza://${year}-${chamber}_${number}&lang=de`;
}
