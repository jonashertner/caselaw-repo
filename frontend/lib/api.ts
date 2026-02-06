// API utilities for Swiss Case Law AI

export const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/+$/, "") ?? "http://localhost:8000";

export type SearchParams = {
  query: string;
  limit?: number;
  offset?: number;
  level?: string;
  canton?: string;
  language?: string;
  date_from?: string;
  date_to?: string;
};

export async function searchDecisions(
  params: SearchParams,
  apiKey?: string
): Promise<{
  hits: Array<{
    decision: any;
    score: number;
    snippet: string;
  }>;
  total?: number;
}> {
  const headers: HeadersInit = { "Content-Type": "application/json" };
  if (apiKey) headers["X-OpenAI-Key"] = apiKey;

  const response = await fetch(`${API_BASE}/api/search`, {
    method: "POST",
    headers,
    body: JSON.stringify(params),
  });

  if (!response.ok) {
    throw new Error(`Search failed: ${response.status}`);
  }

  return response.json();
}

export async function getAnswer(
  params: SearchParams,
  apiKey?: string
): Promise<{
  answer: string;
  citations: any[];
  hits_count: number;
}> {
  const headers: HeadersInit = { "Content-Type": "application/json" };
  if (apiKey) headers["X-OpenAI-Key"] = apiKey;

  const response = await fetch(`${API_BASE}/api/answer`, {
    method: "POST",
    headers,
    body: JSON.stringify({ ...params, limit: 20 }),
  });

  if (!response.ok) {
    throw new Error(`Answer generation failed: ${response.status}`);
  }

  return response.json();
}

export async function getDecision(id: string): Promise<any> {
  const response = await fetch(`${API_BASE}/api/decisions/${id}`);

  if (!response.ok) {
    throw new Error(`Failed to fetch decision: ${response.status}`);
  }

  return response.json();
}

export async function getStats(): Promise<any> {
  const response = await fetch(`${API_BASE}/api/stats`);

  if (!response.ok) {
    throw new Error(`Failed to fetch stats: ${response.status}`);
  }

  return response.json();
}

export async function getCitations(id: string): Promise<any[]> {
  const response = await fetch(`${API_BASE}/api/citations/${id}`);

  if (!response.ok) {
    return [];
  }

  return response.json();
}

export async function getCitedBy(ref: string): Promise<any[]> {
  const response = await fetch(`${API_BASE}/api/citing/${encodeURIComponent(ref)}`);

  if (!response.ok) {
    return [];
  }

  return response.json();
}
