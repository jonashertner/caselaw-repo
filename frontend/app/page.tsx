"use client";

import { useEffect, useMemo, useState, useCallback } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";

// Hooks
import { useTheme, ThemeToggle } from "@/hooks/useTheme";
import { useToast } from "@/hooks/useToast";
import { useRecentSearches } from "@/hooks/useLocalStorage";
import { useCollections } from "@/hooks/useCollections";
import { useAlerts } from "@/hooks/useAlerts";

// Components
import { SearchBar, SearchButton } from "@/components/search/SearchBar";
import { ResultCard, ResultCardSkeleton } from "@/components/results/ResultCard";
import { DecisionViewer } from "@/components/decision/DecisionViewer";
import { CommandPalette, useCommandPaletteState, CommandItem } from "@/components/layout/CommandPalette";
import { CollectionPanel } from "@/components/collections/CollectionPanel";
import { AlertManager, AlertBadge } from "@/components/alerts/AlertManager";

// Types
import type { SearchDecision, SearchHit, AnswerResponse, SavedSearch, Lang } from "@/lib/types";

// UI Translations
const translations = {
  en: {
    title: "Swiss Case Law",
    subtitle: "decisions",
    searchPlaceholder: "Search court decisions...",
    search: "Search",
    filters: "Filters",
    allLevels: "All levels",
    federal: "Federal",
    cantonal: "Cantonal",
    allCantons: "All cantons",
    anyLanguage: "Any language",
    sortBy: "Sort by",
    relevance: "Relevance",
    dateDesc: "Newest first",
    dateAsc: "Oldest first",
    resultsPerPage: "Results",
    results: "results",
    loadMore: "Load more",
    loading: "Loading...",
    searching: "Searching...",
    aiAnswer: "AI Answer",
    sources: "Sources",
    generating: "Generating answer...",
    noResults: "No results found",
    tryDifferent: "Try different keywords or broaden your filters",
    searchPrompt: "Search Swiss court decisions",
    searchHint: "Enter a legal term, case number, or topic",
    settings: "Settings",
    apiKey: "OpenAI API Key",
    keySet: "Key set",
    noKeySet: "No key set",
    clear: "Clear",
    clearAll: "Clear all",
    apiKeyNote: "Your API key is stored locally and never sent to our servers.",
    savedSearches: "Saved Searches",
    saveSearch: "Save Search",
    noSavedSearches: "No saved searches yet",
    savedSearchHint: "Click the bookmark icon to save your current search",
    delete: "Delete",
    close: "Close",
    source: "Source",
    pdf: "PDF",
    copy: "Copy",
    copied: "Copied!",
    menu: "Menu",
    stats: "Statistics",
    dataSources: "Data Sources",
    apiDocs: "API",
    ai: "AI",
    from: "From",
    to: "To",
    examples: "Examples:",
    exampleQueries: ["BGE 144 III 93", "Arbeitsvertrag Kündigung", "Art. 41 OR"],
    collections: "Collections",
    alerts: "Alerts",
    tableOfContents: "Table of Contents",
    error: "Failed to load decision",
    save: "Save",
    saved: "Saved",
    newCollection: "New Collection",
    newFolder: "New Folder",
    name: "Name",
    description: "Description",
    create: "Create",
    cancel: "Cancel",
    deleteConfirm: "Are you sure you want to delete this collection?",
    export: "Export",
    import: "Import",
    noCollections: "No collections yet",
    decisions: "decisions",
    addToCollection: "Add to collection",
    removeFromCollection: "Remove from collection",
    newAlert: "New Alert",
    createFromSearch: "Create from current search",
    query: "Query",
    frequency: "Frequency",
    daily: "Daily",
    weekly: "Weekly",
    enable: "Enable",
    disable: "Disable",
    check: "Check now",
    checking: "Checking...",
    noAlerts: "No alerts yet",
    newResults: "new",
    lastChecked: "Last checked",
    viewResults: "View results",
  },
  de: {
    title: "Schweizer Rechtsprechung",
    subtitle: "Entscheide",
    searchPlaceholder: "Gerichtsentscheide durchsuchen...",
    search: "Suchen",
    filters: "Filter",
    allLevels: "Alle Ebenen",
    federal: "Bund",
    cantonal: "Kantonal",
    allCantons: "Alle Kantone",
    anyLanguage: "Alle Sprachen",
    sortBy: "Sortieren",
    relevance: "Relevanz",
    dateDesc: "Neueste zuerst",
    dateAsc: "Älteste zuerst",
    resultsPerPage: "Ergebnisse",
    results: "Ergebnisse",
    loadMore: "Mehr laden",
    loading: "Laden...",
    searching: "Suche läuft...",
    aiAnswer: "KI-Antwort",
    sources: "Quellen",
    generating: "Antwort wird generiert...",
    noResults: "Keine Ergebnisse",
    tryDifferent: "Versuchen Sie andere Begriffe oder erweitern Sie die Filter",
    searchPrompt: "Schweizer Gerichtsentscheide durchsuchen",
    searchHint: "Rechtsbegriff, Aktenzeichen oder Thema eingeben",
    settings: "Einstellungen",
    apiKey: "OpenAI API-Schlüssel",
    keySet: "Schlüssel gesetzt",
    noKeySet: "Kein Schlüssel",
    clear: "Löschen",
    clearAll: "Alle löschen",
    apiKeyNote: "Ihr API-Schlüssel wird lokal gespeichert und nie an unsere Server gesendet.",
    savedSearches: "Gespeicherte Suchen",
    saveSearch: "Suche speichern",
    noSavedSearches: "Noch keine Suchen gespeichert",
    savedSearchHint: "Klicken Sie auf das Lesezeichen-Symbol, um Ihre aktuelle Suche zu speichern",
    delete: "Löschen",
    close: "Schliessen",
    source: "Quelle",
    pdf: "PDF",
    copy: "Kopieren",
    copied: "Kopiert!",
    menu: "Menu",
    stats: "Statistik",
    dataSources: "Datenquellen",
    apiDocs: "API",
    ai: "KI",
    from: "Von",
    to: "Bis",
    examples: "Beispiele:",
    exampleQueries: ["BGE 144 III 93", "Arbeitsvertrag Kündigung", "Art. 41 OR"],
    collections: "Sammlungen",
    alerts: "Benachrichtigungen",
    tableOfContents: "Inhaltsverzeichnis",
    error: "Entscheid konnte nicht geladen werden",
    save: "Speichern",
    saved: "Gespeichert",
    newCollection: "Neue Sammlung",
    newFolder: "Neuer Ordner",
    name: "Name",
    description: "Beschreibung",
    create: "Erstellen",
    cancel: "Abbrechen",
    deleteConfirm: "Möchten Sie diese Sammlung wirklich löschen?",
    export: "Exportieren",
    import: "Importieren",
    noCollections: "Noch keine Sammlungen",
    decisions: "Entscheide",
    addToCollection: "Zur Sammlung hinzufügen",
    removeFromCollection: "Aus Sammlung entfernen",
    newAlert: "Neue Benachrichtigung",
    createFromSearch: "Aus aktueller Suche erstellen",
    query: "Suchanfrage",
    frequency: "Häufigkeit",
    daily: "Täglich",
    weekly: "Wöchentlich",
    enable: "Aktivieren",
    disable: "Deaktivieren",
    check: "Jetzt prüfen",
    checking: "Wird geprüft...",
    noAlerts: "Noch keine Benachrichtigungen",
    newResults: "neu",
    lastChecked: "Zuletzt geprüft",
    viewResults: "Ergebnisse anzeigen",
  },
  fr: {
    title: "Jurisprudence Suisse",
    subtitle: "décisions",
    searchPlaceholder: "Rechercher des décisions...",
    search: "Rechercher",
    filters: "Filtres",
    allLevels: "Tous les niveaux",
    federal: "Fédéral",
    cantonal: "Cantonal",
    allCantons: "Tous les cantons",
    anyLanguage: "Toutes les langues",
    sortBy: "Trier par",
    relevance: "Pertinence",
    dateDesc: "Plus récent",
    dateAsc: "Plus ancien",
    resultsPerPage: "Résultats",
    results: "résultats",
    loadMore: "Charger plus",
    loading: "Chargement...",
    searching: "Recherche en cours...",
    aiAnswer: "Réponse IA",
    sources: "Sources",
    generating: "Génération en cours...",
    noResults: "Aucun résultat",
    tryDifferent: "Essayez d'autres termes ou élargissez les filtres",
    searchPrompt: "Rechercher la jurisprudence suisse",
    searchHint: "Entrez un terme juridique, un numéro de dossier ou un sujet",
    settings: "Paramètres",
    apiKey: "Clé API OpenAI",
    keySet: "Clé définie",
    noKeySet: "Pas de clé",
    clear: "Effacer",
    clearAll: "Tout effacer",
    apiKeyNote: "Votre clé API est stockée localement et jamais envoyée à nos serveurs.",
    savedSearches: "Recherches sauvegardées",
    saveSearch: "Sauvegarder",
    noSavedSearches: "Aucune recherche sauvegardée",
    savedSearchHint: "Cliquez sur le signet pour sauvegarder votre recherche actuelle",
    delete: "Supprimer",
    close: "Fermer",
    source: "Source",
    pdf: "PDF",
    copy: "Copier",
    copied: "Copié!",
    menu: "Menu",
    stats: "Statistiques",
    dataSources: "Sources de données",
    apiDocs: "API",
    ai: "IA",
    from: "Du",
    to: "Au",
    examples: "Exemples:",
    exampleQueries: ["ATF 144 III 93", "Contrat de travail résiliation", "Art. 41 CO"],
    collections: "Collections",
    alerts: "Alertes",
    tableOfContents: "Table des matières",
    error: "Échec du chargement",
    save: "Sauvegarder",
    saved: "Sauvegardé",
    newCollection: "Nouvelle collection",
    newFolder: "Nouveau dossier",
    name: "Nom",
    description: "Description",
    create: "Créer",
    cancel: "Annuler",
    deleteConfirm: "Voulez-vous vraiment supprimer cette collection?",
    export: "Exporter",
    import: "Importer",
    noCollections: "Aucune collection",
    decisions: "décisions",
    addToCollection: "Ajouter à la collection",
    removeFromCollection: "Retirer de la collection",
    newAlert: "Nouvelle alerte",
    createFromSearch: "Créer depuis la recherche",
    query: "Requête",
    frequency: "Fréquence",
    daily: "Quotidien",
    weekly: "Hebdomadaire",
    enable: "Activer",
    disable: "Désactiver",
    check: "Vérifier maintenant",
    checking: "Vérification...",
    noAlerts: "Aucune alerte",
    newResults: "nouveau",
    lastChecked: "Dernière vérification",
    viewResults: "Voir les résultats",
  },
  it: {
    title: "Giurisprudenza Svizzera",
    subtitle: "decisioni",
    searchPlaceholder: "Cerca decisioni...",
    search: "Cerca",
    filters: "Filtri",
    allLevels: "Tutti i livelli",
    federal: "Federale",
    cantonal: "Cantonale",
    allCantons: "Tutti i cantoni",
    anyLanguage: "Tutte le lingue",
    sortBy: "Ordina per",
    relevance: "Rilevanza",
    dateDesc: "Più recenti",
    dateAsc: "Più vecchi",
    resultsPerPage: "Risultati",
    results: "risultati",
    loadMore: "Carica altri",
    loading: "Caricamento...",
    searching: "Ricerca in corso...",
    aiAnswer: "Risposta IA",
    sources: "Fonti",
    generating: "Generazione in corso...",
    noResults: "Nessun risultato",
    tryDifferent: "Prova altri termini o amplia i filtri",
    searchPrompt: "Cerca nella giurisprudenza svizzera",
    searchHint: "Inserisci un termine legale, numero di pratica o argomento",
    settings: "Impostazioni",
    apiKey: "Chiave API OpenAI",
    keySet: "Chiave impostata",
    noKeySet: "Nessuna chiave",
    clear: "Cancella",
    clearAll: "Cancella tutto",
    apiKeyNote: "La chiave API è memorizzata localmente e mai inviata ai nostri server.",
    savedSearches: "Ricerche salvate",
    saveSearch: "Salva ricerca",
    noSavedSearches: "Nessuna ricerca salvata",
    savedSearchHint: "Clicca sul segnalibro per salvare la ricerca corrente",
    delete: "Elimina",
    close: "Chiudi",
    source: "Fonte",
    pdf: "PDF",
    copy: "Copia",
    copied: "Copiato!",
    menu: "Menu",
    stats: "Statistiche",
    dataSources: "Fonti dati",
    apiDocs: "API",
    ai: "IA",
    from: "Da",
    to: "A",
    examples: "Esempi:",
    exampleQueries: ["DTF 144 III 93", "Contratto di lavoro disdetta", "Art. 41 CO"],
    collections: "Collezioni",
    alerts: "Avvisi",
    tableOfContents: "Sommario",
    error: "Caricamento fallito",
    save: "Salva",
    saved: "Salvato",
    newCollection: "Nuova collezione",
    newFolder: "Nuova cartella",
    name: "Nome",
    description: "Descrizione",
    create: "Crea",
    cancel: "Annulla",
    deleteConfirm: "Vuoi davvero eliminare questa collezione?",
    export: "Esporta",
    import: "Importa",
    noCollections: "Nessuna collezione",
    decisions: "decisioni",
    addToCollection: "Aggiungi alla collezione",
    removeFromCollection: "Rimuovi dalla collezione",
    newAlert: "Nuovo avviso",
    createFromSearch: "Crea dalla ricerca",
    query: "Query",
    frequency: "Frequenza",
    daily: "Giornaliero",
    weekly: "Settimanale",
    enable: "Attiva",
    disable: "Disattiva",
    check: "Controlla ora",
    checking: "Controllo...",
    noAlerts: "Nessun avviso",
    newResults: "nuovo",
    lastChecked: "Ultimo controllo",
    viewResults: "Vedi risultati",
  },
  rm: {
    title: "Giurisprudenza Svizra",
    subtitle: "decisiuns",
    searchPlaceholder: "Tschertgar decisiuns...",
    search: "Tschertgar",
    filters: "Filters",
    allLevels: "Tut ils nivels",
    federal: "Federal",
    cantonal: "Chantunal",
    allCantons: "Tut ils chantuns",
    anyLanguage: "Tuttas linguas",
    sortBy: "Zavrar tenor",
    relevance: "Relevanza",
    dateDesc: "Ils pli novs",
    dateAsc: "Ils pli vegls",
    resultsPerPage: "Resultats",
    results: "resultats",
    loadMore: "Chargiar dapli",
    loading: "Chargiar...",
    searching: "Tschertgar...",
    aiAnswer: "Resposta IA",
    sources: "Funtaunas",
    generating: "Generar...",
    noResults: "Nagins resultats",
    tryDifferent: "Empruvai auters terms u schlargiai ils filters",
    searchPrompt: "Tschertgar en la giurisprudenza svizra",
    searchHint: "Endatai in term giuridic, numer d'act u tema",
    settings: "Parameters",
    apiKey: "Clav API OpenAI",
    keySet: "Clav definida",
    noKeySet: "Nagina clav",
    clear: "Stizzar",
    clearAll: "Stizzar tut",
    apiKeyNote: "Vossa clav API vegn memorisada localmain e mai tramessa a noss servers.",
    savedSearches: "Tschertgas memorisadas",
    saveSearch: "Memorisar tschertga",
    noSavedSearches: "Anc naginas tschertgas memorisadas",
    savedSearchHint: "Cliccar sin il segnsudel per memorisar la tschertga actuala",
    delete: "Stizzar",
    close: "Serrar",
    source: "Funtauna",
    pdf: "PDF",
    copy: "Copiar",
    copied: "Copià!",
    menu: "Menu",
    stats: "Statistica",
    dataSources: "Funtaunas da datas",
    apiDocs: "API",
    ai: "IA",
    from: "Da",
    to: "Enfin",
    examples: "Exempels:",
    exampleQueries: ["BGE 144 III 93", "Contract da lavur disditga", "Art. 41 OR"],
    collections: "Collecziuns",
    alerts: "Avis",
    tableOfContents: "Tavla da cuntegn",
    error: "Errur da chargiar",
    save: "Memorisar",
    saved: "Memorisà",
    newCollection: "Nova collecziun",
    newFolder: "Nov ordinatur",
    name: "Num",
    description: "Descripziun",
    create: "Crear",
    cancel: "Interrumper",
    deleteConfirm: "Vulais Vus propi stizzar questa collecziun?",
    export: "Exportar",
    import: "Importar",
    noCollections: "Naginas collecziuns",
    decisions: "decisiuns",
    addToCollection: "Agiuntar a la collecziun",
    removeFromCollection: "Allontanar da la collecziun",
    newAlert: "Nov avis",
    createFromSearch: "Crear da la tschertga",
    query: "Query",
    frequency: "Frequenza",
    daily: "Mintgadi",
    weekly: "Mintg'emna",
    enable: "Activar",
    disable: "Deactivar",
    check: "Controllar ussa",
    checking: "Controllar...",
    noAlerts: "Nagins avis",
    newResults: "nov",
    lastChecked: "Ultima controlla",
    viewResults: "Mussar resultats",
  },
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/+$/, "") ?? "http://localhost:8000";

const CANTONS = ["AG","AI","AR","BE","BL","BS","FR","GE","GL","GR","JU","LU","NE","NW","OW","SG","SH","SO","SZ","TG","TI","UR","VD","VS","ZG","ZH"];
const LANGUAGES = ["de", "fr", "it", "en"];
const PAGE_SIZES = [25, 50, 100, 200];

export default function HomePage() {
  const router = useRouter();
  const { success } = useToast();
  const { addSearch, searches: recentSearches } = useRecentSearches();
  const { isDecisionSaved, addDecision, collections } = useCollections();
  const { hasNewResults } = useAlerts();
  const commandPalette = useCommandPaletteState();

  // UI Language
  const [lang, setLang] = useState<Lang>("de");
  const t = translations[lang];

  // Search state
  const [query, setQuery] = useState("");
  const [level, setLevel] = useState("");
  const [canton, setCanton] = useState("");
  const [docLang, setDocLang] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [sortBy, setSortBy] = useState<"relevance" | "date_desc" | "date_asc">("relevance");
  const [pageSize, setPageSize] = useState(50);

  // Results
  const [hits, setHits] = useState<SearchHit[]>([]);
  const [loading, setLoading] = useState(false);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [totalCount, setTotalCount] = useState<number | null>(null);
  const [hasSearched, setHasSearched] = useState(false);

  // AI
  const [aiEnabled, setAiEnabled] = useState(true);
  const [answer, setAnswer] = useState<AnswerResponse | null>(null);
  const [answerLoading, setAnswerLoading] = useState(false);

  // UI state
  const [showFilters, setShowFilters] = useState(false);
  const [showMenu, setShowMenu] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [showAiPanel, setShowAiPanel] = useState(false);
  const [activeDecision, setActiveDecision] = useState<any | null>(null);
  const [decisionLoading, setDecisionLoading] = useState(false);
  const [showCollections, setShowCollections] = useState(false);
  const [showAlerts, setShowAlerts] = useState(false);
  const [selectedDecisionForCollection, setSelectedDecisionForCollection] = useState<string | null>(null);

  // Settings
  const [apiKey, setApiKey] = useState("");

  // Saved searches
  const [savedSearches, setSavedSearches] = useState<SavedSearch[]>([]);
  const [showSavedSearches, setShowSavedSearches] = useState(false);

  useEffect(() => {
    const saved = localStorage.getItem("openai_api_key");
    if (saved) setApiKey(saved);
    const savedLang = localStorage.getItem("ui_lang") as Lang;
    if (savedLang && translations[savedLang]) setLang(savedLang);
    const savedSearchesStr = localStorage.getItem("saved_searches");
    if (savedSearchesStr) {
      try {
        setSavedSearches(JSON.parse(savedSearchesStr));
      } catch {}
    }
  }, []);

  const saveCurrentSearch = () => {
    if (!query.trim()) return;
    const newSearch: SavedSearch = {
      id: Date.now().toString(),
      query: query.trim(),
      filters: {
        level: level || undefined,
        canton: canton || undefined,
        language: docLang || undefined,
        dateFrom: dateFrom || undefined,
        dateTo: dateTo || undefined,
      },
      createdAt: new Date().toISOString(),
    };
    const updated = [newSearch, ...savedSearches].slice(0, 20);
    setSavedSearches(updated);
    localStorage.setItem("saved_searches", JSON.stringify(updated));
    success(t.saveSearch);
  };

  const deleteSavedSearch = (id: string) => {
    const updated = savedSearches.filter((s) => s.id !== id);
    setSavedSearches(updated);
    localStorage.setItem("saved_searches", JSON.stringify(updated));
  };

  const loadSavedSearch = (search: SavedSearch) => {
    setQuery(search.query);
    setLevel(search.filters.level || "");
    setCanton(search.filters.canton || "");
    setDocLang(search.filters.language || "");
    setDateFrom(search.filters.dateFrom || "");
    setDateTo(search.filters.dateTo || "");
    setShowSavedSearches(false);
    setTimeout(() => runSearch(0, search.query), 150);
  };

  const saveApiKey = (key: string) => {
    setApiKey(key);
    key ? localStorage.setItem("openai_api_key", key) : localStorage.removeItem("openai_api_key");
  };

  const changeLang = (newLang: Lang) => {
    setLang(newLang);
    localStorage.setItem("ui_lang", newLang);
  };

  const getHeaders = (): HeadersInit => {
    const h: Record<string, string> = { "content-type": "application/json" };
    if (apiKey) h["X-OpenAI-Key"] = apiKey;
    return h;
  };

  // Active filters
  const activeFilters = useMemo(() => {
    const filters: { key: string; label: string; value: string }[] = [];
    if (level) filters.push({ key: "level", label: level === "federal" ? t.federal : t.cantonal, value: level });
    if (canton) filters.push({ key: "canton", label: canton, value: canton });
    if (docLang) filters.push({ key: "docLang", label: docLang.toUpperCase(), value: docLang });
    if (dateFrom) filters.push({ key: "dateFrom", label: `${t.from}: ${dateFrom}`, value: dateFrom });
    if (dateTo) filters.push({ key: "dateTo", label: `${t.to}: ${dateTo}`, value: dateTo });
    return filters;
  }, [level, canton, docLang, dateFrom, dateTo, t]);

  const clearFilter = (key: string) => {
    if (key === "level") setLevel("");
    if (key === "canton") setCanton("");
    if (key === "docLang") setDocLang("");
    if (key === "dateFrom") setDateFrom("");
    if (key === "dateTo") setDateTo("");
  };

  const clearAllFilters = () => {
    setLevel("");
    setCanton("");
    setDocLang("");
    setDateFrom("");
    setDateTo("");
  };

  // Sort hits client-side
  const sortedHits = useMemo(() => {
    if (sortBy === "relevance") return hits;
    return [...hits].sort((a, b) => {
      const dateA = a.decision.decision_date || "";
      const dateB = b.decision.decision_date || "";
      return sortBy === "date_desc" ? dateB.localeCompare(dateA) : dateA.localeCompare(dateB);
    });
  }, [hits, sortBy]);

  const runSearch = useCallback(async (newOffset = 0, searchQuery?: string) => {
    const q = (searchQuery || query).trim();
    if (!q) return;

    // Track search
    addSearch(q);

    setLoading(true);
    setHasSearched(true);
    if (newOffset === 0) {
      setAnswer(null);
      setHits([]);
      setTotalCount(null);
    }
    setShowFilters(false);
    setShowMenu(false);
    setOffset(newOffset);

    const body: any = { query: q, limit: pageSize, offset: newOffset };
    if (level) body.level = level;
    if (canton) body.canton = canton;
    if (docLang) body.language = docLang;
    if (dateFrom) body.date_from = dateFrom;
    if (dateTo) body.date_to = dateTo;

    try {
      const res = await fetch(`${API_BASE}/api/search`, {
        method: "POST",
        headers: getHeaders(),
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        throw new Error(`Search failed: ${res.status} ${res.statusText}`);
      }
      const data = await res.json();
      const newHits = Array.isArray(data?.hits) ? data.hits : [];
      setHits(prev => newOffset === 0 ? newHits : [...prev, ...newHits]);
      setHasMore(newHits.length === pageSize);
      if (data.total != null) setTotalCount(data.total);
    } catch (err) {
      console.error("Search error:", err);
      setHits([]);
      setHasMore(false);
      // Show error toast if available
      if (typeof window !== "undefined") {
        const errorMsg = err instanceof Error ? err.message : "Search failed. Please try again.";
        console.error("Search failed:", errorMsg);
      }
    } finally {
      setLoading(false);
    }

    if (aiEnabled && newOffset === 0) {
      setAnswerLoading(true);
      try {
        const res = await fetch(`${API_BASE}/api/answer`, {
          method: "POST",
          headers: getHeaders(),
          body: JSON.stringify({ ...body, offset: 0, limit: 20 }),
        });
        const data = await res.json();
        setAnswer(data);
        if (data.answer) setShowAiPanel(true);
      } catch {
        setAnswer({ answer: "AI answer unavailable. Add your OpenAI API key in Settings.", citations: [], hits_count: 0 });
      } finally {
        setAnswerLoading(false);
      }
    }
  }, [query, level, canton, docLang, dateFrom, dateTo, pageSize, aiEnabled, apiKey, addSearch]);

  const openDecision = async (id: string) => {
    setDecisionLoading(true);
    setActiveDecision(null);
    setShowAiPanel(false);
    try {
      const res = await fetch(`${API_BASE}/api/decisions/${id}`);
      setActiveDecision(await res.json());
    } catch {
      setActiveDecision({ error: "failed" });
    } finally {
      setDecisionLoading(false);
    }
  };

  const handleExampleClick = (example: string) => {
    setQuery(example);
    setTimeout(() => runSearch(0, example), 100);
  };

  const handleSaveDecision = useCallback((decisionId: string) => {
    if (collections.length === 0) {
      setSelectedDecisionForCollection(decisionId);
      setShowCollections(true);
    } else {
      const defaultCollection = collections[0];
      addDecision(defaultCollection.id, decisionId);
      success(`Added to ${defaultCollection.name}`);
    }
  }, [collections, addDecision, success]);

  // Command palette commands
  const commands: CommandItem[] = useMemo(() => {
    const items: CommandItem[] = [];

    // Recent searches
    recentSearches.slice(0, 5).forEach((search, i) => {
      items.push({
        id: `recent-${i}`,
        label: search,
        category: "recent",
        onSelect: () => {
          setQuery(search);
          runSearch(0, search);
        },
      });
    });

    // Navigation
    items.push({
      id: "nav-home",
      label: t.search,
      category: "navigation",
      icon: <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" /></svg>,
      onSelect: () => router.push("/"),
    });

    items.push({
      id: "nav-stats",
      label: t.stats,
      category: "navigation",
      icon: <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" /></svg>,
      onSelect: () => router.push("/stats"),
    });

    // Actions
    items.push({
      id: "action-collections",
      label: t.collections,
      category: "actions",
      icon: <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" /></svg>,
      onSelect: () => setShowCollections(true),
    });

    items.push({
      id: "action-alerts",
      label: t.alerts,
      category: "actions",
      icon: <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" /></svg>,
      onSelect: () => setShowAlerts(true),
    });

    items.push({
      id: "action-settings",
      label: t.settings,
      category: "actions",
      icon: <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" /><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" /></svg>,
      onSelect: () => setShowSettings(true),
    });

    // Saved searches
    savedSearches.slice(0, 5).forEach((search, i) => {
      items.push({
        id: `saved-${search.id}`,
        label: search.query,
        description: Object.values(search.filters).filter(Boolean).join(", "),
        category: "saved",
        onSelect: () => loadSavedSearch(search),
      });
    });

    return items;
  }, [recentSearches, savedSearches, t, router, runSearch]);

  return (
    <div className="min-h-screen bg-bg flex flex-col">
      {/* Header */}
      <header className="sticky top-0 z-sticky bg-bg-elevated border-b border-border">
        <div className="safe-area-top" />
        <div className="px-4 py-3 lg:max-w-4xl lg:mx-auto">
          <div className="flex items-center justify-between mb-3">
            <Link href="/" className="flex items-baseline gap-2">
              <h1 className="text-xl font-semibold tracking-tight text-fg">{t.title}</h1>
            </Link>
            <div className="flex items-center gap-2">
              {/* Theme Toggle */}
              <ThemeToggle />

              {/* Alert Badge */}
              <AlertBadge onClick={() => setShowAlerts(true)} />

              {/* Language Switcher - Desktop */}
              <div className="hidden sm:flex items-center gap-0.5 text-xs">
                {(["de", "fr", "it", "rm", "en"] as Lang[]).map((l) => (
                  <button
                    key={l}
                    onClick={() => changeLang(l)}
                    className={`px-2 py-1 rounded font-medium transition-colors ${
                      lang === l ? "bg-fg text-bg" : "text-fg-subtle hover:text-fg"
                    }`}
                  >
                    {l.toUpperCase()}
                  </button>
                ))}
              </div>
              <button onClick={() => setShowMenu(!showMenu)} className="lg:hidden p-2 -mr-2 text-fg-subtle">
                <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4 6h16M4 12h16M4 18h16" />
                </svg>
              </button>
              <nav className="hidden lg:flex items-center gap-5 text-sm">
                <label className="flex items-center gap-2 cursor-pointer text-fg-subtle hover:text-fg">
                  <input type="checkbox" checked={aiEnabled} onChange={(e) => setAiEnabled(e.target.checked)} className="w-4 h-4" />
                  {t.ai}
                </label>
                <Link href="/stats" className="text-fg-subtle hover:text-fg">{t.stats}</Link>
                <button onClick={() => setShowCollections(true)} className="text-fg-subtle hover:text-fg">{t.collections}</button>
                <button onClick={() => setShowSettings(true)} className="text-fg-subtle hover:text-fg">{t.settings}</button>
              </nav>
            </div>
          </div>

          {/* Search Bar */}
          <div className="flex gap-2">
            <SearchBar
              value={query}
              onChange={setQuery}
              onSearch={(q) => runSearch(0, q)}
              placeholder={t.searchPlaceholder}
              loading={loading}
            />
            {/* Saved Searches Button */}
            <button
              onClick={() => setShowSavedSearches(!showSavedSearches)}
              className={`px-4 rounded-xl border transition-all ${
                savedSearches.length > 0
                  ? "border-warning bg-warning-subtle text-warning hover:bg-warning-muted"
                  : "border-border text-fg-subtle hover:border-border-strong bg-bg-elevated"
              }`}
              title={t.savedSearches}
            >
              <svg className="w-5 h-5" fill={savedSearches.length > 0 ? "currentColor" : "none"} stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" />
              </svg>
            </button>
            <button
              onClick={() => setShowFilters(!showFilters)}
              className={`px-4 rounded-xl border transition-all ${
                activeFilters.length > 0
                  ? "border-fg bg-fg text-bg"
                  : "border-border text-fg-subtle hover:border-border-strong bg-bg-elevated"
              }`}
            >
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
              </svg>
              {activeFilters.length > 0 && <span className="ml-1 text-xs">{activeFilters.length}</span>}
            </button>
            <SearchButton onClick={() => runSearch()} loading={loading} />
          </div>

          {/* Active Filter Pills */}
          {activeFilters.length > 0 && (
            <div className="flex flex-wrap items-center gap-2 mt-3">
              {activeFilters.map((f) => (
                <button
                  key={f.key}
                  onClick={() => clearFilter(f.key)}
                  className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-bg-muted text-sm text-fg hover:bg-bg-subtle transition-colors"
                >
                  {f.label}
                  <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              ))}
              <button onClick={clearAllFilters} className="text-sm text-fg-subtle hover:text-fg px-2">
                {t.clearAll}
              </button>
            </div>
          )}

          {/* Filter Panel */}
          {showFilters && (
            <div className="mt-3 pt-3 border-t border-border grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
              <select value={level} onChange={(e) => setLevel(e.target.value)}>
                <option value="">{t.allLevels}</option>
                <option value="federal">{t.federal}</option>
                <option value="cantonal">{t.cantonal}</option>
              </select>
              <select value={canton} onChange={(e) => setCanton(e.target.value)} disabled={level === "federal"}>
                <option value="">{t.allCantons}</option>
                {CANTONS.map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
              <select value={docLang} onChange={(e) => setDocLang(e.target.value)}>
                <option value="">{t.anyLanguage}</option>
                {LANGUAGES.map((l) => <option key={l} value={l}>{l.toUpperCase()}</option>)}
              </select>
              <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
              <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
              <select value={sortBy} onChange={(e) => setSortBy(e.target.value as any)}>
                <option value="relevance">{t.relevance}</option>
                <option value="date_desc">{t.dateDesc}</option>
                <option value="date_asc">{t.dateAsc}</option>
              </select>
            </div>
          )}

          {/* Saved Searches Dropdown */}
          {showSavedSearches && (
            <>
              <div className="fixed inset-0 z-dropdown" onClick={() => setShowSavedSearches(false)} />
              <div className="absolute right-4 mt-2 w-80 bg-bg-elevated rounded-xl border border-border shadow-lg z-dropdown animate-slide-up">
                <div className="px-4 py-3 border-b border-border flex items-center justify-between">
                  <span className="font-medium text-sm">{t.savedSearches}</span>
                  {query.trim() && (
                    <button onClick={saveCurrentSearch} className="text-xs px-3 py-1.5 bg-fg text-bg rounded-lg hover:bg-fg/90">
                      {t.saveSearch}
                    </button>
                  )}
                </div>
                <div className="max-h-80 overflow-auto">
                  {savedSearches.length === 0 ? (
                    <div className="p-4 text-center text-sm text-fg-subtle">
                      <p>{t.noSavedSearches}</p>
                      <p className="text-xs mt-1">{t.savedSearchHint}</p>
                    </div>
                  ) : (
                    savedSearches.map((search) => (
                      <div key={search.id} className="px-4 py-3 hover:bg-bg-subtle border-b border-border last:border-0 flex items-start justify-between gap-2">
                        <button onClick={() => loadSavedSearch(search)} className="flex-1 text-left">
                          <div className="font-medium text-sm text-fg truncate">{search.query}</div>
                          <div className="flex flex-wrap gap-1.5 mt-1">
                            {search.filters.level && <span className="text-xs px-1.5 py-0.5 bg-bg-muted rounded">{search.filters.level}</span>}
                            {search.filters.canton && <span className="text-xs px-1.5 py-0.5 bg-bg-muted rounded">{search.filters.canton}</span>}
                            {search.filters.language && <span className="text-xs px-1.5 py-0.5 bg-bg-muted rounded">{search.filters.language.toUpperCase()}</span>}
                          </div>
                        </button>
                        <button onClick={() => deleteSavedSearch(search.id)} className="shrink-0 p-1 text-fg-faint hover:text-error" title={t.delete}>
                          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                          </svg>
                        </button>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </>
          )}
        </div>
      </header>

      {/* Mobile Menu */}
      {showMenu && (
        <div className="lg:hidden fixed inset-0 z-modal bg-overlay" onClick={() => setShowMenu(false)}>
          <div className="absolute top-0 right-0 w-72 bg-bg-elevated h-full shadow-2xl animate-slide-in-right" onClick={(e) => e.stopPropagation()}>
            <div className="safe-area-top" />
            <div className="p-4 border-b border-border flex justify-between items-center">
              <span className="font-medium">{t.menu}</span>
              <button onClick={() => setShowMenu(false)} className="p-2 -mr-2 text-fg-faint">
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <div className="p-3">
              <div className="flex gap-1 p-1 bg-bg-muted rounded-lg mb-4">
                {(["de", "fr", "it", "rm", "en"] as Lang[]).map((l) => (
                  <button
                    key={l}
                    onClick={() => changeLang(l)}
                    className={`flex-1 py-2 rounded-md text-sm font-medium transition-colors ${
                      lang === l ? "bg-bg-elevated text-fg shadow-sm" : "text-fg-subtle"
                    }`}
                  >
                    {l.toUpperCase()}
                  </button>
                ))}
              </div>
              <label className="flex items-center gap-3 p-3 rounded-xl hover:bg-bg-subtle cursor-pointer">
                <input type="checkbox" checked={aiEnabled} onChange={(e) => setAiEnabled(e.target.checked)} className="w-5 h-5" />
                <span>{t.ai}</span>
              </label>
              <Link href="/stats" className="flex items-center gap-3 p-3 rounded-xl hover:bg-bg-subtle">{t.stats}</Link>
              <button onClick={() => { setShowMenu(false); setShowCollections(true); }} className="w-full flex items-center gap-3 p-3 rounded-xl hover:bg-bg-subtle text-left">
                {t.collections}
              </button>
              <button onClick={() => { setShowMenu(false); setShowAlerts(true); }} className="w-full flex items-center gap-3 p-3 rounded-xl hover:bg-bg-subtle text-left">
                {t.alerts}
                {hasNewResults && <span className="ml-auto w-2 h-2 bg-accent rounded-full" />}
              </button>
              <button onClick={() => { setShowMenu(false); setShowSettings(true); }} className="w-full flex items-center justify-between p-3 rounded-xl hover:bg-bg-subtle text-left">
                <span>{t.settings}</span>
                {apiKey && <span className="text-xs text-success bg-success-subtle px-2 py-0.5 rounded-full">✓</span>}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Main Content */}
      <main className="flex-1 lg:max-w-4xl lg:mx-auto w-full lg:px-4 lg:py-6">
        <div className={`lg:grid lg:gap-8 ${aiEnabled && answer ? "lg:grid-cols-5" : ""}`}>
          {/* Results */}
          <section className={aiEnabled && answer ? "lg:col-span-3" : ""}>
            {/* Loading Skeletons */}
            {loading && hits.length === 0 && (
              <div className="divide-y divide-border">
                {[...Array(5)].map((_, i) => <ResultCardSkeleton key={i} />)}
              </div>
            )}

            {/* Empty State - Initial */}
            {!loading && !hasSearched && (
              <div className="flex items-center justify-center py-16 px-4">
                <div className="text-center max-w-md">
                  <div className="w-20 h-20 mx-auto mb-6 rounded-2xl bg-bg-muted flex items-center justify-center">
                    <svg className="w-10 h-10 text-fg-faint" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 6l3 1m0 0l-3 9a5.002 5.002 0 006.001 0M6 7l3 9M6 7l6-2m6 2l3-1m-3 1l-3 9a5.002 5.002 0 006.001 0M18 7l3 9m-3-9l-6-2m0-2v2m0 16V5m0 16H9m3 0h3" />
                    </svg>
                  </div>
                  <h2 className="text-xl font-semibold text-fg mb-2">{t.searchPrompt}</h2>
                  <p className="text-fg-subtle mb-6">{t.searchHint}</p>
                  <div className="text-sm text-fg-faint">
                    <span>{t.examples}</span>
                    <div className="flex flex-wrap justify-center gap-2 mt-2">
                      {t.exampleQueries.map((ex) => (
                        <button
                          key={ex}
                          onClick={() => handleExampleClick(ex)}
                          className="px-3 py-1.5 rounded-lg bg-bg-muted text-fg-subtle hover:bg-bg-subtle transition-colors"
                        >
                          {ex}
                        </button>
                      ))}
                    </div>
                  </div>
                  {/* Keyboard shortcut hint */}
                  <p className="mt-6 text-xs text-fg-faint">
                    Press <kbd className="px-1.5 py-0.5 bg-bg-muted rounded border border-border">⌘K</kbd> to open command palette
                  </p>
                </div>
              </div>
            )}

            {/* Empty State - No Results */}
            {!loading && hasSearched && hits.length === 0 && (
              <div className="flex items-center justify-center py-16 px-4">
                <div className="text-center max-w-sm">
                  <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-bg-muted flex items-center justify-center">
                    <svg className="w-8 h-8 text-fg-faint" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                  </div>
                  <h3 className="text-lg font-medium text-fg mb-1">{t.noResults}</h3>
                  <p className="text-fg-subtle text-sm">{t.tryDifferent}</p>
                </div>
              </div>
            )}

            {/* Results List */}
            {sortedHits.length > 0 && (
              <div>
                <div className="px-4 py-3 lg:px-0 flex items-center justify-between">
                  <span className="text-sm text-fg-subtle">
                    {sortedHits.length}{totalCount != null && totalCount > sortedHits.length ? ` / ${totalCount.toLocaleString()}` : ""} {t.results}
                  </span>
                  <div className="flex items-center gap-3">
                    <select value={sortBy} onChange={(e) => setSortBy(e.target.value as any)} className="text-sm border-0 bg-transparent text-fg-subtle pr-7 py-1 focus:ring-0 cursor-pointer">
                      <option value="relevance">{t.relevance}</option>
                      <option value="date_desc">{t.dateDesc}</option>
                      <option value="date_asc">{t.dateAsc}</option>
                    </select>
                    {aiEnabled && answer && (
                      <button onClick={() => setShowAiPanel(true)} className="lg:hidden px-3 py-1.5 rounded-full bg-fg text-bg text-xs font-medium">
                        {t.aiAnswer}
                      </button>
                    )}
                  </div>
                </div>

                <div className="divide-y divide-border">
                  {sortedHits.map((h, idx) => (
                    <ResultCard
                      key={`${h.decision.id}-${idx}`}
                      decision={h.decision}
                      score={h.score}
                      snippet={h.snippet}
                      query={query}
                      onClick={() => openDecision(h.decision.id)}
                      onSave={() => handleSaveDecision(h.decision.id)}
                      isSaved={isDecisionSaved(h.decision.id)}
                      translations={{
                        federal: t.federal,
                        cantonal: t.cantonal,
                        copy: t.copy,
                        copied: t.copied,
                        source: t.source,
                        pdf: t.pdf,
                        save: t.save,
                        saved: t.saved,
                      }}
                    />
                  ))}
                </div>

                {hasMore && (
                  <div className="px-4 py-6 lg:px-0">
                    <button
                      onClick={() => runSearch(offset + pageSize)}
                      disabled={loading}
                      className="w-full py-3 rounded-xl border border-border text-fg font-medium hover:bg-bg-subtle disabled:opacity-50 transition-colors"
                    >
                      {loading ? (
                        <span className="inline-flex items-center gap-2">
                          <span className="w-4 h-4 border-2 border-border border-t-accent rounded-full animate-spin" />
                          {t.loading}
                        </span>
                      ) : t.loadMore}
                    </button>
                  </div>
                )}
              </div>
            )}
          </section>

          {/* AI Sidebar - Desktop */}
          {aiEnabled && (
            <aside className="hidden lg:block lg:col-span-2">
              <div className="sticky top-24 bg-bg-elevated border border-border rounded-2xl overflow-hidden">
                <div className="px-5 py-4 border-b border-border flex items-center justify-between">
                  <span className="font-medium text-fg">{t.aiAnswer}</span>
                  {answer?.hits_count ? (
                    <span className="text-xs text-fg-faint bg-bg-muted px-2 py-1 rounded-full">
                      {answer.hits_count} {t.sources.toLowerCase()}
                    </span>
                  ) : null}
                </div>
                <div className="p-5">
                  {answerLoading && (
                    <div className="flex items-center gap-3 text-sm text-fg-subtle">
                      <span className="w-5 h-5 border-2 border-border border-t-accent rounded-full animate-spin" />
                      {t.generating}
                    </div>
                  )}
                  {!answerLoading && !answer && (
                    <p className="text-sm text-fg-subtle">{t.searchHint}</p>
                  )}
                  {answer && (
                    <>
                      <div className="text-sm text-fg leading-relaxed whitespace-pre-wrap">{answer.answer}</div>
                      {answer.citations?.length > 0 && (
                        <div className="mt-5 pt-4 border-t border-border">
                          <div className="text-xs font-medium text-fg-subtle mb-3">{t.sources}</div>
                          <div className="space-y-1.5">
                            {answer.citations.map((c) => (
                              <button
                                key={c.marker}
                                onClick={() => openDecision(c.decision_id)}
                                className="text-sm text-left w-full p-2.5 rounded-lg hover:bg-bg-subtle transition-colors border border-transparent hover:border-border"
                              >
                                <span className="text-fg font-medium">[{c.marker}]</span>{" "}
                                <span className="text-fg-subtle">{c.docket || c.source_name}</span>
                                {c.decision_date && <span className="text-fg-faint ml-2">{c.decision_date}</span>}
                              </button>
                            ))}
                          </div>
                        </div>
                      )}
                    </>
                  )}
                </div>
              </div>
            </aside>
          )}
        </div>
      </main>

      {/* Mobile AI Panel */}
      {showAiPanel && aiEnabled && answer && (
        <div className="lg:hidden fixed inset-0 z-modal bg-overlay" onClick={() => setShowAiPanel(false)}>
          <div className="absolute bottom-0 left-0 right-0 bg-bg-elevated rounded-t-2xl max-h-[80vh] flex flex-col animate-slide-up" onClick={(e) => e.stopPropagation()}>
            <div className="flex justify-center pt-3 pb-2">
              <div className="w-10 h-1 bg-bg-muted rounded-full" />
            </div>
            <div className="px-4 pb-3 flex items-center justify-between border-b border-border">
              <span className="font-medium">{t.aiAnswer}</span>
              <button onClick={() => setShowAiPanel(false)} className="p-2 -mr-2 text-fg-faint">
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <div className="flex-1 overflow-auto p-4">
              <div className="text-sm text-fg leading-relaxed whitespace-pre-wrap">{answer.answer}</div>
              {answer.citations?.length > 0 && (
                <div className="mt-5 pt-4 border-t border-border">
                  <div className="text-xs font-medium text-fg-subtle mb-3">{t.sources}</div>
                  {answer.citations.map((c) => (
                    <button
                      key={c.marker}
                      onClick={() => openDecision(c.decision_id)}
                      className="text-sm text-left w-full p-3 rounded-xl bg-bg-subtle mb-2"
                    >
                      <span className="text-fg font-medium">[{c.marker}]</span>{" "}
                      <span className="text-fg-subtle">{c.docket || c.source_name}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div className="safe-area-bottom" />
          </div>
        </div>
      )}

      {/* Decision Viewer Modal */}
      {activeDecision && (
        <DecisionViewer
          decision={activeDecision}
          onClose={() => setActiveDecision(null)}
          translations={{
            federal: t.federal,
            cantonal: t.cantonal,
            source: t.source,
            pdf: t.pdf,
            copy: t.copy,
            copied: t.copied,
            close: t.close,
            tableOfContents: t.tableOfContents,
            loading: t.loading,
            error: t.error,
          }}
        />
      )}

      {/* Settings Modal */}
      {showSettings && (
        <div className="fixed inset-0 z-modal bg-overlay flex items-end lg:items-center justify-center" onClick={() => setShowSettings(false)}>
          <div className="w-full lg:max-w-md bg-bg-elevated rounded-t-2xl lg:rounded-2xl animate-slide-up lg:animate-scale-in" onClick={(e) => e.stopPropagation()}>
            <div className="flex justify-center pt-3 pb-2 lg:hidden">
              <div className="w-10 h-1 bg-bg-muted rounded-full" />
            </div>
            <div className="px-4 py-3 border-b border-border flex items-center justify-between">
              <span className="font-semibold">{t.settings}</span>
              <button onClick={() => setShowSettings(false)} className="text-sm text-fg-subtle hover:text-fg">{t.close}</button>
            </div>
            <div className="p-4 space-y-5">
              <div>
                <label className="text-sm font-medium text-fg block mb-2">{t.apiKey}</label>
                <input
                  type="password"
                  value={apiKey}
                  onChange={(e) => saveApiKey(e.target.value)}
                  placeholder="sk-..."
                  className="w-full"
                />
                <div className="mt-2 flex items-center justify-between">
                  <span className={`text-sm ${apiKey ? "text-success" : "text-fg-faint"}`}>
                    {apiKey ? `${t.keySet}: ${apiKey.slice(0, 7)}...` : t.noKeySet}
                  </span>
                  {apiKey && (
                    <button onClick={() => saveApiKey("")} className="text-sm text-error hover:text-error/80">{t.clear}</button>
                  )}
                </div>
                <p className="mt-3 text-xs text-fg-faint">{t.apiKeyNote}</p>
              </div>
              <div>
                <label className="text-sm font-medium text-fg block mb-2">{t.resultsPerPage}</label>
                <select value={pageSize} onChange={(e) => setPageSize(Number(e.target.value))} className="w-full">
                  {PAGE_SIZES.map((s) => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>
            </div>
            <div className="safe-area-bottom" />
          </div>
        </div>
      )}

      {/* Command Palette */}
      <CommandPalette
        isOpen={commandPalette.isOpen}
        onClose={commandPalette.close}
        commands={commands}
        recentSearches={recentSearches}
        onSearch={(q) => {
          setQuery(q);
          runSearch(0, q);
        }}
        placeholder={t.searchPlaceholder}
      />

      {/* Collections Panel */}
      <CollectionPanel
        isOpen={showCollections}
        onClose={() => {
          setShowCollections(false);
          setSelectedDecisionForCollection(null);
        }}
        selectedDecisionId={selectedDecisionForCollection || undefined}
        translations={{
          title: t.collections,
          newCollection: t.newCollection,
          newFolder: t.newFolder,
          name: t.name,
          description: t.description,
          create: t.create,
          cancel: t.cancel,
          delete: t.delete,
          deleteConfirm: t.deleteConfirm,
          export: t.export,
          import: t.import,
          noCollections: t.noCollections,
          decisions: t.decisions,
          addToCollection: t.addToCollection,
          removeFromCollection: t.removeFromCollection,
          close: t.close,
        }}
      />

      {/* Alerts Manager */}
      <AlertManager
        isOpen={showAlerts}
        onClose={() => setShowAlerts(false)}
        onRunSearch={(q, filters) => {
          setQuery(q);
          if (filters.level) setLevel(filters.level);
          if (filters.canton) setCanton(filters.canton);
          if (filters.language) setDocLang(filters.language);
          if (filters.dateFrom) setDateFrom(filters.dateFrom);
          if (filters.dateTo) setDateTo(filters.dateTo);
          runSearch(0, q);
        }}
        currentQuery={query}
        currentFilters={{ level, canton, language: docLang, dateFrom, dateTo }}
        translations={{
          title: t.alerts,
          newAlert: t.newAlert,
          createFromSearch: t.createFromSearch,
          name: t.name,
          query: t.query,
          frequency: t.frequency,
          daily: t.daily,
          weekly: t.weekly,
          create: t.create,
          cancel: t.cancel,
          delete: t.delete,
          enable: t.enable,
          disable: t.disable,
          check: t.check,
          checking: t.checking,
          noAlerts: t.noAlerts,
          newResults: t.newResults,
          lastChecked: t.lastChecked,
          viewResults: t.viewResults,
          close: t.close,
        }}
      />
    </div>
  );
}
