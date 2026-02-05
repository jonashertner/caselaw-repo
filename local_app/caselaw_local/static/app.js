/* Swiss Caselaw Local UI — vanilla JS */

const $ = (sel) => document.querySelector(sel);
const el = (tag, cls) => { const x=document.createElement(tag); if(cls) x.className=cls; return x; };

const state = {
  q: "",
  filters: { language: [], canton: [], source_id: [], level: [], date_from: null, date_to: null },
  page: 1,
  page_size: 20,
  sort: "relevance",
  results: [],
  facets: {},
  total: 0,
  total_capped: false,
  selectedIndex: -1,
  selectedId: null,
  suggest: [],
  suggestIndex: -1,
  error: null,
  errorSuggestion: null,
  didYouMean: null,
  stats: null,
  statsVisible: false,
  // Query builder state
  qbConditions: [],
  qbOperator: "AND",
  // Saved searches
  savedSearches: [],
  // Recent searches
  recentSearches: [],
};

let searchTimer = null;
let suggestTimer = null;
let updatePollTimer = null;
let isSearching = false;

function toast(title, body, kind="") {
  const t = $("#toast");
  t.innerHTML = "";
  const a = el("div","t-title"); a.textContent = title;
  const b = el("div","t-body"); b.textContent = body || "";
  t.appendChild(a); t.appendChild(b);
  t.classList.remove("hidden");
  clearTimeout(window.__toastTimer);
  window.__toastTimer = setTimeout(()=>t.classList.add("hidden"), 3800);
}

function setTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem("theme", theme);
}

function initTheme() {
  const saved = localStorage.getItem("theme");
  if(saved==="light" || saved==="dark") setTheme(saved);
  else setTheme("dark");
}

function ftsHelpHtml() {
  return `
  <div class="muted">
    <p>Query supports SQLite FTS5 syntax.</p>
    <pre><code>phrase search: "steuerpflicht"
boolean: steuer AND veranlagung
prefix: veranlag* 
column: title:"rückerstattung" 
column: docket:6B_123
grouping: (steuer OR abgabe) AND kanton:ZH</code></pre>
    <p>Filters on the left apply on top of full‑text matching.</p>
    <p class="mono">Keyboard: / focus · j/k navigate · enter open · esc close</p>
  </div>`;
}

async function api(path, opts={}) {
  const r = await fetch(path, { headers: { "Content-Type":"application/json" }, ...opts });
  if(!r.ok) {
    const txt = await r.text();
    throw new Error(txt || `HTTP ${r.status}`);
  }
  return await r.json();
}

function currentPayload() {
  const f = {...state.filters};
  // ensure empty arrays not null
  for(const k of ["language","canton","source_id","level"]) f[k] = Array.isArray(f[k]) ? f[k] : [];
  if(!f.date_from) delete f.date_from;
  if(!f.date_to) delete f.date_to;
  return {
    q: state.q,
    filters: f,
    page: state.page,
    page_size: state.page_size,
    sort: state.sort,
  };
}

function setSummary() {
  const s = $("#summary");
  const total = state.total_capped ? `${state.total}+` : `${state.total}`;
  s.textContent = state.q.trim() ? `${total} matches` : `${total} decisions`;
}

function setPageInfo() {
  $("#pageInfo").textContent = `Page ${state.page}`;
}

function chip(label, count, active, onClick) {
  const c = el("div", "chip" + (active ? " active":""));
  const t = el("span"); t.textContent = label || "—";
  const n = el("span","count"); n.textContent = `${count}`;
  c.appendChild(t); c.appendChild(n);
  c.addEventListener("click", onClick);
  return c;
}

function renderFacets() {
  const f = state.facets || {};
  const lang = $("#facetLanguage");
  const canton = $("#facetCanton");
  const source = $("#facetSource");
  lang.innerHTML=""; canton.innerHTML=""; source.innerHTML="";

  const langs = f.language || [];
  const cantons = f.canton || [];
  const sources = f.source_name || [];

  langs.forEach(x=>{
    const v = x.value || "";
    lang.appendChild(chip(v || "—", x.count||0, state.filters.language.includes(v), ()=>{
      toggleFilter("language", v);
    }));
  });
  cantons.forEach(x=>{
    const v = x.value || "";
    canton.appendChild(chip(v || "—", x.count||0, state.filters.canton.includes(v), ()=>{
      toggleFilter("canton", v);
    }));
  });
  sources.forEach(x=>{
    const v = x.value || "";
    // We don't have source_id in facet output; keep by name here.
    // For filtering by source_id, the backend supports source_id. If your dataset has stable source_id, change facet query to return it.
    source.appendChild(chip(v || "—", x.count||0, false, ()=>{
      // fallback: treat as a free-text constraint
      state.q = state.q ? `${state.q} "${v}"` : `"${v}"`;
      $("#q").value = state.q;
      scheduleSearch(0);
    }));
  });
}

function renderResults() {
  const list = $("#list");
  list.innerHTML = "";
  state.results.forEach((r, idx)=>{
    const card = el("div", "card" + (idx===state.selectedIndex ? " active":""));
    card.dataset.idx = String(idx);
    const title = el("div","card-title");
    title.textContent = r.title || r.docket || r.id;
    const meta = el("div","card-meta");
    meta.innerHTML = `
      <span>${escapeHtml(r.source_name || r.source_id || "")}</span>
      <span>${escapeHtml(r.canton || "")}</span>
      <span>${escapeHtml(r.language || "")}</span>
      <span>${escapeHtml(r.decision_date || "")}</span>
      <span class="kbd">${escapeHtml(r.docket || "")}</span>
    `;
    const sn = el("div","card-snippet");
    sn.innerHTML = (r.snippet || "").toString();

    card.appendChild(title);
    card.appendChild(meta);
    card.appendChild(sn);

    card.addEventListener("click", ()=>openIdx(idx));
    list.appendChild(card);
  });
}

function escapeHtml(s) {
  return (s||"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
}

function toggleFilter(key, value) {
  const arr = state.filters[key] || [];
  const i = arr.indexOf(value);
  if(i>=0) arr.splice(i,1);
  else arr.push(value);
  state.filters[key] = arr;
  state.page = 1;
  scheduleSearch(0);
}

async function doSearch() {
  const payload = currentPayload();
  state.error = null;
  state.errorSuggestion = null;
  state.didYouMean = null;
  isSearching = true;
  hideError();
  hideSuggest();
  showLoading(true);

  try{
    const data = await api("/api/search", { method:"POST", body: JSON.stringify(payload) });
    isSearching = false;
    showLoading(false);

    // Check for error response
    if(data.error) {
      isSearching = false;
      showLoading(false);
      state.error = data.message || "Search error";
      state.errorSuggestion = data.suggestion || null;
      state.results = [];
      state.facets = {};
      state.total = 0;
      showError(state.error, state.errorSuggestion);
      renderResults();
      setSummary();
      return;
    }

    state.results = data.results || [];
    state.facets = data.facets || {};
    state.total = data.total || 0;
    state.total_capped = !!data.total_capped;
    state.didYouMean = data.did_you_mean || null;
    state.selectedIndex = state.results.length ? 0 : -1;

    setSummary();
    renderFacets();
    renderResults();
    renderDidYouMean();
    setPageInfo();

    // Track successful searches
    if(state.q.trim()) {
      addRecentSearch(state.q);
    }

    // Handle URL-based document selection
    if(state.urlDocId) {
      const idx = state.results.findIndex(r => r.id === state.urlDocId);
      if(idx >= 0) {
        state.selectedIndex = idx;
        await openIdx(idx, true);
      }
      state.urlDocId = null; // Clear after handling
    } else if(state.selectedIndex>=0) {
      await openIdx(state.selectedIndex, /*focus*/false);
    }
  } catch(e){
    isSearching = false;
    showLoading(false);
    toast("Search error", e.message || String(e));
    $("#summary").textContent = "Error";
  }
}

function showLoading(isLoading) {
  const indicator = $("#loadingIndicator");
  const summary = $("#summary");
  if(indicator) {
    indicator.classList.toggle("hidden", !isLoading);
  }
  if(summary && isLoading) {
    summary.textContent = "";
  }
}

function showError(message, suggestion) {
  const errBox = $("#errorBox");
  if(!errBox) return;

  let html = `<div class="error-message">${escapeHtml(message)}</div>`;
  if(suggestion) {
    html += `<div class="error-suggestion">Did you mean: <a href="#" class="suggestion-link">${escapeHtml(suggestion)}</a>?</div>`;
  }
  errBox.innerHTML = html;
  errBox.classList.remove("hidden");

  // Bind click handler for suggestion
  const link = errBox.querySelector(".suggestion-link");
  if(link) {
    link.addEventListener("click", (e) => {
      e.preventDefault();
      $("#q").value = suggestion;
      state.q = suggestion;
      hideError();
      scheduleSearch(0);
    });
  }
}

function hideError() {
  const errBox = $("#errorBox");
  if(errBox) {
    errBox.classList.add("hidden");
    errBox.innerHTML = "";
  }
}

function renderDidYouMean() {
  const dymBox = $("#didYouMean");
  if(!dymBox) return;

  if(!state.didYouMean || state.results.length > 0) {
    dymBox.classList.add("hidden");
    dymBox.innerHTML = "";
    return;
  }

  dymBox.innerHTML = `Did you mean: <a href="#" class="suggestion-link">${escapeHtml(state.didYouMean)}</a>?`;
  dymBox.classList.remove("hidden");

  const link = dymBox.querySelector(".suggestion-link");
  if(link) {
    link.addEventListener("click", (e) => {
      e.preventDefault();
      $("#q").value = state.didYouMean;
      state.q = state.didYouMean;
      state.didYouMean = null;
      scheduleSearch(0);
    });
  }
}

function scheduleSearch(delayMs=250) {
  clearTimeout(searchTimer);
  clearTimeout(suggestTimer);
  searchTimer = setTimeout(doSearch, delayMs);
}

async function openIdx(idx, focus=true) {
  if(idx<0 || idx>=state.results.length) return;
  state.selectedIndex = idx;
  state.selectedId = state.results[idx].id;
  // update card highlight
  document.querySelectorAll(".card").forEach(c=>c.classList.remove("active"));
  const card = document.querySelector(`.card[data-idx="${idx}"]`);
  if(card) card.classList.add("active");
  if(focus && card) card.scrollIntoView({block:"nearest"});

  const id = state.selectedId;
  try{
    const doc = await api(`/api/doc/${encodeURIComponent(id)}`);
    $("#detailTitle").textContent = doc.title || doc.docket || doc.id;
    $("#detailMeta").innerHTML = `
      <span class="pill">${escapeHtml(doc.source_name || doc.source_id || "")}</span>
      <span class="pill">${escapeHtml(doc.canton || "")}</span>
      <span class="pill">${escapeHtml(doc.language || "")}</span>
      <span class="pill">${escapeHtml(doc.decision_date || "")}</span>
      <span class="pill mono">${escapeHtml(doc.docket || "")}</span>
    `;
    const url = doc.url || "#";
    const pdf = doc.pdf_url || "#";
    $("#detailUrl").href = url;
    $("#detailPdf").href = pdf;
    $("#detailPdf").classList.toggle("hidden", pdf==="#" || !pdf);
    $("#detailBody").textContent = doc.content_text || "(no text extracted)";
  } catch(e){
    toast("Open error", e.message || String(e));
  }
}

function nextPage(delta) {
  const np = Math.max(1, state.page + delta);
  if(np === state.page) return;
  state.page = np;
  scheduleSearch(0);
}

function bindInputs() {
  $("#q").addEventListener("input", (ev)=>{
    state.q = ev.target.value;
    state.page = 1;
    scheduleSearch(220);
    // Show suggestions while typing (before search completes)
    scheduleSuggest();
  });

  // Hide suggestions when focus leaves search input
  $("#q").addEventListener("blur", ()=>{
    // Delay to allow clicking on suggestions
    setTimeout(hideSuggest, 200);
  });

  $("#sort").addEventListener("change", (ev)=>{
    state.sort = ev.target.value;
    state.page = 1;
    scheduleSearch(0);
  });

  $("#dateFrom").addEventListener("change",(ev)=>{
    state.filters.date_from = ev.target.value || null;
    state.page = 1;
    scheduleSearch(0);
  });
  $("#dateTo").addEventListener("change",(ev)=>{
    state.filters.date_to = ev.target.value || null;
    state.page = 1;
    scheduleSearch(0);
  });

  $("#btnClear").addEventListener("click", ()=>{
    state.filters = { language: [], canton: [], source_id: [], level: [], date_from: null, date_to: null };
    $("#dateFrom").value = "";
    $("#dateTo").value = "";
    updateLevelToggle();
    state.page = 1;
    scheduleSearch(0);
  });

  $("#prev").addEventListener("click", ()=>nextPage(-1));
  $("#next").addEventListener("click", ()=>nextPage(+1));

  $("#btnTheme").addEventListener("click", ()=>{
    const cur = document.documentElement.getAttribute("data-theme") || "dark";
    setTheme(cur === "dark" ? "light" : "dark");
  });

  $("#btnHelp").addEventListener("click", ()=>{
    openModal("Help", ftsHelpHtml());
  });

  $("#modalClose").addEventListener("click", closeModal);
  $("#modal").addEventListener("click", (ev)=>{ if(ev.target.id==="modal") closeModal(); });

  $("#btnUpdate").addEventListener("click", startUpdate);

  // Stats toggle
  const btnStats = $("#btnStats");
  if(btnStats) {
    btnStats.addEventListener("click", toggleStats);
  }
}

function bindKeyboard() {
  window.addEventListener("keydown", (ev)=>{
    if(ev.key === "/" && document.activeElement !== $("#q")){
      ev.preventDefault();
      $("#q").focus();
      return;
    }

    if(ev.key === "Escape"){
      closeModal();
      hideSuggest();
      return;
    }

    // navigation only when not typing in input
    const isTyping = ["INPUT","TEXTAREA"].includes(document.activeElement.tagName);
    if(isTyping) return;

    if(ev.key === "j"){
      if(state.selectedIndex < state.results.length-1) openIdx(state.selectedIndex+1);
      return;
    }
    if(ev.key === "k"){
      if(state.selectedIndex > 0) openIdx(state.selectedIndex-1);
      return;
    }
    if(ev.key === "Enter"){
      // noop: detail already open
      return;
    }
  });
}

function openModal(title, html) {
  $("#modalTitle").textContent = title;
  $("#modalBody").innerHTML = html;
  $("#modal").classList.remove("hidden");
}

function closeModal() {
  $("#modal").classList.add("hidden");
}

async function loadStatus() {
  try{
    const st = await api("/api/status");
    const loc = st.local;
    if(!loc){
      $("#status").innerHTML = `No local dataset. Click <b>Update</b>.`;
      return;
    }
    $("#status").innerHTML = `Local: <span class="kbd">${loc.count}</span> docs · last update <span class="kbd">${escapeHtml(loc.last_update||"")}</span>`;
  } catch(e){
    $("#status").textContent = "Status unavailable";
  }
}

async function loadStats() {
  try {
    const data = await api("/api/stats");
    state.stats = data;
    renderStats();
  } catch(e) {
    console.error("Failed to load stats:", e);
  }
}

function toggleStats() {
  state.statsVisible = !state.statsVisible;
  const panel = $("#statsPanel");
  const btn = $("#btnStats");
  if(panel) {
    panel.classList.toggle("hidden", !state.statsVisible);
  }
  if(btn) {
    btn.textContent = state.statsVisible ? "Hide Stats" : "Show Stats";
  }
  if(state.statsVisible && !state.stats) {
    loadStats();
  }
}

function renderStats() {
  const panel = $("#statsContent");
  if(!panel || !state.stats) return;

  const s = state.stats;
  const total = s.total_decisions || 0;

  // Format number with commas
  const fmt = (n) => n.toLocaleString();

  // Date range
  const minYear = s.date_range?.min ? s.date_range.min.substring(0, 4) : "?";
  const maxYear = s.date_range?.max ? s.date_range.max.substring(0, 4) : "?";

  // Level split
  const federal = s.by_level?.find(l => l.level === "federal")?.count || 0;
  const cantonal = s.by_level?.find(l => l.level === "cantonal")?.count || 0;
  const federalPct = total > 0 ? Math.round((federal / total) * 100) : 0;
  const cantonalPct = total > 0 ? Math.round((cantonal / total) * 100) : 0;

  // Top languages (max 5)
  const topLangs = (s.by_language || []).slice(0, 5);
  const maxLangCount = topLangs.length > 0 ? topLangs[0].count : 1;

  // Top cantons (max 5)
  const topCantons = (s.by_canton || []).slice(0, 5);
  const maxCantonCount = topCantons.length > 0 ? topCantons[0].count : 1;

  let html = `
    <div class="stats-row">
      <div class="stats-label">Total Decisions</div>
      <div class="stats-value">${fmt(total)}</div>
    </div>
    <div class="stats-row">
      <div class="stats-label">Date Coverage</div>
      <div class="stats-value">${minYear} — ${maxYear}</div>
    </div>
    <div class="stats-row">
      <div class="stats-label">Federal / Cantonal</div>
      <div class="stats-value">${federalPct}% / ${cantonalPct}%</div>
    </div>

    <div class="stats-section">
      <div class="stats-section-title">Top Languages</div>
      ${topLangs.map(l => `
        <div class="stats-bar-row">
          <span class="stats-bar-label">${escapeHtml(l.language || "?")}</span>
          <div class="stats-bar-container">
            <div class="stats-bar" style="width: ${(l.count / maxLangCount) * 100}%"></div>
          </div>
          <span class="stats-bar-count">${l.percentage || 0}%</span>
        </div>
      `).join("")}
    </div>

    <div class="stats-section">
      <div class="stats-section-title">Top Cantons</div>
      ${topCantons.map(c => `
        <div class="stats-bar-row">
          <span class="stats-bar-label">${escapeHtml(c.canton || "?")}</span>
          <div class="stats-bar-container">
            <div class="stats-bar" style="width: ${(c.count / maxCantonCount) * 100}%"></div>
          </div>
          <span class="stats-bar-count">${fmt(c.count)}</span>
        </div>
      `).join("")}
    </div>
  `;

  panel.innerHTML = html;
}

async function startUpdate() {
  toast("Update", "Starting…");
  try{
    await api("/api/update", { method:"POST", body: JSON.stringify({}) });
    pollUpdate();
  } catch(e){
    toast("Update error", e.message || String(e));
  }
}

async function pollUpdate() {
  clearTimeout(updatePollTimer);
  try{
    const st = await api("/api/update/status");
    if(st.status === "running"){
      toast("Update", "Downloading / applying…");
      updatePollTimer = setTimeout(pollUpdate, 1100);
      return;
    }
    if(st.status === "done"){
      toast("Update", "Done");
      await loadStatus();
      scheduleSearch(0);
      return;
    }
    if(st.status === "error"){
      toast("Update failed", st.error || "unknown error");
      return;
    }
  } catch(e){
    toast("Update error", e.message || String(e));
  }
}

function hideSuggest(){
  $("#suggest").classList.add("hidden");
  $("#suggest").innerHTML = "";
  state.suggest = [];
  state.suggestIndex = -1;
}

async function doSuggest(){
  const q = state.q.trim();
  if(q.length < 2){ hideSuggest(); return; }
  try{
    const data = await api(`/api/suggest?q=${encodeURIComponent(q)}&limit=8`, { method:"GET" });
    const items = data.items || [];
    state.suggest = items;
    renderSuggest();
  } catch(_e){
    // ignore
  }
}

function scheduleSuggest(){
  clearTimeout(suggestTimer);
  suggestTimer = setTimeout(doSuggest, 180);
}

function renderSuggest(){
  const box = $("#suggest");
  box.innerHTML = "";
  // Don't show suggestions while search is in progress or if no suggestions
  if(!state.suggest.length || isSearching){ hideSuggest(); return; }

  state.suggest.forEach((it, idx)=>{
    const row = el("div","suggest-item");
    const left = el("div","suggest-left");
    const title = el("div","suggest-title"); title.textContent = it.title || it.docket || it.id;
    const meta = el("div","suggest-meta"); meta.textContent = `${it.source_name||""} · ${it.decision_date||""} · ${it.docket||""}`;
    left.appendChild(title); left.appendChild(meta);
    const k = el("div","suggest-kbd"); k.textContent = idx===0 ? "enter" : "";
    row.appendChild(left); row.appendChild(k);
    row.addEventListener("click", ()=>{
      $("#q").value = it.title ? `"${it.title}"` : (it.docket || it.id);
      state.q = $("#q").value;
      hideSuggest();
      scheduleSearch(0);
    });
    box.appendChild(row);
  });
  box.classList.remove("hidden");
}

// ============ Query Builder ============
function openQueryBuilder() {
  const qb = $("#queryBuilder");
  if(qb) qb.classList.remove("hidden");
}

function closeQueryBuilder() {
  const qb = $("#queryBuilder");
  if(qb) qb.classList.add("hidden");
}

function addQbCondition() {
  const field = $(".qb-field")?.value || "";
  const term = $(".qb-term")?.value.trim() || "";
  if(!term) return;

  const condition = field ? `${field}"${term}"` : term;
  state.qbConditions.push(condition);
  $(".qb-term").value = "";
  renderQbConditions();
  updateQbPreview();
}

function removeQbCondition(index) {
  state.qbConditions.splice(index, 1);
  renderQbConditions();
  updateQbPreview();
}

function renderQbConditions() {
  const container = $("#qbConditions");
  if(!container) return;
  container.innerHTML = "";

  state.qbConditions.forEach((cond, idx) => {
    const div = el("div", "qb-condition");
    const text = el("span", "qb-condition-text");
    text.textContent = cond;
    const removeBtn = el("button", "qb-condition-remove btn ghost");
    removeBtn.textContent = "×";
    removeBtn.addEventListener("click", () => removeQbCondition(idx));
    div.appendChild(text);
    div.appendChild(removeBtn);

    // Add operator toggle between conditions
    if(idx < state.qbConditions.length - 1) {
      const opDiv = el("div", "qb-operator-row");
      const andBtn = el("button", `qb-operator ${state.qbOperator === "AND" ? "active" : ""}`);
      andBtn.textContent = "AND";
      andBtn.addEventListener("click", () => { state.qbOperator = "AND"; renderQbConditions(); updateQbPreview(); });
      const orBtn = el("button", `qb-operator ${state.qbOperator === "OR" ? "active" : ""}`);
      orBtn.textContent = "OR";
      orBtn.addEventListener("click", () => { state.qbOperator = "OR"; renderQbConditions(); updateQbPreview(); });
      opDiv.appendChild(andBtn);
      opDiv.appendChild(orBtn);
      div.appendChild(opDiv);
    }

    container.appendChild(div);
  });
}

function updateQbPreview() {
  const preview = $("#qbPreview");
  if(!preview) return;
  const query = state.qbConditions.join(` ${state.qbOperator} `);
  preview.textContent = query || "(empty)";
}

function applyQbQuery() {
  const query = state.qbConditions.join(` ${state.qbOperator} `);
  if(query) {
    $("#q").value = query;
    state.q = query;
    closeQueryBuilder();
    scheduleSearch(0);
  }
}

function clearQbQuery() {
  state.qbConditions = [];
  state.qbOperator = "AND";
  renderQbConditions();
  updateQbPreview();
}

function bindQueryBuilder() {
  const btnOpen = $("#btnQueryBuilder");
  const btnClose = $("#qbClose");
  const btnAdd = $(".qb-add");
  const btnApply = $("#qbApply");
  const btnClear = $("#qbClear");
  const termInput = $(".qb-term");

  if(btnOpen) btnOpen.addEventListener("click", openQueryBuilder);
  if(btnClose) btnClose.addEventListener("click", closeQueryBuilder);
  if(btnAdd) btnAdd.addEventListener("click", addQbCondition);
  if(btnApply) btnApply.addEventListener("click", applyQbQuery);
  if(btnClear) btnClear.addEventListener("click", clearQbQuery);
  if(termInput) {
    termInput.addEventListener("keydown", (e) => {
      if(e.key === "Enter") {
        e.preventDefault();
        addQbCondition();
      }
    });
  }
}

// ============ Saved Searches ============
const SAVED_SEARCHES_KEY = "swiss_caselaw_saved_searches";
const RECENT_SEARCHES_KEY = "swiss_caselaw_recent_searches";
const MAX_RECENT = 10;

function loadSavedSearches() {
  try {
    const data = localStorage.getItem(SAVED_SEARCHES_KEY);
    state.savedSearches = data ? JSON.parse(data) : [];
  } catch(e) {
    state.savedSearches = [];
  }
}

function saveSavedSearches() {
  localStorage.setItem(SAVED_SEARCHES_KEY, JSON.stringify(state.savedSearches));
}

function openSavedSearches() {
  const panel = $("#savedSearchesPanel");
  if(panel) panel.classList.remove("hidden");
  renderSavedSearches();
}

function closeSavedSearches() {
  const panel = $("#savedSearchesPanel");
  if(panel) panel.classList.add("hidden");
}

function renderSavedSearches() {
  const list = $("#savedSearchesList");
  if(!list) return;

  if(state.savedSearches.length === 0) {
    list.innerHTML = '<div class="muted small">No saved searches yet. Click ★ to save a search.</div>';
    return;
  }

  list.innerHTML = "";
  state.savedSearches.forEach((search, idx) => {
    const item = el("div", "ss-item");
    const query = el("span", "ss-item-query");
    query.textContent = search.query || "(empty)";
    query.title = search.query;
    const date = el("span", "ss-item-date");
    date.textContent = search.savedAt ? new Date(search.savedAt).toLocaleDateString() : "";
    const deleteBtn = el("button", "ss-item-delete btn ghost");
    deleteBtn.textContent = "×";
    deleteBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      deleteSavedSearch(idx);
    });

    item.appendChild(query);
    item.appendChild(date);
    item.appendChild(deleteBtn);
    item.addEventListener("click", () => applySavedSearch(search));
    list.appendChild(item);
  });
}

function saveCurrentSearch() {
  const query = state.q.trim();
  if(!query) {
    toast("Save Search", "Enter a query first");
    return;
  }

  // Check if already saved
  const exists = state.savedSearches.some(s => s.query === query);
  if(exists) {
    toast("Already Saved", "This search is already saved");
    return;
  }

  const search = {
    query,
    filters: JSON.parse(JSON.stringify(state.filters)),
    savedAt: new Date().toISOString(),
  };
  state.savedSearches.unshift(search);
  saveSavedSearches();
  toast("Search Saved", query.substring(0, 40) + (query.length > 40 ? "..." : ""));
}

function deleteSavedSearch(idx) {
  state.savedSearches.splice(idx, 1);
  saveSavedSearches();
  renderSavedSearches();
}

function applySavedSearch(search) {
  state.q = search.query || "";
  state.filters = search.filters || { language: [], canton: [], source_id: [], level: [], date_from: null, date_to: null };
  state.page = 1;
  $("#q").value = state.q;
  $("#dateFrom").value = state.filters.date_from || "";
  $("#dateTo").value = state.filters.date_to || "";
  updateLevelToggle();
  closeSavedSearches();
  scheduleSearch(0);
}

function bindSavedSearches() {
  const btnSaved = $("#btnSavedSearches");
  const btnClose = $("#ssClose");
  const btnSave = $("#btnSaveSearch");

  if(btnSaved) btnSaved.addEventListener("click", openSavedSearches);
  if(btnClose) btnClose.addEventListener("click", closeSavedSearches);
  if(btnSave) btnSave.addEventListener("click", saveCurrentSearch);
}

// ============ Recent Searches ============
function loadRecentSearches() {
  try {
    const data = localStorage.getItem(RECENT_SEARCHES_KEY);
    state.recentSearches = data ? JSON.parse(data) : [];
  } catch(e) {
    state.recentSearches = [];
  }
}

function addRecentSearch(query) {
  if(!query.trim()) return;
  // Remove if already exists
  state.recentSearches = state.recentSearches.filter(q => q !== query);
  // Add to front
  state.recentSearches.unshift(query);
  // Limit
  if(state.recentSearches.length > MAX_RECENT) {
    state.recentSearches = state.recentSearches.slice(0, MAX_RECENT);
  }
  localStorage.setItem(RECENT_SEARCHES_KEY, JSON.stringify(state.recentSearches));
}

// ============ Date Picker Presets ============
function setDatePreset(preset) {
  const now = new Date();
  let fromDate = null;
  let toDate = now.toISOString().split("T")[0];

  switch(preset) {
    case "1y":
      fromDate = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate()).toISOString().split("T")[0];
      break;
    case "5y":
      fromDate = new Date(now.getFullYear() - 5, now.getMonth(), now.getDate()).toISOString().split("T")[0];
      break;
    case "10y":
      fromDate = new Date(now.getFullYear() - 10, now.getMonth(), now.getDate()).toISOString().split("T")[0];
      break;
    case "all":
      fromDate = null;
      toDate = null;
      break;
  }

  state.filters.date_from = fromDate;
  state.filters.date_to = toDate;
  $("#dateFrom").value = fromDate || "";
  $("#dateTo").value = toDate || "";
  state.page = 1;
  scheduleSearch(0);
}

function clearDateInput(targetId) {
  const input = $(`#${targetId}`);
  if(input) {
    input.value = "";
    if(targetId === "dateFrom") {
      state.filters.date_from = null;
    } else if(targetId === "dateTo") {
      state.filters.date_to = null;
    }
    state.page = 1;
    scheduleSearch(0);
  }
}

function bindDatePicker() {
  // Preset buttons
  document.querySelectorAll("[data-preset]").forEach(btn => {
    btn.addEventListener("click", () => {
      const preset = btn.dataset.preset;
      setDatePreset(preset);
    });
  });

  // Clear buttons
  document.querySelectorAll(".date-clear").forEach(btn => {
    btn.addEventListener("click", () => {
      const target = btn.dataset.target;
      clearDateInput(target);
    });
  });
}

// ============ Level Toggle ============
function setLevelFilter(level) {
  if(level === "") {
    state.filters.level = [];
  } else {
    state.filters.level = [level];
  }
  updateLevelToggle();
  state.page = 1;
  scheduleSearch(0);
}

function updateLevelToggle() {
  const levelBtns = document.querySelectorAll(".level-btn");
  const currentLevel = state.filters.level.length > 0 ? state.filters.level[0] : "";
  levelBtns.forEach(btn => {
    const lvl = btn.dataset.level;
    btn.classList.toggle("active", lvl === currentLevel);
  });
}

function bindLevelToggle() {
  document.querySelectorAll(".level-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const level = btn.dataset.level;
      setLevelFilter(level);
    });
  });
}

// ============ Export CSV ============
async function exportCSV() {
  const payload = {
    q: state.q,
    filters: state.filters,
    max_results: 1000,
  };

  toast("Exporting", "Preparing CSV download...");

  try {
    const response = await fetch("/api/export/csv", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if(!response.ok) {
      throw new Error(`Export failed: ${response.status}`);
    }

    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "swiss_caselaw_export.csv";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    toast("Export Complete", "CSV file downloaded");
  } catch(e) {
    toast("Export Error", e.message || String(e));
  }
}

function bindExport() {
  const btn = $("#btnExportCSV");
  if(btn) btn.addEventListener("click", exportCSV);
}

// ============ Citation & Share ============
async function copyCitation() {
  const id = state.selectedId;
  if(!id) {
    toast("No Selection", "Select a decision first");
    return;
  }

  try {
    const data = await api("/api/cite", {
      method: "POST",
      body: JSON.stringify({ id, format: "standard" }),
    });

    await navigator.clipboard.writeText(data.citation);
    toast("Copied", "Citation copied to clipboard");
  } catch(e) {
    toast("Error", e.message || "Failed to copy citation");
  }
}

async function shareLink() {
  const id = state.selectedId;
  if(!id) {
    toast("No Selection", "Select a decision first");
    return;
  }

  // Build share URL with current search state
  const params = new URLSearchParams();
  if(state.q) params.set("q", state.q);
  if(id) params.set("id", id);

  const url = `${window.location.origin}/?${params.toString()}`;

  try {
    await navigator.clipboard.writeText(url);
    toast("Copied", "Share link copied to clipboard");
  } catch(e) {
    // Fallback for browsers without clipboard API
    prompt("Copy this link:", url);
  }
}

function bindCitationAndShare() {
  const btnCite = $("#btnCopyCite");
  const btnShare = $("#btnShareLink");

  if(btnCite) btnCite.addEventListener("click", copyCitation);
  if(btnShare) btnShare.addEventListener("click", shareLink);
}

// ============ URL State Handling ============
function parseUrlParams() {
  const params = new URLSearchParams(window.location.search);
  if(params.has("q")) {
    state.q = params.get("q");
    $("#q").value = state.q;
  }
  if(params.has("id")) {
    // Will be handled after search completes
    state.urlDocId = params.get("id");
  }
}

function init(){
  initTheme();
  loadSavedSearches();
  loadRecentSearches();
  parseUrlParams();
  bindInputs();
  bindKeyboard();
  bindQueryBuilder();
  bindSavedSearches();
  bindDatePicker();
  bindLevelToggle();
  bindExport();
  bindCitationAndShare();
  loadStatus();
  scheduleSearch(0);
}

window.addEventListener("load", init);
