# Swiss Caselaw

Consolidated repository for scraping, indexing, and searching 843,000+ Swiss federal and cantonal court decisions.

## Architecture

```
caselaw-repo/
├── backend/          # Scrapers, FastAPI, ingestion (PostgreSQL)
├── frontend/         # Next.js search UI
├── pipeline/         # ETL: daily deltas, weekly snapshots (SQLite + HuggingFace)
├── local_app/        # Offline local search app (SQLite)
├── mcp_server/       # Claude Code MCP integration
├── tests/            # Test suite
├── docker-compose.yml
└── Dockerfile.spaces # HuggingFace Spaces deployment
```

## Quick Start (Local Search)

```bash
cd local_app
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# Download database (~15 GB, one-time)
caselaw-local update

# Start web UI
caselaw-local serve
```

Open **http://127.0.0.1:8787**

## Quick Start (Full Stack)

```bash
cp .env.example .env
docker compose up --build
```

- Backend API: http://localhost:8000
- Frontend: http://localhost:3003

## Claude Code Integration

Add to `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "python3",
      "args": ["/absolute/path/to/caselaw-repo/mcp_server/server.py"]
    }
  }
}
```

### MCP Tools

| Tool | Description |
|------|-------------|
| `search_caselaw` | Full-text search with filters (language, canton, date, court level) |
| `get_decision` | Get complete text of a decision by ID |
| `find_citing_decisions` | Find cases citing a specific reference (e.g., `BGE 140 III 264`) |
| `analyze_search_results` | Aggregate analysis: breakdowns by year, canton, level, court |
| `get_caselaw_statistics` | Database coverage and statistics |
| `search_by_court` | Search decisions from a specific court |
| `list_cantons` | List all cantons with decision counts |

The MCP server supports both PostgreSQL (`DATABASE_URL`) and SQLite (`CASELAW_DB_PATH`) backends. Use `--huggingface` to auto-download from HuggingFace.

## Search Syntax

| Query | Matches |
|-------|---------|
| `steuerpflicht` | Contains "steuerpflicht" |
| `"bundesgericht zürich"` | Exact phrase |
| `steuer AND veranlagung` | Both terms |
| `steuer OR abgabe` | Either term |
| `veranlag*` | Prefix match |
| `title:BGE` | Search title field |
| `docket:6B_123` | Search docket number |

## Database Coverage

| Metric | Value |
|--------|-------|
| Total decisions | 843,970 |
| Date range | 1901 – 2026 |
| Federal courts | 374,512 |
| Cantonal courts | 469,458 |
| Languages | DE (55%), FR (35%), IT (10%) |

## Automated Updates

| Schedule | Action |
|----------|--------|
| **Daily** (04:00 UTC) | Scrape new decisions, export, build delta, publish to HuggingFace |
| **Weekly** (Sunday 03:30 UTC) | Consolidate deltas into new snapshot |
| **Weekly** (Sunday 05:00 UTC) | Gap verification via entscheidsuche.ch |

## Development

```bash
make venv-backend    # Backend virtualenv
make venv-pipeline   # Pipeline virtualenv
make venv-local      # Local app virtualenv
make dev             # Docker compose up --build
make test            # Run tests
make lint            # Ruff check
make fmt             # Ruff format
```

## Sources

All decisions scraped directly from official court portals:

**Federal:** BGer, BVGer, BStGer, BPatGer
**Cantonal:** Dedicated scrapers for all 26 cantons, with gap checking against entscheidsuche.ch

## License

MIT. Court decisions are public records.
