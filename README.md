# Swiss Caselaw

Offline access to 843,000+ Swiss federal and cantonal court decisions. Search locally, completely private, no internet required after setup.

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/jonashertner/caselaw-repo.git
cd caselaw-repo/local_app
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# 2. Download database (~15 GB, one-time)
caselaw-local update
```

---

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

Restart Claude Code. Then ask in natural language:

```
Search for data protection cases from the Federal Supreme Court
```

```
Find decisions citing BGE 140 III 264
```

```
Show recent criminal law cases from Zurich involving fraud
```

```
Finde Entscheide betreffend Verfahrenseinstellung und Beschwerde der Privatklägerschaft
```

### MCP Tools

| Tool | Description |
|------|-------------|
| `search_caselaw` | Full-text search with filters (language, canton, date, court level) |
| `get_decision` | Get complete text of a decision by ID |
| `find_citing_decisions` | Find cases citing a specific reference (e.g., `BGE 140 III 264`) |
| `get_caselaw_statistics` | Database coverage and statistics |

### Search Parameters

| Parameter | Description |
|-----------|-------------|
| `query` | Search query (supports `AND`, `OR`, `NOT`, `"phrases"`, `field:value`, `prefix*`) |
| `language` | `de`, `fr`, `it`, `rm` |
| `canton` | `ZH`, `BE`, `GE`, `VD`, `TI`, etc. |
| `level` | `federal` or `cantonal` |
| `date_from` / `date_to` | Date range (YYYY-MM-DD) |
| `limit` | Max results (default 20, max 100) |

### Custom Database Path

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "python3",
      "args": ["/path/to/mcp_server/server.py"],
      "env": {
        "CASELAW_DB_PATH": "/custom/path/caselaw.sqlite"
      }
    }
  }
}
```

---

## Web Interface

```bash
caselaw-local serve
```

Open **http://127.0.0.1:8787**

**Features:**
- Full-text search with Boolean operators
- Filter by date, language, canton, court level
- Export results to CSV
- Save searches locally
- Clickable citation links
- Keyboard navigation (`/` to search, `j`/`k` to navigate)

---

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

---

## Database

### Updates

```bash
caselaw-local update
```

New decisions added weekly.

### Coverage

| Metric | Value |
|--------|-------|
| Total decisions | 843,970 |
| Date range | 1901 – 2026 |
| Federal courts | 374,512 |
| Cantonal courts | 469,458 |
| Languages | DE (55%), FR (35%), IT (10%) |

### Location

The database is stored at:
- macOS: `~/Library/Application Support/swiss-caselaw/caselaw.sqlite`
- Linux: `~/.local/share/swiss-caselaw/caselaw.sqlite`

### Sources

[entscheidsuche.ch](https://entscheidsuche.ch), Bundesgericht, Bundesverwaltungsgericht, Bundespatentgericht, Bundesstrafgericht, all 26 cantonal courts.

---

## Requirements

- Python 3.10+
- 20 GB disk space
- macOS, Linux, or Windows

---

## License

MIT. Court decisions are public records.
