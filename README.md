# Swiss Caselaw — Offline Legal Search

Search 500,000+ Swiss court decisions locally on your computer. No account needed, no internet required after setup, completely private.

## Quick Start (5 minutes)

### Requirements
- macOS, Linux, or Windows with WSL
- Python 3.10 or newer
- 20 GB free disk space

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/jonashertner/caselaw-repo.git
cd caselaw-repo/local_app

# 2. Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
pip install -e .

# 4. Download the database (one-time, ~15 GB download)
python -m caselaw_local.cli update

# 5. Start the search server
python -m caselaw_local.cli serve
```

Open your browser to **http://127.0.0.1:8787** — that's it!

---

## Features

### Full-Text Search
Search across all Swiss federal and cantonal court decisions using powerful query syntax:

| Query | What it finds |
|-------|---------------|
| `steuerpflicht` | Decisions containing "steuerpflicht" |
| `"bundesgericht zürich"` | Exact phrase match |
| `steuer AND veranlagung` | Both terms must appear |
| `steuer OR abgabe` | Either term |
| `veranlag*` | Prefix matching |
| `title:"rückerstattung"` | Search only in title |
| `docket:6B_123` | Search by docket number |

### Filters
- **Date range**: Filter by decision date with quick presets (1 year, 5 years, 10 years)
- **Level**: Federal courts, cantonal courts, or both
- **Language**: German, French, Italian, Romansh
- **Canton**: Filter by canton (ZH, BE, GE, etc.)
- **Court/Source**: Filter by specific court

### Query Builder
Click **Builder** next to the search box for a visual query constructor:
- Select fields (title, docket, content)
- Add multiple conditions
- Choose AND/OR operators
- Preview the generated query

### Export Results
Click **Export CSV** to download your search results as a spreadsheet. Includes:
- Decision ID, docket, title
- Date, court, canton, language
- Source URL and PDF link

### Save Searches
- Click the **★** button to save your current search
- Click **Saved** to view and reload saved searches
- Searches are stored locally in your browser

### Citations
Select any decision and use:
- **Copy Citation**: Copy a formatted legal citation
- **Share**: Copy a link to share the search with others

### Statistics Dashboard
Click **Show Stats** in the sidebar to see:
- Total decisions in database
- Date coverage (earliest to latest)
- Federal vs. cantonal split
- Top languages and cantons

---

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `/` | Focus search box |
| `j` / `k` | Navigate results (down/up) |
| `Enter` | Open selected result |
| `Esc` | Close modals |

---

## Updating the Database

The database is updated weekly with new decisions. To get the latest:

```bash
python -m caselaw_local.cli update
```

This downloads only what's changed since your last update.

---

## Configuration

### Custom Data Directory

By default, data is stored in:
- macOS: `~/Library/Application Support/swiss-caselaw/`
- Linux: `~/.local/share/swiss-caselaw/`
- Windows: `%APPDATA%\swiss-caselaw\`

To use a different location:

```bash
export CASELAW_DATA_DIR="/path/to/your/data"
```

### Custom Port

```bash
python -m caselaw_local.cli serve --port 9000
```

### Using a Different Data Source

```bash
export CASELAW_MANIFEST_URL="https://your-server.com/manifest.json"
```

---

## Troubleshooting

### "Dataset not installed"
Run the update command first:
```bash
python -m caselaw_local.cli update
```

### Search is slow
The first search after starting may take a few seconds while the database warms up. Subsequent searches should be fast (<100ms).

### Not enough disk space
The full database requires ~15-20 GB. Ensure you have sufficient free space before running `update`.

### Port already in use
If port 8787 is taken, use a different port:
```bash
python -m caselaw_local.cli serve --port 8788
```

---

## Data Sources

This project aggregates decisions from:
- **Federal courts**: Bundesgericht, Bundesverwaltungsgericht, Bundespatentgericht, Bundesstrafgericht
- **Cantonal courts**: All 26 cantons where publicly available
- **Source**: [entscheidsuche.ch](https://entscheidsuche.ch) and official court portals

Data is refreshed weekly with daily incremental updates.

---

## For Developers

### Project Structure

```
caselaw-repo/
├── local_app/          # Local search application
│   ├── caselaw_local/
│   │   ├── cli.py      # Command-line interface
│   │   ├── server.py   # FastAPI web server
│   │   ├── search.py   # Search logic (FTS5)
│   │   ├── db.py       # Database management
│   │   ├── static/     # CSS and JavaScript
│   │   └── templates/  # HTML templates
│   └── requirements.txt
│
└── pipeline/           # Data pipeline (for maintainers)
    └── ...
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/search` | POST | Full-text search with filters |
| `/api/doc/{id}` | GET | Get full document by ID |
| `/api/suggest` | GET | Autocomplete suggestions |
| `/api/stats` | GET | Database statistics |
| `/api/export/csv` | POST | Export results to CSV |
| `/api/cite` | POST | Generate citation |
| `/api/status` | GET | Database status |
| `/api/update` | POST | Trigger database update |

### Running in Development

```bash
cd local_app
source .venv/bin/activate
python -m caselaw_local.cli serve --port 8787
```

The server auto-reloads on code changes.

---

## Pipeline (For Data Maintainers)

If you're maintaining the data pipeline (not required for users), see the `pipeline/` directory.

### Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Scrapers  │────▶│  Pipeline   │────▶│ HuggingFace │
│ (swiss-caselaw)   │  (this repo)│     │  (hosting)  │
└─────────────┘     └─────────────┘     └─────────────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │ Local App   │
                                        │ (users)     │
                                        └─────────────┘
```

### Publishing Schedule
- **Daily**: Scrape new decisions, publish delta files
- **Weekly**: Consolidate into new snapshot, push to HuggingFace

### Setup

1. Create HuggingFace dataset repo
2. Set GitHub secrets: `HF_TOKEN`, `HF_DATASET_REPO`
3. Run initial backfill (see pipeline README)
4. GitHub Actions handles daily/weekly updates

---

## License & Legal

This tool provides access to publicly available court decisions. Use responsibly and respect each court's terms of service.

## Contributing

Issues and pull requests welcome at [github.com/jonashertner/caselaw-repo](https://github.com/jonashertner/caselaw-repo).

## Acknowledgments

Data sourced from [entscheidsuche.ch](https://entscheidsuche.ch) and official Swiss court portals.
