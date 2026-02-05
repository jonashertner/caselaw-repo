# Swiss Caselaw MCP Server

An MCP (Model Context Protocol) server that provides legal research tools for Claude Code, enabling AI-assisted analysis of Swiss court decisions.

## Features

- **search_caselaw**: Full-text search across 843,000+ Swiss court decisions
- **get_decision**: Retrieve complete decision text and metadata
- **get_caselaw_statistics**: Database coverage and statistics
- **find_citing_decisions**: Track how precedents are cited

## Prerequisites

1. **Database**: Download the Swiss caselaw database using the local app:
   ```bash
   cd ../local_app
   pip install -e .
   caselaw-local update
   ```

   Or download directly from [HuggingFace](https://huggingface.co/datasets/voilaj/swiss-caselaw-artifacts).

2. **Python 3.9+** with sqlite3 support (included in standard library)

## Installation

### Option 1: Add to Claude Code settings

Add to `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "python3",
      "args": ["/path/to/mcp_server/server.py"]
    }
  }
}
```

### Option 2: With custom database path

```json
{
  "mcpServers": {
    "swiss-caselaw": {
      "command": "python3",
      "args": ["/path/to/mcp_server/server.py"],
      "env": {
        "CASELAW_DB_PATH": "/path/to/caselaw.sqlite"
      }
    }
  }
}
```

## Database Locations

The server searches for the database in this order:

1. `CASELAW_DB_PATH` environment variable
2. `~/Library/Application Support/swiss-caselaw/caselaw.sqlite` (macOS)
3. `~/.local/share/swiss-caselaw/caselaw.sqlite` (Linux)
4. `./caselaw.sqlite` (current directory)

## Usage Examples

After restarting Claude Code, you can use natural language:

```
Search for data protection cases from the Federal Supreme Court in 2024
```

```
Find decisions citing BGE 140 III 264
```

```
Show me recent tax law decisions from Zurich
```

```
Get statistics on the caselaw database
```

## Tool Reference

### search_caselaw

Search Swiss court decisions with full-text search and filters.

**Parameters:**
- `query` (required): Search query supporting FTS5 syntax
  - Quoted phrases: `"Bundesgericht"`
  - Boolean: `steuer AND einkommen`
  - Field prefix: `title:BGE`, `docket:6B_123`
- `language`: Filter by language (`de`, `fr`, `it`, `rm`)
- `canton`: Filter by canton code (`ZH`, `BE`, `VD`, etc.)
- `level`: Filter by court level (`federal`, `cantonal`)
- `date_from`: Start date (YYYY-MM-DD)
- `date_to`: End date (YYYY-MM-DD)
- `limit`: Max results (default 20, max 100)

### get_decision

Retrieve complete decision by ID.

**Parameters:**
- `decision_id` (required): UUID of the decision

### get_caselaw_statistics

Get database statistics. No parameters.

### find_citing_decisions

Find decisions citing a case reference.

**Parameters:**
- `citation` (required): Citation string (e.g., `BGE 140 III 264`, `6B_123/2024`)
- `limit`: Max results (default 20)

## License

MIT License - See repository root for details.
