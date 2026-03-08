# architecture-analytics-mcp

A suite of three open-source MCP (Model Context Protocol) tools that make your
firm's project database conversational through Claude.

Architecture firms accumulate years of project data (comparable fees, phase
durations, staffing patterns, burn rates) but that data sits locked in SQL
tables that project managers don't query. These tools close that gap. Ask
questions in plain language, get answers grounded in your own historical record.

---

## The three tools

The suite follows the natural arc of a project:

| Tool | When to use it | What it does |
|---|---|---|
| [`comparable_projects_mcp`](./comparable_projects_mcp/README.md) | **Pursuit / proposal** | Ranked similarity search across past projects. Given a typology, sqft, and location, returns your firm's closest historical matches scored across multiple dimensions. |
| [`phase_fee_estimator_mcp`](./phase_fee_estimator_mcp/README.md) | **Fee proposal** | Statistically grounded fee ranges (25th, 75th, 90th percentile) derived from your own historical actuals, with cited comps and an analyst narrative. |
| [`project_risk_mcp`](./project_risk_mcp/README.md) | **Project delivery** | Benchmarks a live project's current fee burn rate against completed projects of similar typology and size. Returns a risk classification and PM-facing action guidance. |

Start with `comparable_projects_mcp`. Run `validate_connection` first to confirm
your database connection and verify that column auto-detection is working before
doing anything else.

---

## Requirements

- Python 3.11+
- SQL Server database containing your firm's project history
- [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- Claude Desktop (or any MCP-compatible client)
- LM Studio (local) or any OpenAI-compatible LLM API

Each tool connects directly to your database at runtime. No data leaves your
environment except what you send to the LLM for interpretation.

---

## What your database needs

All three tools work against a single project table in SQL Server. The schema
is fully configurable — no code changes required for standard deployments, just
`.env` mappings.

At minimum your table should have:

- Project name and identifier
- Project status (active vs. completed)
- Typology / market sector
- Gross square footage
- Year
- Per-phase fee columns (planned/budgeted and actuals-to-date)

Each tool's README documents its specific column requirements and auto-detection
heuristics in detail.

---

## Quick start

### 1. Clone the repo

```bash
git clone https://github.com/your-username/architecture-analytics-mcp.git
cd architecture-analytics-mcp
```

### 2. Install dependencies for the tool you want to use

```bash
cd comparable_projects_mcp
pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.template .env
# Fill in your SQL Server credentials and column mappings
```

### 4. Verify

```bash
python server.py
# Then in Claude: call validate_connection
```

### 5. Connect to Claude Desktop

Add whichever tools you want to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "comparable-projects": {
      "command": "python",
      "args": ["/path/to/comparable_projects_mcp/server.py"]
    },
    "phase-fee-estimator": {
      "command": "python",
      "args": ["/path/to/phase_fee_estimator_mcp/server.py"]
    },
    "project-risk": {
      "command": "python",
      "args": ["/path/to/project_risk_mcp/server.py"]
    }
  }
}
```

You can connect all three at once or just the ones you need.

---

## Honest expectations

**The tools are only as good as your data.** If your typology values are
inconsistent, your status field hasn't been maintained, or your per-phase fee
columns are sparsely populated, benchmark pools will be thin and outputs
unreliable. Run `validate_connection` and check the reported row counts before
drawing conclusions.

**Benchmarks are against your own history, not the industry.** A firm with a
narrow project history will get narrow benchmarks. The tools report pool size
and will tell you when filters had to be relaxed  *pay attention to those notes*.

**The LLM interprets; it does not decide.** Analyst narratives are grounded in
your data but should be treated as a starting point for a PM conversation, not
a substitute for one.

---

## Security

All three tools enforce read-only SQL. `INSERT`, `UPDATE`, `DELETE`, `DROP`,
`ALTER`, `TRUNCATE`, `CREATE`, and `EXEC` are blocked at the query layer before
execution. Use a dedicated read-only database user. Never connect as `sa` or
`dbo`.

Credentials live in `.env` files and are never committed to source control. A
`.gitignore` covering `.env` is included.

---

## License

MIT — free to use, modify, and distribute.

No proprietary data, firm-specific documents, or hardcoded column names are
embedded anywhere in this codebase. All intelligence comes from your own
database.
