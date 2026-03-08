# project-risk-mcp

**Tool 3 of the architecture-analytics-mcp suite.**

An MCP server that benchmarks a live project's fee burn rate against historical
completed projects — and tells you whether your project is tracking normally,
running elevated, or heading toward a fee overrun before it's too late to act.

---

## What problem this solves

By the time a project manager realizes a phase is over budget, it's usually too
late. The fee is spent. The scope didn't change. The PM had a vague sense
something was off but no data to confirm it.

This tool closes that gap. Given any active project and phase, it computes the
current fee burn rate, pulls the historical distribution from comparable completed
projects, and tells you exactly where this project sits in that distribution —
with a risk classification and PM-facing action guidance.

---

## What it does

Four MCP tools:

| Tool | Purpose |
|---|---|
| `diagnose_project_risk` | Core burn rate diagnosis with risk classification and analyst interpretation |
| `list_active_projects` | Discover which in-delivery projects are available for diagnosis |
| `list_benchmark_pool` | Inspect the historical comps that would be used for a given phase + filters |
| `validate_connection` | Health-check for DB, schema detection, and LLM connectivity |

### How burn rate works

Burn rate = fee spent to date / planned fee for the phase.

A burn rate of **1.00** means the project is exactly on budget at this point in
the phase. A burn rate of **1.35** means 35% more fee has been spent than planned.

The tool benchmarks the live project's burn rate against the P25/P75/P90
distribution of historical completed projects at the same phase, typology, and
size — and classifies risk accordingly.

### Risk classification

| Level | Condition | Meaning |
|---|---|---|
| NORMAL | Burn rate ≤ 110% of historical P75 | Tracking as expected |
| ELEVATED | 110%–125% of historical P75 | Monitor closely |
| AT RISK | 125%–150% of historical P75 | Recommend scope review |
| CRITICAL | > 150% of historical P75 | Immediate action required |

Thresholds are configurable via `.env`.

### Example interaction

```
User: Diagnose fee burn risk for the Eastside Health Center project in DD phase.

Tool:
  PROJECT RISK DIAGNOSIS  [⚠⚠ AT RISK]

  Project:  Eastside Health Center
  Phase:    DD
  Actuals:  $312,000   (from DD_Actuals column)
  Planned:  $245,000   (from DD_Planned_Fee column)
  Phase % complete: 72%

  Current burn rate: 1.27  (1.00 = exactly on budget)

  Burn rate of 1.27 is 27% above the historical P75 of 1.00.
  Recommend scope review and PM conversation.

  BENCHMARK  (18 comparable completed projects)
  Median burn rate:  0.94
  P25 / P75:         0.82 / 1.00
  P90:               1.18

  ANALYST INTERPRETATION
  The Eastside Health Center is running 27% over its DD fee budget with 72% of
  the phase complete. For healthcare projects of this size, a burn rate above 1.20
  at this stage is atypical and suggests unplanned coordination scope or consultant
  management overhead. Review consultant fee tracking and outstanding RFIs before
  phase close.
```

---

## Honest capability map

### What is fully built

| Capability | Status | Notes |
|---|---|---|
| Burn rate computation | ✅ Built | actuals / planned fee; handles nulls and zero-planned gracefully |
| Historical benchmark pool | ✅ Built | Queries completed projects from your database |
| Risk classification (4 levels) | ✅ Built | NORMAL / ELEVATED / AT RISK / CRITICAL |
| Configurable risk thresholds | ✅ Built | Override via RISK_ELEVATED_MULT, RISK_AT_RISK_MULT, RISK_CRITICAL_MULT in .env |
| Benchmark stats (P25/P75/P90/median) | ✅ Built | Full distribution reported alongside classification |
| Progressive filter relaxation | ✅ Built | Drops sqft range → typology when benchmark pool is too small |
| LLM analyst interpretation | ✅ Built | PM-facing action guidance, not just raw numbers |
| Partial project name matching | ✅ Built | Use 'Eastside' to match 'Eastside Health Center Phase 2' |
| Pct-complete context | ✅ Built | Read from DB column or passed as override; used in analyst prompt |
| Active project listing | ✅ Built | list_active_projects for discovery before diagnosis |
| Benchmark pool inspection | ✅ Built | list_benchmark_pool to verify data quality before diagnosis |
| Zero-config schema detection | ✅ Built | Auto-detects actuals and planned fee column pairs from schema |
| Read-only SQL safety filter | ✅ Built | Blocks INSERT/UPDATE/DELETE/DROP at query layer |

### What is partially built or requires configuration

| Capability | Status | Notes |
|---|---|---|
| Actuals column detection | ⚠ Heuristic | Looks for 'actual', 'spent', 'to_date' in column names; set ACTUALS_COLS in .env if auto-detect fails |
| Planned fee column detection | ⚠ Heuristic | Looks for 'fee', 'planned', 'budget' in column names; set PLANNED_COLS in .env if needed |
| Pct-complete column detection | ⚠ Heuristic | Optional; looks for 'pct', 'percent', 'complete' in column names |
| Active/completed status values | ⚠ Configurable | Defaults to 'Active' and 'Completed'; set ACTIVE_STATUS_VALUE and COMPLETED_STATUS_VALUE in .env |
| Benchmark pool size | ⚠ Configurable | Defaults to MIN_BENCH_COMPS=5; increase for higher statistical confidence |

### What is not built (known gaps)

| Capability | Gap | Impact |
|---|---|---|
| Estimate at Completion (EAC) | ❌ Not built | Tool diagnoses current burn but does not project final cost at completion |
| Staffing / hour burn rate | ❌ Not built | Fee burn only; no labor hour or FTE burn analysis |
| Multi-phase portfolio view | ❌ Not built | One project + one phase per call; no dashboard across all active projects |
| Cross-firm benchmarking | ❌ Not built | Benchmark is your own historical data only |
| Fee inflation normalization | ❌ Not built | Historical projects weighted equally regardless of year |
| Scope change detection | ❌ Not built | Tool cannot distinguish fee overrun from legitimate approved scope additions |
| Time-series burn tracking | ❌ Not built | Snapshot only — does not track burn rate trend over time within a phase |
| Earned value analysis | ❌ Not built | No EVM (BCWS/BCWP/ACWP) — burn rate is a simpler proxy |
| Phase-completion-adjusted benchmarking | ❌ Not built | The benchmark pool is composed of fully completed projects, so their burn rates reflect final outcomes — not mid-phase snapshots. A project that finished at burn 1.05 may have been at 1.40 halfway through and recovered. The tool does not account for this: a live project at 50% complete is benchmarked against final burn rates, not against what comparable projects looked like at the 50% mark. In practice the signal is still directionally correct — a burn rate well above the historical P75 is a real flag regardless — but the classification thresholds are less precise for projects early in a phase. Fixing this properly would require a time-series actuals table, not a single-row-per-project schema. |

---

## What your database needs

This tool works with any SQL Server table that tracks project fee actuals
alongside planned fees per phase.

For best results your table should have:

- A project name and identifier column
- A project status column with distinct active vs. completed values
- Per-phase columns for both **actuals spent to date** and **planned/budgeted fee**
  (e.g., `DD_Actuals` and `DD_Planned_Fee` for the DD phase)
- Optionally: per-phase percent-complete columns (0–100 or 0.0–1.0)
- A typology / market sector column for benchmark filtering
- A gross square footage column for sqft-range filtering
- A year column shown on benchmark comps

If auto-detection fails, specify column pairs explicitly via `ACTUALS_COLS` and
`PLANNED_COLS` in `.env`.

**Example .env configuration for explicit column mapping:**

```
ACTUALS_COLS=CONCEPT:Concept_Actuals,SD:SD_Actuals,DD:DD_Actuals,CD:CD_Actuals,CA:CA_Actuals
PLANNED_COLS=CONCEPT:Concept_Labor_Fee,SD:SD_Fee,DD:DD_Fee,CD:CD_Fee,CA:CA_Fee
PCT_COLS=SD:SD_Pct_Complete,DD:DD_Pct_Complete,CD:CD_Pct_Complete
```

---

## Quick start

### 1. Prerequisites

- Python 3.11+
- [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- LM Studio (local) or OpenAI API access
- A SQL Server database with per-phase actuals and planned fee data

### 2. Install

```bash
pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.template .env
# Fill in SQL Server credentials, table name, and column mappings
```

### 4. Verify setup

```bash
python server.py
# Then in your MCP client: call validate_connection
```

`validate_connection` will report detected column pairs and active/completed
project counts. If actuals columns are not auto-detected, set `ACTUALS_COLS`
and `PLANNED_COLS` in `.env`.

### 5. Discover active projects

```
# In your MCP client:
list_active_projects
```

Pick a project name, then run a diagnosis:

```
diagnose_project_risk  project_name="Eastside Health Center"  phase="DD"
```

### 6. Connect to Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "project-risk": {
      "command": "python",
      "args": ["/path/to/server.py"]
    }
  }
}
```

---

## Environment variables

### Required

| Variable | Default | Description |
|---|---|---|
| `SQL_SERVER` | `localhost` | SQL Server hostname or IP |
| `SQL_DB` | `ProjectData` | Database name |
| `SQL_UID` | `reader` | SQL Server username |
| `SQL_PWD` | _(empty)_ | SQL Server password |
| `PRIMARY_TABLE` | `dbo.Projects` | Fully qualified table name |

### Column mappings (override if auto-detect fails)

| Variable | Example | Description |
|---|---|---|
| `ACTUALS_COLS` | `DD:DD_Actuals,CD:CD_Actuals` | Per-phase actuals spent-to-date columns |
| `PLANNED_COLS` | `DD:DD_Fee,CD:CD_Fee` | Per-phase planned/budgeted fee columns |
| `PCT_COLS` | `DD:DD_Pct_Complete` | Per-phase percent-complete columns (optional) |
| `COL_PROJECT_NAME` | `Project_Name` | Project name column |
| `COL_TYPOLOGY` | `Market_Sector` | Typology / market sector column |
| `COL_STATUS` | `Project_Status` | Project status column |
| `COL_SQFT` | `Gross_Square_Footage` | Gross square footage column |
| `ACTIVE_STATUS_VALUE` | `Active` | Value that means "in delivery" in status column |
| `COMPLETED_STATUS_VALUE` | `Completed` | Value that means "done" in status column |

### Risk thresholds

| Variable | Default | Meaning |
|---|---|---|
| `RISK_ELEVATED_MULT` | `1.10` | Burn rate > 110% of P75 = ELEVATED |
| `RISK_AT_RISK_MULT` | `1.25` | Burn rate > 125% of P75 = AT RISK |
| `RISK_CRITICAL_MULT` | `1.50` | Burn rate > 150% of P75 = CRITICAL |
| `MIN_BENCH_COMPS` | `5` | Minimum comparable projects before relaxing filters |
| `SQFT_TOLERANCE` | `0.50` | ±50% sqft range for benchmark matching |

---

## Security

- All SQL is read-only. INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE,
  and EXEC are blocked at the query layer before execution.
- Use a dedicated read-only database user. Never use `sa` or `dbo`.
- Credentials live in `.env` and are never committed to source control.

---

## Part of a larger suite

This is Tool 3 of three tools in the `architecture-analytics-mcp` suite:

| Tool | Status | Description |
|---|---|---|
| Tool 1: `find_comparable_projects` | ✅ Built | Ranked similarity search across past projects for proposal research |
| Tool 2: `estimate_phase_fee` | ✅ Built | Statistically grounded fee ranges from historical actuals |
| Tool 3: `diagnose_project_risk` | ✅ This tool | Benchmark a live project's fee burn against historical norms |

---

## License

MIT — free to use, modify, and distribute.

This package contains no proprietary data, firm-specific documents,
or hardcoded column names. All intelligence comes from your own database.
