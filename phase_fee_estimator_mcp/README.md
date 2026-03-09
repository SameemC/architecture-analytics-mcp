# phase-fee-estimator-mcp

Tool 2 of the architecture-analytics-mcp suite.

A Model Context Protocol (MCP) server that gives architects and project managers
a statistically grounded fee estimate for any project phase, or the total project
fee across all phases, drawn directly from their firm's historical project data.

---

## What problem this solves

Every architecture firm faces the same proposal moment: a project manager sits
down to write a fee and has to answer: is this reasonable? Too low? Leaving
money on the table?

For a single phase that answer has always come from memory or manually digging
through old proposals. For a full project fee it's even harder: firms are
typically summing phase guesses without any statistical grounding or 
project comparables to anchor the total.

This tool makes both of those a database query.

---

## What it does

Four MCP tools:

| Tool | Purpose |
|---|---|
| `estimate_phase_fee` | Given project parameters, returns a fee range with cited comparables for a single phase |
| `estimate_total_fee` | All-phases roll-up in one call — per-phase breakdown plus a total range derived from real per-project totals |
| `list_typologies` | Discover project types and phases available in your database |
| `validate_connection` | Health-check for DB, schema detection, and LLM connectivity |

### Example: single phase

```
User: Estimate the SD fee for a 60,000 sqft civic project in Washington state.

Tool output:
  Phase: SD
  Based on 14 comparable projects
  Confidence: GOOD

  Recommended range:   $145,000  –  $210,000  (25th–75th percentile)
  Median fee:          $178,000
  Mean fee:            $183,000
  Full observed range: $92,000  –  $315,000
  90th percentile:     $268,000  (ceiling for complex or high-risk projects)

  TOP 5 COMPARABLE PROJECTS
  Central Library Renovation     |  $198,000  |  61,200 sqft  |  2022
  Civic Arts Center Phase 1      |  $185,000  |  57,800 sqft  |  2023
  County Health Services Bldg    |  $172,000  |  63,400 sqft  |  2021
```

### Example: total project roll-up

```
User: What should the total fee be for a 60,000 sqft civic project in WA?

Tool output:
  TOTAL PROJECT FEE ESTIMATE  (all-phases roll-up)

  Based on 9 projects with fee data across phases
  Phases included: CONCEPT, SD, DD, CD, CA

  Recommended total range:  $620,000  –  $910,000
  Total median fee:         $745,000
  Total mean fee:           $762,000
  Full observed range:      $480,000  –  $1,240,000
  90th percentile:          $1,050,000

  (Total range is derived from per-project sums, not added medians.)

  PER-PHASE BREAKDOWN
  Phase       Median       P25 – P75 Range       Comps  Confidence
  CONCEPT      $42,000   $32,000 – $58,000          14  GOOD
  SD          $178,000  $145,000 – $210,000          14  GOOD
  DD          $195,000  $160,000 – $235,000          12  GOOD
  CD          $215,000  $175,000 – $260,000          11  GOOD
  CA          $112,000   $88,000 – $148,000           9  MODERATE

  TOP COMPARABLE PROJECTS  (by total fee)
  Civic Arts Center Phase 1   |  $892,000 total  |  57,800 sqft  |  2023
  Central Library Renovation  |  $841,000 total  |  61,200 sqft  |  2022
  County Health Services Bldg |  $718,000 total  |  63,400 sqft  |  2021
```

---

## Why the total is not just added medians

A naive approach to total-fee estimation adds the median fee for each phase:

```
Concept median + SD median + DD median + CD median + CA median = "total"
```

This is statistically wrong. Projects that run expensive in SD tend to run
expensive in DD — the phases are correlated. Adding medians ignores that
correlation and systematically understates total-fee variance.

`estimate_total_fee` instead computes the **sum of actual per-phase fees for
each individual comparable project**, then runs distribution statistics on those
per-project totals. This preserves real variance across the full lifecycle and
gives an honest picture of what total fees actually look like in your portfolio.

---

## Honest capability map

Published with full transparency so firms know exactly what they're getting
before deploying.

### What is fully built

| Capability | Status | Notes |
|---|---|---|
| Single-phase fee estimation | ✅ Built | `estimate_phase_fee` — range, median, mean, P90, confidence, cited comps |
| All-phases total fee roll-up | ✅ Built | `estimate_total_fee` — per-phase breakdown + total derived from per-project actuals |
| Correct roll-up methodology | ✅ Built | Sums per-project totals, not per-phase medians — preserves real variance |
| Partial-phase roll-up | ✅ Built | Pass `phases: ['SD','DD','CD']` to estimate any subset of phases |
| Phases-missing-data handling | ✅ Built | Phases with no data are excluded and flagged; total reflects only available phases |
| Fallback to median-sum | ✅ Built | When per-project totals unavailable, falls back to median sum with explicit warning |
| Phase fee column auto-detection | ✅ Built | Reads schema at runtime; overridable via PHASE_COLS in .env |
| Duration column auto-detection | ✅ Built | Optional; shown alongside fee estimate when available |
| Statistical range (P10/P25/P75/P90) | ✅ Built | Computed from actuals; zeros and nulls excluded |
| Confidence labeling | ✅ Built | Based on sample size and coefficient of variation |
| Progressive filter relaxation | ✅ Built | Drops location → sqft range → typology when comps are sparse |
| LLM analyst interpretation | ✅ Built | Separate prompts for single-phase and roll-up contexts |
| Comparable project citation | ✅ Built | Top 10 comps named with fee, sqft, year; total-fee comps in roll-up |
| Zero-config schema detection | ✅ Built | No hardcoded column names; reads your schema at runtime |
| Read-only SQL safety filter | ✅ Built | Blocks INSERT/UPDATE/DELETE/DROP at query layer |

### What is partially built or requires configuration

| Capability | Status | Notes |
|---|---|---|
| Phase column detection | ⚠️ Heuristic | Works for common naming patterns; set PHASE_COLS in .env if auto-detect fails |
| Sqft tolerance | ⚠️ Configurable | Defaults to ±40%; adjust SQFT_TOLERANCE for your data density |
| Status filter | ⚠️ Manual | Pass `status_filter="Completed"` to restrict to actuals; not enforced by default |

### What is not built (known gaps)

| Capability | Gap | Impact |
|---|---|---|
| Cross-firm benchmarking | ❌ Not built | Estimates are only as good as your own historical data; no industry comparison |
| Vector similarity ranking | ❌ Not built | Comp matching uses structured SQL filters; no semantic similarity on project descriptions |
| Scope complexity scoring | ❌ Not built | No adjustment for project complexity, client type, or contract structure |
| Multi-database federation | ❌ Not built | Single SQL Server database only |
| Fee inflation normalization | ❌ Not built | Historical fees treated equally regardless of year |
| Formal confidence intervals | ❌ Not built | Percentile ranges reported but no statistical confidence intervals |

---

## Quick start

### 1. Prerequisites

- Python 3.11+
- [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- LM Studio (local) or any OpenAI-compatible API
- A SQL Server database with historical project fee data

### 2. Install

```bash
pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.template .env
# Fill in SQL Server credentials, table name, and LLM endpoint
```

Minimum required: `SQL_SERVER`, `SQL_DB`, `SQL_UID`, `SQL_PWD`, `PRIMARY_TABLE`.

### 4. Verify

```
call validate_connection
```

This confirms DB connectivity, phase column detection, and LLM reachability
before you run any estimates.

### 5. Connect to Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "phase-fee-estimator": {
      "command": "python",
      "args": ["/path/to/server.py"]
    }
  }
}
```

---

## What your database needs

No required column names — the server auto-detects them from your schema.

For best results, your primary table should have:

- A project identifier and name column
- A typology / market sector column (for filtering by project type)
- Numeric fee columns per phase (e.g. Concept_Fee, SD_Fee, DD_Fee, CD_Fee, CA_Fee)
- A project status column (to filter for completed projects with actuals)
- A gross square footage column (for sqft-range comp matching)
- A year column (shown on comparables)

If auto-detection fails, specify columns explicitly via `PHASE_COLS` in `.env`.

---

## Security

- All SQL is read-only. INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE,
  and EXEC are blocked at the query layer before execution.
- Use a dedicated read-only database user.
- Store credentials in `.env`. Never commit `.env` to source control.

---

## Part of a larger suite

| Tool | Status | Description |
|---|---|---|
| Tool 1: `find_comparable_projects` | See Git | Vector + SQL similarity search across past projects |
| Tool 2: `estimate_phase_fee` | This tool | Single-phase and total-project fee estimation from historical actuals |
| Tool 3: `diagnose_project_risk` | See Git | Benchmark a live project's fee burn against historical norms |

---

## License

MIT — free to use, modify, and distribute.

This package contains no proprietary data, firm-specific documents,
or hardcoded column names. All intelligence comes from your own database.
