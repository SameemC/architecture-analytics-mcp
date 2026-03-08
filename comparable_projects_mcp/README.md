# comparable-projects-mcp

**Tool 1 of the architecture-analytics-mcp suite.**

A Model Context Protocol (MCP) server that finds the most similar past projects
from your firm's database for a proposed new project — ranked by a composite
similarity score across square footage, construction cost, year, and location.

---

## What problem this solves

When a firm starts a new proposal, the instinct is to ask: "what's the closest
thing we've done before?" The answer usually comes from whoever has been around
the longest. It lives in someone's head, not in a query.

This tool makes that question answerable from your database in seconds. Give it
a typology, a square footage, and a rough cost estimate and it returns a ranked
list of your most comparable past projects — with their actual fees attached —
so a PM has named precedents and a fee anchor before the first proposal meeting.

---

## What it does

Four MCP tools:

| Tool | Purpose |
|---|---|
| `find_comparable_projects` | Core similarity search — ranked results with scores, fee data, and analyst narrative |
| `get_project_detail` | Full database profile for a single named project; supports partial name match |
| `list_dimensions` | Discover typologies, locations, statuses, year range, and scoring weights in your database |
| `validate_connection` | Health-check for DB, schema detection, scoring columns, and LLM connectivity |

---

## How the similarity engine works

This is not a vector search. There is no embedding model and no external database
required. The engine has two layers.

**Layer 1 — SQL filter pass**

Narrows the full project table to a candidate pool (default: top 200 rows) using
structured filters: typology exact match, sqft within ±50%, location, status,
and year range. This is fast, deterministic, and runs entirely in SQL Server.

**Layer 2 — Numeric distance scoring**

For each candidate in the pool, a composite similarity score (0–100) is computed
across up to four dimensions:

| Dimension | Default weight | Method |
|---|---|---|
| `sqft` | 45% | Log-ratio distance — exponential decay from 1.0 at identical to ~0 at 4× difference |
| `const_cost` | 25% | Same log-ratio distance on estimated construction cost |
| `year` | 15% | Linear decay from 1.0 at same year to 0 at 10+ years apart |
| `location` | 15% | Binary: 1.0 if same state, 0.5 if location unknown, 0.0 if different |

All weights are configurable via `SCORE_WEIGHTS` in `.env`. If a target value is
not provided for a dimension (e.g. no construction cost given), that dimension's
weight is redistributed proportionally across the remaining dimensions.

**Layer 3 — LLM interpretation**

The top 8 scored candidates are sent to the analyst LLM, which identifies the
2-3 strongest comps, explains specifically why each is relevant, flags differences
a PM should be aware of, and notes if the overall match quality is weak.

### Why log-ratio distance for sqft and cost

A linear distance would treat the gap between 10,000 and 20,000 sqft the same as
the gap between 100,000 and 110,000 sqft. That's wrong for architecture — doubling
a project's size is a fundamentally different scale difference than adding 10,000
sqft to a large project. Log-ratio distance is scale-invariant: a 2× difference
always produces the same score penalty regardless of the absolute values involved.

---

## Example output

```
find_comparable_projects(
    typology="Civic",
    sqft=58000,
    const_cost=22000000,
    location="WA",
    status_filter="Completed",
    target_year=2025
)

================================================================
COMPARABLE PROJECTS
================================================================

Found 8 results  (scored from a pool of 47 candidates)

Scoring dimensions: sqft (45%), const_cost (25%), year (15%), location (15%)

----------------------------------------------------------------
RANKED RESULTS
----------------------------------------------------------------

   1. Central Library Renovation  [94/100]
      Civic | 61,200 sqft | WA | 2022 | est. $23,500,000
      Fees: CONCEPT: $42,000 | SD: $198,000 | DD: $215,000 | CD: $228,000 | CA: $118,000
      Score breakdown: sqft=96 | const_cost=91 | year=70 | location=100

   2. Civic Arts Center Phase 1  [87/100]
      Civic | 57,800 sqft | WA | 2021 | est. $19,800,000
      Fees: CONCEPT: $38,000 | SD: $185,000 | DD: $195,000 | CD: $212,000 | CA: $105,000
      Score breakdown: sqft=99 | const_cost=83 | year=60 | location=100

   3. County Health Services Building  [79/100]
      Civic | 63,400 sqft | OR | 2023 | est. $24,100,000
      Fees: SD: $172,000 | DD: $188,000 | CD: $204,000 | CA: $112,000
      Score breakdown: sqft=91 | const_cost=87 | year=80 | location=0

----------------------------------------------------------------
ANALYST INTERPRETATION
----------------------------------------------------------------

The Central Library Renovation is your strongest comparable at 94 out of 100.
It matches almost exactly on square footage and construction cost, is in the
same state, and was completed three years ago — recent enough that fee levels
are still relevant. The Civic Arts Center Phase 1 is nearly as close on size
and is also a Washington project, though the construction cost is about 10%
lower, suggesting a somewhat leaner scope. The County Health Services Building
scores well on size and cost but is across the border in Oregon, so local
labor and permit cost differences should be noted when using it as a fee anchor.
All three projects show SD and DD fees in the $170,000–$215,000 range for
this scale of civic work, which provides a solid statistical foundation.
```

---

## Quick start

### 1. Prerequisites

- Python 3.11+
- [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- LM Studio (local) or any OpenAI-compatible LLM API
- A SQL Server database with historical project data

### 2. Install

```bash
pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.template .env
# Fill in SQL Server credentials, table name, and column mappings
```

Minimum required: `SQL_SERVER`, `SQL_DB`, `SQL_UID`, `SQL_PWD`, `PRIMARY_TABLE`.

### 4. Verify

```
call validate_connection
```

This confirms DB connectivity, detects scoring columns, checks phase fee
columns, and verifies LLM reachability before you run any searches.

### 5. Discover your data

```
call list_dimensions
```

Returns all valid typology, location, and status values so you can pass
exact strings to `find_comparable_projects`.

### 6. Connect to Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "comparable-projects": {
      "command": "python",
      "args": ["/path/to/server.py"]
    }
  }
}
```

## Honest capability map

### What is fully built

| Capability | Status | Notes |
|---|---|---|
| Structured SQL candidate filtering | ✅ Built | Typology, sqft range, location, status, year range |
| Sqft similarity scoring | ✅ Built | Log-ratio distance; scale-invariant |
| Construction cost scoring | ✅ Built | Same log-ratio method |
| Year proximity scoring | ✅ Built | Linear decay over configurable max gap |
| Location match scoring | ✅ Built | Binary state match; neutral score when location unknown |
| Composite weighted score (0–100) | ✅ Built | All dimension weights configurable via .env |
| Per-dimension score breakdown | ✅ Built | Each result shows sqft/cost/year/location sub-scores |
| Configurable scoring weights | ✅ Built | Set `SCORE_WEIGHTS` in .env; auto-normalised |
| Weight redistribution on missing inputs | ✅ Built | Unused dimensions' weights redistributed proportionally |
| Phase fee display on results | ✅ Built | All detected phase fees shown per result |
| LLM analyst interpretation | ✅ Built | Identifies top 2-3 comps, flags differences, notes weak matches |
| Single project detail lookup | ✅ Built | `get_project_detail` with partial name match fallback |
| Dimension discovery | ✅ Built | `list_dimensions` returns all valid filter values |
| Exclude-self support | ✅ Built | Pass `exclude_project` to find comps for an existing project |
| Read-only SQL safety | ✅ Built | Mutation keywords blocked at query layer |
| Zero hardcoded column names | ✅ Built | All column names via env vars with sensible defaults |

### What is partially built or requires configuration

| Capability | Status | Notes |
|---|---|---|
| Phase fee column detection | ⚠️ Heuristic | Works for common naming patterns; set PHASE_COLS in .env if it fails |
| Construction cost column | ⚠️ Configurable | Defaults to `Estimated_Construction_Cost`; override via COL_CONST_COST |
| Location filter relaxation | ⚠️ Partial | Drops location filter if zero results; does not cascade further |
| Candidate pool size | ⚠️ Configurable | Default 200; increase CANDIDATE_POOL if your database is very large |

### What is not built (known gaps)

| Capability | Gap | Impact |
|---|---|---|
| Semantic / vector similarity | ❌ Not built | Matching is purely numeric; project description text is not used |
| Natural language query input | ❌ Not built | Must provide structured parameters; no "find projects like this RFP" |
| Cross-firm benchmarking | ❌ Not built | Single database only |
| Multi-table joins | ❌ Not built | Assumes all data is in one primary table |
| Scope / complexity scoring | ❌ Not built | No adjustment for contract type, delivery method, or program complexity |
| Saved search / watchlist | ❌ Not built | No persistence between sessions |
| Fee inflation normalization | ❌ Not built | Older project fees are not adjusted to current dollars |

---

## What your database needs

No required column names — the server reads your schema at runtime. For best
results, your primary table should include:

- Project identifier and name columns
- Typology / market sector (for SQL filtering)
- Gross square footage (primary scoring dimension)
- Estimated construction cost (secondary scoring dimension)
- Project status (to filter for completed actuals)
- State or location column (scoring + optional filter)
- Year column (recency scoring)
- Phase fee columns (displayed on results; used by Tool 2 for fee estimation)

Columns that are missing are skipped gracefully — their scoring weight is
redistributed to the remaining dimensions.

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
| Tool 1: `find_comparable_projects` | ✅ This tool | Ranked similarity search across past projects |
| Tool 2: `estimate_phase_fee` | ✅ Built | Single-phase and total-project fee estimation from historical actuals |
| Tool 3: `diagnose_project_risk` | ✅ Built | Benchmark a live project's fee burn against historical norms |

---

## License

MIT — free to use, modify, and distribute.

This package contains no proprietary data, firm-specific documents,
or hardcoded column names. All intelligence comes from your own database.
