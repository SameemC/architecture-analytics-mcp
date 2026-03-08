"""
phase-fee-estimator-mcp
========================
MCP Tool 2: Structured Phase Fee Estimation for Architecture Projects

Given a project's parameters (typology, square footage, phase, location),
returns a statistically grounded fee range derived from your firm's historical
project data, with cited comparable projects.

This tool exposes four MCP endpoints:
  • estimate_phase_fee      — single-phase fee range + cited comps
  • estimate_total_fee      — all-phases roll-up in one call with per-phase breakdown
  • list_typologies         — discover project types and phases in the database
  • validate_connection     — health check for DB + LLM

NO firm-specific data, column names, or documents are embedded.
All schema knowledge is read dynamically from your database at runtime.
Configure via .env — no code changes required for standard deployments.

License: MIT
"""

import os
import re
import math
import asyncio
import warnings
from typing import Any

import pyodbc
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

LM_BASE     = os.getenv("LM_BASE_URL",  "http://localhost:1234/v1")
LM_MODEL    = os.getenv("LM_MODEL",     "lmstudio-community/Meta-Llama-3-8B-Instruct")
LM_API_KEY  = os.getenv("LM_API_KEY",   "lm-studio")

SQL_SERVER  = os.getenv("SQL_SERVER", "localhost")
SQL_PORT    = int(os.getenv("SQL_PORT", "1433"))
SQL_DB      = os.getenv("SQL_DB",     "ProjectData")
SQL_UID     = os.getenv("SQL_UID",    "reader")
SQL_PWD     = os.getenv("SQL_PWD",    "")

COL_PROJECT_ID   = os.getenv("COL_PROJECT_ID",   "Project_ID")
COL_PROJECT_NAME = os.getenv("COL_PROJECT_NAME",  "Project_Name")
COL_TYPOLOGY     = os.getenv("COL_TYPOLOGY",      "Market_Sector")
COL_STATUS       = os.getenv("COL_STATUS",        "Project_Status")
COL_SQFT         = os.getenv("COL_SQFT",          "Gross_Square_Footage")
COL_LOCATION     = os.getenv("COL_LOCATION",      "State")
COL_YEAR         = os.getenv("COL_YEAR",          "Year")

PRIMARY_TABLE   = os.getenv("PRIMARY_TABLE",  "dbo.Projects")
MIN_COMPS       = int(os.getenv("MIN_COMPS",   "3"))
SQFT_TOLERANCE  = float(os.getenv("SQFT_TOLERANCE", "0.40"))
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.15"))

# Phase fee column map — set in .env or auto-detected from schema
# Format: PHASE_COLS=CONCEPT:Concept_Labor_Fee,SD:SD_Fee,...
_PHASE_COLS_RAW = os.getenv("PHASE_COLS", "")


def _parse_phase_cols(raw: str) -> dict[str, str]:
    result = {}
    if not raw:
        return result
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" not in pair:
            continue
        phase, col = pair.split(":", 1)
        result[phase.strip().upper()] = col.strip()
    return result


PHASE_COL_MAP: dict[str, str] = _parse_phase_cols(_PHASE_COLS_RAW)

# Canonical phase order for roll-up display
PHASE_ORDER = ["CONCEPT", "SD", "DD", "CD", "CA"]

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|EXEC)\b", re.I
)


def _connect() -> pyodbc.Connection:
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SQL_SERVER},{SQL_PORT};"
        f"DATABASE={SQL_DB};UID={SQL_UID};PWD={SQL_PWD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def safe_run_sql(sql: str) -> pd.DataFrame:
    sql = sql.strip().rstrip(";")
    if not re.match(r"(?is)^\s*(with\b.*select\b|select\b)", sql):
        raise ValueError("Only SELECT queries are permitted.")
    if FORBIDDEN.search(sql):
        raise ValueError("Query blocked: mutation keyword detected.")
    with _connect() as conn:
        return pd.read_sql_query(sql, conn)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA AUTO-DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def _get_columns() -> pd.DataFrame:
    return safe_run_sql(f"""
        SELECT c.name AS column_name, ty.name AS data_type
        FROM sys.columns c
        JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID('{PRIMARY_TABLE}')
        ORDER BY c.column_id;
    """)


def _detect_phase_fee_columns(cols_df: pd.DataFrame) -> dict[str, str]:
    """
    Auto-detect phase fee columns from schema when PHASE_COLS env var is not set.
    Returns {PHASE_LABEL: column_name}.
    """
    if PHASE_COL_MAP:
        return PHASE_COL_MAP

    numeric_types = {
        "int", "bigint", "smallint", "tinyint", "float", "real",
        "decimal", "numeric", "money", "smallmoney",
    }
    fee_keywords = ["fee", "cost", "labor", "effort", "amount"]
    phase_patterns = {
        "CONCEPT": ["concept"],
        "SD":      ["sd_", "schematic"],
        "DD":      ["dd_", "design_dev", "designdev"],
        "CD":      ["cd_", "construction_doc", "constructiondoc"],
        "CA":      ["ca_", "cca_", "construction_admin", "constructionadmin"],
    }
    detected: dict[str, str] = {}
    for _, row in cols_df.iterrows():
        col   = str(row["column_name"]).lower()
        dtype = str(row["data_type"]).lower()
        if dtype not in numeric_types:
            continue
        if not any(kw in col for kw in fee_keywords):
            continue
        for label, triggers in phase_patterns.items():
            if label not in detected and any(t in col for t in triggers):
                detected[label] = row["column_name"]
                break
    return detected


def _detect_duration_columns(cols_df: pd.DataFrame) -> dict[str, str]:
    """Auto-detect phase duration (months/days) columns."""
    duration_keywords = ["month", "duration", "length", "weeks", "days"]
    phase_patterns = {
        "CONCEPT": ["concept"],
        "SD":      ["sd_", "schematic"],
        "DD":      ["dd_", "design_dev"],
        "CD":      ["cd_", "construction_doc"],
        "CA":      ["ca_", "cca_", "construction_admin"],
    }
    numeric_types = {
        "int", "bigint", "smallint", "tinyint", "float", "real",
        "decimal", "numeric", "money", "smallmoney",
    }
    detected: dict[str, str] = {}
    for _, row in cols_df.iterrows():
        col   = str(row["column_name"]).lower()
        dtype = str(row["data_type"]).lower()
        if dtype not in numeric_types:
            continue
        if not any(kw in col for kw in duration_keywords):
            continue
        for label, triggers in phase_patterns.items():
            if label not in detected and any(t in col for t in triggers):
                detected[label] = row["column_name"]
                break
    return detected


# ─────────────────────────────────────────────────────────────────────────────
# STATISTICS
# ─────────────────────────────────────────────────────────────────────────────

def _sanitize(obj: Any) -> Any:
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def compute_fee_stats(values: pd.Series) -> dict[str, Any]:
    """Compute distribution stats. Excludes zeros and nulls."""
    clean = values.dropna()
    clean = clean[clean > 0]
    if clean.empty:
        return {}
    return {
        "count":  int(len(clean)),
        "min":    float(clean.min()),
        "p10":    float(clean.quantile(0.10)),
        "p25":    float(clean.quantile(0.25)),
        "median": float(clean.quantile(0.50)),
        "mean":   float(clean.mean()),
        "p75":    float(clean.quantile(0.75)),
        "p90":    float(clean.quantile(0.90)),
        "max":    float(clean.max()),
        "std":    float(clean.std()) if len(clean) > 1 else 0.0,
        "cv":     float(clean.std() / clean.mean())
                  if clean.mean() > 0 and len(clean) > 1 else None,
    }


def _confidence_label(stats: dict, comp_count: int) -> str:
    cv = stats.get("cv")
    if comp_count < MIN_COMPS:
        return "LOW — insufficient comparable projects"
    base = "MODERATE" if comp_count < 8 else ("GOOD" if comp_count < 20 else "HIGH")
    if cv is not None and cv > 0.5:
        return f"{base} (wide variance — treat range cautiously)"
    return base


def build_fee_range_text(stats: dict, phase: str, comp_count: int) -> str:
    if not stats:
        return f"Insufficient data to estimate {phase} fees for this project type."
    conf = _confidence_label(stats, comp_count)
    return "\n".join([
        f"Phase: {phase}",
        f"Based on {comp_count} comparable projects",
        f"Confidence: {conf}",
        "",
        f"Recommended range:   ${stats['p25']:,.0f}  –  ${stats['p75']:,.0f}  (25th–75th percentile)",
        f"Median fee:          ${stats['median']:,.0f}",
        f"Mean fee:            ${stats['mean']:,.0f}",
        f"Full observed range: ${stats['min']:,.0f}  –  ${stats['max']:,.0f}",
        f"90th percentile:     ${stats['p90']:,.0f}  (ceiling for complex or high-risk projects)",
    ])


# ─────────────────────────────────────────────────────────────────────────────
# COMP QUERY BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _build_comp_query(
    fee_cols: list[str],
    typology: str | None,
    sqft: float | None,
    location: str | None,
    status_filter: str | None,
    cols_df: pd.DataFrame,
) -> str:
    """
    Builds a read-only T-SQL query to fetch comparable projects.
    Accepts a list of fee columns so the same builder powers both
    single-phase queries and all-phases roll-up queries.
    """
    actual = {str(c).lower() for c in cols_df["column_name"].tolist()}

    # Identity + all requested fee columns
    select_cols = [f"[{COL_PROJECT_ID}]", f"[{COL_PROJECT_NAME}]"]
    select_cols += [f"[{c}]" for c in fee_cols]

    # Optional dimension columns
    for dim in [COL_SQFT, COL_TYPOLOGY, COL_LOCATION, COL_YEAR]:
        if dim.lower() in actual and f"[{dim}]" not in select_cols:
            select_cols.append(f"[{dim}]")

    # At least one fee column must be populated and positive
    fee_presence = " OR ".join(
        f"([{c}] IS NOT NULL AND [{c}] > 0)" for c in fee_cols
    )
    where = [f"({fee_presence})"]

    if status_filter and COL_STATUS.lower() in actual:
        safe_s = status_filter.replace("'", "''")
        where.append(f"UPPER(LTRIM(RTRIM([{COL_STATUS}]))) = '{safe_s.upper()}'")
    if typology and COL_TYPOLOGY.lower() in actual:
        safe_t = typology.replace("'", "''")
        where.append(f"[{COL_TYPOLOGY}] = '{safe_t}'")
    if sqft and sqft > 0 and COL_SQFT.lower() in actual:
        lo, hi = sqft * (1 - SQFT_TOLERANCE), sqft * (1 + SQFT_TOLERANCE)
        where.append(f"[{COL_SQFT}] BETWEEN {lo:.0f} AND {hi:.0f}")
    if location and COL_LOCATION.lower() in actual:
        safe_l = location.replace("'", "''")
        where.append(f"[{COL_LOCATION}] = '{safe_l}'")

    return (
        f"SELECT {', '.join(select_cols)}\n"
        f"FROM {PRIMARY_TABLE} WITH (NOLOCK)\n"
        f"WHERE {chr(10) + '  AND '.join(where)}\n"
        f"ORDER BY [{fee_cols[0]}] DESC;"
    )


def _relax_and_query(
    fee_cols: list[str],
    typology: str | None,
    sqft: float | None,
    location: str | None,
    status_filter: str | None,
    cols_df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str], str]:
    """
    Runs comp query with progressive filter relaxation when too few comps are found.
    Order: drop location → drop sqft range → drop typology.
    Returns (dataframe, relaxation_notes, sql_used).
    """
    relaxations: list[str] = []

    def _try(typ, sq, loc) -> tuple[pd.DataFrame, str]:
        sql = _build_comp_query(fee_cols, typ, sq, loc, status_filter, cols_df)
        return safe_run_sql(sql), sql

    df, sql = _try(typology, sqft, location)

    if len(df) < MIN_COMPS and location:
        relaxations.append(f"Location filter removed (was: {location})")
        df, sql = _try(typology, sqft, None)

    if len(df) < MIN_COMPS and sqft:
        relaxations.append(
            f"Sqft range filter removed (was: {sqft:,.0f} sqft ±{SQFT_TOLERANCE*100:.0f}%)"
        )
        df, sql = _try(typology, None, None)

    if len(df) < MIN_COMPS and typology:
        relaxations.append(f"Typology filter removed (was: {typology})")
        df, sql = _try(None, None, None)

    return df, relaxations, sql


# ─────────────────────────────────────────────────────────────────────────────
# LLM LAYER
# ─────────────────────────────────────────────────────────────────────────────
client = OpenAI(base_url=LM_BASE, api_key=LM_API_KEY)

_ANALYST_SYSTEM = """
You are a senior project finance analyst for an architecture firm.
Interpret fee estimation results and provide clear, actionable guidance
to a project manager preparing a fee proposal.

You receive:
- Project parameters (typology, sqft, phase, location)
- Fee statistics (min, percentiles, mean, max) from comparable projects
- A list of the most relevant comparable projects with actual fees

YOUR RESPONSE MUST:
1. State the recommended fee range in plain dollar amounts
2. Explain what drives fee variation in this category (1-2 sentences)
3. Flag any risk factors (low confidence, wide variance, thin sample)
4. Cite 2-3 comparable projects by name as reference points
5. Stay under 220 words total

FORMATTING (STRICT):
- Plain text only. No markdown, no asterisks, no bullet symbols, no backticks.
- Always space between numbers and words.
- Dollar amounts as $X,XXX or $X,XXX,XXX.
- Do not suggest follow-up questions or additional analyses.
""".strip()

_ROLLUP_ANALYST_SYSTEM = """
You are a senior project finance analyst for an architecture firm.
Interpret a total project fee roll-up and provide actionable guidance
to a project manager preparing a comprehensive fee proposal.

You receive:
- Project parameters (typology, sqft, location)
- Per-phase fee statistics and the projected total fee range
- Named comparable projects with their actual total fees

YOUR RESPONSE MUST:
1. State the total projected fee range in plain dollar amounts
2. Identify which phase(s) carry the most fee weight and why that is typical
3. Flag any phases with LOW confidence or thin samples by name
4. Cite 2-3 comparable projects by name and their total fees as anchors
5. Note if any phases were excluded due to missing data
6. Stay under 250 words total

FORMATTING (STRICT):
- Plain text only. No markdown, no asterisks, no bullet symbols, no backticks.
- Always space between numbers and words.
- Dollar amounts as $X,XXX or $X,XXX,XXX.
- Do not suggest follow-up questions or additional analyses.
""".strip()


def _postprocess(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"[*_`#]", "", text)
    text = re.sub(r"(\d),\s+(\d{3})", r"\1,\2", text)
    text = re.sub(r"(\d)([A-Za-z])", r"\1 \2", text)
    text = re.sub(r"([A-Za-z])(\d)", r"\1 \2", text)
    text = re.sub(r"\.(?=[A-Z])", ". ", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def llm_interpret_estimate(
    params: dict, stats: dict, comps_df: pd.DataFrame, phase: str, fee_col: str
) -> str:
    rows = []
    if not comps_df.empty and fee_col in comps_df.columns:
        for _, row in comps_df.nlargest(5, fee_col).iterrows():
            line = f"  {row.get(COL_PROJECT_NAME, 'Unknown')}"
            fee  = row.get(fee_col)
            sqft = row.get(COL_SQFT)
            yr   = row.get(COL_YEAR)
            if fee:  line += f" — ${float(fee):,.0f}"
            if sqft and float(sqft) > 0: line += f" ({float(sqft):,.0f} sqft)"
            if yr:   line += f" [{int(yr)}]"
            rows.append(line)

    context = (
        f"PROJECT PARAMETERS:\n"
        f"  Typology: {params.get('typology', 'Not specified')}\n"
        f"  Phase:    {phase}\n"
        f"  Sqft:     {f\"{params.get('sqft', 0):,.0f}\" if params.get('sqft') else 'Not specified'}\n"
        f"  Location: {params.get('location', 'Not specified')}\n\n"
        f"FEE STATISTICS ({stats.get('count', 0)} comparable projects):\n"
        f"  Recommended range: ${stats.get('p25', 0):,.0f} – ${stats.get('p75', 0):,.0f}\n"
        f"  Median:  ${stats.get('median', 0):,.0f}\n"
        f"  Mean:    ${stats.get('mean', 0):,.0f}\n"
        f"  Min/Max: ${stats.get('min', 0):,.0f} / ${stats.get('max', 0):,.0f}\n"
        f"  90th pct: ${stats.get('p90', 0):,.0f}\n\n"
        f"TOP COMPARABLE PROJECTS:\n"
        f"{chr(10).join(rows) if rows else '  No named comparables available.'}"
    )
    try:
        resp = client.chat.completions.create(
            model=LM_MODEL,
            messages=[
                {"role": "system", "content": _ANALYST_SYSTEM},
                {"role": "user",   "content": context},
            ],
            temperature=LLM_TEMPERATURE,
        )
        raw = (resp.choices[0].message.content or "") if resp.choices else ""
        return _postprocess(raw) or "Analysis could not be generated."
    except Exception as e:
        return f"LLM analysis error: {e}"


def llm_interpret_rollup(
    params: dict,
    phase_results: list[dict],
    total_p25: float,
    total_median: float,
    total_p75: float,
    total_p90: float,
    comps_with_totals: list[dict],
) -> str:
    phase_lines = []
    for pr in phase_results:
        s = pr.get("fee_stats", {})
        if not s:
            phase_lines.append(f"  {pr['phase']:8s}  No data")
            continue
        rng  = f"${s.get('p25', 0):,.0f}–${s.get('p75', 0):,.0f}"
        conf = _confidence_label(s, s.get("count", 0))
        phase_lines.append(
            f"  {pr['phase']:8s}  range {rng}  "
            f"median ${s.get('median', 0):,.0f}  "
            f"({s.get('count', 0)} comps, {conf})"
        )

    comp_lines = []
    for c in comps_with_totals[:5]:
        line = f"  {c.get('name', 'Unknown')}"
        total = c.get("total_fee")
        sqft  = c.get("sqft")
        yr    = c.get("year")
        if total: line += f" — ${float(total):,.0f} total"
        if sqft and float(sqft) > 0: line += f" ({float(sqft):,.0f} sqft)"
        if yr:    line += f" [{int(yr)}]"
        comp_lines.append(line)

    context = (
        f"PROJECT PARAMETERS:\n"
        f"  Typology: {params.get('typology', 'Not specified')}\n"
        f"  Sqft:     {f\"{params.get('sqft', 0):,.0f}\" if params.get('sqft') else 'Not specified'}\n"
        f"  Location: {params.get('location', 'Not specified')}\n\n"
        f"TOTAL PROJECT FEE PROJECTION:\n"
        f"  Recommended total range: ${total_p25:,.0f} – ${total_p75:,.0f}\n"
        f"  Total median:   ${total_median:,.0f}\n"
        f"  Total 90th pct: ${total_p90:,.0f}\n\n"
        f"PER-PHASE BREAKDOWN:\n"
        f"{chr(10).join(phase_lines)}\n\n"
        f"COMPARABLE PROJECTS (total fees):\n"
        f"{chr(10).join(comp_lines) if comp_lines else '  No total-fee comparables available.'}"
    )
    try:
        resp = client.chat.completions.create(
            model=LM_MODEL,
            messages=[
                {"role": "system", "content": _ROLLUP_ANALYST_SYSTEM},
                {"role": "user",   "content": context},
            ],
            temperature=LLM_TEMPERATURE,
        )
        raw = (resp.choices[0].message.content or "") if resp.choices else ""
        return _postprocess(raw) or "Analysis could not be generated."
    except Exception as e:
        return f"LLM analysis error: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE-PHASE ESTIMATION PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def run_fee_estimation(
    phase: str,
    typology: str | None = None,
    sqft: float | None = None,
    location: str | None = None,
    status_filter: str | None = None,
) -> dict[str, Any]:
    try:
        cols_df = _get_columns()
    except Exception as e:
        return {"error": f"Schema retrieval failed: {e}"}

    phase_cols  = _detect_phase_fee_columns(cols_df)
    dur_cols    = _detect_duration_columns(cols_df)
    phase_upper = phase.strip().upper()

    if not phase_cols:
        return {"error": "Could not auto-detect phase fee columns. Set PHASE_COLS in .env."}
    if phase_upper not in phase_cols:
        return {
            "error": (
                f"Phase '{phase}' not found. "
                f"Available: {', '.join(sorted(phase_cols.keys()))}"
            )
        }

    fee_col = phase_cols[phase_upper]
    dur_col = dur_cols.get(phase_upper)

    try:
        comps_df, relaxations, comp_sql = _relax_and_query(
            [fee_col], typology, sqft, location, status_filter, cols_df
        )
    except Exception as e:
        return {"error": f"Comp query failed: {e}"}

    if comps_df is None or comps_df.empty:
        return {"phase": phase_upper, "comp_count": 0,
                "error": "No comparable projects found."}

    stats     = compute_fee_stats(comps_df[fee_col])
    dur_stats = (
        compute_fee_stats(comps_df[dur_col])
        if dur_col and dur_col in comps_df.columns else {}
    )

    if not stats:
        return {"phase": phase_upper, "comp_count": len(comps_df),
                "error": "Fee column returned no usable numeric values."}

    params    = {"typology": typology, "sqft": sqft, "location": location}
    analysis  = llm_interpret_estimate(params, stats, comps_df, phase_upper, fee_col)
    range_txt = build_fee_range_text(stats, phase_upper, len(comps_df))

    top_comps = []
    display   = [c for c in [COL_PROJECT_NAME, fee_col, COL_SQFT, COL_YEAR]
                 if c in comps_df.columns]
    for _, row in comps_df.nlargest(10, fee_col)[display].iterrows():
        top_comps.append({
            k: (None if isinstance(v, float) and math.isnan(v) else v)
            for k, v in row.items()
        })

    return _sanitize({
        "phase":              phase_upper,
        "fee_column":         fee_col,
        "duration_column":    dur_col,
        "comp_count":         len(comps_df),
        "filters_applied":    {"typology": typology, "sqft": sqft,
                               "location": location, "status": status_filter},
        "filter_relaxations": relaxations,
        "fee_stats":          stats,
        "duration_stats":     dur_stats or None,
        "range_summary":      range_txt,
        "analysis":           analysis,
        "top_comparables":    top_comps,
        "sql_used":           comp_sql,
    })


# ─────────────────────────────────────────────────────────────────────────────
# ALL-PHASES ROLL-UP PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def _compute_total_fee_per_project(
    comps_df: pd.DataFrame,
    phase_cols: dict[str, str],
    phases_included: list[str],
) -> pd.Series:
    """
    For each project row, sum the fee columns across all included phases.

    This is the correct approach for roll-up statistics. Adding per-phase
    medians together would understate total-fee variance because it ignores
    correlation between phases — projects that run expensive in SD tend to
    run expensive in DD too. Summing per-project preserves that relationship.

    Returns a Series of per-project total fees, excluding projects where
    every phase fee column is null or zero.
    """
    cols_to_sum = [
        phase_cols[p] for p in phases_included
        if phase_cols.get(p) and phase_cols[p] in comps_df.columns
    ]
    if not cols_to_sum:
        return pd.Series(dtype=float)
    totals = comps_df[cols_to_sum].apply(
        lambda row: row.dropna().clip(lower=0).sum(), axis=1
    )
    return totals[totals > 0]


def run_total_fee_estimation(
    typology: str | None = None,
    sqft: float | None = None,
    location: str | None = None,
    status_filter: str | None = None,
    phases: list[str] | None = None,
) -> dict[str, Any]:
    """
    All-phases roll-up pipeline.

    Step 1 — Per-phase stats: runs each phase through the same single-phase
              comp query and stats engine.

    Step 2 — Per-project totals: queries projects with at least one fee column
              populated, then sums fees per project to get real total-project
              actuals. This is then used to build the total fee distribution.

    Step 3 — LLM interpretation: sends per-phase breakdown + total stats to
              the roll-up analyst prompt.

    Total range = distribution of real per-project totals, not naive median sums.
    """
    try:
        cols_df = _get_columns()
    except Exception as e:
        return {"error": f"Schema retrieval failed: {e}"}

    phase_cols = _detect_phase_fee_columns(cols_df)
    if not phase_cols:
        return {"error": "Could not auto-detect phase fee columns. Set PHASE_COLS in .env."}

    # Resolve which phases to include
    if phases:
        requested    = [p.strip().upper() for p in phases]
        unknown      = [p for p in requested if p not in phase_cols]
        if unknown:
            return {
                "error": (
                    f"Unknown phases: {', '.join(unknown)}. "
                    f"Available: {', '.join(sorted(phase_cols.keys()))}"
                )
            }
        phases_to_run = [p for p in PHASE_ORDER if p in requested]
    else:
        phases_to_run = [p for p in PHASE_ORDER if p in phase_cols]

    # ── Step 1: per-phase stats ───────────────────────────────────────────────
    phase_results:       list[dict] = []
    overall_relaxations: list[str]  = []

    for phase in phases_to_run:
        fee_col = phase_cols[phase]
        try:
            comps_df, relaxations, _ = _relax_and_query(
                [fee_col], typology, sqft, location, status_filter, cols_df
            )
            stats = compute_fee_stats(comps_df[fee_col]) if not comps_df.empty else {}
        except Exception as e:
            stats       = {}
            relaxations = [f"Query failed: {e}"]

        if relaxations:
            overall_relaxations += [f"{phase}: {r}" for r in relaxations]

        phase_results.append({
            "phase":         phase,
            "fee_column":    fee_col,
            "comp_count":    len(comps_df) if not comps_df.empty else 0,
            "fee_stats":     stats,
            "range_summary": build_fee_range_text(stats, phase, len(comps_df)
                                                   if not comps_df.empty else 0),
        })

    # ── Step 2: real per-project total fees ──────────────────────────────────
    all_fee_cols           = [phase_cols[p] for p in phases_to_run]
    total_stats:     dict  = {}
    comps_with_totals      = []
    total_comp_sql         = ""

    try:
        all_comps_df, _, total_comp_sql = _relax_and_query(
            all_fee_cols, typology, sqft, location, status_filter, cols_df
        )
        if not all_comps_df.empty:
            per_project = _compute_total_fee_per_project(
                all_comps_df, phase_cols, phases_to_run
            )
            total_stats = compute_fee_stats(per_project)

            if not per_project.empty:
                df_with_total = all_comps_df.copy()
                df_with_total["_total_fee"] = per_project
                for _, row in df_with_total.nlargest(10, "_total_fee").iterrows():
                    comps_with_totals.append({
                        "name":      str(row.get(COL_PROJECT_NAME, "Unknown")),
                        "total_fee": row.get("_total_fee"),
                        "sqft":      row.get(COL_SQFT),
                        "year":      row.get(COL_YEAR),
                    })
    except Exception as e:
        overall_relaxations.append(f"Total-fee roll-up query failed: {e}")

    # ── Step 3: LLM roll-up interpretation ───────────────────────────────────
    params              = {"typology": typology, "sqft": sqft, "location": location}
    phases_with_data    = [pr for pr in phase_results if pr.get("fee_stats")]

    if total_stats:
        analysis = llm_interpret_rollup(
            params,
            phase_results,
            total_p25    = total_stats.get("p25", 0),
            total_median = total_stats.get("median", 0),
            total_p75    = total_stats.get("p75", 0),
            total_p90    = total_stats.get("p90", 0),
            comps_with_totals=comps_with_totals,
        )
    elif phases_with_data:
        # Fallback: naive median sum — flag it clearly
        naive_total = sum(
            pr["fee_stats"].get("median", 0) for pr in phases_with_data
        )
        analysis = (
            f"Total fee projection based on sum of per-phase medians: "
            f"${naive_total:,.0f}. "
            "Note: this is a simplified estimate used as a fallback because "
            "insufficient projects have fees recorded across all phases. "
            "For a statistically grounded total, ensure completed projects "
            "have actuals recorded for all phases in your database."
        )
    else:
        analysis = "Insufficient data to generate a total fee projection."

    phases_missing = [pr["phase"] for pr in phase_results if not pr.get("fee_stats")]

    return _sanitize({
        "phases_included":     phases_to_run,
        "phases_missing_data": phases_missing,
        "filters_applied":     {"typology": typology, "sqft": sqft,
                                 "location": location, "status": status_filter},
        "filter_relaxations":  overall_relaxations,
        "per_phase_results":   phase_results,
        "total_fee_stats":     total_stats,
        "total_comp_count":    len(comps_with_totals),
        "top_comparables":     comps_with_totals,
        "analysis":            analysis,
        "sql_used":            total_comp_sql,
        "methodology_note": (
            "Total fee range is derived from the sum of actual per-phase fees "
            "on each comparable project — not from adding per-phase medians. "
            "This preserves real variance across the full project lifecycle."
        ),
    })


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT FORMATTERS
# ─────────────────────────────────────────────────────────────────────────────

def _format_single_phase(result: dict) -> str:
    lines = ["=" * 62, "PHASE FEE ESTIMATE", "=" * 62, "",
             result.get("range_summary", "")]

    dur = result.get("duration_stats")
    if dur:
        lines += [
            "", f"Phase Duration  ({dur.get('count', 0)} projects):",
            f"  Median: {dur.get('median', 0):.1f} months",
            f"  Range:  {dur.get('min', 0):.1f} – {dur.get('max', 0):.1f} months",
        ]

    lines += ["", "-" * 62, "ANALYST INTERPRETATION", "-" * 62, "",
              result.get("analysis", "")]

    if result.get("filter_relaxations"):
        lines += ["", "Note: Filters relaxed to find sufficient comparables:"]
        lines += [f"  - {r}" for r in result["filter_relaxations"]]

    comps   = result.get("top_comparables", [])
    fee_col = result.get("fee_column", "Fee")
    if comps:
        lines += ["", "-" * 62, f"TOP {len(comps)} COMPARABLE PROJECTS", "-" * 62]
        for c in comps:
            row  = f"  {c.get(COL_PROJECT_NAME, 'Unknown')}"
            fee  = c.get(fee_col)
            sqft = c.get(COL_SQFT)
            yr   = c.get(COL_YEAR)
            if fee:  row += f"  |  ${float(fee):,.0f}"
            if sqft and float(sqft) > 0: row += f"  |  {float(sqft):,.0f} sqft"
            if yr:   row += f"  |  {int(yr)}"
            lines.append(row)
    return "\n".join(lines)


def _format_rollup(result: dict) -> str:
    ts    = result.get("total_fee_stats", {})
    lines = [
        "=" * 62,
        "TOTAL PROJECT FEE ESTIMATE  (all-phases roll-up)",
        "=" * 62, "",
    ]

    if ts:
        lines += [
            f"Based on {result.get('total_comp_count', 0)} projects with fee data across phases",
            f"Phases included: {', '.join(result.get('phases_included', []))}",
            "",
            f"Recommended total range:  ${ts.get('p25', 0):,.0f}  –  ${ts.get('p75', 0):,.0f}",
            f"Total median fee:         ${ts.get('median', 0):,.0f}",
            f"Total mean fee:           ${ts.get('mean', 0):,.0f}",
            f"Full observed range:      ${ts.get('min', 0):,.0f}  –  ${ts.get('max', 0):,.0f}",
            f"90th percentile:          ${ts.get('p90', 0):,.0f}",
            "",
            f"({result.get('methodology_note', '')})",
        ]
    else:
        lines.append("Total fee distribution could not be computed from available data.")

    # Per-phase breakdown table
    per_phase = result.get("per_phase_results", [])
    if per_phase:
        lines += ["", "-" * 62, "PER-PHASE BREAKDOWN", "-" * 62]
        lines.append(
            f"  {'Phase':<10} {'Median':>12}  {'P25 – P75 Range':>26}  {'Comps':>5}  Confidence"
        )
        lines.append("  " + "-" * 58)
        for pr in per_phase:
            s     = pr.get("fee_stats", {})
            phase = pr.get("phase", "?")
            if not s:
                lines.append(f"  {phase:<10}  {'No data':>12}")
                continue
            rng  = f"${s.get('p25', 0):,.0f} – ${s.get('p75', 0):,.0f}"
            conf = _confidence_label(s, s.get("count", 0))
            lines.append(
                f"  {phase:<10}  ${s.get('median', 0):>11,.0f}  "
                f"{rng:>26}  {s.get('count', 0):>4}  {conf}"
            )

    missing = result.get("phases_missing_data", [])
    if missing:
        lines += [
            "",
            f"Phases excluded (no data): {', '.join(missing)}",
            "Total range above covers only phases with sufficient data.",
        ]

    lines += ["", "-" * 62, "ANALYST INTERPRETATION", "-" * 62, "",
              result.get("analysis", "")]

    if result.get("filter_relaxations"):
        lines += ["", "Note: Filters relaxed to find sufficient comparables:"]
        lines += [f"  - {r}" for r in result["filter_relaxations"]]

    comps = result.get("top_comparables", [])
    if comps:
        lines += ["", "-" * 62,
                  f"TOP {len(comps)} COMPARABLE PROJECTS  (by total fee)", "-" * 62]
        for c in comps:
            row   = f"  {c.get('name', 'Unknown')}"
            total = c.get("total_fee")
            sqft  = c.get("sqft")
            yr    = c.get("year")
            if total: row += f"  |  ${float(total):,.0f} total"
            if sqft and float(sqft) > 0: row += f"  |  {float(sqft):,.0f} sqft"
            if yr:    row += f"  |  {int(yr)}"
            lines.append(row)
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENT COERCION HELPERS
# ─────────────────────────────────────────────────────────────────────────────
# LLMs can pass numbers as strings, floats as ints, or None for optional args.
# These helpers normalise everything safely so call_tool never raises on input.

def _coerce_float(val: Any, param: str) -> float | None:
    """Return float or None. Raises ValueError with param name on bad input."""
    if val is None or val == "":
        return None
    try:
        result = float(val)
        if not math.isfinite(result):
            raise ValueError(f"'{param}' must be a finite number, got {val!r}")
        return result
    except (TypeError, ValueError):
        raise ValueError(f"'{param}' must be a number, got {val!r}")


def _coerce_int(val: Any, param: str) -> int | None:
    """Return int or None. Accepts '2023', 2023.0, etc."""
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        raise ValueError(f"'{param}' must be an integer, got {val!r}")


def _coerce_str(val: Any) -> str | None:
    """Strip and return string, or None if blank."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _coerce_str_list(val: Any, param: str) -> list[str] | None:
    """Accept a JSON array of strings, or None."""
    if val is None:
        return None
    if not isinstance(val, list):
        raise ValueError(f"'{param}' must be a list of strings, got {val!r}")
    return [str(v).strip() for v in val if str(v).strip()]


# ─────────────────────────────────────────────────────────────────────────────
# MCP SERVER
# ─────────────────────────────────────────────────────────────────────────────

server = Server("phase-fee-estimator")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="estimate_phase_fee",
            description=(
                "Estimate the fee for a single architecture project phase based on "
                "comparable historical projects. Returns a statistically grounded "
                "fee range (P25–P75), median, mean, confidence level, and named "
                "comparable projects. Use this for a focused view of one phase."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "phase": {
                        "type": "string",
                        "description": (
                            "Phase to estimate. Common values: CONCEPT, SD, DD, CD, CA. "
                            "Call list_typologies to see what your database supports."
                        ),
                    },
                    "typology":      {"type": "string",  "description": "Project type / market sector. Optional."},
                    "sqft":          {"type": "number",  "description": "Gross square footage. Optional."},
                    "location":      {"type": "string",  "description": "State abbreviation (e.g. WA). Optional."},
                    "status_filter": {"type": "string",  "description": "Project status filter, e.g. Completed. Optional."},
                },
                "required": ["phase"],
            },
        ),
        Tool(
            name="estimate_total_fee",
            description=(
                "Estimate the TOTAL project fee across all phases in a single call. "
                "Returns a per-phase breakdown table AND a total fee range derived "
                "from real per-project totals — not from naively adding per-phase "
                "medians, which understates variance. Also returns comparable "
                "projects with their actual total fees cited. "
                "Use this for final proposal review or full project go/no-go decisions. "
                "Pass 'phases' to restrict to a subset (e.g. SD through CD only)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "typology":      {"type": "string",  "description": "Project type / market sector. Optional."},
                    "sqft":          {"type": "number",  "description": "Gross square footage. Optional."},
                    "location":      {"type": "string",  "description": "State abbreviation. Optional."},
                    "status_filter": {"type": "string",  "description": "Project status filter. Recommended: Completed. Optional."},
                    "phases": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Specific phases to include, e.g. ['SD', 'DD', 'CD']. "
                            "Omit to include all detected phases."
                        ),
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="list_typologies",
            description=(
                "Returns distinct project typologies and auto-detected phase / duration "
                "columns from the connected database. Run before estimating to see "
                "exact filter values."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="validate_connection",
            description=(
                "Health-check: verifies database connectivity, table existence, "
                "auto-detected columns, and LLM reachability. Run when setting up "
                "a new database connection."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    loop = asyncio.get_running_loop()

    if name == "estimate_phase_fee":
        phase = _coerce_str(arguments.get("phase"))
        if not phase:
            return [TextContent(type="text", text="Error: 'phase' is required.")]

        try:
            sqft          = _coerce_float(arguments.get("sqft"),  "sqft")
            typology      = _coerce_str(arguments.get("typology"))
            location      = _coerce_str(arguments.get("location"))
            status_filter = _coerce_str(arguments.get("status_filter"))
        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid argument: {e}")]

        result = await loop.run_in_executor(
            None,
            lambda: run_fee_estimation(
                phase,
                typology      = typology,
                sqft          = sqft,
                location      = location,
                status_filter = status_filter,
            ),
        )
        if "error" in result:
            return [TextContent(type="text", text=f"Error: {result['error']}")]
        return [TextContent(type="text", text=_format_single_phase(result))]

    elif name == "estimate_total_fee":
        try:
            sqft          = _coerce_float(arguments.get("sqft"), "sqft")
            typology      = _coerce_str(arguments.get("typology"))
            location      = _coerce_str(arguments.get("location"))
            status_filter = _coerce_str(arguments.get("status_filter"))
            phases        = _coerce_str_list(arguments.get("phases"), "phases")
        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid argument: {e}")]

        result = await loop.run_in_executor(
            None,
            lambda: run_total_fee_estimation(
                typology      = typology,
                sqft          = sqft,
                location      = location,
                status_filter = status_filter,
                phases        = phases,
            ),
        )
        if "error" in result:
            return [TextContent(type="text", text=f"Error: {result['error']}")]
        return [TextContent(type="text", text=_format_rollup(result))]

    elif name == "list_typologies":
        try:
            cols_df    = await loop.run_in_executor(None, _get_columns)
            phase_cols = _detect_phase_fee_columns(cols_df)
            dur_cols   = _detect_duration_columns(cols_df)
            actual     = {str(c).lower() for c in cols_df["column_name"].tolist()}
            typ_list:  list[str] = []
            if COL_TYPOLOGY.lower() in actual:
                typ_df = await loop.run_in_executor(
                    None,
                    lambda: safe_run_sql(
                        f"SELECT DISTINCT [{COL_TYPOLOGY}] AS v "
                        f"FROM {PRIMARY_TABLE} WITH (NOLOCK) "
                        f"WHERE [{COL_TYPOLOGY}] IS NOT NULL "
                        f"ORDER BY [{COL_TYPOLOGY}];"
                    ),
                )
                typ_list = typ_df["v"].dropna().astype(str).tolist() if typ_df is not None else []

            lines = [
                "AVAILABLE PHASES (auto-detected):",
                *[f"  {p}  →  fee column: [{c}]" for p, c in sorted(phase_cols.items())],
                "",
                "DURATION COLUMNS (auto-detected):",
                *([f"  {p}  →  [{c}]" for p, c in sorted(dur_cols.items())]
                  if dur_cols else ["  None detected"]),
                "",
                f"PROJECT TYPOLOGIES ({len(typ_list)} found):",
                *([f"  {t}" for t in typ_list] if typ_list else ["  None found"]),
            ]
            return [TextContent(type="text", text="\n".join(lines))]
        except Exception as e:
            return [TextContent(type="text", text=f"Error: {e}")]

    elif name == "validate_connection":
        ok:     list[str] = []
        issues: list[str] = []
        try:
            await loop.run_in_executor(None, lambda: safe_run_sql("SELECT 1 AS ping;"))
            ok.append(f"Database connected: {SQL_SERVER}:{SQL_PORT} / {SQL_DB}")
        except Exception as e:
            issues.append(f"Database connection failed: {e}")
        try:
            df = await loop.run_in_executor(
                None,
                lambda: safe_run_sql(
                    f"SELECT COUNT(*) AS n FROM {PRIMARY_TABLE} WITH (NOLOCK);"
                ),
            )
            n = int(df.iloc[0, 0])
            ok.append(f"Primary table found: {PRIMARY_TABLE} ({n:,} rows)")
        except Exception as e:
            issues.append(f"Primary table not accessible ({PRIMARY_TABLE}): {e}")
        try:
            cols_df    = await loop.run_in_executor(None, _get_columns)
            phase_cols = _detect_phase_fee_columns(cols_df)
            dur_cols   = _detect_duration_columns(cols_df)
            if phase_cols:
                ok.append(f"Phase fee columns detected: {', '.join(sorted(phase_cols.keys()))}")
            else:
                issues.append(
                    "No phase fee columns detected. "
                    "Set PHASE_COLS in .env, e.g.: "
                    "PHASE_COLS=CONCEPT:Concept_Fee,SD:SD_Fee,DD:DD_Fee,CD:CD_Fee,CA:CA_Fee"
                )
            ok.append(
                f"Duration columns detected: {', '.join(sorted(dur_cols.keys()))}"
                if dur_cols else "No duration columns detected (optional)"
            )
        except Exception as e:
            issues.append(f"Schema introspection failed: {e}")
        try:
            await loop.run_in_executor(
                None,
                lambda: client.chat.completions.create(
                    model=LM_MODEL,
                    messages=[{"role": "user", "content": "ping"}],
                    max_tokens=5,
                ),
            )
            ok.append(f"LLM connected: {LM_BASE} / {LM_MODEL}")
        except Exception as e:
            issues.append(f"LLM connection failed: {e}")

        status = "OK" if not issues else "DEGRADED"
        return [TextContent(
            type="text",
            text="\n".join(
                [f"Status: {status}", ""] + ok
                + (["", "Issues:"] + issues if issues else [])
            ),
        )]

    else:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    async with stdio_server() as (reader, writer):
        await server.run(reader, writer, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
