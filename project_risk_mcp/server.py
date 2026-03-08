"""
project-risk-mcp
=================
MCP Tool 3: Project Fee Burn Rate Risk Diagnostics for Architecture Projects

Given a live project currently in delivery, benchmarks its fee burn rate against
historical completed projects of similar typology, size, and phase — and flags
whether the burn rate is normal, elevated, or at risk.

This tool exposes four MCP endpoints:
  • diagnose_project_risk   — core burn rate benchmark for a live project
  • list_active_projects    — discover in-flight projects available for diagnosis
  • list_benchmark_pool     — inspect what historical comps exist for a given context
  • validate_connection     — health check for DB + LLM

What this tool does:
  Step 1 — Query the live project's current fee actuals-to-date and planned fee
            per phase, along with estimated phase completion percentage.
  Step 2 — Pull a historical benchmark pool of completed projects at similar
            typology, sqft, and phase.
  Step 3 — Compute burn rate: (fee_spent / planned_fee) at current % complete.
            Compare against the distribution of historical burn rates at the same
            phase completion band.
  Step 4 — Classify risk level: NORMAL / ELEVATED / AT RISK / CRITICAL based on
            where the current burn rate falls within the historical distribution.
  Step 5 — LLM analyst interpretation with PM-facing action guidance.

What this tool does NOT do (see README for full gap map):
  - Predict final cost at completion (EAC/ETC forecasting)
  - Multi-project portfolio risk aggregation
  - Staffing / FTE burn analysis (fee burn only)
  - Cross-firm benchmarking

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

LM_BASE    = os.getenv("LM_BASE_URL",  "http://localhost:1234/v1")
LM_MODEL   = os.getenv("LM_MODEL",     "lmstudio-community/Meta-Llama-3-8B-Instruct")
LM_API_KEY = os.getenv("LM_API_KEY",   "lm-studio")

SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
SQL_PORT   = int(os.getenv("SQL_PORT", "1433"))
SQL_DB     = os.getenv("SQL_DB",     "ProjectData")
SQL_UID    = os.getenv("SQL_UID",    "reader")
SQL_PWD    = os.getenv("SQL_PWD",    "")

# ── Column name mappings — override via .env if your schema differs ──────────
COL_PROJECT_ID      = os.getenv("COL_PROJECT_ID",      "Project_ID")
COL_PROJECT_NAME    = os.getenv("COL_PROJECT_NAME",     "Project_Name")
COL_TYPOLOGY        = os.getenv("COL_TYPOLOGY",         "Market_Sector")
COL_STATUS          = os.getenv("COL_STATUS",           "Project_Status")
COL_SQFT            = os.getenv("COL_SQFT",             "Gross_Square_Footage")
COL_LOCATION        = os.getenv("COL_LOCATION",         "State")
COL_YEAR            = os.getenv("COL_YEAR",             "Year")

# ── Actuals / planned fee columns ─────────────────────────────────────────────
# These columns hold current spend-to-date and the original planned fee per phase.
# The tool computes burn rate as: actuals_col / planned_col
# Format: ACTUALS_COLS=CONCEPT:Concept_Actuals,SD:SD_Actuals,...
# Format: PLANNED_COLS=CONCEPT:Concept_Planned_Fee,SD:SD_Planned_Fee,...
# If not set, falls back to auto-detection from schema.
_ACTUALS_COLS_RAW = os.getenv("ACTUALS_COLS", "")
_PLANNED_COLS_RAW = os.getenv("PLANNED_COLS", "")

# Phase percent-complete columns — one per phase showing 0–100 or 0.0–1.0
# Format: PCT_COLS=CONCEPT:Concept_Pct_Complete,SD:SD_Pct_Complete,...
# Optional — if absent the tool uses fee-only burn rate without % weighting.
_PCT_COLS_RAW = os.getenv("PCT_COLS", "")

PRIMARY_TABLE         = os.getenv("PRIMARY_TABLE",         "dbo.Projects")
ACTIVE_STATUS_VALUE   = os.getenv("ACTIVE_STATUS_VALUE",   "Active")
COMPLETED_STATUS_VALUE = os.getenv("COMPLETED_STATUS_VALUE", "Completed")

MIN_BENCH_COMPS  = int(os.getenv("MIN_BENCH_COMPS",  "5"))
SQFT_TOLERANCE   = float(os.getenv("SQFT_TOLERANCE", "0.50"))   # ±50% for benchmark pool
LLM_TEMPERATURE  = float(os.getenv("LLM_TEMPERATURE", "0.15"))

# ── Risk threshold config ─────────────────────────────────────────────────────
# At what multiple of historical P75 burn rate do we escalate risk level?
# Defaults: >1.10 = ELEVATED, >1.25 = AT RISK, >1.50 = CRITICAL
RISK_ELEVATED_MULT = float(os.getenv("RISK_ELEVATED_MULT", "1.10"))
RISK_AT_RISK_MULT  = float(os.getenv("RISK_AT_RISK_MULT",  "1.25"))
RISK_CRITICAL_MULT = float(os.getenv("RISK_CRITICAL_MULT", "1.50"))


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN MAP PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_col_map(raw: str) -> dict[str, str]:
    """Parse 'PHASE:ColName,PHASE:ColName' env var into {PHASE: col} dict."""
    result: dict[str, str] = {}
    if not raw:
        return result
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" not in pair:
            continue
        phase, col = pair.split(":", 1)
        result[phase.strip().upper()] = col.strip()
    return result


ACTUALS_COL_MAP: dict[str, str] = _parse_col_map(_ACTUALS_COLS_RAW)
PLANNED_COL_MAP: dict[str, str] = _parse_col_map(_PLANNED_COLS_RAW)
PCT_COL_MAP:     dict[str, str] = _parse_col_map(_PCT_COLS_RAW)


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
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
    """Execute a read-only SQL query; raises on any mutation attempt."""
    sql = sql.strip().rstrip(";")
    if not re.match(r"(?is)^\s*(with\b.*select\b|select\b)", sql):
        raise ValueError("Only SELECT / WITH…SELECT queries are permitted.")
    if FORBIDDEN.search(sql):
        raise ValueError("Query blocked by safety filter (mutation keyword detected).")
    with _connect() as conn:
        return pd.read_sql_query(sql, conn)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA AUTO-DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def _get_columns() -> pd.DataFrame:
    sql = f"""
    SELECT c.name AS column_name, ty.name AS data_type
    FROM sys.columns c
    JOIN sys.types ty ON ty.user_type_id = c.user_type_id
    WHERE c.object_id = OBJECT_ID('{PRIMARY_TABLE}')
    ORDER BY c.column_id;
    """
    return safe_run_sql(sql)


def _col_exists(cols_df: pd.DataFrame, col_name: str) -> bool:
    actual = {str(c).lower() for c in cols_df["column_name"].tolist()}
    return col_name.lower() in actual


_NUMERIC_TYPES = {
    "int", "bigint", "smallint", "tinyint", "float", "real",
    "decimal", "numeric", "money", "smallmoney"
}


def _detect_phase_col_pairs(
    cols_df: pd.DataFrame,
) -> tuple[dict[str, str], dict[str, str]]:
    """
    Auto-detect (actuals_cols, planned_cols) from schema.

    Actuals heuristic: numeric columns containing 'actual' or 'spent' or 'to_date'
    AND a phase keyword.
    Planned heuristic: numeric columns containing 'fee' or 'planned' or 'budget'
    AND a phase keyword. Falls back to any fee column if 'planned' isn't present.
    """
    if ACTUALS_COL_MAP and PLANNED_COL_MAP:
        return ACTUALS_COL_MAP, PLANNED_COL_MAP

    actuals_kw  = ["actual", "spent", "to_date", "todate", "incurred", "billed"]
    planned_kw  = ["planned", "budget", "fee", "contract", "original"]
    phase_patterns = {
        "CONCEPT": ["concept"],
        "SD":      ["sd_", "schematic"],
        "DD":      ["dd_", "design_dev", "designdev"],
        "CD":      ["cd_", "construction_doc", "constructiondoc"],
        "CA":      ["ca_", "cca_", "construction_admin", "constructionadmin"],
    }

    detected_actuals: dict[str, str] = {}
    detected_planned: dict[str, str] = {}

    for _, row in cols_df.iterrows():
        col   = str(row["column_name"]).lower()
        dtype = str(row["data_type"]).lower()
        orig  = str(row["column_name"])

        if dtype not in _NUMERIC_TYPES:
            continue

        is_actual  = any(kw in col for kw in actuals_kw)
        is_planned = any(kw in col for kw in planned_kw)

        for phase_label, triggers in phase_patterns.items():
            if not any(t in col for t in triggers):
                continue
            if is_actual and phase_label not in detected_actuals:
                detected_actuals[phase_label] = orig
            if is_planned and phase_label not in detected_planned:
                detected_planned[phase_label] = orig

    # Override with env map where provided
    if ACTUALS_COL_MAP:
        detected_actuals = ACTUALS_COL_MAP
    if PLANNED_COL_MAP:
        detected_planned = PLANNED_COL_MAP

    return detected_actuals, detected_planned


def _detect_pct_cols(cols_df: pd.DataFrame) -> dict[str, str]:
    """Auto-detect phase percent-complete columns."""
    if PCT_COL_MAP:
        return PCT_COL_MAP

    pct_kw = ["pct", "percent", "complete", "progress"]
    phase_patterns = {
        "CONCEPT": ["concept"],
        "SD":      ["sd_", "schematic"],
        "DD":      ["dd_", "design_dev"],
        "CD":      ["cd_", "construction_doc"],
        "CA":      ["ca_", "cca_", "construction_admin"],
    }
    detected: dict[str, str] = {}
    for _, row in cols_df.iterrows():
        col   = str(row["column_name"]).lower()
        dtype = str(row["data_type"]).lower()
        orig  = str(row["column_name"])
        if dtype not in _NUMERIC_TYPES:
            continue
        if not any(kw in col for kw in pct_kw):
            continue
        for phase_label, triggers in phase_patterns.items():
            if phase_label not in detected and any(t in col for t in triggers):
                detected[phase_label] = orig
                break
    return detected


# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENT COERCION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _coerce_float(val: Any, param: str) -> float | None:
    if val is None or val == "":
        return None
    try:
        result = float(val)
        if not math.isfinite(result):
            raise ValueError(f"'{param}' must be a finite number, got {val!r}")
        return result
    except (TypeError, ValueError):
        raise ValueError(f"'{param}' must be a number, got {val!r}")


def _coerce_str(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


# ─────────────────────────────────────────────────────────────────────────────
# SANITIZE / POSTPROCESS
# ─────────────────────────────────────────────────────────────────────────────

def _sanitize(obj: Any) -> Any:
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def _postprocess(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"[*_`#]", "", text)
    text = re.sub(r"(\d),\s+(\d{3})", r"\1,\2", text)
    text = re.sub(r"(\d)([A-Za-z])", r"\1 \2", text)
    text = re.sub(r"([A-Za-z])(\d)", r"\1 \2", text)
    text = re.sub(r"\.(?=[A-Z])", ". ", text)
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# BURN RATE ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _compute_burn_rate(actuals: float, planned: float) -> float | None:
    """
    Burn rate = actuals_spent / planned_fee.

    A burn rate of 1.0 = exactly on budget.
    A burn rate of 1.25 = 25% over budget.
    Returns None if planned is zero or missing.
    """
    if not planned or planned <= 0:
        return None
    if actuals is None or not math.isfinite(actuals):
        return None
    return actuals / planned


def _compute_burn_stats(burn_rates: pd.Series) -> dict[str, Any]:
    """
    Compute distribution stats for a series of historical burn rates.
    Excludes nulls; keeps zeros (a 0.0 burn rate is valid — project not yet started).
    """
    clean = burn_rates.dropna()
    if clean.empty:
        return {}
    return {
        "count":  int(len(clean)),
        "min":    float(clean.min()),
        "p25":    float(clean.quantile(0.25)),
        "median": float(clean.median()),
        "mean":   float(clean.mean()),
        "p75":    float(clean.quantile(0.75)),
        "p90":    float(clean.quantile(0.90)),
        "max":    float(clean.max()),
        "std":    float(clean.std()) if len(clean) > 1 else 0.0,
    }


def _classify_risk(
    current_burn: float,
    bench_stats:  dict[str, Any],
) -> tuple[str, str]:
    """
    Classify risk level relative to historical P75.
    Returns (risk_level, explanation).
    """
    p75 = bench_stats.get("p75")
    p90 = bench_stats.get("p90")
    med = bench_stats.get("median")

    if p75 is None:
        return "UNKNOWN", "Insufficient benchmark data to classify risk."

    ratio_to_p75 = current_burn / p75 if p75 > 0 else None

    if ratio_to_p75 is None:
        return "UNKNOWN", "Could not compute ratio to benchmark P75."

    if ratio_to_p75 <= RISK_ELEVATED_MULT:
        level = "NORMAL"
        expl  = (
            f"Burn rate of {current_burn:.2f} is within normal range "
            f"(historical P75 is {p75:.2f}). Project is tracking as expected."
        )
    elif ratio_to_p75 <= RISK_AT_RISK_MULT:
        level = "ELEVATED"
        expl  = (
            f"Burn rate of {current_burn:.2f} is {(ratio_to_p75 - 1)*100:.0f}% "
            f"above the historical P75 of {p75:.2f}. "
            f"Monitor closely — this is elevated but not yet at risk threshold."
        )
    elif ratio_to_p75 <= RISK_CRITICAL_MULT:
        level = "AT RISK"
        expl  = (
            f"Burn rate of {current_burn:.2f} is {(ratio_to_p75 - 1)*100:.0f}% "
            f"above the historical P75 of {p75:.2f}. "
            f"Recommend scope review and PM conversation."
        )
    else:
        level = "CRITICAL"
        expl  = (
            f"Burn rate of {current_burn:.2f} exceeds the historical P90 threshold. "
            f"Historical P75 is {p75:.2f}, P90 is {p90:.2f if p90 else 'N/A'}. "
            f"Significant overrun risk — immediate action recommended."
        )

    return level, expl


# ─────────────────────────────────────────────────────────────────────────────
# LLM ANALYST
# ─────────────────────────────────────────────────────────────────────────────

client = OpenAI(base_url=LM_BASE, api_key=LM_API_KEY)

_RISK_ANALYST_SYSTEM = """
You are a senior project controls analyst for an architecture firm.
Your role is to interpret fee burn rate data and provide clear, actionable
guidance to a project manager whose project is currently in delivery.

You will receive:
- The live project's name, phase, current burn rate, and risk classification
- Statistical benchmark of historical burn rates for comparable projects
- A list of historical comps used for the benchmark
- Context: typology, approximate sqft, and phase

YOUR RESPONSE MUST:
1. State the risk level and what it means in plain language (1 sentence)
2. Put the burn rate in context — is this typical for this phase and typology?
3. Identify the most likely cause of elevated burn if applicable (1-2 sentences)
4. Give 1-2 concrete actions the PM should take right now
5. Keep response under 200 words total

FORMATTING RULES (STRICT):
- Plain text only. No markdown, no asterisks, no bullet symbols, no backticks.
- Always include a space between numbers and words.
- Write percentages as "X%" with no space before the percent sign.
- Replace dashes used as bullets with plain sentences.
- Do not suggest follow-up questions.
- Do not propose additional analyses.
""".strip()


def llm_interpret_risk(
    project_name:    str,
    phase:           str,
    current_burn:    float,
    risk_level:      str,
    risk_expl:       str,
    bench_stats:     dict[str, Any],
    pct_complete:    float | None,
    typology:        str | None,
    sqft:            float | None,
    bench_comps:     list[dict],
) -> str:
    """Sends burn rate + benchmark context to the LLM for PM-facing interpretation."""

    comp_lines = []
    for c in bench_comps[:6]:
        name     = c.get("name", "Unknown")
        burn     = c.get("burn_rate")
        yr       = c.get("year")
        sqft_val = c.get("sqft")
        line = f"  {name}"
        if burn is not None:
            line += f" — burn rate {burn:.2f}"
        if sqft_val:
            line += f" ({float(sqft_val):,.0f} sqft)"
        if yr:
            line += f" [{int(yr)}]"
        comp_lines.append(line)

    comps_text = "\n".join(comp_lines) if comp_lines else "No named comparables available."

    pct_text = f"{pct_complete * 100:.0f}%" if pct_complete is not None else "Not provided"

    context = f"""
LIVE PROJECT:
  Name:          {project_name}
  Phase:         {phase}
  Typology:      {typology or 'Not specified'}
  Sqft:          {f"{sqft:,.0f}" if sqft else 'Not specified'}
  Phase % complete: {pct_text}
  Current burn rate: {current_burn:.2f}  (1.0 = exactly on budget)
  Risk level:    {risk_level}
  Assessment:    {risk_expl}

BENCHMARK STATISTICS ({bench_stats.get('count', 0)} historical projects):
  Median burn rate: {bench_stats.get('median', 0):.2f}
  P25 / P75:        {bench_stats.get('p25', 0):.2f} / {bench_stats.get('p75', 0):.2f}
  P90:              {bench_stats.get('p90', 0):.2f}
  Min / Max:        {bench_stats.get('min', 0):.2f} / {bench_stats.get('max', 0):.2f}

BENCHMARK COMPARABLE PROJECTS:
{comps_text}
""".strip()

    messages = [
        {"role": "system", "content": _RISK_ANALYST_SYSTEM},
        {"role": "user",   "content": context},
    ]
    try:
        resp = client.chat.completions.create(
            model=LM_MODEL,
            messages=messages,
            temperature=LLM_TEMPERATURE,
        )
        raw = (resp.choices[0].message.content or "") if resp.choices else ""
        return _postprocess(raw) or "Analysis could not be generated."
    except Exception as e:
        return f"LLM analysis error: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# QUERY BUILDERS
# ─────────────────────────────────────────────────────────────────────────────

def _build_live_project_query(
    project_name: str,
    actuals_cols: dict[str, str],
    planned_cols: dict[str, str],
    pct_cols:     dict[str, str],
    cols_df:      pd.DataFrame,
) -> str:
    """
    Fetch the live project's fee actuals, planned fees, and pct-complete columns
    by project name (partial match supported).
    """
    select_parts = [
        f"[{COL_PROJECT_ID}]",
        f"[{COL_PROJECT_NAME}]",
    ]

    if _col_exists(cols_df, COL_TYPOLOGY):
        select_parts.append(f"[{COL_TYPOLOGY}]")
    if _col_exists(cols_df, COL_SQFT):
        select_parts.append(f"[{COL_SQFT}]")
    if _col_exists(cols_df, COL_STATUS):
        select_parts.append(f"[{COL_STATUS}]")
    if _col_exists(cols_df, COL_YEAR):
        select_parts.append(f"[{COL_YEAR}]")

    for col in list(actuals_cols.values()) + list(planned_cols.values()) + list(pct_cols.values()):
        if _col_exists(cols_df, col):
            bracketed = f"[{col}]"
            if bracketed not in select_parts:
                select_parts.append(bracketed)

    safe_name = project_name.replace("'", "''")
    sql = (
        f"SELECT TOP 5 {', '.join(select_parts)}\n"
        f"FROM {PRIMARY_TABLE} WITH (NOLOCK)\n"
        f"WHERE [{COL_PROJECT_NAME}] LIKE '%{safe_name}%'\n"
        f"ORDER BY [{COL_PROJECT_NAME}];"
    )
    return sql


def _build_benchmark_query(
    phase:        str,
    actuals_col:  str,
    planned_col:  str,
    typology:     str | None,
    sqft:         float | None,
    cols_df:      pd.DataFrame,
) -> str:
    """
    Pull completed historical projects for burn rate benchmarking.
    Returns projects that have both actuals and planned fee > 0 for the given phase.
    """
    select_parts = [
        f"[{COL_PROJECT_ID}]",
        f"[{COL_PROJECT_NAME}]",
        f"[{actuals_col}]",
        f"[{planned_col}]",
    ]
    if _col_exists(cols_df, COL_SQFT):
        select_parts.append(f"[{COL_SQFT}]")
    if _col_exists(cols_df, COL_YEAR):
        select_parts.append(f"[{COL_YEAR}]")
    if _col_exists(cols_df, COL_TYPOLOGY):
        select_parts.append(f"[{COL_TYPOLOGY}]")

    where = [
        f"[{actuals_col}] IS NOT NULL",
        f"[{actuals_col}] > 0",
        f"[{planned_col}] IS NOT NULL",
        f"[{planned_col}] > 0",
    ]

    if _col_exists(cols_df, COL_STATUS):
        safe_status = COMPLETED_STATUS_VALUE.replace("'", "''")
        where.append(
            f"UPPER(LTRIM(RTRIM([{COL_STATUS}]))) = '{safe_status.upper()}'"
        )

    if typology and _col_exists(cols_df, COL_TYPOLOGY):
        safe_typ = typology.replace("'", "''")
        where.append(f"[{COL_TYPOLOGY}] = '{safe_typ}'")

    if sqft and sqft > 0 and _col_exists(cols_df, COL_SQFT):
        lo = sqft * (1 - SQFT_TOLERANCE)
        hi = sqft * (1 + SQFT_TOLERANCE)
        where.append(f"[{COL_SQFT}] BETWEEN {lo:.0f} AND {hi:.0f}")

    sql = (
        f"SELECT {', '.join(select_parts)}\n"
        f"FROM {PRIMARY_TABLE} WITH (NOLOCK)\n"
        f"WHERE {chr(10) + '  AND '.join(where)}\n"
        f"ORDER BY [{COL_YEAR}] DESC;"
    )
    return sql


def _build_active_projects_query(cols_df: pd.DataFrame) -> str:
    """List active (in-delivery) projects."""
    select_parts = [f"[{COL_PROJECT_ID}]", f"[{COL_PROJECT_NAME}]"]
    if _col_exists(cols_df, COL_TYPOLOGY):
        select_parts.append(f"[{COL_TYPOLOGY}]")
    if _col_exists(cols_df, COL_SQFT):
        select_parts.append(f"[{COL_SQFT}]")
    if _col_exists(cols_df, COL_YEAR):
        select_parts.append(f"[{COL_YEAR}]")

    where = []
    if _col_exists(cols_df, COL_STATUS):
        safe = ACTIVE_STATUS_VALUE.replace("'", "''")
        where.append(f"UPPER(LTRIM(RTRIM([{COL_STATUS}]))) = '{safe.upper()}'")

    where_clause = f"WHERE {chr(10).join(where)}" if where else ""

    return (
        f"SELECT TOP 100 {', '.join(select_parts)}\n"
        f"FROM {PRIMARY_TABLE} WITH (NOLOCK)\n"
        f"{where_clause}\n"
        f"ORDER BY [{COL_PROJECT_NAME}];"
    )


# ─────────────────────────────────────────────────────────────────────────────
# CORE PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

def run_diagnose_project_risk(
    project_name:  str,
    phase:         str,
    typology:      str | None = None,
    sqft_override: float | None = None,
    pct_complete_override: float | None = None,
) -> dict[str, Any]:
    """
    Full pipeline:
      1. Fetch live project actuals + planned fee
      2. Pull historical benchmark pool
      3. Compute burn rates for all historical comps
      4. Classify risk level vs. distribution
      5. LLM interpretation
    Returns a structured result dict.
    """
    # 1. Get schema
    try:
        cols_df = _get_columns()
    except Exception as e:
        return {"error": f"Schema retrieval failed: {e}"}

    actuals_cols, planned_cols = _detect_phase_col_pairs(cols_df)
    pct_cols = _detect_pct_cols(cols_df)

    phase_upper = phase.strip().upper()

    if not actuals_cols or not planned_cols:
        return {
            "error": (
                "Could not auto-detect actuals/planned fee columns. "
                "Set ACTUALS_COLS and PLANNED_COLS in .env, e.g.: "
                "ACTUALS_COLS=CONCEPT:Concept_Actuals,SD:SD_Actuals "
                "PLANNED_COLS=CONCEPT:Concept_Fee,SD:SD_Fee"
            )
        }

    if phase_upper not in actuals_cols or phase_upper not in planned_cols:
        available = sorted(set(actuals_cols.keys()) & set(planned_cols.keys()))
        return {
            "error": (
                f"Phase '{phase}' not found in detected column pairs. "
                f"Available phases with both actuals and planned columns: "
                f"{', '.join(available) if available else 'None detected'}"
            )
        }

    actuals_col = actuals_cols[phase_upper]
    planned_col = planned_cols[phase_upper]
    pct_col     = pct_cols.get(phase_upper)

    # 2. Fetch live project row
    try:
        live_sql = _build_live_project_query(
            project_name, actuals_cols, planned_cols, pct_cols, cols_df
        )
        live_df = safe_run_sql(live_sql)
    except Exception as e:
        return {"error": f"Live project query failed: {e}"}

    if live_df is None or live_df.empty:
        return {
            "error": (
                f"No project found matching '{project_name}'. "
                f"Use list_active_projects to see available projects."
            )
        }

    # Use first matching row
    live_row   = live_df.iloc[0]
    found_name = str(live_row.get(COL_PROJECT_NAME, project_name))

    # Extract live metrics
    raw_actuals = live_row.get(actuals_col)
    raw_planned = live_row.get(planned_col)
    raw_pct     = live_row.get(pct_col) if pct_col else None

    if raw_actuals is None or raw_planned is None:
        return {
            "project":   found_name,
            "phase":     phase_upper,
            "error": (
                f"Project found but fee columns are NULL for phase {phase_upper}. "
                f"Actuals column: [{actuals_col}], Planned column: [{planned_col}]."
            ),
        }

    try:
        actuals_val = float(raw_actuals)
        planned_val = float(raw_planned)
    except (TypeError, ValueError) as e:
        return {"error": f"Fee data could not be parsed as numbers: {e}"}

    # Handle pct_complete: env override > db column > None
    pct_complete: float | None = None
    if pct_complete_override is not None:
        pct_complete = pct_complete_override
    elif raw_pct is not None:
        try:
            pct_val = float(raw_pct)
            # Normalise 0-100 scale to 0.0-1.0
            pct_complete = pct_val / 100.0 if pct_val > 1.0 else pct_val
        except (TypeError, ValueError):
            pct_complete = None

    # Extract project context (typology, sqft)
    proj_typology = typology or (
        str(live_row.get(COL_TYPOLOGY, "")) if _col_exists(cols_df, COL_TYPOLOGY) else None
    )
    proj_typology = proj_typology if proj_typology else None

    proj_sqft = sqft_override
    if proj_sqft is None and _col_exists(cols_df, COL_SQFT):
        raw_sqft = live_row.get(COL_SQFT)
        if raw_sqft is not None:
            try:
                proj_sqft = float(raw_sqft)
            except (TypeError, ValueError):
                proj_sqft = None

    # 3. Compute live burn rate
    current_burn = _compute_burn_rate(actuals_val, planned_val)
    if current_burn is None:
        return {
            "project": found_name,
            "phase":   phase_upper,
            "error": (
                f"Could not compute burn rate. "
                f"Actuals: {actuals_val}, Planned: {planned_val}. "
                f"Planned fee must be > 0."
            ),
        }

    # 4. Pull benchmark pool (completed historical projects)
    relaxation_notes: list[str] = []
    try:
        bench_sql = _build_benchmark_query(
            phase_upper, actuals_col, planned_col,
            proj_typology, proj_sqft, cols_df
        )
        bench_df = safe_run_sql(bench_sql)
    except Exception as e:
        return {"error": f"Benchmark query failed: {e}"}

    # Relax sqft if too few comps
    if len(bench_df) < MIN_BENCH_COMPS and proj_sqft:
        relaxation_notes.append(
            f"Sqft range filter removed (was: {proj_sqft:,.0f} sqft ±{SQFT_TOLERANCE*100:.0f}%)"
        )
        try:
            bench_sql = _build_benchmark_query(
                phase_upper, actuals_col, planned_col,
                proj_typology, None, cols_df
            )
            bench_df = safe_run_sql(bench_sql)
        except Exception:
            pass

    # Relax typology if still too few
    if len(bench_df) < MIN_BENCH_COMPS and proj_typology:
        relaxation_notes.append(
            f"Typology filter removed (was: {proj_typology})"
        )
        try:
            bench_sql = _build_benchmark_query(
                phase_upper, actuals_col, planned_col,
                None, None, cols_df
            )
            bench_df = safe_run_sql(bench_sql)
        except Exception:
            pass

    if bench_df is None or bench_df.empty:
        return {
            "project":      found_name,
            "phase":        phase_upper,
            "current_burn": current_burn,
            "pct_complete": pct_complete,
            "error": (
                "No completed historical projects found for benchmarking. "
                "Risk level cannot be classified without a benchmark pool."
            ),
        }

    # 5. Compute historical burn rates
    bench_df = bench_df.copy()
    bench_df["_burn_rate"] = bench_df.apply(
        lambda r: _compute_burn_rate(
            float(r[actuals_col]) if r.get(actuals_col) is not None else None,
            float(r[planned_col]) if r.get(planned_col) is not None else None,
        ),
        axis=1,
    )
    bench_df = bench_df[bench_df["_burn_rate"].notna()]

    if bench_df.empty:
        return {
            "project":      found_name,
            "phase":        phase_upper,
            "current_burn": current_burn,
            "pct_complete": pct_complete,
            "error":        "Benchmark pool returned no usable burn rates.",
        }

    bench_stats = _compute_burn_stats(bench_df["_burn_rate"])

    # 6. Classify risk
    risk_level, risk_expl = _classify_risk(current_burn, bench_stats)

    # 7. Build bench_comps list for LLM
    bench_comps: list[dict] = []
    for _, row in bench_df.head(8).iterrows():
        bench_comps.append({
            "name":      str(row.get(COL_PROJECT_NAME, "Unknown")),
            "burn_rate": row.get("_burn_rate"),
            "sqft":      row.get(COL_SQFT),
            "year":      row.get(COL_YEAR),
        })

    # 8. LLM interpretation
    analysis = llm_interpret_risk(
        project_name  = found_name,
        phase         = phase_upper,
        current_burn  = current_burn,
        risk_level    = risk_level,
        risk_expl     = risk_expl,
        bench_stats   = bench_stats,
        pct_complete  = pct_complete,
        typology      = proj_typology,
        sqft          = proj_sqft,
        bench_comps   = bench_comps,
    )

    return _sanitize({
        "project":             found_name,
        "phase":               phase_upper,
        "actuals_column":      actuals_col,
        "planned_column":      planned_col,
        "pct_complete_column": pct_col,
        "actuals_value":       actuals_val,
        "planned_value":       planned_val,
        "pct_complete":        pct_complete,
        "current_burn_rate":   current_burn,
        "risk_level":          risk_level,
        "risk_explanation":    risk_expl,
        "filters_applied": {
            "typology": proj_typology,
            "sqft":     proj_sqft,
        },
        "filter_relaxations":  relaxation_notes,
        "benchmark_count":     len(bench_df),
        "benchmark_stats":     bench_stats,
        "benchmark_comps":     bench_comps,
        "analysis":            analysis,
        "bench_sql_used":      bench_sql,
    })


def run_list_active_projects() -> dict[str, Any]:
    """Return active in-delivery projects for selection."""
    try:
        cols_df = _get_columns()
        sql     = _build_active_projects_query(cols_df)
        df      = safe_run_sql(sql)
    except Exception as e:
        return {"error": f"Query failed: {e}"}

    if df is None or df.empty:
        return {
            "projects": [],
            "message": (
                f"No projects found with status '{ACTIVE_STATUS_VALUE}'. "
                f"Check ACTIVE_STATUS_VALUE in .env matches your data."
            ),
        }

    projects = []
    for _, row in df.iterrows():
        entry: dict[str, Any] = {
            "name":     str(row.get(COL_PROJECT_NAME, "")),
            "id":       str(row.get(COL_PROJECT_ID, "")),
            "typology": str(row.get(COL_TYPOLOGY, "")) if COL_TYPOLOGY in df.columns else None,
            "sqft":     row.get(COL_SQFT),
            "year":     row.get(COL_YEAR),
        }
        projects.append(entry)

    return {"projects": projects, "count": len(projects)}


def run_list_benchmark_pool(
    phase:    str,
    typology: str | None = None,
    sqft:     float | None = None,
) -> dict[str, Any]:
    """Return the benchmark pool that would be used for a given phase + filters."""
    try:
        cols_df = _get_columns()
    except Exception as e:
        return {"error": f"Schema retrieval failed: {e}"}

    actuals_cols, planned_cols = _detect_phase_col_pairs(cols_df)
    phase_upper = phase.strip().upper()

    if phase_upper not in actuals_cols or phase_upper not in planned_cols:
        available = sorted(set(actuals_cols.keys()) & set(planned_cols.keys()))
        return {
            "error": (
                f"Phase '{phase}' not found. "
                f"Available: {', '.join(available) if available else 'None detected'}"
            )
        }

    actuals_col = actuals_cols[phase_upper]
    planned_col = planned_cols[phase_upper]

    try:
        sql = _build_benchmark_query(
            phase_upper, actuals_col, planned_col, typology, sqft, cols_df
        )
        bench_df = safe_run_sql(sql)
    except Exception as e:
        return {"error": f"Benchmark query failed: {e}"}

    if bench_df is None or bench_df.empty:
        return {"count": 0, "projects": [], "message": "No matching completed projects found."}

    bench_df = bench_df.copy()
    bench_df["_burn_rate"] = bench_df.apply(
        lambda r: _compute_burn_rate(
            float(r[actuals_col]) if r.get(actuals_col) is not None else None,
            float(r[planned_col]) if r.get(planned_col) is not None else None,
        ),
        axis=1,
    )

    projects = []
    for _, row in bench_df.iterrows():
        projects.append({
            "name":      str(row.get(COL_PROJECT_NAME, "")),
            "burn_rate": row.get("_burn_rate"),
            "sqft":      row.get(COL_SQFT),
            "year":      row.get(COL_YEAR),
            "typology":  row.get(COL_TYPOLOGY),
        })

    stats = _compute_burn_stats(bench_df["_burn_rate"].dropna())

    return _sanitize({
        "phase":    phase_upper,
        "count":    len(bench_df),
        "stats":    stats,
        "projects": projects,
    })


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT FORMATTERS
# ─────────────────────────────────────────────────────────────────────────────

_RISK_COLORS = {
    "NORMAL":   "✓",
    "ELEVATED": "⚠",
    "AT RISK":  "⚠⚠",
    "CRITICAL": "✗✗",
    "UNKNOWN":  "?",
}


def _format_diagnosis(result: dict) -> str:
    risk   = result.get("risk_level", "UNKNOWN")
    icon   = _RISK_COLORS.get(risk, "?")
    burn   = result.get("current_burn_rate")
    pct    = result.get("pct_complete")
    stats  = result.get("benchmark_stats", {})
    comps  = result.get("benchmark_comps", [])
    relax  = result.get("filter_relaxations", [])

    lines = [
        "=" * 60,
        f"PROJECT RISK DIAGNOSIS  [{icon} {risk}]",
        "=" * 60,
        "",
        f"Project:  {result.get('project', 'Unknown')}",
        f"Phase:    {result.get('phase', 'Unknown')}",
        f"Actuals column:  [{result.get('actuals_column', '')}]  →  ${result.get('actuals_value', 0):,.0f}",
        f"Planned column:  [{result.get('planned_column', '')}]  →  ${result.get('planned_value', 0):,.0f}",
        f"Phase % complete: {f'{pct*100:.0f}%' if pct is not None else 'Not available'}",
        "",
        f"Current burn rate:  {burn:.2f}  (1.00 = exactly on budget)",
        "",
        result.get("risk_explanation", ""),
    ]

    if stats:
        lines += [
            "",
            f"BENCHMARK  ({stats.get('count', 0)} comparable completed projects)",
            f"  Median burn rate: {stats.get('median', 0):.2f}",
            f"  P25 / P75:        {stats.get('p25', 0):.2f} / {stats.get('p75', 0):.2f}",
            f"  P90:              {stats.get('p90', 0):.2f}",
            f"  Min / Max:        {stats.get('min', 0):.2f} / {stats.get('max', 0):.2f}",
        ]

    lines += [
        "",
        "-" * 60,
        "ANALYST INTERPRETATION",
        "-" * 60,
        "",
        result.get("analysis", ""),
    ]

    if relax:
        lines += ["", "Note: Filters relaxed to find sufficient benchmark projects:"]
        for r in relax:
            lines.append(f"  - {r}")

    if comps:
        lines += [
            "",
            "-" * 60,
            f"BENCHMARK PROJECTS ({len(comps)} shown)",
            "-" * 60,
        ]
        for c in comps:
            name = c.get("name", "Unknown")
            br   = c.get("burn_rate")
            yr   = c.get("year")
            sf   = c.get("sqft")
            row  = f"  {name}"
            if br is not None:
                row += f"  |  burn {br:.2f}"
            if sf:
                row += f"  |  {float(sf):,.0f} sqft"
            if yr:
                row += f"  |  {int(yr)}"
            lines.append(row)

    return "\n".join(lines)


def _format_active_projects(result: dict) -> str:
    projects = result.get("projects", [])
    if not projects:
        return result.get("message", "No active projects found.")

    lines = [
        f"ACTIVE PROJECTS  ({result.get('count', 0)} found)",
        f"Status filter: '{ACTIVE_STATUS_VALUE}'",
        "",
    ]
    for p in projects:
        row = f"  {p.get('name', '')}"
        if p.get("typology"):
            row += f"  [{p['typology']}]"
        if p.get("sqft"):
            row += f"  {float(p['sqft']):,.0f} sqft"
        if p.get("year"):
            row += f"  {int(p['year'])}"
        lines.append(row)

    return "\n".join(lines)


def _format_benchmark_pool(result: dict) -> str:
    if "error" in result:
        return f"Error: {result['error']}"

    stats    = result.get("stats", {})
    projects = result.get("projects", [])

    lines = [
        f"BENCHMARK POOL — Phase: {result.get('phase', '')}",
        f"Completed projects with actuals + planned fee data: {result.get('count', 0)}",
        "",
    ]

    if stats:
        lines += [
            "Burn rate distribution:",
            f"  Median: {stats.get('median', 0):.2f}",
            f"  P25 / P75: {stats.get('p25', 0):.2f} / {stats.get('p75', 0):.2f}",
            f"  P90: {stats.get('p90', 0):.2f}",
            f"  Min / Max: {stats.get('min', 0):.2f} / {stats.get('max', 0):.2f}",
            "",
        ]

    if projects:
        lines.append("Projects in pool:")
        for p in projects:
            row = f"  {p.get('name', 'Unknown')}"
            br  = p.get("burn_rate")
            if br is not None:
                row += f"  |  burn {br:.2f}"
            if p.get("sqft"):
                row += f"  |  {float(p['sqft']):,.0f} sqft"
            if p.get("year"):
                row += f"  |  {int(p['year'])}"
            lines.append(row)

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# ARGUMENT COERCION HELPERS
# ─────────────────────────────────────────────────────────────────────────────
# (Defined earlier in the file — referenced here for call_tool)


# ─────────────────────────────────────────────────────────────────────────────
# MCP SERVER
# ─────────────────────────────────────────────────────────────────────────────

server = Server("project-risk")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="diagnose_project_risk",
            description=(
                "Diagnose fee burn rate risk for a project currently in delivery. "
                "Compares the project's current fee spend vs. planned fee to compute "
                "a burn rate, then benchmarks it against historical completed projects "
                "of similar typology and size at the same phase. "
                "Returns a risk classification (NORMAL / ELEVATED / AT RISK / CRITICAL), "
                "benchmark distribution stats, and a PM-facing analyst interpretation. "
                "Use this during project delivery to catch fee overruns before they become "
                "unrecoverable — ideally at phase gate reviews or monthly project reviews."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "project_name": {
                        "type": "string",
                        "description": (
                            "Name of the live project to diagnose. "
                            "Partial name match is supported (e.g. 'Riverfront' will match "
                            "'Riverfront Mixed-Use Phase 1'). "
                            "Use list_active_projects to see available project names."
                        ),
                    },
                    "phase": {
                        "type": "string",
                        "description": (
                            "The phase to diagnose burn rate for. "
                            "Common values: CONCEPT, SD, DD, CD, CA. "
                            "Must match a phase that has both an actuals column and a "
                            "planned fee column in your database."
                        ),
                    },
                    "typology": {
                        "type": "string",
                        "description": (
                            "Override the project typology for benchmark filtering. "
                            "If omitted, the typology is read from the project's database record. "
                            "Optional."
                        ),
                    },
                    "sqft": {
                        "type": "number",
                        "description": (
                            "Override the project square footage for benchmark filtering. "
                            "If omitted, sqft is read from the project's database record. "
                            "Optional."
                        ),
                    },
                    "pct_complete": {
                        "type": "number",
                        "description": (
                            "Override the phase percent complete (0.0 to 1.0, e.g. 0.65 = 65%%). "
                            "If omitted, read from the database if a pct-complete column exists. "
                            "Used for context in the analyst interpretation. Optional."
                        ),
                    },
                },
                "required": ["project_name", "phase"],
            },
        ),
        Tool(
            name="list_active_projects",
            description=(
                "Returns all projects currently marked as active (in delivery) "
                "in the database. Use this to find project names before calling "
                "diagnose_project_risk."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="list_benchmark_pool",
            description=(
                "Returns the set of completed historical projects that would be used "
                "as the benchmark pool for a given phase and optional filters. "
                "Use this to inspect benchmark data quality before diagnosing a project, "
                "or to understand what 'normal' burn rate looks like for a given typology."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "phase": {
                        "type": "string",
                        "description": "Phase to inspect the benchmark pool for (e.g. SD, DD, CD).",
                    },
                    "typology": {
                        "type": "string",
                        "description": "Filter benchmark pool by typology. Optional.",
                    },
                    "sqft": {
                        "type": "number",
                        "description": (
                            "Center sqft value for benchmark pool filtering (±50%% by default). "
                            "Optional."
                        ),
                    },
                },
                "required": ["phase"],
            },
        ),
        Tool(
            name="validate_connection",
            description=(
                "Health-check: verifies database connectivity, table existence, "
                "auto-detected actuals/planned/pct-complete column pairs, "
                "and LLM reachability. Run this first when setting up a new connection."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    loop = asyncio.get_running_loop()

    # ── diagnose_project_risk ─────────────────────────────────────────────────
    if name == "diagnose_project_risk":
        project_name = _coerce_str(arguments.get("project_name"))
        phase        = _coerce_str(arguments.get("phase"))

        if not project_name:
            return [TextContent(type="text", text="Error: 'project_name' is required.")]
        if not phase:
            return [TextContent(type="text", text="Error: 'phase' is required.")]

        try:
            typology     = _coerce_str(arguments.get("typology"))
            sqft         = _coerce_float(arguments.get("sqft"),         "sqft")
            pct_complete = _coerce_float(arguments.get("pct_complete"), "pct_complete")
        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid argument: {e}")]

        if pct_complete is not None and not (0.0 <= pct_complete <= 1.0):
            # Accept 0–100 scale and normalise
            if 1.0 < pct_complete <= 100.0:
                pct_complete = pct_complete / 100.0
            else:
                return [TextContent(
                    type="text",
                    text="Invalid argument: 'pct_complete' must be between 0.0 and 1.0 (or 0–100).",
                )]

        result = await loop.run_in_executor(
            None,
            lambda: run_diagnose_project_risk(
                project_name         = project_name,
                phase                = phase,
                typology             = typology,
                sqft_override        = sqft,
                pct_complete_override = pct_complete,
            ),
        )

        if "error" in result and "current_burn_rate" not in result:
            return [TextContent(type="text", text=f"Error: {result['error']}")]

        return [TextContent(type="text", text=_format_diagnosis(result))]

    # ── list_active_projects ──────────────────────────────────────────────────
    elif name == "list_active_projects":
        result = await loop.run_in_executor(None, run_list_active_projects)
        if "error" in result:
            return [TextContent(type="text", text=f"Error: {result['error']}")]
        return [TextContent(type="text", text=_format_active_projects(result))]

    # ── list_benchmark_pool ───────────────────────────────────────────────────
    elif name == "list_benchmark_pool":
        phase = _coerce_str(arguments.get("phase"))
        if not phase:
            return [TextContent(type="text", text="Error: 'phase' is required.")]

        try:
            typology = _coerce_str(arguments.get("typology"))
            sqft     = _coerce_float(arguments.get("sqft"), "sqft")
        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid argument: {e}")]

        result = await loop.run_in_executor(
            None,
            lambda: run_list_benchmark_pool(phase=phase, typology=typology, sqft=sqft),
        )
        return [TextContent(type="text", text=_format_benchmark_pool(result))]

    # ── validate_connection ───────────────────────────────────────────────────
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
            cols_df = await loop.run_in_executor(None, _get_columns)
            actuals_cols, planned_cols = _detect_phase_col_pairs(cols_df)
            pct_cols = _detect_pct_cols(cols_df)

            paired = sorted(set(actuals_cols.keys()) & set(planned_cols.keys()))
            if paired:
                ok.append(f"Phases with actuals + planned columns: {', '.join(paired)}")
            else:
                issues.append(
                    "No actuals/planned column pairs detected. "
                    "Set ACTUALS_COLS and PLANNED_COLS in .env."
                )

            if pct_cols:
                ok.append(f"Pct-complete columns detected: {', '.join(sorted(pct_cols.keys()))}")
            else:
                ok.append("No pct-complete columns detected (optional — not required for burn rate)")

            # Report active / completed project counts
            if _col_exists(cols_df, COL_STATUS):
                try:
                    safe_active = ACTIVE_STATUS_VALUE.replace("'", "''")
                    safe_comp   = COMPLETED_STATUS_VALUE.replace("'", "''")
                    cnt_df = await loop.run_in_executor(
                        None,
                        lambda: safe_run_sql(
                            f"SELECT [{COL_STATUS}], COUNT(*) AS n "
                            f"FROM {PRIMARY_TABLE} WITH (NOLOCK) "
                            f"WHERE UPPER(LTRIM(RTRIM([{COL_STATUS}]))) "
                            f"  IN ('{safe_active.upper()}', '{safe_comp.upper()}') "
                            f"GROUP BY [{COL_STATUS}];"
                        ),
                    )
                    for _, r in cnt_df.iterrows():
                        ok.append(f"  Status '{r[COL_STATUS]}': {int(r['n']):,} projects")
                except Exception:
                    pass

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
