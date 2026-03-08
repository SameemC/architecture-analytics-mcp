"""
comparable-projects-mcp
========================
MCP Tool 1: Find Comparable Architecture Projects

Given a project description or parameters, returns the most similar past
projects from your firm's database — ranked by a composite similarity score
that combines structured SQL filtering with numeric distance scoring across
multiple dimensions.

This tool exposes four MCP endpoints:
  • find_comparable_projects  — core similarity search with ranked results
  • get_project_detail        — full profile for a single named project
  • list_dimensions           — discover filterable fields in your database
  • validate_connection       — health check for DB + LLM

Architecture of the similarity engine:
  Layer 1 — SQL filter pass: narrows the candidate pool by typology, sqft
            range, location, status, and year range. This is fast and
            deterministic.
  Layer 2 — Numeric distance scoring: for each candidate, computes a
            weighted similarity score across sqft proximity, fee proximity,
            and any other configured numeric dimensions. Pure Python / pandas,
            no external vector DB required.
  Layer 3 — LLM ranking interpretation: sends the top-N scored candidates
            to the analyst LLM, which synthesises a human-readable comparison
            narrative and flags the closest matches.

What this tool does NOT do (see README for full gap map):
  - Semantic / vector similarity on project descriptions or narrative text
  - Cross-firm comparison (single database only)

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

COL_PROJECT_ID    = os.getenv("COL_PROJECT_ID",    "Project_ID")
COL_PROJECT_NAME  = os.getenv("COL_PROJECT_NAME",  "Project_Name")
COL_TYPOLOGY      = os.getenv("COL_TYPOLOGY",      "Market_Sector")
COL_STATUS        = os.getenv("COL_STATUS",        "Project_Status")
COL_SQFT          = os.getenv("COL_SQFT",          "Gross_Square_Footage")
COL_LOCATION      = os.getenv("COL_LOCATION",      "State")
COL_YEAR          = os.getenv("COL_YEAR",          "Year")
COL_CONST_COST    = os.getenv("COL_CONST_COST",    "Estimated_Construction_Cost")
COL_CLIENT        = os.getenv("COL_CLIENT",        "Client")
COL_DESIGN_PRIN   = os.getenv("COL_DESIGN_PRIN",   "Design_Principal")
COL_CITY          = os.getenv("COL_CITY",          "City")

PRIMARY_TABLE     = os.getenv("PRIMARY_TABLE",     "dbo.Projects")

# Similarity tuning — all configurable
MAX_RESULTS       = int(os.getenv("MAX_RESULTS",      "10"))
CANDIDATE_POOL    = int(os.getenv("CANDIDATE_POOL",   "200"))  # SQL pulls this many before scoring
SQFT_TOLERANCE    = float(os.getenv("SQFT_TOLERANCE", "0.50")) # ±50% for initial SQL filter
LLM_TEMPERATURE   = float(os.getenv("LLM_TEMPERATURE", "0.15"))

# Similarity scoring weights (must sum to 1.0, configurable)
# SCORE_WEIGHTS=sqft:0.45,const_cost:0.25,year:0.15,location:0.15
_WEIGHTS_RAW = os.getenv(
    "SCORE_WEIGHTS",
    "sqft:0.45,const_cost:0.25,year:0.15,location:0.15"
)

def _parse_weights(raw: str) -> dict[str, float]:
    result = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" not in pair:
            continue
        key, val = pair.split(":", 1)
        try:
            result[key.strip()] = float(val.strip())
        except ValueError:
            pass
    # Normalise so weights always sum to 1.0
    total = sum(result.values())
    if total > 0:
        result = {k: v / total for k, v in result.items()}
    return result

SCORE_WEIGHTS: dict[str, float] = _parse_weights(_WEIGHTS_RAW)

# Phase fee column map — set in .env or auto-detected from schema
_PHASE_COLS_RAW = os.getenv("PHASE_COLS", "")
PHASE_ORDER     = ["CONCEPT", "SD", "DD", "CD", "CA"]

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


def _col_exists(cols_df: pd.DataFrame, col_name: str) -> bool:
    actual = {str(c).lower() for c in cols_df["column_name"].tolist()}
    return col_name.lower() in actual


def _detect_phase_fee_columns(cols_df: pd.DataFrame) -> dict[str, str]:
    if PHASE_COL_MAP:
        return PHASE_COL_MAP
    numeric_types = {
        "int", "bigint", "smallint", "tinyint", "float", "real",
        "decimal", "numeric", "money", "smallmoney",
    }
    fee_keywords   = ["fee", "cost", "labor", "effort", "amount"]
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
        if dtype not in numeric_types or not any(kw in col for kw in fee_keywords):
            continue
        for label, triggers in phase_patterns.items():
            if label not in detected and any(t in col for t in triggers):
                detected[label] = row["column_name"]
                break
    return detected


def _build_wide_select(cols_df: pd.DataFrame, phase_cols: dict[str, str]) -> list[str]:
    """
    Build the full SELECT column list for the candidate query.
    Includes identity, all known dimension columns, all phase fee columns,
    and construction cost — anything useful for display and scoring.
    """
    always = [
        COL_PROJECT_ID, COL_PROJECT_NAME, COL_TYPOLOGY,
        COL_SQFT, COL_LOCATION, COL_YEAR, COL_STATUS,
        COL_CONST_COST, COL_CLIENT, COL_DESIGN_PRIN, COL_CITY,
    ]
    cols = []
    for c in always:
        if _col_exists(cols_df, c) and f"[{c}]" not in cols:
            cols.append(f"[{c}]")

    # All phase fee columns
    for phase in PHASE_ORDER:
        c = phase_cols.get(phase)
        if c and _col_exists(cols_df, c) and f"[{c}]" not in cols:
            cols.append(f"[{c}]")

    return cols


# ─────────────────────────────────────────────────────────────────────────────
# SIMILARITY SCORING ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _ratio_distance(target: float, actual: float) -> float:
    """
    Returns a 0–1 similarity score based on the ratio between target and actual.
    1.0 = identical. Drops to 0 as the ratio diverges from 1.0.
    Uses a smooth exponential decay so nearby values score high and distant
    values score near-zero without a hard cutoff.

    Examples:
        target=50000, actual=50000  → 1.00
        target=50000, actual=60000  → ~0.82
        target=50000, actual=100000 → ~0.37
        target=50000, actual=200000 → ~0.14
    """
    if target <= 0 or actual <= 0:
        return 0.0
    ratio = actual / target
    log_dist = abs(math.log(ratio))   # 0 when identical, grows as ratio diverges
    return math.exp(-log_dist)        # smooth decay: 1.0 at identical, → 0 at extremes


def _year_distance(target_year: int, actual_year: int, max_gap: int = 10) -> float:
    """
    Returns a 0–1 similarity score based on year proximity.
    1.0 = same year. Linear decay to 0 at max_gap years apart.
    """
    gap = abs(target_year - actual_year)
    return max(0.0, 1.0 - gap / max_gap)


def _location_match(target_loc: str | None, actual_loc: str | None) -> float:
    """Binary: 1.0 if same location, 0.0 otherwise. Handles None gracefully."""
    if not target_loc or not actual_loc:
        return 0.5  # unknown → neutral, don't penalise
    return 1.0 if target_loc.strip().upper() == actual_loc.strip().upper() else 0.0


def score_candidates(
    df: pd.DataFrame,
    target_sqft: float | None,
    target_const_cost: float | None,
    target_year: int | None,
    target_location: str | None,
    phase_cols: dict[str, str],
) -> pd.DataFrame:
    """
    Computes a composite similarity score (0–100) for each row in the candidate
    DataFrame against the target project parameters.

    Scoring dimensions and default weights (configurable via SCORE_WEIGHTS in .env):
      sqft        0.45  — gross square footage ratio distance
      const_cost  0.25  — estimated construction cost ratio distance
      year        0.15  — project year proximity
      location    0.15  — same state/location binary match

    If a target value is not provided for a dimension, that dimension's weight
    is redistributed proportionally across the remaining dimensions so the
    total always sums to 1.0.
    """
    if df.empty:
        return df

    df = df.copy()

    # Determine which dimensions have a target value
    active: dict[str, float] = {}

    sqft_col  = COL_SQFT         if COL_SQFT in df.columns         else None
    cost_col  = COL_CONST_COST   if COL_CONST_COST in df.columns   else None
    year_col  = COL_YEAR         if COL_YEAR in df.columns          else None
    loc_col   = COL_LOCATION     if COL_LOCATION in df.columns      else None

    if target_sqft       and sqft_col:  active["sqft"]       = SCORE_WEIGHTS.get("sqft",       0.45)
    if target_const_cost and cost_col:  active["const_cost"] = SCORE_WEIGHTS.get("const_cost", 0.25)
    if target_year       and year_col:  active["year"]        = SCORE_WEIGHTS.get("year",       0.15)
    if loc_col:                          active["location"]   = SCORE_WEIGHTS.get("location",   0.15)

    if not active:
        # No scoreable dimensions — assign equal score to all candidates
        df["_similarity_score"] = 50.0
        df["_score_breakdown"]  = [{}] * len(df)
        return df

    # Normalise active weights to sum to 1.0
    total_w = sum(active.values())
    active  = {k: v / total_w for k, v in active.items()}

    scores:     list[float] = []
    breakdowns: list[dict]  = []

    for _, row in df.iterrows():
        component_scores: dict[str, float] = {}

        if "sqft" in active and sqft_col:
            actual = _safe_float(row.get(sqft_col))
            s = _ratio_distance(target_sqft, actual) if actual else 0.0
            component_scores["sqft"] = round(s * 100, 1)

        if "const_cost" in active and cost_col:
            actual = _safe_float(row.get(cost_col))
            s = _ratio_distance(target_const_cost, actual) if actual else 0.0
            component_scores["const_cost"] = round(s * 100, 1)

        if "year" in active and year_col:
            actual_yr = _safe_int(row.get(year_col))
            s = _year_distance(target_year, actual_yr) if actual_yr else 0.0
            component_scores["year"] = round(s * 100, 1)

        if "location" in active and loc_col:
            actual_loc = str(row.get(loc_col) or "")
            s = _location_match(target_location, actual_loc)
            component_scores["location"] = round(s * 100, 1)

        composite = sum(
            active[dim] * (component_scores.get(dim, 0) / 100)
            for dim in active
        ) * 100

        scores.append(round(composite, 1))
        breakdowns.append(component_scores)

    df["_similarity_score"] = scores
    df["_score_breakdown"]  = breakdowns
    return df


def _safe_float(val: Any) -> float | None:
    try:
        f = float(val)
        return f if math.isfinite(f) and f > 0 else None
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> int | None:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CANDIDATE QUERY BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _build_candidate_query(
    select_cols: list[str],
    typology: str | None,
    sqft: float | None,
    location: str | None,
    status_filter: str | None,
    year_min: int | None,
    year_max: int | None,
    cols_df: pd.DataFrame,
    exclude_project_name: str | None = None,
) -> str:
    """
    Pulls a candidate pool from the database using structured filters.
    Deliberately uses a wider sqft range than Tool 2 (default ±50%) so the
    scoring engine has enough candidates to rank meaningfully.
    """
    actual = {str(c).lower() for c in cols_df["column_name"].tolist()}
    where  = ["1=1"]   # always-true anchor so we can append AND clauses cleanly

    if status_filter and COL_STATUS.lower() in actual:
        safe_s = status_filter.replace("'", "''")
        where.append(f"UPPER(LTRIM(RTRIM([{COL_STATUS}]))) = '{safe_s.upper()}'")

    if typology and COL_TYPOLOGY.lower() in actual:
        safe_t = typology.replace("'", "''")
        where.append(f"[{COL_TYPOLOGY}] = '{safe_t}'")

    if sqft and sqft > 0 and COL_SQFT.lower() in actual:
        lo = sqft * (1 - SQFT_TOLERANCE)
        hi = sqft * (1 + SQFT_TOLERANCE)
        where.append(f"[{COL_SQFT}] BETWEEN {lo:.0f} AND {hi:.0f}")

    if location and COL_LOCATION.lower() in actual:
        safe_l = location.replace("'", "''")
        where.append(f"[{COL_LOCATION}] = '{safe_l}'")

    if year_min and COL_YEAR.lower() in actual:
        where.append(f"[{COL_YEAR}] >= {year_min}")

    if year_max and COL_YEAR.lower() in actual:
        where.append(f"[{COL_YEAR}] <= {year_max}")

    # Exclude the base project itself if searching for comps to a named project
    if exclude_project_name and COL_PROJECT_NAME.lower() in actual:
        safe_n = exclude_project_name.replace("'", "''")
        where.append(f"[{COL_PROJECT_NAME}] != '{safe_n}'")

    return (
        f"SELECT TOP ({CANDIDATE_POOL}) {', '.join(select_cols)}\n"
        f"FROM {PRIMARY_TABLE} WITH (NOLOCK)\n"
        f"WHERE {chr(10) + '  AND '.join(where)}\n"
        f"ORDER BY [{COL_PROJECT_ID}];"
    )


# ─────────────────────────────────────────────────────────────────────────────
# LLM ANALYST
# ─────────────────────────────────────────────────────────────────────────────

client = OpenAI(base_url=LM_BASE, api_key=LM_API_KEY)

_COMP_ANALYST_SYSTEM = """
You are a senior project architect helping a project manager identify
the most relevant precedent projects for a new proposal.

You will receive:
- The target project parameters (typology, sqft, location, construction cost)
- A ranked list of comparable past projects with their similarity scores
  and key metrics

YOUR RESPONSE MUST:
1. Identify the 2-3 strongest comps and explain specifically WHY each is
   relevant (size match, typology match, location, cost range)
2. Note any important differences between the top comps and the target project
   that a PM should be aware of (e.g. significantly older, different region)
3. Flag if the overall match quality is weak (e.g. low scores, thin pool)
4. Keep response under 220 words total

FORMATTING (STRICT):
- Plain text only. No markdown, no asterisks, no bullet symbols, no backticks.
- Always include a space between numbers and words.
- Write dollar amounts as $X,XXX or $X,XXX,XXX. Write sqft as X,XXX sqft.
- Do not suggest additional queries or follow-up questions.
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


def llm_interpret_comps(
    target: dict,
    top_comps: list[dict],
    phase_cols: dict[str, str],
) -> str:
    """Sends top comps to the LLM for a concise analyst interpretation."""
    comp_lines = []
    for i, c in enumerate(top_comps[:8], 1):
        score    = c.get("_similarity_score", 0)
        name     = c.get(COL_PROJECT_NAME, "Unknown")
        sqft     = _safe_float(c.get(COL_SQFT))
        yr       = _safe_int(c.get(COL_YEAR))
        loc      = c.get(COL_LOCATION, "")
        typ      = c.get(COL_TYPOLOGY, "")
        cost     = _safe_float(c.get(COL_CONST_COST))
        breakdown = c.get("_score_breakdown", {})

        line = f"  {i}. {name}  [score: {score:.0f}/100]"
        if sqft:  line += f"  {sqft:,.0f} sqft"
        if yr:    line += f"  {yr}"
        if loc:   line += f"  {loc}"
        if typ:   line += f"  ({typ})"
        if cost:  line += f"  est. cost ${cost:,.0f}"
        if breakdown:
            bd_parts = [f"{k}={v:.0f}" for k, v in breakdown.items()]
            line += f"  [breakdown: {', '.join(bd_parts)}]"
        comp_lines.append(line)

    context = (
        f"TARGET PROJECT:\n"
        f"  Typology: {target.get('typology', 'Not specified')}\n"
        f"  Sqft:     {f\"{target.get('sqft', 0):,.0f}\" if target.get('sqft') else 'Not specified'}\n"
        f"  Location: {target.get('location', 'Not specified')}\n"
        f"  Est. construction cost: "
        f"{f\"${target.get('const_cost', 0):,.0f}\" if target.get('const_cost') else 'Not specified'}\n"
        f"  Year:     {target.get('year', 'Not specified')}\n\n"
        f"RANKED COMPARABLE PROJECTS:\n"
        f"{chr(10).join(comp_lines) if comp_lines else '  No comparables found.'}"
    )
    try:
        resp = client.chat.completions.create(
            model=LM_MODEL,
            messages=[
                {"role": "system", "content": _COMP_ANALYST_SYSTEM},
                {"role": "user",   "content": context},
            ],
            temperature=LLM_TEMPERATURE,
        )
        raw = (resp.choices[0].message.content or "") if resp.choices else ""
        return _postprocess(raw) or "Analysis could not be generated."
    except Exception as e:
        return f"LLM analysis error: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# SANITIZE
# ─────────────────────────────────────────────────────────────────────────────

def _sanitize(obj: Any) -> Any:
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def _df_row_to_dict(row: pd.Series) -> dict:
    return {
        k: (None if isinstance(v, float) and math.isnan(v) else v)
        for k, v in row.items()
        if not str(k).startswith("_")   # strip internal scoring columns
    }


# ─────────────────────────────────────────────────────────────────────────────
# CORE PIPELINE: FIND COMPARABLE PROJECTS
# ─────────────────────────────────────────────────────────────────────────────

def run_find_comparables(
    typology: str | None         = None,
    sqft: float | None           = None,
    const_cost: float | None     = None,
    location: str | None         = None,
    status_filter: str | None    = None,
    year_min: int | None         = None,
    year_max: int | None         = None,
    target_year: int | None      = None,
    max_results: int             = MAX_RESULTS,
    exclude_project: str | None  = None,
) -> dict[str, Any]:
    """
    Full pipeline:
      1. Pull candidate pool via SQL filters
      2. Score each candidate by numeric similarity
      3. Return top-N ranked by score
      4. LLM interprets the top results
    """
    try:
        cols_df    = _get_columns()
        phase_cols = _detect_phase_fee_columns(cols_df)
    except Exception as e:
        return {"error": f"Schema retrieval failed: {e}"}

    select_cols = _build_wide_select(cols_df, phase_cols)
    if not select_cols:
        return {"error": "Could not build SELECT list from schema. Check PRIMARY_TABLE in .env."}

    # ── Step 1: SQL candidate pull ────────────────────────────────────────────
    try:
        candidate_sql = _build_candidate_query(
            select_cols, typology, sqft, location, status_filter,
            year_min, year_max, cols_df,
            exclude_project_name=exclude_project,
        )
        candidates_df = safe_run_sql(candidate_sql)
    except Exception as e:
        return {"error": f"Candidate query failed: {e}"}

    if candidates_df is None or candidates_df.empty:
        # Retry without location if no results
        if location:
            try:
                candidate_sql = _build_candidate_query(
                    select_cols, typology, sqft, None, status_filter,
                    year_min, year_max, cols_df,
                    exclude_project_name=exclude_project,
                )
                candidates_df = safe_run_sql(candidate_sql)
            except Exception:
                pass

    if candidates_df is None or candidates_df.empty:
        return {
            "result_count": 0,
            "error": (
                "No candidate projects found with the given filters. "
                "Try relaxing typology, location, or sqft range."
            ),
        }

    raw_count = len(candidates_df)

    # ── Step 2: Numeric similarity scoring ───────────────────────────────────
    scored_df = score_candidates(
        candidates_df,
        target_sqft       = sqft,
        target_const_cost = const_cost,
        target_year       = target_year,
        target_location   = location,
        phase_cols        = phase_cols,
    )

    # Sort by score descending, take top N
    scored_df = scored_df.sort_values("_similarity_score", ascending=False)
    top_df    = scored_df.head(max_results)

    # ── Step 3: Prepare result rows ───────────────────────────────────────────
    result_rows: list[dict] = []
    for _, row in top_df.iterrows():
        d = _df_row_to_dict(row)
        d["similarity_score"]    = round(float(row["_similarity_score"]), 1)
        d["score_breakdown"]     = row.get("_score_breakdown", {})
        result_rows.append(d)

    # ── Step 4: LLM interpretation ────────────────────────────────────────────
    # Pass the raw scored rows (with internal cols) for analysis
    top_with_internals = top_df.to_dict(orient="records")
    target = {
        "typology":   typology,
        "sqft":       sqft,
        "const_cost": const_cost,
        "location":   location,
        "year":       target_year,
    }
    analysis = llm_interpret_comps(target, top_with_internals, phase_cols)

    return _sanitize({
        "result_count":        len(result_rows),
        "candidate_pool_size": raw_count,
        "filters_applied": {
            "typology":      typology,
            "sqft":          sqft,
            "const_cost":    const_cost,
            "location":      location,
            "status":        status_filter,
            "year_min":      year_min,
            "year_max":      year_max,
            "target_year":   target_year,
        },
        "scoring_weights":     SCORE_WEIGHTS,
        "results":             result_rows,
        "analysis":            analysis,
        "sql_used":            candidate_sql,
        "methodology_note": (
            "Projects are scored using weighted numeric distance across sqft, "
            "construction cost, year, and location. Score of 100 = perfect match "
            "on all dimensions. Weights are configurable via SCORE_WEIGHTS in .env."
        ),
    })


# ─────────────────────────────────────────────────────────────────────────────
# CORE PIPELINE: GET PROJECT DETAIL
# ─────────────────────────────────────────────────────────────────────────────

def run_get_project_detail(project_name: str) -> dict[str, Any]:
    """
    Returns the full database profile for a single named project.
    Supports partial name matching (LIKE) as a fallback.
    """
    try:
        cols_df    = _get_columns()
        phase_cols = _detect_phase_fee_columns(cols_df)
    except Exception as e:
        return {"error": f"Schema retrieval failed: {e}"}

    select_cols = _build_wide_select(cols_df, phase_cols)
    safe_name   = project_name.replace("'", "''")

    sql = (
        f"SELECT {', '.join(select_cols)}\n"
        f"FROM {PRIMARY_TABLE} WITH (NOLOCK)\n"
        f"WHERE [{COL_PROJECT_NAME}] = '{safe_name}'\n"
        f"   OR [{COL_PROJECT_NAME}] LIKE '%{safe_name}%'\n"
        f"ORDER BY CASE WHEN [{COL_PROJECT_NAME}] = '{safe_name}' THEN 0 ELSE 1 END;"
    )

    try:
        df = safe_run_sql(sql)
    except Exception as e:
        return {"error": f"Project detail query failed: {e}"}

    if df is None or df.empty:
        return {
            "result_count": 0,
            "error": f"No project found matching '{project_name}'.",
        }

    rows = [_df_row_to_dict(row) for _, row in df.iterrows()]
    return _sanitize({
        "result_count": len(rows),
        "projects":     rows,
    })


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT FORMATTERS
# ─────────────────────────────────────────────────────────────────────────────

def _format_comp_results(result: dict, phase_cols: dict[str, str]) -> str:
    lines = [
        "=" * 64,
        "COMPARABLE PROJECTS",
        "=" * 64,
        "",
        f"Found {result.get('result_count', 0)} results  "
        f"(scored from a pool of {result.get('candidate_pool_size', 0)} candidates)",
        "",
        f"Scoring dimensions: "
        + ", ".join(f"{k} ({v*100:.0f}%)" for k, v in result.get("scoring_weights", {}).items()),
        "",
        f"({result.get('methodology_note', '')})",
    ]

    rows = result.get("results", [])
    if rows:
        lines += ["", "-" * 64, "RANKED RESULTS", "-" * 64]
        for i, row in enumerate(rows, 1):
            score    = row.get("similarity_score", 0)
            name     = row.get(COL_PROJECT_NAME, "Unknown")
            sqft     = _safe_float(row.get(COL_SQFT))
            yr       = _safe_int(row.get(COL_YEAR))
            loc      = row.get(COL_LOCATION) or ""
            typ      = row.get(COL_TYPOLOGY) or ""
            cost     = _safe_float(row.get(COL_CONST_COST))
            client   = row.get(COL_CLIENT) or ""
            breakdown = row.get("score_breakdown", {})

            lines.append(f"\n  {i:>2}. {name}  [{score:.0f}/100]")
            detail_parts = []
            if typ:  detail_parts.append(typ)
            if sqft: detail_parts.append(f"{sqft:,.0f} sqft")
            if loc:  detail_parts.append(loc)
            if yr:   detail_parts.append(str(yr))
            if cost: detail_parts.append(f"est. ${cost:,.0f}")
            if client: detail_parts.append(f"Client: {client}")
            if detail_parts:
                lines.append(f"      {' | '.join(detail_parts)}")

            # Per-phase fees if available
            phase_fee_parts = []
            for phase in PHASE_ORDER:
                fc = phase_cols.get(phase)
                if fc and fc in row:
                    v = _safe_float(row.get(fc))
                    if v:
                        phase_fee_parts.append(f"{phase}: ${v:,.0f}")
            if phase_fee_parts:
                lines.append(f"      Fees: {' | '.join(phase_fee_parts)}")

            # Score breakdown
            if breakdown:
                bd = " | ".join(f"{k}={v:.0f}" for k, v in breakdown.items())
                lines.append(f"      Score breakdown: {bd}")

    lines += [
        "",
        "-" * 64,
        "ANALYST INTERPRETATION",
        "-" * 64,
        "",
        result.get("analysis", ""),
    ]

    return "\n".join(lines)


def _format_project_detail(result: dict, phase_cols: dict[str, str]) -> str:
    projects = result.get("projects", [])
    if not projects:
        return f"No project found.\n{result.get('error', '')}"

    lines = ["=" * 64, f"PROJECT DETAIL  ({result['result_count']} match(es))", "=" * 64]

    for proj in projects:
        lines += [""]
        name  = proj.get(COL_PROJECT_NAME, "Unknown")
        pid   = proj.get(COL_PROJECT_ID, "")
        lines.append(f"  {name}  (ID: {pid})")
        lines.append("  " + "-" * 50)

        for label, key in [
            ("Typology",    COL_TYPOLOGY),
            ("Status",      COL_STATUS),
            ("Location",    COL_LOCATION),
            ("City",        COL_CITY),
            ("Year",        COL_YEAR),
            ("Sqft",        COL_SQFT),
            ("Est. Cost",   COL_CONST_COST),
            ("Client",      COL_CLIENT),
            ("Design Lead", COL_DESIGN_PRIN),
        ]:
            v = proj.get(key)
            if v is not None:
                if key in (COL_CONST_COST,):
                    f_val = _safe_float(v)
                    lines.append(f"  {label:<14} ${f_val:,.0f}" if f_val else f"  {label:<14} {v}")
                elif key == COL_SQFT:
                    f_val = _safe_float(v)
                    lines.append(f"  {label:<14} {f_val:,.0f} sqft" if f_val else f"  {label:<14} {v}")
                else:
                    lines.append(f"  {label:<14} {v}")

        # Phase fees
        fee_parts = []
        for phase in PHASE_ORDER:
            fc  = phase_cols.get(phase)
            val = proj.get(fc) if fc else None
            if val is not None:
                f_val = _safe_float(val)
                if f_val:
                    fee_parts.append(f"{phase}: ${f_val:,.0f}")
        if fee_parts:
            lines.append(f"  {'Fees':<14} {' | '.join(fee_parts)}")

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


# ─────────────────────────────────────────────────────────────────────────────
# MCP SERVER
# ─────────────────────────────────────────────────────────────────────────────

server = Server("comparable-projects")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="find_comparable_projects",
            description=(
                "Find the most similar past projects to a proposed new project. "
                "Pulls a candidate pool from the database using structured filters "
                "(typology, sqft range, location, status, year range), then scores "
                "every candidate on weighted numeric similarity across sqft, "
                "construction cost, year, and location. Returns a ranked list "
                "with per-project similarity scores, score breakdowns, fee data, "
                "and an LLM-generated analyst narrative identifying the strongest comps. "
                "Use this at proposal time to anchor fee estimates and surface "
                "relevant precedents for a project description."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "typology": {
                        "type": "string",
                        "description": (
                            "Project type / market sector (e.g. Civic, Healthcare, "
                            "Residential, Education). Call list_dimensions to see "
                            "exact values. Optional."
                        ),
                    },
                    "sqft": {
                        "type": "number",
                        "description": (
                            "Gross square footage of the proposed project. "
                            "Used both as a SQL range filter (±50% default) and as "
                            "a primary scoring dimension. Optional but strongly "
                            "recommended for meaningful results."
                        ),
                    },
                    "const_cost": {
                        "type": "number",
                        "description": (
                            "Estimated construction cost in dollars. "
                            "Used as a scoring dimension (not a filter). Optional."
                        ),
                    },
                    "location": {
                        "type": "string",
                        "description": (
                            "State abbreviation (e.g. WA, CA, NY). "
                            "Used both as an optional SQL filter and as a "
                            "scoring dimension. Optional."
                        ),
                    },
                    "status_filter": {
                        "type": "string",
                        "description": (
                            "Filter candidates by project status. "
                            "Use 'Completed' to restrict to closed projects with full actuals. "
                            "Optional."
                        ),
                    },
                    "year_min": {
                        "type": "integer",
                        "description": "Earliest project year to include (SQL filter). Optional.",
                    },
                    "year_max": {
                        "type": "integer",
                        "description": "Latest project year to include (SQL filter). Optional.",
                    },
                    "target_year": {
                        "type": "integer",
                        "description": (
                            "The anticipated year of the proposed project. "
                            "Used as the scoring reference for year proximity. Optional."
                        ),
                    },
                    "max_results": {
                        "type": "integer",
                        "description": f"Maximum results to return. Default: {MAX_RESULTS}.",
                    },
                    "exclude_project": {
                        "type": "string",
                        "description": (
                            "Project name to exclude from results. "
                            "Use when finding comps for an existing project in your database."
                        ),
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="get_project_detail",
            description=(
                "Retrieve the full database profile for a single named project, "
                "including all phase fees, square footage, construction cost, "
                "client, design lead, location, and year. "
                "Supports partial name matching. "
                "Use after find_comparable_projects to drill into a specific result."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "project_name": {
                        "type": "string",
                        "description": "Full or partial project name to look up.",
                    }
                },
                "required": ["project_name"],
            },
        ),
        Tool(
            name="list_dimensions",
            description=(
                "Returns the distinct values available for key filter dimensions: "
                "typologies, locations, statuses, and year range. "
                "Also shows auto-detected phase fee columns and scoring weights. "
                "Run this before find_comparable_projects to discover valid filter values."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="validate_connection",
            description=(
                "Health-check: verifies database connectivity, table existence, "
                "schema detection, and LLM reachability."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    loop = asyncio.get_running_loop()

    # ── find_comparable_projects ─────────────────────────────────────────────
    if name == "find_comparable_projects":
        try:
            typology       = _coerce_str(arguments.get("typology"))
            sqft           = _coerce_float(arguments.get("sqft"),        "sqft")
            const_cost     = _coerce_float(arguments.get("const_cost"),  "const_cost")
            location       = _coerce_str(arguments.get("location"))
            status_filter  = _coerce_str(arguments.get("status_filter"))
            year_min       = _coerce_int(arguments.get("year_min"),      "year_min")
            year_max       = _coerce_int(arguments.get("year_max"),      "year_max")
            target_year    = _coerce_int(arguments.get("target_year"),   "target_year")
            exclude_project = _coerce_str(arguments.get("exclude_project"))
            max_results_raw = arguments.get("max_results", MAX_RESULTS)
            max_results    = _coerce_int(max_results_raw, "max_results") or MAX_RESULTS
        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid argument: {e}")]

        result = await loop.run_in_executor(
            None,
            lambda: run_find_comparables(
                typology        = typology,
                sqft            = sqft,
                const_cost      = const_cost,
                location        = location,
                status_filter   = status_filter,
                year_min        = year_min,
                year_max        = year_max,
                target_year     = target_year,
                max_results     = max_results,
                exclude_project = exclude_project,
            ),
        )
        if "error" in result:
            return [TextContent(type="text", text=f"Error: {result['error']}")]

        try:
            cols_df    = await loop.run_in_executor(None, _get_columns)
            phase_cols = _detect_phase_fee_columns(cols_df)
        except Exception:
            phase_cols = {}

        return [TextContent(type="text", text=_format_comp_results(result, phase_cols))]

    # ── get_project_detail ───────────────────────────────────────────────────
    elif name == "get_project_detail":
        project_name = _coerce_str(arguments.get("project_name"))
        if not project_name:
            return [TextContent(type="text", text="Error: 'project_name' is required.")]

        result = await loop.run_in_executor(
            None, lambda: run_get_project_detail(project_name)
        )
        if "error" in result and result.get("result_count", 0) == 0:
            return [TextContent(type="text", text=f"Error: {result['error']}")]

        try:
            cols_df    = await loop.run_in_executor(None, _get_columns)
            phase_cols = _detect_phase_fee_columns(cols_df)
        except Exception:
            phase_cols = {}

        return [TextContent(type="text", text=_format_project_detail(result, phase_cols))]

    # ── list_dimensions ──────────────────────────────────────────────────────
    elif name == "list_dimensions":
        try:
            cols_df    = await loop.run_in_executor(None, _get_columns)
            phase_cols = _detect_phase_fee_columns(cols_df)

            def _distinct(col: str, limit: int = 50) -> list[str]:
                if not _col_exists(cols_df, col):
                    return []
                try:
                    df = safe_run_sql(
                        f"SELECT DISTINCT TOP ({limit}) [{col}] AS v "
                        f"FROM {PRIMARY_TABLE} WITH (NOLOCK) "
                        f"WHERE [{col}] IS NOT NULL ORDER BY [{col}];"
                    )
                    return df["v"].dropna().astype(str).tolist() if df is not None else []
                except Exception:
                    return []

            def _year_range() -> tuple[int | None, int | None]:
                if not _col_exists(cols_df, COL_YEAR):
                    return None, None
                try:
                    df = safe_run_sql(
                        f"SELECT MIN([{COL_YEAR}]) AS mn, MAX([{COL_YEAR}]) AS mx "
                        f"FROM {PRIMARY_TABLE} WITH (NOLOCK) "
                        f"WHERE [{COL_YEAR}] IS NOT NULL;"
                    )
                    if df is not None and not df.empty:
                        return _safe_int(df.iloc[0]["mn"]), _safe_int(df.iloc[0]["mx"])
                except Exception:
                    pass
                return None, None

            typologies     = await loop.run_in_executor(None, lambda: _distinct(COL_TYPOLOGY))
            locations      = await loop.run_in_executor(None, lambda: _distinct(COL_LOCATION))
            statuses       = await loop.run_in_executor(None, lambda: _distinct(COL_STATUS))
            yr_min, yr_max = await loop.run_in_executor(None, _year_range)

            lines = [
                "FILTER DIMENSIONS",
                "",
                f"TYPOLOGIES ({len(typologies)} found in [{COL_TYPOLOGY}]):",
                *([f"  {t}" for t in typologies] if typologies else ["  None found"]),
                "",
                f"LOCATIONS ({len(locations)} found in [{COL_LOCATION}]):",
                *([f"  {l}" for l in locations] if locations else ["  None found"]),
                "",
                f"STATUSES ({len(statuses)} found in [{COL_STATUS}]):",
                *([f"  {s}" for s in statuses] if statuses else ["  None found"]),
                "",
                f"YEAR RANGE:  {yr_min} – {yr_max}" if yr_min else "YEAR RANGE:  Not available",
                "",
                "PHASE FEE COLUMNS (auto-detected):",
                *([f"  {p}  →  [{c}]" for p, c in sorted(phase_cols.items())]
                  if phase_cols else ["  None detected. Set PHASE_COLS in .env."]),
                "",
                "SCORING WEIGHTS (configurable via SCORE_WEIGHTS in .env):",
                *[f"  {k:<14} {v*100:.0f}%" for k, v in SCORE_WEIGHTS.items()],
            ]
            return [TextContent(type="text", text="\n".join(lines))]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {e}")]

    # ── validate_connection ──────────────────────────────────────────────────
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

            required_cols = [COL_PROJECT_ID, COL_PROJECT_NAME]
            for c in required_cols:
                if _col_exists(cols_df, c):
                    ok.append(f"Required column found: [{c}]")
                else:
                    issues.append(
                        f"Required column NOT found: [{c}]. "
                        f"Set COL_PROJECT_ID / COL_PROJECT_NAME in .env."
                    )

            scoring_cols = {
                "sqft":       COL_SQFT,
                "const_cost": COL_CONST_COST,
                "year":       COL_YEAR,
                "location":   COL_LOCATION,
            }
            for dim, col in scoring_cols.items():
                if _col_exists(cols_df, col):
                    ok.append(f"Scoring dimension '{dim}' found: [{col}]")
                else:
                    issues.append(
                        f"Scoring column not found: [{col}] (dim: {dim}). "
                        f"Override via COL_* in .env or this dimension will be skipped."
                    )

            if phase_cols:
                ok.append(f"Phase fee columns detected: {', '.join(sorted(phase_cols.keys()))}")
            else:
                ok.append("No phase fee columns detected (optional for this tool)")

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
