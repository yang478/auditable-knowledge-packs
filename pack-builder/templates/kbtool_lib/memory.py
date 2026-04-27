from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional, Sequence

from . import tokenizer_core

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Feedback type constants
# ---------------------------------------------------------------------------

FEEDBACK_POSITIVE = "positive"
FEEDBACK_NEGATIVE = "negative"
FEEDBACK_NEUTRAL = "neutral"
VALID_FEEDBACK_TYPES = frozenset({FEEDBACK_POSITIVE, FEEDBACK_NEGATIVE, FEEDBACK_NEUTRAL})


# ---------------------------------------------------------------------------
# Query normalization
# ---------------------------------------------------------------------------

def _is_cjk(ch: str) -> bool:
    """Return True if *ch* is a CJK unified ideograph or compatibility ideograph.
    
    Delegates to tokenizer_core.is_cjk for canonical range coverage (12 Unicode blocks).
    """
    return tokenizer_core.is_cjk(ch)


def canonicalize_query_key(raw: str) -> str:
    """Normalize a query for stable matching (memory/history key).

    Steps:
      1. Lowercase ASCII letters.
      2. Remove punctuation (keep CJK and alphanumerics).
      3. Collapse whitespace.
      4. Split into tokens, dedupe, sort alphabetically, rejoin.

    Sorting makes ``"预应力 混凝土"`` and ``"混凝土 预应力"`` equivalent.
    """
    text = str(raw or "").strip().lower()
    if not text:
        return ""

    # Keep CJK, ASCII alphanumerics, and whitespace; drop everything else.
    cleaned_chars: list[str] = []
    for ch in text:
        if _is_cjk(ch):
            cleaned_chars.append(ch)
        elif ch.isalnum():
            cleaned_chars.append(ch)
        else:
            cleaned_chars.append(" ")

    text = "".join(cleaned_chars)
    tokens = [t for t in text.split() if t]
    if not tokens:
        return ""

    # Deduplicate while preserving first-seen order, then sort.
    seen: set[str] = set()
    unique: list[str] = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    unique.sort()
    return " ".join(unique)





# ---------------------------------------------------------------------------
# Schema initialization (idempotent)
# ---------------------------------------------------------------------------

_MEMORY_SCHEMA = """
-- Query execution log: one row per query invocation.
CREATE TABLE IF NOT EXISTS query_log (
    query_id      TEXT PRIMARY KEY,
    query_text    TEXT NOT NULL,
    query_norm    TEXT NOT NULL,
    timestamp     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    cmd           TEXT NOT NULL DEFAULT '',
    preset        TEXT NOT NULL DEFAULT '',
    hits_count    INTEGER NOT NULL DEFAULT 0,
    top_node_ids  TEXT NOT NULL DEFAULT '[]',
    duration_ms   INTEGER NOT NULL DEFAULT 0,
    bundle_path   TEXT NOT NULL DEFAULT '',
    neighbors     INTEGER NOT NULL DEFAULT -1
);

-- Index for fast lookup by normalized query.
CREATE INDEX IF NOT EXISTS idx_query_log_norm ON query_log(query_norm);
CREATE INDEX IF NOT EXISTS idx_query_log_time ON query_log(timestamp DESC);

-- Learned association weights between a normalized query and a node.
-- weight is decayed over time based on last_used.
CREATE TABLE IF NOT EXISTS query_node_weights (
    query_norm    TEXT NOT NULL,
    node_id       TEXT NOT NULL,
    weight        REAL NOT NULL DEFAULT 0.0,
    pos_count     INTEGER NOT NULL DEFAULT 0,
    neg_count     INTEGER NOT NULL DEFAULT 0,
    last_used     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (query_norm, node_id)
);

-- Explicit feedback entries (positive / negative / neutral).
CREATE TABLE IF NOT EXISTS node_feedback (
    feedback_id   TEXT PRIMARY KEY,
    query_id      TEXT NOT NULL,
    node_id       TEXT NOT NULL,
    feedback_type TEXT NOT NULL CHECK (feedback_type IN ('positive', 'negative', 'neutral')),
    context       TEXT NOT NULL DEFAULT '',
    timestamp     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_node_feedback_qid ON node_feedback(query_id);
CREATE INDEX IF NOT EXISTS idx_node_feedback_nid ON node_feedback(node_id);
"""


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check whether a column already exists in a table."""
    # Whitelist validation to prevent SQL injection via table name.
    _SAFE_TABLES = {"query_log", "query_node_weights", "node_feedback"}
    if table not in _SAFE_TABLES:
        raise ValueError(f"Invalid table name: {table!r}")
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(str(row[1]) == column for row in cur.fetchall())


def ensure_memory_tables(conn: sqlite3.Connection) -> None:
    """Create memory tables if they don't exist, migrate columns if needed."""
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='query_log'")
    table_exists = cursor.fetchone() is not None
    if not table_exists:
        for statement in _MEMORY_SCHEMA.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(stmt)
    # Migrate existing tables: add ``neighbors`` column to query_log if missing.
    if not _column_exists(conn, "query_log", "neighbors"):
        conn.execute("ALTER TABLE query_log ADD COLUMN neighbors INTEGER NOT NULL DEFAULT -1")
    conn.commit()


# Counter for lazy pruning check
_prune_counter: int = 0

def _maybe_prune_query_node_weights(conn: sqlite3.Connection) -> None:
    """Periodically prune stale low-weight rows from query_node_weights.
    
    Rows with negligible effective weight that haven't been used in
    _WEIGHT_PRUNE_AGE_DAYS are removed.
    """
    global _prune_counter
    _prune_counter += 1
    if _prune_counter < _PRUNE_CHECK_INTERVAL:
        return
    _prune_counter = 0
    
    conn.execute(
        """
        DELETE FROM query_node_weights
        WHERE weight < ?
          AND last_used < datetime('now', '-' || ? || ' days')
        """,
        (_WEIGHT_PRUNE_THRESHOLD, _WEIGHT_PRUNE_AGE_DAYS),
    )
    deleted = conn.rowcount if hasattr(conn, 'rowcount') else -1
    if deleted:
        logger.debug("Pruned %d stale query_node_weights rows", deleted)


# ---------------------------------------------------------------------------
# Query logging
# ---------------------------------------------------------------------------

def _make_query_id(query_text: str, timestamp_iso: str) -> str:
    # Include random suffix (4 hex chars) to avoid collisions when the same
    # query is issued multiple times within the same microsecond.
    nonce = os.urandom(2).hex()
    payload = f"{query_text}\n{timestamp_iso}\n{nonce}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def log_query(
    conn: sqlite3.Connection,
    *,
    query_text: str,
    cmd: str,
    preset: str = "",
    hit_ids: Sequence[str],
    duration_ms: int = 0,
    bundle_path: str = "",
    neighbors: int = -1,
    autocommit: bool = True,
) -> str:
    """Record a query execution into the learning log.

    Returns the generated query_id.

    Args:
        autocommit: If False, the caller is responsible for committing the
            transaction. Useful when log_query is part of a larger atomic
            operation.
    """
    ensure_memory_tables(conn)
    qnorm = canonicalize_query_key(query_text)
    ts = datetime.now(timezone.utc).isoformat()
    qid = _make_query_id(query_text, ts)

    top_ids = list(hit_ids)[:50]  # cap stored list
    conn.execute(
        """
        INSERT INTO query_log (query_id, query_text, query_norm, cmd, preset,
                               hits_count, top_node_ids, duration_ms, bundle_path, neighbors)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            qid,
            query_text,
            qnorm,
            cmd,
            preset,
            len(top_ids),
            json.dumps(top_ids, ensure_ascii=False),
            duration_ms,
            bundle_path,
            neighbors,
        ),
    )

    # Upsert positive weights for every hit node.
    # Cap weight growth: clamped to a sane maximum so long-running queries
    # don't dominate forever.
    conn.executemany(
        """
        INSERT INTO query_node_weights (query_norm, node_id, weight, pos_count, last_used)
        VALUES (?, ?, 1.0, 1, ?)
        ON CONFLICT (query_norm, node_id) DO UPDATE SET
            weight = MIN(?, query_node_weights.weight + 1.0),
            pos_count = query_node_weights.pos_count + 1,
            last_used = excluded.last_used
        """,
        [(qnorm, nid, ts, _MAX_WEIGHT) for nid in top_ids],
    )

    if autocommit:
        conn.commit()
        _maybe_prune_query_node_weights(conn)
    logger.debug("Logged query %s (norm=%r, hits=%d)", qid, qnorm, len(top_ids))
    return qid


# ---------------------------------------------------------------------------
# Learned ranking boost
# ---------------------------------------------------------------------------

_DECAY_HALF_LIFE_DAYS = 30.0  # weight halves every 30 days
_MAX_WEIGHT = 20.0  # cap for query→node association weight
_WEIGHT_PRUNE_THRESHOLD = 0.01  # negligible effective weight
_WEIGHT_PRUNE_AGE_DAYS = 90  # rows older than this with low weight get pruned
_PRUNE_CHECK_INTERVAL = 100  # check for pruning every N queries


def _decay_factor(last_used_iso: str) -> float:
    """Exponential time decay: factor = exp(-ln(2) * days / half_life)."""
    try:
        last = datetime.fromisoformat(last_used_iso.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        days = (now - last).total_seconds() / 86400.0
        return math.exp(-math.log(2) * days / _DECAY_HALF_LIFE_DAYS)
    except Exception:
        return 1.0


def get_learned_weights(
    conn: sqlite3.Connection,
    query_text: str,
) -> dict[str, float]:
    """Return a mapping ``node_id → decayed_weight`` for the normalized query.

    Only nodes with a positive decayed weight are returned.
    """
    ensure_memory_tables(conn)
    qnorm = canonicalize_query_key(query_text)
    if not qnorm:
        return {}

    rows = conn.execute(
        """
        SELECT node_id, weight, last_used
        FROM query_node_weights
        WHERE query_norm = ? AND weight > 0
        """,
        (qnorm,),
    ).fetchall()

    weights: dict[str, float] = {}
    for row in rows:
        nid = str(row["node_id"])
        raw_weight = float(row["weight"])
        decay = _decay_factor(str(row["last_used"]))
        effective = raw_weight * decay
        if effective > 0.01:
            weights[nid] = effective
    return weights


def apply_learned_boost(
    conn: sqlite3.Connection,
    hit_ids: Sequence[str],
    query_text: str,
    *,
    top_k_learned: int = 5,
) -> list[str]:
    """Reorder ``hit_ids`` by applying a learned boost.

    Strategy (soft rerank, non-destructive to BM25):
      1. Look up decayed weights for ``query_text``.
      2. Take up to ``top_k_learned`` nodes with the highest learned weight
         and move them to the front, preserving their relative weight order.
      3. Append the remaining hit IDs in their original BM25 order,
         excluding any already promoted.

    This guarantees that even with no learning data the order is unchanged.
    """
    weights = get_learned_weights(conn, query_text)
    if not weights:
        return list(hit_ids)

    original = [str(h) for h in hit_ids]

    # Build position index for O(1) lookup instead of O(n) list.index().
    pos_map: dict[str, int] = {nid: idx for idx, nid in enumerate(original)}

    # Nodes that appear in both current hits and have a learned weight.
    scored: list[tuple[float, str]] = []
    for nid in original:
        w = weights.get(nid, 0.0)
        if w > 0:
            scored.append((w, nid))

    if not scored:
        return original

    # Sort by weight descending, then by original position (stable).
    scored.sort(key=lambda x: (-x[0], pos_map.get(x[1], 0)))
    promoted = [nid for _w, nid in scored[:top_k_learned]]
    promoted_set = set(promoted)
    rest = [nid for nid in original if nid not in promoted_set]

    logger.debug(
        "Learned boost promoted %d node(s) for query_norm=%r",
        len(promoted),
        canonicalize_query_key(query_text),
    )
    return promoted + rest


# ---------------------------------------------------------------------------
# Similar-query detection
# ---------------------------------------------------------------------------

def find_similar_queries(
    conn: sqlite3.Connection,
    query_text: str,
    *,
    limit: int = 5,
    max_age_days: int = 90,
) -> list[dict[str, object]]:
    """Find recent historical queries that are identical or highly similar.

    Returns rows with keys: query_id, query_text, timestamp, hits_count, top_node_ids.
    """
    ensure_memory_tables(conn)
    qnorm = canonicalize_query_key(query_text)
    if not qnorm:
        return []

    rows = conn.execute(
        """
        SELECT query_id, query_text, timestamp, hits_count, top_node_ids
        FROM query_log
        WHERE query_norm = ?
          AND timestamp > datetime('now', '-' || ? || ' days')
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (qnorm, max_age_days, limit),
    ).fetchall()

    results: list[dict[str, object]] = []
    for row in rows:
        top_ids = []
        raw_ids = row["top_node_ids"]
        if raw_ids:
            try:
                top_ids = json.loads(str(raw_ids))
            except Exception:
                pass
        results.append(
            {
                "query_id": str(row["query_id"]),
                "query_text": str(row["query_text"]),
                "timestamp": str(row["timestamp"]),
                "hits_count": int(row["hits_count"]),
                "top_node_ids": top_ids,
            }
        )
    return results


# ---------------------------------------------------------------------------
# Feedback recording
# ---------------------------------------------------------------------------

def record_feedback(
    conn: sqlite3.Connection,
    *,
    query_id: str,
    node_id: str,
    feedback_type: str,  # positive / negative / neutral
    context: str = "",
    autocommit: bool = True,
) -> str:
    """Record explicit user feedback for a specific node in a query context.

    Negative feedback penalizes the query→node association weight.
    Returns the feedback_id.
    """
    ensure_memory_tables(conn)
    ft = str(feedback_type or "").strip().lower()
    if ft not in VALID_FEEDBACK_TYPES:
        ft = FEEDBACK_NEUTRAL

    ts = datetime.now(timezone.utc).isoformat()
    fid = hashlib.sha256(f"{query_id}:{node_id}:{ts}".encode()).hexdigest()[:16]

    conn.execute(
        """
        INSERT INTO node_feedback (feedback_id, query_id, node_id, feedback_type, context, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (fid, query_id, node_id, ft, context, ts),
    )

    # Look up the query_norm from query_log (needed for both positive and negative).
    row = conn.execute(
        "SELECT query_norm FROM query_log WHERE query_id = ?", (query_id,)
    ).fetchone()
    if row:
        qnorm = str(row["query_norm"])
        if ft == FEEDBACK_POSITIVE:
            conn.execute(
                """
                INSERT INTO query_node_weights (query_norm, node_id, weight, pos_count, last_used)
                VALUES (?, ?, 2.0, 1, ?)
                ON CONFLICT (query_norm, node_id) DO UPDATE SET
                    weight = MIN(?, query_node_weights.weight + 2.0),
                    pos_count = query_node_weights.pos_count + 1,
                    last_used = excluded.last_used
                """,
                (qnorm, node_id, ts, _MAX_WEIGHT),
            )
        elif ft == FEEDBACK_NEGATIVE:
            conn.execute(
                """
                INSERT INTO query_node_weights (query_norm, node_id, weight, neg_count, last_used)
                VALUES (?, ?, -3.0, 1, ?)
                ON CONFLICT (query_norm, node_id) DO UPDATE SET
                    weight = MAX(-10.0, query_node_weights.weight - 3.0),
                    neg_count = query_node_weights.neg_count + 1,
                    last_used = excluded.last_used
                """,
                (qnorm, node_id, ts),
            )

    if autocommit:
        conn.commit()
    logger.info("Recorded %s feedback for node %s (query %s)", ft, node_id, query_id)
    return fid


# ---------------------------------------------------------------------------
# History retrieval (for CLI ``history`` command)
# ---------------------------------------------------------------------------

def get_query_history(
    conn: sqlite3.Connection,
    *,
    limit: int = 20,
    query_substring: str = "",
) -> list[dict[str, object]]:
    """Return recent query history for inspection."""
    limit = max(1, min(limit, 10000))
    ensure_memory_tables(conn)
    # Escape LIKE wildcards to treat user input as literal.
    escaped = query_substring.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    pattern = f"%{escaped}%"
    rows = conn.execute(
        """
        SELECT query_id, query_text, query_norm, timestamp, cmd, hits_count, bundle_path
        FROM query_log
        WHERE query_text LIKE ?
        ORDER BY timestamp DESC
        LIMIT ?
        """,
        (pattern, limit),
    ).fetchall()

    return [
        {
            "query_id": str(r["query_id"]),
            "query_text": str(r["query_text"]),
            "query_norm": str(r["query_norm"]),
            "timestamp": str(r["timestamp"]),
            "cmd": str(r["cmd"]),
            "hits_count": int(r["hits_count"]),
            "bundle_path": str(r["bundle_path"]),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Query-rewrite suggestions (learned from historical co-occurrence)
# ---------------------------------------------------------------------------

_MAX_SUGGEST_REWRITES = 3


def suggest_rewrites(
    conn: sqlite3.Connection,
    query_text: str,
    hit_ids: Sequence[str],
    *,
    limit: int = _MAX_SUGGEST_REWRITES,
) -> list[str]:
    """Suggest alternative query wordings based on historical queries that hit the same nodes.

    Strategy:
      1. Look up other query_norms that have positive weights for the current hit nodes.
      2. Fetch the most recent original query_text for each distinct query_norm.
      3. Exclude the current query_norm and return the top *limit* suggestions.
    """
    ensure_memory_tables(conn)
    if not hit_ids:
        return []

    qnorm = canonicalize_query_key(query_text)
    # Use a small sample of hit_ids to keep the query fast.
    sample = [str(n) for n in hit_ids[:20]]
    placeholders = ",".join("?" for _ in sample)

    # Find other query_norms that successfully hit the same nodes.
    rows = conn.execute(
        f"""
        SELECT DISTINCT qnw.query_norm
        FROM query_node_weights qnw
        WHERE qnw.node_id IN ({placeholders})
          AND qnw.query_norm != ?
          AND qnw.weight > 0
        ORDER BY qnw.weight DESC
        LIMIT ?
        """,
        (*sample, qnorm, limit * 2),
    ).fetchall()

    # Batch-resolve query_norm → most recent query_text (avoids N+1 queries).
    distinct_norms = list({str(row["query_norm"]) for row in rows})
    if not distinct_norms:
        return []

    norm_placeholders = ",".join("?" for _ in distinct_norms)
    text_rows = conn.execute(
        f"""
        SELECT query_norm, query_text
        FROM query_log
        WHERE query_norm IN ({norm_placeholders})
        GROUP BY query_norm
        HAVING rowid = MAX(rowid)
        """,
        tuple(distinct_norms),
    ).fetchall()
    norm_to_text: dict[str, str] = {
        str(r["query_norm"]): str(r["query_text"]) for r in text_rows
    }

    suggestions: list[str] = []
    seen_norms: set[str] = set()
    for row in rows:
        other_norm = str(row["query_norm"])
        if other_norm in seen_norms:
            continue
        seen_norms.add(other_norm)
        text = norm_to_text.get(other_norm)
        if text:
            suggestions.append(text)
        if len(suggestions) >= limit:
            break

    return suggestions


# ---------------------------------------------------------------------------
# Adaptive neighbors recommendation
# ---------------------------------------------------------------------------

# Heuristic keyword → recommended neighbors mapping.
# Higher number = more context needed (procedural queries).
_NEIGHBORS_KEYWORD_MAP: list[tuple[set[str], int]] = [
    # Process / procedure queries benefit from expanded context.
    ({"步骤", "流程", "过程", "顺序", "操作", "施工", "方法", "如何", "怎么", "做法", "工序", "工艺", "程序"}, 2),
    # Definition / concept queries are usually self-contained.
    ({"定义", "概念", "是什么", "什么意思", "含义", "解释", "意思", "概述", "简介"}, 0),
    # Parameter / data queries are specific; minimal context is enough.
    ({"参数", "规格", "数值", "多少", "尺寸", "标准", "大小", "重量", "范围", "指标", "限值"}, 1),
]

_DEFAULT_NEIGHBORS = 1


def recommend_neighbors(conn: sqlite3.Connection, query_text: str) -> int:
    """Recommend a ``--neighbors`` value based on query semantics + historical feedback.

    The recommendation follows a two-stage strategy:
      1. **Heuristic stage**: classify the query by keywords (process / definition / parameter).
      2. **Validation stage**: check historical feedback for the same query_norm.
         If a neighbors setting historically received negative feedback, it is penalized.

    Returns an integer in ``[0, 2]``.
    """
    ensure_memory_tables(conn)
    qnorm = canonicalize_query_key(query_text)
    if not qnorm:
        return _DEFAULT_NEIGHBORS

    text_lower = query_text.lower()

    # ---- Stage 1: Heuristic classification ----
    heuristic = _DEFAULT_NEIGHBORS
    for keywords, value in _NEIGHBORS_KEYWORD_MAP:
        if any(kw in text_lower for kw in keywords):
            heuristic = value
            break

    # ---- Stage 2: Historical feedback validation ----
    # Look up past queries with the same query_norm that had explicit feedback.
    rows = conn.execute(
        """
        SELECT ql.neighbors, nf.feedback_type
        FROM node_feedback nf
        JOIN query_log ql ON ql.query_id = nf.query_id
        WHERE ql.query_norm = ?
          AND ql.neighbors >= 0
        ORDER BY ql.timestamp DESC
        LIMIT 20
        """,
        (qnorm,),
    ).fetchall()

    if not rows:
        return heuristic

    # Score each neighbors setting: positive +1, negative -2.
    scores: dict[int, int] = {}
    for row in rows:
        # conn may not have row_factory set; use numeric indices.
        n = int(row[0])
        ft = str(row[1])
        delta = 1 if ft == FEEDBACK_POSITIVE else (-2 if ft == FEEDBACK_NEGATIVE else 0)
        scores[n] = scores.get(n, 0) + delta

    # Separate positive-scoring candidates from poor performers.
    positive_candidates = {n: s for n, s in scores.items() if s > 0}
    if positive_candidates:
        # Pick the best among historically positive settings.
        best_score = -10_000
        best_n = heuristic
        for n, score in positive_candidates.items():
            if score > best_score or (score == best_score and abs(n - heuristic) < abs(best_n - heuristic)):
                best_score = score
                best_n = n
        return max(0, min(2, best_n))

    # No positive history: fall back to heuristic, but avoid settings with negative scores.
    fallback = heuristic
    if scores.get(fallback, 0) < 0:
        # Try alternatives 0, 1, 2 that are not negatively scored.
        for alt in range(3):
            if scores.get(alt, 0) >= 0:
                fallback = alt
                break
    return max(0, min(2, fallback))


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def get_stats(conn: sqlite3.Connection) -> dict[str, object]:
    """Return memory layer statistics."""
    ensure_memory_tables(conn)
    total_queries = conn.execute("SELECT COUNT(*) FROM query_log").fetchone()[0]
    total_weights = conn.execute("SELECT COUNT(*) FROM query_node_weights").fetchone()[0]
    total_feedback = conn.execute("SELECT COUNT(*) FROM node_feedback").fetchone()[0]
    recent_7d = conn.execute(
        "SELECT COUNT(*) FROM query_log WHERE timestamp > datetime('now', '-7 days')"
    ).fetchone()[0]
    top_qnorm = conn.execute(
        """
        SELECT query_norm, COUNT(*) AS c
        FROM query_log
        GROUP BY query_norm
        ORDER BY c DESC
        LIMIT 5
        """
    ).fetchall()

    return {
        "total_queries": total_queries,
        "total_weights": total_weights,
        "total_feedback": total_feedback,
        "queries_last_7d": recent_7d,
        "top_recurring_queries": [
            {"query_norm": str(r["query_norm"]), "count": int(r["c"])} for r in top_qnorm
        ],
    }


# ---------------------------------------------------------------------------
# 查询结果缓存 (LRU with TTL)
# ---------------------------------------------------------------------------

_CACHE_TTL_SECONDS = 300  # 5分钟
_CACHE_MAX_ENTRIES = 50


def _ensure_cache_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS query_cache (
            query_norm TEXT PRIMARY KEY,
            result_json TEXT NOT NULL,
            hit_ids TEXT NOT NULL,
            hit_count INTEGER NOT NULL DEFAULT 0,
            cached_at REAL NOT NULL
        )
    """)
    # 清理过期缓存 + 限制条目数
    now = _now()
    conn.execute("DELETE FROM query_cache WHERE cached_at + ? < ?", (_CACHE_TTL_SECONDS, now))
    # 保持最多 50 条
    conn.execute("""
        DELETE FROM query_cache WHERE query_norm NOT IN (
            SELECT query_norm FROM query_cache ORDER BY cached_at DESC LIMIT ?
        )
    """, (_CACHE_MAX_ENTRIES,))


def _now() -> float:
    import time
    return time.time()


def get_cached_result(conn: sqlite3.Connection, query_text: str) -> dict | None:
    """检查并返回未过期的缓存结果。返回 None 表示需重新查询。"""
    import json as _json
    _ensure_cache_table(conn)
    q_norm = _normalize_query_text(query_text)
    now = _now()
    row = conn.execute(
        "SELECT result_json, hit_ids FROM query_cache WHERE query_norm = ? AND cached_at + ? > ?",
        (q_norm, _CACHE_TTL_SECONDS, now),
    ).fetchone()
    if not row:
        return None
    return _json.loads(str(row[0]))


def cache_result(conn: sqlite3.Connection, query_text: str, result: dict, hit_ids: list[str]) -> None:
    """缓存查询结果。"""
    import json as _json
    _ensure_cache_table(conn)
    q_norm = _normalize_query_text(query_text)
    conn.execute(
        "INSERT OR REPLACE INTO query_cache(query_norm, result_json, hit_ids, hit_count, cached_at) VALUES (?, ?, ?, ?, ?)",
        (q_norm, _json.dumps(result, ensure_ascii=False), _json.dumps(hit_ids), len(hit_ids), _now()),
    )


def _normalize_query_text(text: str) -> str:
    """归一化查询文本用于缓存 key。"""
    return " ".join(text.strip().lower().split())
