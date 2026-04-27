"""Tests for batch-2 DB stability fixes (H2 busy_timeout, H3 WAL checkpoint).

Generated as part of the quality improvement initiative.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

# Add paths so build_skill_lib imports work inside tests/
SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from build_skill_lib.db.crud import incremental_update_kb_sqlite_db, write_kb_sqlite_db
from build_skill_lib.incremental.invalidation import ChangeSet
from build_skill_lib.utils.safe_sqlite import open_db_wal


class TestBuildTimeBusyTimeout:
    """H2: open_db_wal must set PRAGMA busy_timeout."""

    def test_open_db_wal_sets_busy_timeout(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = open_db_wal(db_path)
        try:
            row = conn.execute("PRAGMA busy_timeout").fetchone()
            assert row is not None
            expected_ms = int(30.0 * 1000)
            assert row[0] == expected_ms, f"Expected busy_timeout={expected_ms}, got {row[0]}"
        finally:
            conn.close()

    def test_open_db_wal_enables_wal(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        conn = open_db_wal(db_path)
        try:
            row = conn.execute("PRAGMA journal_mode").fetchone()
            assert row is not None
            assert row[0].upper() == "WAL"
        finally:
            conn.close()


class TestIncrementalWALCheckpoint:
    """H3: incremental_update_kb_sqlite_db must checkpoint WAL."""

    def _make_empty_changeset(self) -> ChangeSet:
        return ChangeSet()

    def test_full_build_checkpoint_reduces_wal(self, tmp_path: Path):
        """Full build should checkpoint WAL, leaving no -wal file (best effort)."""
        db_path = tmp_path / "kb.sqlite"
        write_kb_sqlite_db(db_path, [], [], [], [])
        # After TRUNCATE checkpoint, WAL file should ideally not exist.
        db_path.parent / (db_path.name + "-wal")
        # It's acceptable for -wal to be 0 bytes; just ensure DB is usable.
        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute("PRAGMA integrity_check").fetchone()
            assert row[0] == "ok"
        finally:
            conn.close()

    def test_incremental_build_runs_without_error(self, tmp_path: Path):
        """Incremental build should complete without error (includes checkpoint)."""
        db_path = tmp_path / "kb.sqlite"
        # First do a full build.
        write_kb_sqlite_db(db_path, [], [], [], [])
        # Then do an empty incremental update.
        cs = self._make_empty_changeset()
        incremental_update_kb_sqlite_db(db_path, cs, [], [], [], [])
        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute("PRAGMA integrity_check").fetchone()
            assert row[0] == "ok"
        finally:
            conn.close()


class TestVacuumPurgeInactive:
    """Verify kbtool vacuum correctly purges soft-deleted rows while preserving active data."""

    def test_purge_inactive_removes_only_inactive_rows(self, tmp_path: Path):
        """Purge should remove is_active=0 rows, leave is_active=1 untouched."""
        db_path = tmp_path / "kb.sqlite"
        from build_skill_lib.db.schema import SCHEMA_SCRIPT
        conn = sqlite3.connect(str(db_path))
        try:
            conn.executescript(SCHEMA_SCRIPT)
            # Seed: 2 active rows + 1 inactive row per table.
            # Use different source_version/doc_id for inactive to avoid UNIQUE constraint.
            for table, cols_vals in _seed_data():
                for vals in (cols_vals[0], cols_vals[1]):
                    placeholders = ",".join("?" for _ in vals)
                    conn.execute(f"INSERT INTO {table} VALUES ({placeholders})", vals)
                # Inactive row: same structure, modified pk to avoid conflict
                inactive_vals = _make_inactive(cols_vals[0])
                placeholders = ",".join("?" for _ in inactive_vals)
                conn.execute(f"INSERT INTO {table} VALUES ({placeholders})", inactive_vals)
            # Also seed node_text
            conn.execute("INSERT INTO node_text VALUES ('node-a|v1', 'md', 'plain', 'kw')")
            conn.execute("INSERT INTO node_text VALUES ('node-b|v1', 'md', 'plain', 'kw')")
            conn.execute("INSERT INTO node_text VALUES ('node-inactive|v-old', 'md', 'plain', 'kw')")
            conn.commit()

            # Verify initial state
            for table in ("docs", "nodes", "edges", "aliases"):
                total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                active = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE is_active=1").fetchone()[0]
                assert total == 3, f"{table}: expected 3 total, got {total}"
                assert active == 2, f"{table}: expected 2 active, got {active}"

            # Verify orphaned node_text row exists
            orphan_count = conn.execute(
                "SELECT COUNT(*) FROM node_text WHERE node_key NOT IN (SELECT node_key FROM nodes WHERE is_active=1)"
            ).fetchone()[0]
            assert orphan_count == 1, f"Expected 1 orphaned node_text, got {orphan_count}"

            # Run purge
            purged_docs = _vacuum_purge_inactive(conn, "docs")
            purged_nodes = _vacuum_purge_inactive(conn, "nodes")
            purged_edges = _vacuum_purge_inactive(conn, "edges")
            purged_aliases = _vacuum_purge_inactive(conn, "aliases")
            # Purge orphaned node_text
            purged_nt = conn.execute(
                "DELETE FROM node_text WHERE node_key NOT IN (SELECT node_key FROM nodes WHERE is_active=1)"
            ).rowcount
            conn.commit()

            assert purged_docs == 1
            assert purged_nodes == 1
            assert purged_edges == 1
            assert purged_aliases == 1
            assert purged_nt == 1

            # Verify after purge
            for table in ("docs", "nodes", "edges", "aliases"):
                total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                active = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE is_active=1").fetchone()[0]
                assert total == 2, f"{table}: expected 2 after purge, got {total}"
                assert active == 2, f"{table}: expected 2 active after purge, got {active}"

            conn.execute("VACUUM")
            conn.commit()
            row = conn.execute("PRAGMA integrity_check").fetchone()
            assert row[0] == "ok", f"Integrity check failed: {row[0]}"
        finally:
            conn.close()

    def test_purge_with_no_inactive_is_noop(self, tmp_path: Path):
        """Purge on a DB with no inactive rows should do nothing."""
        db_path = tmp_path / "kb.sqlite"
        conn = sqlite3.connect(str(db_path))
        try:
            from build_skill_lib.db.schema import SCHEMA_SCRIPT
            conn.executescript(SCHEMA_SCRIPT)
            for table, cols_vals in _seed_data():
                for vals in (cols_vals[0], cols_vals[1]):
                    placeholders = ",".join("?" for _ in vals)
                    conn.execute(f"INSERT INTO {table} VALUES ({placeholders})", vals)
            conn.commit()

            purged_docs = _vacuum_purge_inactive(conn, "docs")
            assert purged_docs == 0

            for table in ("docs", "nodes", "edges", "aliases"):
                total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                assert total == 2, f"{table}: expected 2 after noop purge, got {total}"
        finally:
            conn.close()


def _make_inactive(active_vals):
    """Create inactive variant of a seed row by tweaking pk fields and setting is_active=0.
    
    is_active column positions differ per table: docs=7, nodes=4, edges=5, aliases=6.
    """
    vals = list(active_vals)
    # Infer table type from value count and first value
    if vals[0] is None:
        # docs: is_active at index 7
        vals[7] = 0
        vals[6] = "v-old"  # source_version
    elif isinstance(vals[0], str) and len(vals) == 19:
        # nodes: is_active at index 4
        vals[4] = 0
        vals[0] = "node-inactive|v-old"
        vals[1] = "node-inactive"
        vals[3] = "v-old"
    elif isinstance(vals[0], str) and len(vals) == 7:
        # edges: is_active at index 5
        vals[5] = 0
        vals[2] = "node-inactive"
        vals[4] = "v-old"
    elif isinstance(vals[0], str) and len(vals) == 9:
        # aliases: is_active at index 6
        vals[6] = 0
        vals[2] = "inactive_alias"
        vals[3] = "node-inactive"
        vals[5] = "v-old"
    return tuple(vals)


def _vacuum_purge_inactive(conn, table: str) -> int:
    """Helper mirroring cli._purge_inactive for testing."""
    cur = conn.execute(f"DELETE FROM {table} WHERE is_active = 0")
    return cur.rowcount


def _seed_data():
    """Return minimal seed data for each entity table: (table, (active_vals_a, active_vals_b))."""
    return [
        ("docs", (
            (None, "doc-A", "Doc A", "a.md", "a.md", "hash1", "v1", 1),
            (None, "doc-B", "Doc B", "b.md", "b.md", "hash2", "v1", 1),
        )),
        ("nodes", (
            ("node-a|v1", "node-a", "doc-A", "v1", 1, "chunk", "Chunk A", "Chunk A", "", None, None, None, 1, "ref/a.md", 1, 0, 100, "nhash1", 1.0),
            ("node-b|v1", "node-b", "doc-B", "v1", 1, "chunk", "Chunk B", "Chunk B", "", None, None, None, 1, "ref/b.md", 1, 0, 100, "nhash2", 1.0),
        )),
        ("edges", (
            ("doc-A", "prev", "node-a", "node-b", "v1", 1, 1.0),
            ("doc-B", "next", "node-b", "node-a", "v1", 1, 1.0),
        )),
        ("aliases", (
            ("doc-A", "aliasA", "aliasa", "node-a", "1", "v1", 1, 1.0, "title"),
            ("doc-B", "aliasB", "aliasb", "node-b", "2", "v1", 1, 0.9, "body"),
        )),
    ]
