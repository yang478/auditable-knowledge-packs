from __future__ import annotations

SCHEMA_SCRIPT = """
CREATE TABLE docs (
  doc_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id TEXT NOT NULL,
  doc_title TEXT NOT NULL,
  source_file TEXT NOT NULL,
  source_path TEXT NOT NULL,
  doc_hash TEXT NOT NULL,
  source_version TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  UNIQUE (doc_id, source_version)
);

CREATE TABLE nodes (
  node_key TEXT PRIMARY KEY,
  node_id TEXT NOT NULL,
  doc_id TEXT NOT NULL,
  source_version TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  kind TEXT NOT NULL,
  label TEXT NOT NULL,
  title TEXT NOT NULL,
  heading_path TEXT NOT NULL DEFAULT '',
  parent_id TEXT,
  prev_id TEXT,
  next_id TEXT,
  ordinal INTEGER NOT NULL,
  ref_path TEXT NOT NULL,
  is_leaf INTEGER NOT NULL,
  raw_span_start INTEGER NOT NULL,
  raw_span_end INTEGER NOT NULL,
  node_hash TEXT NOT NULL,
  confidence REAL NOT NULL,
  UNIQUE (node_id, source_version)
);

CREATE TABLE edges (
  doc_id TEXT NOT NULL,
  edge_type TEXT NOT NULL,
  from_node_id TEXT NOT NULL,
  to_node_id TEXT NOT NULL,
  source_version TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  confidence REAL NOT NULL,
  PRIMARY KEY (edge_type, from_node_id, to_node_id, source_version)
);

CREATE TABLE aliases (
  doc_id TEXT NOT NULL,
  alias TEXT NOT NULL,
  normalized_alias TEXT NOT NULL,
  target_node_id TEXT NOT NULL,
  alias_level TEXT NOT NULL,
  source_version TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  confidence REAL NOT NULL,
  source TEXT NOT NULL,
  PRIMARY KEY (normalized_alias, target_node_id, alias_level, source_version)
);

CREATE TABLE node_text (
  node_key TEXT PRIMARY KEY,
  body_md TEXT NOT NULL,
  body_plain TEXT NOT NULL,
  keywords TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (node_key) REFERENCES nodes(node_key)
);
"""

INDEX_SCRIPT = """
CREATE INDEX idx_nodes_doc_id_active ON nodes(doc_id, is_active);
CREATE INDEX idx_nodes_node_id_active ON nodes(node_id, is_active);
CREATE INDEX idx_docs_doc_id_active ON docs(doc_id, is_active);
CREATE INDEX idx_nodes_parent_id ON nodes(parent_id);
CREATE INDEX idx_nodes_prev_id ON nodes(prev_id);
CREATE INDEX idx_nodes_next_id ON nodes(next_id);
CREATE INDEX idx_edges_from_node_active ON edges(from_node_id, is_active);
CREATE INDEX idx_edges_to_node_active ON edges(to_node_id, is_active);
CREATE INDEX idx_edges_type_active ON edges(edge_type, is_active);
CREATE INDEX idx_aliases_norm_active ON aliases(normalized_alias, is_active);
"""
