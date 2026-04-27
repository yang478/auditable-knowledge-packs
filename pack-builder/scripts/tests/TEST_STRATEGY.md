# Pack-Builder 测试覆盖缺口与策略

> 生成日期：2026-04-26
> 覆盖模块：extract.py / build.py / ir/io.py / kbtool_assets.py / incremental/ / index/ / render/ / fingerprint/ / tokenizer_core.py / kbtool_lib/reindex.py / kbtool_lib/runtime.py / kbtool_lib/grep.py / kbtool_lib/locate.py
> 状态：部分已实现，部分仍待补充

---

## 1. 测试优先级总览

| 优先级 | 模块 | 理由 | 预估用例数 |
|--------|------|------|-----------|
| **P0** | `tokenizer_core.py` | 零依赖纯函数，CJK分词是检索正确性的根基 | 25 |
| **P0** | `extract.py` | 所有输入数据的入口，格式支持多、异常路径复杂 | 20 |
| **P0** | `build.py` | 核心编排流程，6个Phase的集成 correctness | 18 |
| **P1** | `ir/io.py` | IR JSONL是外部数据交换格式，校验逻辑密集 | 15 |
| **P1** | `incremental/` | 增量构建是核心卖点，invalidation逻辑易错 | 18 |
| **P1** | `fingerprint/` | 数据完整性校验，直接影响增量判断 | 12 |
| **P1** | `index/` | 索引分片决定运行时性能与正确性 | 14 |
| **P2** | `render/` | SKILL.md与节点frontmatter渲染，输出格式契约 | 12 |
| **P2** | `kbtool_assets.py` | 资产打包与入口脚本生成，跨平台兼容 | 10 |
| **P3** | `kbtool_lib/reindex.py` | 运行时重建，依赖完整的skill目录结构 | 8 |
| **P3** | `kbtool_lib/runtime.py` | Hook系统、DB超时、路径安全 | 10 |
| **P3** | `kbtool_lib/grep.py` + `locate.py` | 外部工具(rg/fd)包装，环境依赖强 | 8 |

---

## 2. 每个模块的测试用例清单

### 2.1 `tokenizer_core.py` (P0)

**Mock需求**：无（零依赖纯函数， easiest to test）

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_is_cjk_basic` | `is_cjk('中') == True`, `is_cjk('A') == False` |
| 2 | `test_is_cjk_extension_a` | Extension A 字符（U+3400-U+4DBF）返回 True |
| 3 | `test_is_cjk_compatibility` | Compatibility 字符（U+F900-U+FAFF）返回 True |
| 4 | `test_tokenize_cjk_2gram_short_word` | 2-4字短词保留完整串 + 2gram（例：`"中国"` → `['中国']`） |
| 5 | `test_tokenize_cjk_2gram_long_word` | 5字以上仅保留2gram，不保留完整串 |
| 6 | `test_tokenize_cjk_mixed_with_ascii` | CJK与ASCII混排时正确拆分run |
| 7 | `test_tokenize_cjk_single_char` | 单字CJK保留为单token |
| 8 | `test_fts_tokens_mixed` | `fts_tokens("Hello 中国")` 同时包含ASCII word和CJK 2gram |
| 9 | `test_build_match_query_basic` | OR查询正确构建，去重、去空、截断 |
| 10 | `test_build_match_query_truncation` | `max_tokens=3` 时仅保留前3个，stderr输出警告 |
| 11 | `test_build_match_all_basic` | AND查询正确构建 |
| 12 | `test_query_terms_whitespace` | 多空格、制表符、换行正确处理 |
| 13 | `test_build_match_expression_and_mode` | `query_mode="and"` 时各part用AND连接 |
| 14 | `test_build_match_expression_or_mode` | `query_mode="or"` 时用OR连接 |
| 15 | `test_build_match_expression_must_terms` | `must_terms` 生成独立子句并与query_clause AND组合 |
| 16 | `test_count_occurrences` | 边界：空串、无命中、重叠不计 |
| 17 | `test_extract_window_hit` | 命中窗口前后扩展，加省略号 |
| 18 | `test_extract_window_no_hit` | 无命中时返回前max_chars |
| 19 | `test_markdown_to_plain` | 去除heading标记、代码反引号、链接、加粗斜体 |
| 20 | `test_parse_frontmatter_valid` | `---\nkey: val\n---` 正确解析 |
| 21 | `test_parse_frontmatter_no_frontmatter` | 无前缀返回空dict |
| 22 | `test_strip_frontmatter` | 正确剥离frontmatter返回body |
| 23 | `test_stable_hash_deterministic` | 相同输入相同输出，不同输入大概率不同 |
| 24 | `test_derive_source_version` | `title="V2规范"` → `"v2"`，无版本→`"current"` |
| 25 | `test_extract_keywords_basic` | 混合CJK/ASCII文本提取top_k关键词，去停词、去重子串 |

**伪代码示例**（`test_tokenize_cjk_2gram_short_word`）：

```python
def test_tokenize_cjk_2gram_short_word():
    assert tokenize_cjk_2gram("中国") == ["中国"]
    assert tokenize_cjk_2gram("中国人民") == ["中国人民", "中国", "国人", "人民"]
    assert tokenize_cjk_2gram("中华人民共和国") == [
        "中华", "华人", "人民", "民共", "共和", "和国"
    ]  # 5字以上不保留完整串
```

---

### 2.2 `extract.py` (P0)

**Mock需求**：
- `subprocess.run`（`pdftotext`调用）
- `shutil.which`（检测`pdftotext`是否存在）
- `builtins.__import__` / `importlib.import_module`（`pypdf` / `PyPDF2` fallback）
- `pathlib.Path.read_text`（`.md` / `.txt`读取）
- `build_skill_lib.utils.fs.die`（**关键**：所有异常路径都通过`die()`退出，需要monkeypatch为抛出自定义异常以便捕获）

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_extract_to_markdown_md` | `.md`文件直接返回内容 |
| 2 | `test_extract_to_markdown_txt` | `.txt`文件经过`_infer_text_headings_to_markdown`转换 |
| 3 | `test_extract_to_markdown_txt_underline_h1` | `===`下划线转H1 |
| 4 | `test_extract_to_markdown_txt_underline_h2` | `---`下划线转H2 |
| 5 | `test_extract_to_markdown_txt_chapter_pattern` | `第X章` → H1 |
| 6 | `test_extract_to_markdown_txt_numbered_heading` | `1.1 标题` → H2, `1.2.3` → H3 |
| 7 | `test_extract_to_markdown_txt_short_line_heuristic` | <60字符短行（非列表）→ H3 |
| 8 | `test_extract_to_markdown_unsupported` | 不支持的扩展名调用`die` |
| 9 | `test_extract_pdf_with_pdftotext` | `pdftotext`存在时调用subprocess，返回stdout |
| 10 | `test_extract_pdf_pdftotext_failure` | `returncode != 0`时调用`die` |
| 11 | `test_extract_pdf_pypdf_fallback` | `pdftotext`缺失+`--pdf-fallback pypdf`时正确导入并提取 |
| 12 | `test_extract_pdf_pypdf_import_error` | pypdf未安装时调用`die` |
| 13 | `test_extract_pdf_pypdf_empty_text` | pypdf提取为空时调用`die` |
| 14 | `test_extract_docx_basic` | 正确解析paragraphs，heading样式转markdown |
| 15 | `test_extract_docx_bad_zip` | 损坏zip调用`die` |
| 16 | `test_extract_docx_missing_document_xml` | 缺少`word/document.xml`调用`die` |
| 17 | `test_extract_docx_parse_error` | 非法XML调用`die` |
| 18 | `test_extract_docx_with_images` | 含`(1.2.3)`编号的段落正确插入`[[IMAGE:...]]` |
| 19 | `test_spans_from_markdown_block_ranges` | 正确按空行分割block，计算char_start/char_end |
| 20 | `test_spans_from_markdown_reading_order` | reading_order按block顺序递增 |

**伪代码示例**：

```python
import pytest
from unittest.mock import patch, MagicMock

class MockPdfPage:
    def extract_text(self):
        return "Page 1 text"

class MockPdfReader:
    pages = [MockPdfPage()]

@pytest.fixture(autouse=True)
def no_die(monkeypatch):
    """将所有die调用转为抛出PackBuilderTestError以便pytest捕获。"""
    def fake_die(msg, code=2):
        raise PackBuilderTestError(msg)
    monkeypatch.setattr("build_skill_lib.utils.fs.die", fake_die)
    monkeypatch.setattr("build_skill_lib.extract.die", fake_die)

def test_extract_pdf_with_pdftotext(tmp_path, monkeypatch):
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"fake pdf")
    monkeypatch.setattr("build_skill_lib.extract.which", lambda cmd: "/usr/bin/pdftotext")
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="Extracted PDF text\n", stderr="")
        result = _extract_pdf_to_text(pdf, pdf_fallback="none")
        assert result == "Extracted PDF text\n"
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0][0] == "/usr/bin/pdftotext"

def test_extract_pdf_pypdf_fallback(tmp_path, monkeypatch):
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"fake pdf")
    monkeypatch.setattr("build_skill_lib.extract.which", lambda cmd: None)
    with patch("builtins.__import__", side_effect=lambda name, *a, **kw: {
        "pypdf": MagicMock(PdfReader=MockPdfReader)
    }[name]):
        result = _extract_pdf_to_text(pdf, pdf_fallback="pypdf")
        assert "Page 1 text" in result
```

---

### 2.3 `build.py` (P0)

**Mock需求**：
- 大量内部Phase函数需要**部分mock**（使用`unittest.mock.patch`在特定测试点替换）
- `tempfile.TemporaryDirectory`（可接受真实临时目录）
- `shutil.move`, `shutil.rmtree`, `shutil.copytree`
- 所有子模块的IO操作（建议用**端到端集成测试**替代大量细粒度mock）

**测试策略**：**"细粒度单元测试 + 端到端集成测试"** 双轨制。

| # | 用例名称 | 类型 | 断言要点 |
|---|---------|------|---------|
| 1 | `test_build_skill_chunk_size_validation` | 单元 | `chunk_size <= 0`, `overlap < 0`, `overlap >= chunk_size` 均调用die |
| 2 | `test_build_skill_target_exists_no_force` | 单元 | target存在且force=False时die |
| 3 | `test_build_skill_preserve_bin_hooks` | 单元 | force=True时`bin/`和`hooks/`被复制到tmp |
| 4 | `test_write_manifest_structure` | 单元 | manifest.json包含skill_name/title/docs列表 |
| 5 | `test_safe_canonical_version` | 单元 | 特殊字符替换为`-`，去首尾`-` |
| 6 | `test_canonical_text_rel_path` | 单元 | 路径格式正确 |
| 7 | `test_load_existing_corpus_manifest_missing` | 单元 | 文件不存在返回`("", {})` |
| 8 | `test_load_existing_corpus_manifest_malformed` | 单元 | JSON损坏返回`("", {})` |
| 9 | `test_write_corpus_manifest_preserves_existing` | 单元 | force=True时从existing_root加载旧canonical_text |
| 10 | `test_atomic_replace_success` | 单元 | tmp→target，旧target被备份并删除 |
| 11 | `test_atomic_replace_restore_on_failure` | 单元 | move失败时从backup恢复 |
| 12 | `test_extract_documents_from_inputs` | 集成 | 给定.md输入，产出正确的docs/headings/nodes/canonical_texts |
| 13 | `test_extract_documents_from_ir_jsonl` | 集成 | 给定IR JSONL，正确读取并生成doc和nodes |
| 14 | `test_merge_build_history_with_force` | 集成 | force=True时读取existing_db并合并历史 |
| 15 | `test_write_indexes_and_assets_outputs` | 集成 | 产出headings/kw索引、SKILL.md、scripts |
| 16 | `test_build_skill_e2e_single_md` | E2E | 单文档构建后所有预期文件存在且格式正确 |
| 17 | `test_build_skill_e2e_force_rebuild` | E2E | force重建后manifest更新，旧数据被合并 |
| 18 | `test_build_skill_e2e_ir_jsonl` | E2E | 从IR JSONL构建与从原始文档构建结果等价 |

**伪代码示例**（`test_atomic_replace_restore_on_failure`）：

```python
def test_atomic_replace_restore_on_failure(tmp_path):
    from build_skill_lib.build import _atomic_replace
    target = tmp_path / "skill"
    target.mkdir()
    (target / "old.txt").write_text("old")
    backup = tmp_path / "skill.old"

    tmp = tmp_path / "skill.tmp"
    tmp.mkdir()
    (tmp / "new.txt").write_text("new")

    def bad_move(src, dst):
        if "skill.tmp" in str(src):
            raise OSError("disk full")
        shutil.move(str(src), str(dst))

    with patch("shutil.move", side_effect=bad_move):
        with pytest.raises(OSError):
            _atomic_replace(tmp, target)

    # 恢复后target应仍然存在
    assert target.exists()
    assert (target / "old.txt").read_text() == "old"
```

---

### 2.4 `ir/io.py` (P1)

**Mock需求**：
- `build_skill_lib.utils.fs.die`（monkeypatch为抛异常）
- `pathlib.Path.open`（使用真实文件或`tmp_path`）

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_read_ir_jsonl_basic` | 混合doc/node行，正确返回InputDoc列表和NodeRecord列表 |
| 2 | `test_read_ir_jsonl_invalid_json` | 非法JSON行调用die并报告行号 |
| 3 | `test_read_ir_jsonl_empty_lines_skipped` | 空行被跳过 |
| 4 | `test_read_ir_jsonl_missing_doc_id_in_doc_row` | doc行缺少doc_id调用die |
| 5 | `test_read_ir_jsonl_missing_doc_id_in_node_row` | node行缺少doc_id调用die |
| 6 | `test_read_ir_jsonl_unknown_doc_id` | node引用未知doc_id时自动创建兜底doc |
| 7 | `test_read_ir_jsonl_invalid_row_type` | 非法type字段调用die |
| 8 | `test_read_ir_jsonl_sibling_prev_next_links` | 同parent同kind的node自动补全prev_id/next_id |
| 9 | `test_read_ir_jsonl_doc_hash_from_nodes` | doc行无doc_hash时从nodes内容计算 |
| 10 | `test_parse_ir_aliases_none` | `None`返回空tuple |
| 11 | `test_parse_ir_aliases_list` | 列表去重、normalize、保留原始值 |
| 12 | `test_parse_ir_aliases_string` | 字符串包装为tuple |
| 13 | `test_ir_node_file_index_from_ordinal` | ordinal>0时直接返回 |
| 14 | `test_ir_node_file_index_from_node_id` | 从node_id尾部数字推断 |
| 15 | `test_read_ir_jsonl_encoding_replace` | 含非法UTF-8序列时使用errors="replace"不崩溃 |

**伪代码示例**：

```python
def test_read_ir_jsonl_sibling_prev_next_links(tmp_path):
    path = tmp_path / "test.ir.jsonl"
    path.write_text(
        json.dumps({"type": "doc", "doc_id": "d1", "title": "Doc"}) + "\n"
        json.dumps({"type": "node", "doc_id": "d1", "node_id": "n1", "kind": "chunk", "ordinal": 1}) + "\n"
        json.dumps({"type": "node", "doc_id": "d1", "node_id": "n2", "kind": "chunk", "ordinal": 2}) + "\n",
        encoding="utf-8",
    )
    docs, nodes = read_ir_jsonl(path)
    n1 = next(n for n in nodes if n.node_id == "n1")
    n2 = next(n for n in nodes if n.node_id == "n2")
    assert n1.next_id == "n2"
    assert n2.prev_id == "n1"
```

---

### 2.5 `incremental/` (P1)

包含 `state.py` + `invalidation.py`。

**Mock需求**：
- `fingerprint.utils` 中的文件读取函数（`source_fingerprint_for_path` / `extracted_text_fingerprint_for_path`）
- `spans_from_markdown`
- `pathlib.Path.read_text`
- `build_skill_lib.utils.contract` 中的函数

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_detect_changed_documents_new_doc` | 新增文档 → changed + rebuild |
| 2 | `test_detect_changed_documents_unchanged` | 指纹完全匹配 → unchanged |
| 3 | `test_detect_changed_documents_metadata_only` | source指纹变但text指纹不变 → metadata_only |
| 4 | `test_detect_changed_documents_content_changed` | text指纹变 → changed + rebuild |
| 5 | `test_detect_changed_documents_removed` | 旧state中有但current无 → removed + rebuild |
| 6 | `test_detect_changed_documents_empty_previous` | previous为空 → 所有current均为new |
| 7 | `test_diff_ir_no_changes` | 前后state相同 → 所有changed集合为空 |
| 8 | `test_diff_ir_span_changed` | span指纹变化 → changed_span_ids包含对应id |
| 9 | `test_diff_ir_node_changed` | node指纹变化 → changed_node_ids |
| 10 | `test_diff_ir_alias_changed` | alias指纹变化 → changed_alias_keys |
| 11 | `test_diff_ir_edge_changed` | edge指纹变化 → changed_edge_keys |
| 12 | `test_diff_ir_export_changed` | export_sha256变化 → changed_export_doc_ids |
| 13 | `test_diff_ir_fallback_global_aliases` | doc级别无alias变化但全局aliases变化 →  fallback捕获 |
| 14 | `test_build_state_from_artifact_structure` | 生成的state包含artifact_version/documents/indexes等顶层键 |
| 15 | `test_build_state_document_fingerprints` | 每个doc包含7类指纹 |
| 16 | `test_build_state_infer_active_parser` | 从node kinds推断parser类型 |
| 17 | `test_write_build_state_deterministic` | 相同输入两次生成相同JSON（sort_keys） |
| 18 | `test_compute_toolchain_checksum` | 缺失key_files时仍返回hexdigest |

**伪代码示例**：

```python
def test_detect_changed_documents_content_changed(monkeypatch):
    from incremental.invalidation import detect_changed_documents
    previous = {
        "documents": {
            "doc1": {
                "source_fingerprint": "old_src",
                "extracted_text_fingerprint": "old_txt",
            }
        }
    }
    doc = InputDoc(path=Path("/tmp/dummy.md"), doc_id="doc1", title="T")
    monkeypatch.setattr(
        "build_skill_lib.incremental.invalidation.source_fingerprint_for_path",
        lambda p: "new_src",
    )
    monkeypatch.setattr(
        "build_skill_lib.incremental.invalidation.extracted_text_fingerprint_for_path",
        lambda p: "new_txt",
    )
    cs = detect_changed_documents(previous, [doc])
    assert "doc1" in cs.changed_doc_ids
    assert "doc1" in cs.rebuild_doc_ids
    assert "doc1" not in cs.metadata_only_doc_ids
```

---

### 2.6 `fingerprint/utils.py` (P1)

**Mock需求**：
- `pathlib.Path.read_bytes` / `read_text`
- `builtins.open`（OSError模拟）

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_sha256_text` | 确定性、不同输入不同输出 |
| 2 | `test_sha256_bytes` | 同上 |
| 3 | `test_fingerprint_summary_structure` | 返回dict包含4个预期键 |
| 4 | `test_source_fingerprint_for_path` | 读取文件字节并sha256 |
| 5 | `test_extracted_text_fingerprint_for_path` | 调用extract+canonical_text后sha256 |
| 6 | `test_source_fingerprint_oserror_fallback` | 文件读取失败时fallback到fallback参数的sha256 |
| 7 | `test_node_fingerprint_stable` | 相同NodeRecord相同指纹，不同字段不同指纹 |
| 8 | `test_alias_fingerprint_stable` | 同上 |
| 9 | `test_edge_fingerprint_stable` | 同上 |
| 10 | `test_node_fingerprint_ignores_body_md` | 确认node_fingerprint不包含body_md（从代码看确实不包含） |
| 11 | `test_fingerprint_order_independence` | stable_payload内部排序确保顺序无关 |
| 12 | `test_source_fingerprint_for_path_not_found` | 文件不存在时抛OSError（调用方处理） |

---

### 2.7 `index/` (P1)

包含 `builder.py` + `refresh.py`。

**Mock需求**：
- `build_skill_lib.utils.fs.write_tsv`（验证调用参数）

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_build_keywords_from_title` | 中英文混合标题正确拆分，去重，过滤<2字符 |
| 2 | `test_build_keywords_from_title_punctuation` | `、/，,；:`等分隔符正确处理 |
| 3 | `test_shard_name_from_key_empty` | `""` → `"_EMPTY"` |
| 4 | `test_shard_name_from_key_windows_reserved` | `CON` → Unicode转义 |
| 5 | `test_shard_name_from_key_invalid_chars` | `<>:"/\|?*` 和 `.` ` ` → Unicode转义 |
| 6 | `test_shard_name_from_key_long` | >32字符截断 |
| 7 | `test_shard_rows_by_prefix_balanced` | 行数<=max_rows时不分裂 |
| 8 | `test_shard_rows_by_prefix_oversize_split` | 超限时按前缀逐级分裂 |
| 9 | `test_shard_rows_by_prefix_max_prefix_len` | 达到max_prefix_len后不再分裂 |
| 10 | `test_write_sharded_index_creates_files` | 目录和_tsv文件正确创建 |
| 11 | `test_write_sharded_index_shard_map` | `_shards.tsv`包含所有shard映射 |
| 12 | `test_incremental_reindex_full_refresh` | rebuild_doc_ids非空 → 所有索引刷新 |
| 13 | `test_incremental_reindex_metadata_only` | metadata_only → 仅aliases+edges |
| 14 | `test_incremental_reindex_no_changes` | 空changeset → 空refreshed_indexes |

**伪代码示例**：

```python
def test_shard_rows_by_prefix_oversize_split():
    from build_skill_lib.index.builder import _shard_rows_by_prefix
    rows = [(f"keyword{i:03d}",) for i in range(250)]
    shards = _shard_rows_by_prefix(rows, primary_index=0, max_rows=100, max_prefix_len=4)
    # 所有shard都应<=100
    assert all(len(v) <= 100 for v in shards.values())
    # 总行数守恒
    assert sum(len(v) for v in shards.values()) == 250
```

---

### 2.8 `render/` (P2)

包含 `skill_md.py` + `node.py`。

**Mock需求**：
- `build_skill_lib.utils.fs.write_text`（验证输出）

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_render_generated_skill_md_frontmatter` | 包含`name`和`description` frontmatter |
| 2 | `test_render_generated_skill_md_doc_list` | 包含文档列表表格，最多5个doc标题 |
| 3 | `test_render_generated_skill_md_triage_example` | 包含triage命令示例 |
| 4 | `test_render_generated_skill_md_truncation` | >5个docs时追加`...` |
| 5 | `test_frontmatter_kb_node` | YAML格式正确，包含所有必需字段 |
| 6 | `test_frontmatter_kb_node_heading_stack` | heading_stack作为JSON数组插入 |
| 7 | `test_render_kb_node_frontmatter_with_aliases` | 含aliases时在`---`前插入aliases行 |
| 8 | `test_frontmatter_chapter_section` | chapter/section格式正确，section可选 |
| 9 | `test_write_doc_metadata` | metadata.md包含标题、源文件、版本、哈希 |
| 10 | `test_write_doc_metadata_with_parser` | 含active_parser时追加行 |
| 11 | `test_write_structure_report` | JSON结构含selected_parser/runner_ups/outline |
| 12 | `test_write_structure_report_minimal` | 可选字段为空时结构完整 |

---

### 2.9 `kbtool_assets.py` (P2)

**Mock需求**：
- `shutil.which`（PyInstaller检测）
- `subprocess.run`（PyInstaller调用）
- `pathlib.Path.exists` / `is_file` / `read_text`
- `build_skill_lib.utils.fs.die`

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_write_reindex_script` | 从模板复制并chmod 755 |
| 2 | `test_write_kbtool_script` | 复制kbtool.py + kbtool_lib目录，忽略pycache |
| 3 | `test_write_kbtool_sha1` | 对scripts目录下所有.py计算跨平台稳定hash |
| 4 | `test_write_kbtool_sha1_missing_files` | 缺失kbtool.py或kbtool_lib时die |
| 5 | `test_write_root_kbtool_entrypoints` | 生成shell和cmd脚本，内容非空 |
| 6 | `test_maybe_package_kbtool_pyinstaller_not_found` | PyInstaller不在PATH时返回None并log warning |
| 7 | `test_maybe_package_kbtool_pyinstaller_success` | 成功调用subprocess，返回exe路径 |
| 8 | `test_maybe_package_kbtool_pyinstaller_failure` | returncode!=0时die |
| 9 | `test_copy_search_binaries_missing_dir` | 无bundled bin/时静默跳过 |
| 10 | `test_copy_search_binaries_copies_tools` | rg/fd被复制并chmod 755 |

---

### 2.10 `kbtool_lib/reindex.py` (P3)

**Mock需求**：
- `sqlite3.connect`
- `pathlib.Path` 操作（大规模，建议用真实临时目录）
- `subprocess.run`（外部工具）

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_build_nodes_from_references_structure` | 从references/目录正确重建DocRow+NodeRow |
| 2 | `test_build_nodes_from_references_no_refs_dir` | 缺失references/时调用die |
| 3 | `test_build_nodes_from_references_leaf_inference` | chapter/section有children时is_leaf=False |
| 4 | `test_build_nodes_from_references_sibling_links` | prev/next自动补全 |
| 5 | `test_extract_alias_rows_from_titles` | 从leaf node标题生成exact/abbreviation别名 |
| 6 | `test_extract_reference_edges` | body_plain中含`参见第X条`时生成references边 |
| 7 | `test_write_corpus_manifest` | 生成corpus_manifest.json并写canonical_text文件 |
| 8 | `test_write_build_state` | 生成build_state.json含documents/indexes |

**注意**：`reindex.py`与`build.py`有功能重叠，建议用**集成测试**验证从已构建的skill目录重新索引后state一致。

---

### 2.11 `kbtool_lib/runtime.py` (P3)

**Mock需求**：
- `sqlite3.connect` / `Connection.set_progress_handler`
- `time.monotonic`（timeout测试必须mock）
- `importlib.util.spec_from_file_location` / `exec_module`
- `os.environ`
- `pathlib.Path`（is_symlink等）

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_infer_skill_root_frozen` | `sys.frozen=True`时使用exe路径 |
| 2 | `test_infer_skill_root_unfrozen` | 正常模式下向上查找scripts父目录 |
| 3 | `test_resolve_root_empty` | 空字符串 fallback 到 infer_skill_root |
| 4 | `test_resolve_root_not_dir` | 非目录路径调用die |
| 5 | `test_open_db_missing` | 数据库不存在时die |
| 6 | `test_sqlite_timeout_no_timeout` | timeout_ms<=0时不设置handler |
| 7 | `test_sqlite_timeout_triggers` | mock time.monotonic超deadline后handler返回1 |
| 8 | `test_safe_output_path_outside_root` | 路径解析出root外时die |
| 9 | `test_safe_output_path_is_root` | 指向root目录本身时die |
| 10 | `test_run_hook_invalid_name` | 非法hook名返回空dict |
| 11 | `test_run_hook_symlink_rejected` | symlink hook调用die |
| 12 | `test_run_hook_allowlist_reject` | 不在allowlist中的hook调用die |
| 13 | `test_run_hook_success` | 正常hook执行，返回dict和sha1 |
| 14 | `test_run_hook_exception` | hook抛异常时die并包含traceback（debug模式下） |

**伪代码示例**（timeout测试必须mock time）：

```python
def test_sqlite_timeout_triggers():
    conn = sqlite3.connect(":memory:")
    with patch("time.monotonic", side_effect=[0.0, 0.5, 1.5]):
        with SqliteTimeout(conn, timeout_ms=1000) as st:
            # 第一次检查 0.0 < deadline(1.0) → 继续
            # 第二次检查 1.5 >= deadline → timed_out=True
            conn.set_progress_handler(lambda: st.timed_out, 10_000)  # 触发检查
            # 这里我们直接调用handler来验证
            handler = conn.get_progress_handler()  # sqlite3无此API，需间接测试
    # 实际测试中通过执行长查询或反射调用内部handler
```

**修正**：sqlite3无`get_progress_handler`，应在测试中通过执行实际SQL触发或反射获取闭包。更简单的方式是直接调用内部handler闭包（如果可访问），或测试timeout_ms<=0分支。

---

### 2.12 `kbtool_lib/grep.py` + `locate.py` (P3)

**Mock需求**：
- `subprocess.run`（rg/fd调用）
- `templates.kbtool_lib.tools.resolve_tool_binary`
- `templates.kbtool_lib.tools.parse_rg_output`

| # | 用例名称 | 断言要点 |
|---|---------|---------|
| 1 | `test_cmd_search_pattern_mode` | `--pattern`构造rg args含`--fixed-strings`或regex |
| 2 | `test_cmd_search_query_mode` | `--query`多term生成多个`-e`参数，限制32个term |
| 3 | `test_cmd_search_rg_not_found` | rg缺失时die |
| 4 | `test_cmd_search_timeout` | subprocess.TimeoutExpired时返回JSON错误，exit 1 |
| 5 | `test_cmd_search_punct_fallback` | fixed-string无命中时尝试punctuation-tolerant regex |
| 6 | `test_cmd_search_audit_output` | `--out`时写入audit markdown |
| 7 | `test_cmd_files_basic` | `--pattern`构造fd args，结果转为相对路径 |
| 8 | `test_cmd_files_fd_not_found` | fd缺失时die |
| 9 | `test_cmd_files_timeout` | timeout时返回JSON错误，exit 1 |
| 10 | `test_cmd_files_audit_output` | `--out`时写入audit markdown |

**伪代码示例**：

```python
def test_cmd_search_timeout(monkeypatch):
    from kbtool_lib.grep import cmd_search
    import argparse

    args = argparse.Namespace(root="/tmp/skill", pattern="test", query="", fixed=False, limit=50, out="")
    monkeypatch.setattr(
        "kbtool_lib.grep.resolve_tool_binary",
        lambda name, search_paths: Path("/bin/rg"),
    )
    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("rg", 60)):
        rc = cmd_search(args)
        assert rc == 1
```

---

## 3. 集成测试设计（端到端场景）

### 3.1 核心E2E场景

| 场景ID | 名称 | 步骤 | 验证点 |
|--------|------|------|--------|
| E2E-1 | **从零构建单文档知识库** | 1.准备handbook.md → 2.调用`build_skill` → 3.检查输出目录 | kb.sqlite、manifest.json、build_state.json、references/、indexes/、SKILL.md、scripts/、kbtool 均存在且结构正确 |
| E2E-2 | **IR JSONL round-trip** | 1.构建原始文档 → 2.读取phase_a_artifact.json → 3.用IR重新构建 → 4.对比两次build_state | 两次build_state的document指纹一致（允许timestamp差异） |
| E2E-3 | **增量构建-内容未变** | 1.首次构建 → 2.复制build_state.json → 3.再次构建相同输入 → 4.对比build_state | unchanged_doc_ids包含所有doc，timestamp更新但指纹不变 |
| E2E-4 | **增量构建-内容变更** | 1.首次构建 → 2.修改源文件 → 3.再次构建 → 4.对比build_state | changed_doc_ids包含修改的doc，canonical_text更新，旧版本数据合并 |
| E2E-5 | **force重建保留用户资产** | 1.构建并手动添加bin/hooks内容 → 2.force重建 → 3.检查输出 | bin/和hooks/内容被保留 |
| E2E-6 | **运行时查询闭环** | 1.构建skill → 2.调用kbtool search → 3.调用kbtool triage → 4.调用kbtool bundle | 各命令返回有效JSON，audit文件正确生成，结果包含预期文本 |
| E2E-7 | **损坏输入容错** | 1.准备损坏的DOCX/Bad ZIP → 2.尝试构建 → 3.验证错误信息 | 调用die并给出可操作错误信息（不裸抛Python traceback） |
| E2E-8 | **多文档混合类型构建** | 1.同时传入.md + .txt + .docx + .pdf → 2.构建 → 3.验证 | 所有文档正确提取，doc_id不冲突，manifest包含全部 |

### 3.2 E2E测试fixture设计

```python
# conftest.py 建议添加
import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "scripts" / "tests" / "fixtures" / "retrieval"

@pytest.fixture
def handbook_path() -> Path:
    return FIXTURES / "handbook.md"

@pytest.fixture
def sample_txt(tmp_path: Path) -> Path:
    p = tmp_path / "sample.txt"
    p.write_text("第一章\n\n这是第一节的内容。\n\n1.1 子节标题\n\n详细说明在这里。\n")
    return p

@pytest.fixture
def sample_ir_jsonl(tmp_path: Path) -> Path:
    p = tmp_path / "sample.ir.jsonl"
    p.write_text(
        '{"type":"doc","doc_id":"d1","title":"Doc One"}\n'
        '{"type":"node","doc_id":"d1","node_id":"n1","kind":"chunk","body_md":"Hello"}\n'
    )
    return p

@pytest.fixture
def built_skill(tmp_path: Path, handbook_path: Path) -> Path:
    """返回已构建的skill根目录（缓存级fixture，scope=module可提高速度）。"""
    from scripts.build_skill import build_skill
    out = tmp_path / "out"
    build_skill("test-skill", "Test KB", [handbook_path], out, force=True)
    return out / "test-skill"
```

---

## 4. 异常路径测试矩阵

| 异常类型 | 触发方式 | 预期行为 | 覆盖模块 |
|---------|---------|---------|---------|
| **损坏文件** | 构造非ZIP的.docx、截断的PDF、BOM乱码的.txt | `die()`抛出SystemExit，消息包含文件名和建议 | extract.py |
| **空文件** | 0字节的.md/.txt | 正常流程（可能产生空canonical_text和0个node） | extract.py, build.py |
| **权限错误** | chmod 000的输入文件/输出目录 | OSError被捕获或向上传播（视调用层而定） | build.py, kbtool_assets.py, runtime.py |
| **磁盘满** | mock `shutil.move` / `Path.write_text` 抛OSError | `_atomic_replace`尝试从backup恢复 | build.py |
| **外部工具超时** | mock `subprocess.run` 抛`TimeoutExpired` | 返回JSON错误，exit code 1，不崩溃 | grep.py, locate.py |
| **外部工具缺失** | mock `shutil.which` 返回None | 记录warning或`die()`提示安装 | extract.py, kbtool_assets.py, grep.py, locate.py |
| **非法路径遍历** | `--out ../../../etc/passwd` | `safe_output_path` / `die()`拒绝 | runtime.py |
| **Symlink攻击** | hooks/下放置symlink | `run_hook`拒绝执行symlink | runtime.py |
| **非法hook名** | hook_name含`../`或特殊字符 | `run_hook`返回空dict不执行 | runtime.py |
| **SQLite查询超时** | 模拟超长时间查询 | `SqliteTimeout`触发中断，设置timed_out标志 | runtime.py |
| **JSON损坏** | corpus_manifest.json / build_state.json 内容非法 | 返回默认值{}或""，不崩溃 | build.py, incremental/state.py, reindex.py |
| **IR行格式错误** | JSONL中缺少必填字段 | `die()`报告行号和字段名 | ir/io.py |
| **Node kind非法** | 构造不在`KNOWN_NODE_KINDS`中的kind | `NodeRecord.__post_init__`发出`UserWarning` | types.py |

---

## 5. 测试基础设施改进建议

### 5.1 需补充的基础设施

1. **`scripts/tests/conftest.py`**（已存在，需扩展）
   - 补充 `no_die` fixture：将`build_skill_lib.utils.fs.die`和模块局部`die` monkeypatch为抛出自定义异常（如`PackBuilderTestError`），使pytest能正常捕获断言。
   - 补充 `tmp_skill_root` fixture：提供标准skill目录结构模板（含references/, kb.sqlite, corpus_manifest.json等），供kbtool_lib模块测试使用。
   - 补充 `mock_time` fixture：用于timeout测试。

2. **`scripts/tests/fixtures/extraction/`**
   - 添加最小有效PDF（可用纯文本构造的trivial PDF或mock）。
   - 添加最小有效DOCX（可用Python `zipfile`动态生成，避免提交二进制）。
   - 添加损坏的DOCX（非ZIP内容）。
   - 添加各种编码的TXT（UTF-8, UTF-8-SIG, GB18030）。

3. **`scripts/tests/fixtures/ir/`**
   - 添加有效IR JSONL样本（含doc+node混合行）。
   - 添加损坏IR JSONL（非法JSON、缺失doc_id、未知type）。

### 5.2 pytest配置增强（`pyproject.toml`）— 已落地

```toml
[tool.pytest.ini_options]
testpaths = ["scripts/tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "-ra",
    "--strict-markers",
    "--tb=short",
]
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "e2e: marks end-to-end integration tests",
    "requires_external: tests requiring pdftotext/rg/fd/pyinstaller",
]
```

### 5.3 测试分类与CI策略

| 测试层 | 标记 | 执行频率 | 平均耗时 |
|--------|------|---------|---------|
| 单元测试（纯函数/无IO） | 无标记 | 每次提交 | <10s |
| 单元测试（含文件IO） | 无标记 | 每次提交 | <30s |
| 集成测试（build_skill） | `e2e` | 每次提交 | ~60s |
| 外部工具依赖测试 | `requires_external` | PR合并前/夜间 | ~120s |

### 5.4 现有测试框架（pytest）评估结论

| 评估项 | 结论 | 说明 |
|--------|------|------|
| **测试框架** | ✅ 足够 | pytest已配置，无需切换 |
| **fixtures** | ⚠️ 需补充 | `conftest.py` 已存在，die()处理、skill目录模板、编码样本仍需补充 |
| **mock工具** | ✅ 足够 | `unittest.mock` + `pytest.monkeypatch` 覆盖全部需求 |
| **临时文件** | ✅ 足够 | `tmp_path` fixture 已内置 |
| **覆盖率** | ⚠️ 建议补充 | 建议添加`pytest-cov`，目标覆盖率：整体>80%，tokenizer_core/extract/ir>95% |
| **参数化测试** | ✅ 足够 | `@pytest.mark.parametrize` 适合tokenizer_core等密集case |
| **并行执行** | ⚠️ 建议补充 | 集成测试慢，建议添加`pytest-xdist`加速CI |

### 5.5 关键实现注意事项

1. **`die()`处理模式**：
   由于所有异常路径都通过`die()`→`SystemExit`退出，测试时必须统一monkeypatch。建议`conftest.py`中：
   ```python
   class PackBuilderTestError(Exception):
       pass

   @pytest.fixture(autouse=True)
   def patch_die(monkeypatch):
       modules = [
           "build_skill_lib.utils.fs",
           "build_skill_lib.extract",
           "build_skill_lib.ir.io",
           # ... 其他含die的模块
       ]
       for mod in modules:
           monkeypatch.setattr(f"{mod}.die", lambda msg, code=2: (_ for _ in ()).throw(PackBuilderTestError(msg)))
   ```

2. **模板模块导入**：
   `kbtool_lib/`位于`templates/`下，不在`scripts/`包内。测试时需确保Python path包含`templates/`目录，或通过`importlib`动态加载。

3. **外部工具条件跳过**：
   ```python
   rg = shutil.which("rg")
   pytestmark = pytest.mark.skipif(not rg, reason="ripgrep not installed")
   ```

---

## 6. 快速启动：首批实现的3个测试文件

建议按以下顺序实施，快速建立测试基线：

### 文件1：`scripts/tests/test_tokenizer_core.py`（最容易，最高价值）
- 覆盖全部25个tokenizer_core用例
- 零mock，纯参数化测试
- 预计2小时完成

### 文件2：`scripts/tests/test_extract.py`（输入 gatekeeper）
- 覆盖md/txt/docx/pdf提取
- 重点mock `subprocess.run`, `shutil.which`, `die`
- 预计4小时完成

### 文件3：`scripts/tests/test_ir_io.py`（数据契约核心）
- 覆盖IR JSONL读写与校验
- 使用`tmp_path`写真实文件
- 预计3小时完成

---

*本文档应与代码同步维护。新增模块或修改异常处理路径时，应同步更新对应的测试用例清单。*
