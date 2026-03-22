# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

- Research-first workflow for skills:
  - Add `kbtool research` (one round = bundle + verify + structured trace outputs).
  - `kbtool --skill` now advertises `research` (hides legacy `bundle`) and includes evidence-only + planner audit contracts.
- Stronger audit trail:
  - `research` records effective params + planner metadata into `trace.roundNN.json` and appends `trace.jsonl`.
  - `verify.roundNN.json` fails with `audit_incomplete` when required planner fields are missing.
- Evidence-only guardrails:
  - `bundle.roundNN.md` includes an explicit “Evidence-only” answering contract at the top.
- DOCX equation robustness:
  - When equations are stored as images, extraction now emits `[[IMAGE:...]]` markers instead of silently dropping them.
