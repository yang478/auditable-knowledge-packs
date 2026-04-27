from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LocatorAlignment:
    text: str
    char_start: int
    char_end: int


def exact_substring_matches(canonical_text: str, target_text: str) -> list[LocatorAlignment]:
    if not canonical_text or not target_text:
        return []
    matches: list[LocatorAlignment] = []
    start = 0
    while True:
        index = canonical_text.find(target_text, start)
        if index < 0:
            break
        matches.append(
            LocatorAlignment(
                text=target_text,
                char_start=index,
                char_end=index + len(target_text),
            )
        )
        start = index + 1
    return matches


def choose_match(matches: list[LocatorAlignment], previous_end: int) -> LocatorAlignment | None:
    for match in matches:
        if match.char_start >= previous_end:
            return match
    return None


def align_candidate_texts(candidate_texts: list[str], canonical_text: str) -> list[LocatorAlignment]:
    previous_end = 0
    aligned: list[LocatorAlignment] = []
    for text in candidate_texts:
        match = choose_match(exact_substring_matches(canonical_text, text), previous_end)
        if match is None:
            continue
        aligned.append(match)
        previous_end = match.char_end
    return aligned


def align_candidates(candidate_texts: list[str], canonical_text: str) -> list[LocatorAlignment]:
    return align_candidate_texts(candidate_texts, canonical_text)
