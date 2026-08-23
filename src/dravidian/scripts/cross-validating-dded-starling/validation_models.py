"""
Shared dataclasses for the Starling-to-Burrow tree validation pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from burrow_entry_parser import LanguageAttestation


@dataclass
class LanguageEntry:
    """A single leaf-language attestation extracted from a Starling node."""

    language: str
    headword: str
    headword_raw: str
    meaning: str
    source_node_label: str


@dataclass
class TreeNode:
    """A node in the Starling etymon tree."""

    label: str
    headword: str
    meaning: str
    ded_number: Optional[str] = None
    notes: str = ""
    additional_forms: str = ""
    source_url: str = ""
    source_hash: str = ""
    is_proto: bool = True
    depth: int = 0
    language_entries: List[LanguageEntry] = field(default_factory=list)
    children: List[TreeNode] = field(default_factory=list)

    def all_language_entries(self) -> List[LanguageEntry]:
        """Recursively collect all leaf language entries under this node."""
        entries = list(self.language_entries)
        for child in self.children:
            entries.extend(child.all_language_entries())
        return entries


@dataclass
class ValidationResult:
    """Validation result for one language entry within one branch."""

    record_num: int
    pd_headword: str
    pd_meaning: str
    branch_label: str
    branch_headword: str
    ded_number: Optional[str]
    language: str
    starling_headword: str
    proto_node_depth: int = 0
    matched: bool = False
    starling_meaning: str = ""
    source_node_label: str = ""
    burrow_headword: str = ""
    burrow_gloss: str = ""
    burrow_language_abbrev: str = ""
    match_type: str = ""
    match_confidence: float = 0.0
    branch_status: str = ""
    notes: str = ""
    proto_chain: str = ""
    proto_label_path: str = ""
    proto_headword_path: str = ""
    proto_depth_path: str = ""
    branch_notes: str = ""
    ancestor_notes: str = ""
    branch_additional_forms: str = ""
    ancestor_additional_forms: str = ""
    ancestor_proto_count: int = 0
    burrow_full_text: str = ""
    burrow_source: str = ""
    burrow_gloss_parsed: str = ""


@dataclass
class BurrowParagraph:
    """Cached Burrow paragraph grouped by DED number."""

    attestations: List[LanguageAttestation]
    raw_html: str = ""
    full_text: str = ""
    page: int = 0
