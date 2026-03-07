"""
Enhanced Cross-Validation Workflow with Detailed Entry Parsing.
Validates StarlingDB data against detailed Burrow DED entries.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, TypeGuard
import unicodedata
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from burrow_entry_parser import BurrowEntryParser, LanguageAttestation
from burrow_language_mappings import get_burrow_abbrev_for_display, match_language_variant


class EnhancedValidationWorkflow:
    """
    Enhanced validation that queries full DED entries and matches
    by language + headword within the detailed attestations.
    """
    
    def __init__(self, csv_path: str, output_dir: str = "enhanced_validation"):
        self.csv_path = Path(csv_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.base_url = "https://dsal.uchicago.edu/cgi-bin/app/burrow_query.py"
        self.parser = BurrowEntryParser()

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
        )

        self.df: pd.DataFrame | None = None
        self.validation_results: list[dict[str, Any]] = []
        self.cache: Dict[str, str] = {}
        self.corpus_entries_by_ded: Dict[str, list[Dict[str, Any]]] = {}
        self.corpus_attestations_by_headword: Dict[str, list[Dict[str, Any]]] = {}
        self.corpus_loaded: bool = False  # Track if corpus is loaded to skip web requests
        self.checkpoint_file: Path = self.output_dir / "validation_checkpoint.json"

    def _require_data_loaded(self) -> pd.DataFrame:
        """Return loaded DataFrame or raise if not loaded."""
        if self.df is None:
            raise ValueError("Starling data not loaded. Call load_data() first.")
        return self.df

    @staticmethod
    def _has_scalar_value(value: Any) -> TypeGuard[int | float | str]:
        """Return True when a scalar value is present and not NaN."""
        return value is not None and not pd.isna(value)

    @staticmethod
    def _normalize_headword_for_index(text: str) -> str:
        """
        Normalize headwords for robust corpus indexing and lookup.

        Mirrors the logic used in BurrowEntryParser.find_matching_attestation:
        - strip stars, hyphens, parentheses
        - lowercase
        - Unicode NFKD + drop combining marks
        - collapse internal whitespace
        """
        base = (
            text.replace("*", "")
            .replace("-", " ")
            .replace("(", " ")
            .replace(")", " ")
            .strip()
            .lower()
        )
        decomposed = unicodedata.normalize("NFKD", base)
        filtered = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
        return " ".join(filtered.split())
    
    def load_data(self):
        """Load StarlingDB CSV"""
        print(f"Loading: {self.csv_path}")

        # Support both CSV and Excel inputs transparently.
        suffix = self.csv_path.suffix.lower()
        if suffix in {".xlsx", ".xls"}:
            # Let pandas handle Excel encoding internally (cells are already Unicode).
            self.df = pd.read_excel(self.csv_path)
            print(f"Loaded {len(self.df)} entries from Excel file '{self.csv_path.name}'")
        else:
            encodings_to_try = ["utf-8-sig", "utf-8", "latin1"]
            last_error: Exception | None = None

            for enc in encodings_to_try:
                try:
                    self.df = pd.read_csv(self.csv_path, encoding=enc)
                    print(f"Loaded {len(self.df)} entries using encoding '{enc}'")
                    break
                except UnicodeDecodeError as exc:
                    last_error = exc
                    print(f"Failed to read with encoding '{enc}': {exc}")
            else:
                # If we exhausted all encodings, re-raise the last error for visibility.
                raise last_error if last_error is not None else UnicodeDecodeError(
                    "utf-8", b"", 0, 1, "Unable to decode file with tried encodings"
                )

        ded_count = (
            self.df["Number in DED"].notna().sum()
            if "Number in DED" in self.df.columns
            else 0
        )
        print(
            f"Entries with DED numbers: {ded_count} "
            f"({ded_count/len(self.df)*100:.1f}%)"
        )

    def load_burrow_corpus(self, corpus_path: str) -> None:
        """
        Load Burrow corpus JSON produced by burrow_corpus_scraper.py and
        build indices for fast lookup by DED number and headword.
        """
        corpus_file = Path(corpus_path)
        print(f"Loading Burrow corpus from: {corpus_file}")

        if not corpus_file.exists():
            raise FileNotFoundError(f"Burrow corpus not found: {corpus_file}")

        with open(corpus_file, "r", encoding="utf-8-sig") as f:
            data = json.load(f)

        entries = data.get("entries", [])
        if not isinstance(entries, list):
            raise ValueError("Invalid corpus format: 'entries' is not a list")

        entries_by_ded: Dict[str, list[Dict[str, Any]]] = {}
        att_by_head: Dict[str, list[Dict[str, Any]]] = {}

        for entry in entries:
            page = entry.get("page")
            ded_number = entry.get("ded_number")
            attestations_data = entry.get("attestations", [])

            attestations: list[LanguageAttestation] = []
            for att_data in attestations_data:
                try:
                    attestations.append(LanguageAttestation(**att_data))
                except TypeError:
                    continue

            corpus_entry = {
                "page": page,
                "ded_number": ded_number,
                "attestations": attestations,
            }

            if ded_number is not None:
                ded_key = str(ded_number)
                entries_by_ded.setdefault(ded_key, []).append(corpus_entry)

            for att in attestations:
                for hw in att.headwords:
                    key = self._normalize_headword_for_index(hw)
                    att_by_head.setdefault(key, []).append(
                        {
                            "attestation": att,
                            "ded_number": ded_number,
                            "page": page,
                        }
                    )

        self.corpus_entries_by_ded = entries_by_ded
        self.corpus_attestations_by_headword = att_by_head
        self.corpus_loaded = True  # Mark corpus as loaded

        print(
            f"Loaded Burrow corpus: {len(entries)} entries, "
            f"{len(self.corpus_attestations_by_headword)} unique headword keys."
        )
        print("✓ Corpus loaded - web requests will be skipped for faster validation.")
    
    def query_ded_entry(self, ded_number: str) -> Optional[str]:
        """
        Query DED number and get full page HTML.
        Returns HTML of the detailed entry page.
        """
        if ded_number in self.cache:
            return self.cache[ded_number]
        
        clean_ded = self.parser.clean_ded_number(ded_number)
        
        search_params = {'qs': clean_ded}
        response = self.session.get(self.base_url, params=search_params, timeout=30)
        
        if not response.ok:
            return None
        
        page_url = self.parser.extract_page_url(response.text, clean_ded)
        
        if not page_url:
            return None
        
        full_url = f"https://dsal.uchicago.edu{page_url}"

        # Be nice to the server.
        import time  # local import to avoid unused at module level

        time.sleep(0.5)

        page_response = self.session.get(full_url, timeout=30)
        
        if not page_response.ok:
            return None
        
        self.cache[ded_number] = page_response.text
        return page_response.text

    def _validate_via_meaning(
        self,
        meaning: str,
        language: str,
        headword: str,
    ) -> Optional[LanguageAttestation]:
        """
        Fallback: search Burrow by meaning, then match language+headword
        within the hw_result blocks.
        """
        query = meaning.strip()
        if not query:
            return None

        try:
            response = self.session.get(
                self.base_url,
                params={"qs": query},
                timeout=30,
            )
        except requests.RequestException:
            return None

        if not response.ok:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        raw_results = soup.find_all("div", class_="hw_result")
        results: list[Tag] = [r for r in raw_results if isinstance(r, Tag)]

        for result_div in results:
            # Prefer following the full entry page link if available.
            page_link: Optional[Tag] = None
            for link in result_div.find_all("a"):
                if not isinstance(link, Tag):
                    continue
                href_val = link.get("href")
                if isinstance(href_val, str) and "page=" in href_val:
                    page_link = link
                    break

            full_entry_html: Optional[str] = None
            if page_link is not None:
                href_val = page_link.get("href")
                if isinstance(href_val, str):
                    full_url = f"https://dsal.uchicago.edu{href_val}"
                    try:
                        page_resp = self.session.get(full_url, timeout=30)
                        if page_resp.ok:
                            full_entry_html = page_resp.text
                    except requests.RequestException:
                        full_entry_html = None

            # Fall back to using the hw_result block itself if full page not fetched.
            html_to_parse = full_entry_html if full_entry_html is not None else str(
                result_div
            )

            attestations = self.parser.parse_language_sections(html_to_parse, None)
            if not attestations:
                continue

            match = self.parser.find_matching_attestation(
                attestations, language, headword
            )
            if match:
                return match

        return None

    def _lookup_in_corpus_by_ded(
        self, ded_number_str: str, language: str, headword: str
    ) -> Optional[LanguageAttestation]:
        """Try to resolve an entry via the local corpus using DED number."""
        if not self.corpus_entries_by_ded:
            return None

        candidates = self.corpus_entries_by_ded.get(ded_number_str)
        if not candidates:
            return None

        for entry in candidates:
            attestations: list[LanguageAttestation] = entry.get(
                "attestations", []
            )
            match = self.parser.find_matching_attestation(
                attestations, language, headword
            )
            if match:
                return match

        return None

    def _lookup_in_corpus_by_headword(
        self, language: str, headword: str
    ) -> Optional[LanguageAttestation]:
        """Try to resolve an entry via the local corpus using language+headword."""
        if not self.corpus_attestations_by_headword:
            return None

        key = self._normalize_headword_for_index(headword)
        refs = self.corpus_attestations_by_headword.get(key)
        if not refs:
            return None

        # Try exact language match first
        for ref in refs:
            att: LanguageAttestation = ref["attestation"]
            if match_language_variant(att.language_abbrev, language):
                return att
        
        # If no exact match, try more flexible matching (case-insensitive, partial)
        # This helps with variant names and slight mismatches
        lang_lower = language.lower()
        for ref in refs:
            att: LanguageAttestation = ref["attestation"]
            att_lang_lower = att.language_name.lower()
            # Check if language names overlap (e.g. "Maria Gondi" contains "Gondi")
            if lang_lower in att_lang_lower or att_lang_lower in lang_lower:
                return att

        return None
    
    def _lookup_in_corpus_by_meaning(
        self, meaning: str, language: str, headword: str
    ) -> Optional[LanguageAttestation]:
        """
        Search corpus by meaning keywords, then match language+headword.
        This is much faster than web requests when corpus is loaded.
        """
        if not self.corpus_attestations_by_headword:
            return None
        
        # Normalize meaning for keyword matching
        meaning_words = set(meaning.lower().split())
        if not meaning_words:
            return None
        
        # Search through corpus attestations for matching glosses
        # This is a simple keyword match - could be improved with better NLP
        for headword_key, refs in self.corpus_attestations_by_headword.items():
            for ref in refs:
                att: LanguageAttestation = ref["attestation"]
                gloss_lower = att.gloss.lower()
                
                # Check if any meaning words appear in the gloss
                if any(word in gloss_lower for word in meaning_words if len(word) > 3):
                    # Now check language and headword match
                    if match_language_variant(att.language_abbrev, language):
                        # Check headword match
                        hw_normalized = self._normalize_headword_for_index(headword)
                        for burrow_hw in att.headwords:
                            burrow_hw_normalized = self._normalize_headword_for_index(burrow_hw)
                            if hw_normalized == burrow_hw_normalized:
                                return att
        
        return None

    def validate_entry(self, row: pd.Series) -> Dict[str, Any]:
        """Validate single entry with detailed parsing"""
        starling_id = str(row.get("ID", "unknown"))
        headword = str(row.get("Headword", "") or "")
        meaning = str(row.get("Meaning", "") or "")
        language = str(row.get("Language", "") or "")
        ded_number_raw = row.get("Number in DED", None)
        
        validation = {
            "starling_id": starling_id,
            "starling_headword": headword,
            "starling_meaning": meaning,
            "starling_language": language,
            "starling_ded_number": ded_number_raw,
            "burrow_found": False,
            "burrow_language_matched": False,
            "burrow_headword_matched": False,
            "burrow_attestation": None,
            "match_status": "not_validated",
            "notes": [],
        }
        
        has_ded = self._has_scalar_value(ded_number_raw)
        ded_number_str = str(ded_number_raw) if has_ded else ""

        # 1) Try DED-based resolution, preferring the local corpus when available.
        if has_ded:
            # 1a. Local corpus via DED number.
            corpus_match = self._lookup_in_corpus_by_ded(
                ded_number_str, language, headword
            )
            if corpus_match:
                validation["burrow_found"] = True
                validation["burrow_language_matched"] = True
                validation["burrow_headword_matched"] = True
                validation["burrow_attestation"] = {
                    "language": corpus_match.language_name,
                    "language_abbrev": corpus_match.language_abbrev,
                    "headwords": corpus_match.headwords,
                    "gloss": corpus_match.gloss,
                }
                validation["match_status"] = "full_match"
                validation["notes"].append("Matched via local Burrow corpus (DED).")
                return validation

            # 1b. Live DSAL DED query as fallback (ONLY if corpus not loaded).
            if not self.corpus_loaded:
                try:
                    entry_html = self.query_ded_entry(ded_number_str)

                    if not entry_html:
                        validation["match_status"] = "ded_not_found"
                        validation["notes"].append(
                            f"DED #{ded_number_str} not found in Burrow"
                        )
                    else:
                        attestations = self.parser.parse_language_sections(
                            entry_html, ded_number_str
                        )

                        if not attestations:
                            validation["match_status"] = "parse_failed"
                            validation["notes"].append("Could not parse Burrow entry")
                        else:
                            matching_attestation = self.parser.find_matching_attestation(
                                attestations, language, headword
                            )

                            if matching_attestation:
                                validation["burrow_found"] = True
                                validation["burrow_language_matched"] = True
                                validation["burrow_headword_matched"] = True
                                validation["burrow_attestation"] = {
                                    "language": matching_attestation.language_name,
                                    "language_abbrev": matching_attestation.language_abbrev,
                                    "headwords": matching_attestation.headwords,
                                    "gloss": matching_attestation.gloss,
                                }
                                validation["match_status"] = "full_match"
                                return validation
                            validation["burrow_found"] = True
                            validation["match_status"] = "no_attestation_match"
                            validation["notes"].append(
                                f"DED #{ded_number_str} found but {language} form "
                                f"'{headword}' not in entry"
                            )

                            lang_matches = [
                                a
                                for a in attestations
                                if match_language_variant(a.language_abbrev, language)
                            ]
                            if lang_matches:
                                validation["notes"].append(
                                    f"Language {language} present with forms: "
                                    f"{[a.headwords for a in lang_matches]}"
                                )

                except Exception as exc:  # pragma: no cover - defensive
                    validation["match_status"] = "error"
                    validation["notes"].append(f"Error during DED-based lookup: {str(exc)}")
            else:
                # Corpus loaded but no match found - provide diagnostic info
                validation["match_status"] = "no_attestation_match"
                validation["notes"].append(
                    f"DED #{ded_number_str} not found in corpus or {language} form '{headword}' not matched"
                )
                # Check if DED exists in corpus at all and provide diagnostics
                if ded_number_str in self.corpus_entries_by_ded:
                    entry = self.corpus_entries_by_ded[ded_number_str][0]
                    attestations: list[LanguageAttestation] = entry.get("attestations", [])
                    all_langs = {att.language_abbrev for att in attestations}
                    validation["notes"].append(
                        f"DED #{ded_number_str} exists in corpus with languages: {sorted(all_langs)}"
                    )
                    # Check if language exists but headword doesn't match
                    lang_attestations = [
                        att for att in attestations
                        if match_language_variant(att.language_abbrev, language)
                    ]
                    if lang_attestations:
                        all_headwords = []
                        for att in lang_attestations:
                            all_headwords.extend(att.headwords)
                        validation["notes"].append(
                            f"Language {language} found in DED #{ded_number_str} with headwords: {all_headwords}"
                        )
                        validation["burrow_language_matched"] = True
                        validation["burrow_found"] = True
                else:
                    validation["notes"].append(
                        f"DED #{ded_number_str} not found in corpus"
                    )
        else:
            validation["match_status"] = "no_ded_number"
            validation["notes"].append("No DED number in StarlingDB")

        # 2) Try corpus-based headword lookup (language + headword), independent of DED.
        corpus_hw_match = self._lookup_in_corpus_by_headword(language, headword)
        if corpus_hw_match:
            validation["burrow_found"] = True
            validation["burrow_language_matched"] = True
            validation["burrow_headword_matched"] = True
            validation["burrow_attestation"] = {
                "language": corpus_hw_match.language_name,
                "language_abbrev": corpus_hw_match.language_abbrev,
                "headwords": corpus_hw_match.headwords,
                "gloss": corpus_hw_match.gloss,
            }
            previous_status = validation["match_status"]
            if previous_status not in ("not_validated", "corpus_headword_match"):
                validation["notes"].append(
                    f"Matched via local Burrow corpus (headword) "
                    f"(previous status: {previous_status})"
                )
            validation["match_status"] = "corpus_headword_match"
            return validation

        # 3) Fallback: search by meaning (corpus first, then web if corpus not loaded).
        meaning_match: Optional[LanguageAttestation] = None
        if meaning:
            if self.corpus_loaded:
                # Use corpus-based meaning search (much faster)
                try:
                    meaning_match = self._lookup_in_corpus_by_meaning(meaning, language, headword)
                except Exception as exc:  # pragma: no cover - defensive
                    validation["notes"].append(
                        f"Corpus meaning search error: {str(exc)}"
                    )
            else:
                # Fall back to web-based meaning search only if corpus not loaded
                try:
                    meaning_match = self._validate_via_meaning(meaning, language, headword)
                except Exception as exc:  # pragma: no cover - defensive
                    validation["notes"].append(
                        f"Meaning-based search error: {str(exc)}"
                    )

        if meaning_match:
            validation["burrow_found"] = True
            validation["burrow_language_matched"] = True
            validation["burrow_headword_matched"] = True
            validation["burrow_attestation"] = {
                "language": meaning_match.language_name,
                "language_abbrev": meaning_match.language_abbrev,
                "headwords": meaning_match.headwords,
                "gloss": meaning_match.gloss,
            }
            previous_status = validation["match_status"]
            if previous_status not in ("not_validated", "meaning_match"):
                validation["notes"].append(
                    f"Matched via meaning search (previous status: {previous_status})"
                )
            validation["match_status"] = "meaning_match"
            return validation

        if validation["match_status"] == "no_ded_number" and meaning:
            validation["notes"].append("Meaning-based search found no matching entry")

        return validation
    
    def save_checkpoint(self):
        """Save current progress to checkpoint file"""
        checkpoint_data = {
            "processed_count": len(self.validation_results),
            "results": self.validation_results,
        }
        with open(self.checkpoint_file, "w", encoding="utf-8-sig") as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
    
    def load_checkpoint(self) -> int:
        """Load checkpoint and return number of entries already processed"""
        if not self.checkpoint_file.exists():
            return 0
        
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8-sig") as f:
                checkpoint_data = json.load(f)
            self.validation_results = checkpoint_data.get("results", [])
            processed = len(self.validation_results)
            print(f"✓ Loaded checkpoint: {processed} entries already processed")
            return processed
        except Exception as exc:
            print(f"⚠ Could not load checkpoint: {exc}. Starting fresh.")
            return 0
    
    def validate_batch(self, start_idx: int, end_idx: int, total: int):
        """Validate batch with progress"""
        df = self._require_data_loaded()
        batch = df.iloc[start_idx:end_idx]
        
        batch_start_time = time.time()
        
        for batch_pos, (idx, row) in enumerate(batch.iterrows()):
            validation = self.validate_entry(row)
            self.validation_results.append(validation)
            
            # Show progress every 10 entries or at batch boundaries
            current = start_idx + batch_pos + 1
            if (current % 10 == 0) or (batch_pos == len(batch) - 1):
                elapsed = time.time() - batch_start_time
                rate = (batch_pos + 1) / elapsed if elapsed > 0 else 0
                eta_seconds = (total - current) / rate if rate > 0 else 0
                eta_minutes = eta_seconds / 60
                
                status_symbol = {
                    "full_match": "✓",
                    "no_attestation_match": "✗",
                    "ded_not_found": "?",
                    "no_ded_number": "-",
                    "error": "!",
                    "corpus_headword_match": "○",
                    "meaning_match": "○",
                }.get(validation["match_status"], "·")
                
                print(
                    f"  [{current}/{total} ({current/total*100:.1f}%)] "
                    f"{status_symbol} {validation['starling_id']}: "
                    f"{validation['match_status']} | "
                    f"Rate: {rate:.1f}/s | ETA: {eta_minutes:.1f}m"
                )
        
        # Save checkpoint after each batch
        self.save_checkpoint()
    
    def validate_all(self, batch_size: int = 20, resume: bool = True):
        """Validate all entries with checkpoint/resume support"""
        df = self._require_data_loaded()
        total = len(df)
        
        start_idx = 0
        if resume:
            start_idx = self.load_checkpoint()
            if start_idx > 0:
                print(f"Resuming from entry {start_idx + 1}...")
        
        print(f"\n{'='*70}")
        print(f"ENHANCED VALIDATION: {total} entries")
        if self.corpus_loaded:
            print("✓ Using local corpus - NO web requests")
        else:
            print("⚠ WARNING: No corpus loaded - will make web requests (SLOW!)")
        print(f"{'='*70}\n")
        
        overall_start_time = time.time()
        
        for start in range(start_idx, total, batch_size):
            end = min(start + batch_size, total)
            self.validate_batch(start, end, total)
        
        total_time = time.time() - overall_start_time
        print(f"\n{'='*70}")
        print(f"Validation complete! Processed {total} entries in {total_time/60:.1f} minutes")
        print(f"Average rate: {total/total_time:.1f} entries/second")
        print(f"{'='*70}\n")
        
        # Clean up checkpoint file on successful completion
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
            print("✓ Checkpoint file removed (validation complete)")
        
        self.save_results()
    
    def _build_user_friendly_df(self) -> pd.DataFrame:
        """Build a DataFrame with clear column names and Burrow language as abbrev (e.g. Ka., Ta.)."""
        rows = []
        status_display = {
            "full_match": "Full match",
            "no_attestation_match": "No attestation match",
            "ded_not_found": "DED not found",
            "no_ded_number": "No DED number",
            "parse_failed": "Parse failed",
            "error": "Error",
            "corpus_headword_match": "Match (corpus, headword)",
            "meaning_match": "Match (meaning search)",
            "not_validated": "Not validated",
        }
        for r in self.validation_results:
            att = r.get("burrow_attestation")
            if att:
                lang_abbrev = get_burrow_abbrev_for_display(
                    att.get("language", ""), att.get("language_abbrev")
                )
                burrow_headwords = ", ".join(att.get("headwords") or [])
                burrow_gloss = att.get("gloss") or ""
            else:
                lang_abbrev = ""
                burrow_headwords = ""
                burrow_gloss = ""
            notes = r.get("notes") or []
            notes_str = " | ".join(str(n) for n in notes) if notes else ""
            ded = r.get("starling_ded_number")
            ded_display = "" if (ded is None or pd.isna(ded)) else ded
            rows.append({
                "ID (Starling)": r.get("starling_id", ""),
                "Headword": r.get("starling_headword", ""),
                "Meaning": r.get("starling_meaning", ""),
                "Language (Starling)": r.get("starling_language", ""),
                "Number in DED": ded_display,
                "Match status": status_display.get(
                    r.get("match_status", ""), r.get("match_status", "")
                ),
                "Burrow language": lang_abbrev,
                "Burrow headwords": burrow_headwords,
                "Burrow gloss": burrow_gloss,
                "Notes": notes_str,
            })
        return pd.DataFrame(rows)

    def save_results(self):
        """Save validation results (CSV, JSON, and user-friendly Excel)."""
        results_df = pd.DataFrame(self.validation_results)

        csv_file = self.output_dir / "enhanced_validation_results.csv"
        results_df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        print(f"\nResults saved: {csv_file}")

        json_file = self.output_dir / "enhanced_validation_results.json"
        with open(json_file, "w", encoding="utf-8-sig") as f:
            json.dump(self.validation_results, f, ensure_ascii=False, indent=2)
        print(f"JSON saved: {json_file}")

        # User-friendly Excel: Burrow language as original abbrev (Ka., Ta., etc.)
        excel_df = self._build_user_friendly_df()
        xlsx_file = self.output_dir / "enhanced_validation_results.xlsx"
        excel_df.to_excel(xlsx_file, index=False, engine="openpyxl")
        print(f"Excel (user-friendly): {xlsx_file}")

        self.generate_summary()
    
    def generate_summary(self):
        """Generate summary statistics"""
        df = pd.DataFrame(self.validation_results)
        
        total = len(df)
        full_match = (df['match_status'] == 'full_match').sum()
        no_ded = (df['match_status'] == 'no_ded_number').sum()
        
        summary = {
            "total_entries": int(total),
            "full_matches": int(full_match),
            "no_ded_number": int(no_ded),
            "validated": int(total - no_ded),
            "match_rate": round(
                float(full_match / (total - no_ded) * 100), 2
            )
            if (total - no_ded) > 0
            else 0,
            "status_breakdown": {
                k: int(v) for k, v in df["match_status"].value_counts().items()
            },
        }
        
        summary_file = self.output_dir / "enhanced_validation_summary.json"
        with open(summary_file, "w", encoding="utf-8-sig") as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n{'='*70}")
        print("VALIDATION SUMMARY")
        print(f"{'='*70}")
        print(f"Total entries: {total}")
        print(f"Full matches: {full_match} ({full_match/total*100:.1f}%)")
        print(f"No DED number: {no_ded}")
        print(f"\nMatch Status Breakdown:")
        for status, count in df['match_status'].value_counts().items():
            print(f"  {status}: {count}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Enhanced Burrow validation with detailed parsing"
    )
    parser.add_argument("csv_file", help="StarlingDB CSV/Excel export")
    parser.add_argument("--output-dir", default="enhanced_validation")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument(
        "--burrow-corpus",
        help="Optional path to burrow_corpus.json for offline validation",
    )
    parser.add_argument(
        "--test", action="store_true", help="Test with first 10 entries"
    )
    parser.add_argument(
        "--no-resume", action="store_true", help="Don't resume from checkpoint (start fresh)"
    )
    parser.add_argument(
        "--auto-corpus", action="store_true",
        help="Automatically load corpus from validation_output/burrow_cache/burrow_corpus.json if it exists"
    )
    
    args = parser.parse_args()
    
    workflow = EnhancedValidationWorkflow(args.csv_file, args.output_dir)

    # Auto-load corpus if requested and available
    if args.auto_corpus:
        default_corpus = Path("validation_output/burrow_cache/burrow_corpus.json")
        if default_corpus.exists():
            print(f"Auto-loading corpus from: {default_corpus}")
            workflow.load_burrow_corpus(str(default_corpus))
        else:
            print(f"⚠ Auto-corpus requested but not found at: {default_corpus}")
            print("  Run with --burrow-corpus to specify a different path")
    elif args.burrow_corpus:
        workflow.load_burrow_corpus(args.burrow_corpus)

    workflow.load_data()
    
    if args.test:
        print("\n*** TEST MODE: First 10 entries only ***")
        workflow.df = workflow._require_data_loaded().head(10)
    
    workflow.validate_all(batch_size=args.batch_size, resume=not args.no_resume)


if __name__ == "__main__":
    main()