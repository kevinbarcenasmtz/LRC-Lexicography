"""
Enhanced Cross-Validation Workflow with Detailed Entry Parsing.
Validates StarlingDB data against detailed Burrow DED entries.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, TypeGuard
import unicodedata

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from burrow_entry_parser import BurrowEntryParser, LanguageAttestation
from burrow_language_mappings import match_language_variant


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

        print(
            f"Loaded Burrow corpus: {len(entries)} entries, "
            f"{len(self.corpus_attestations_by_headword)} unique headword keys."
        )
    
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

        for ref in refs:
            att: LanguageAttestation = ref["attestation"]
            if match_language_variant(att.language_abbrev, language):
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
                    "headwords": corpus_match.headwords,
                    "gloss": corpus_match.gloss,
                }
                validation["match_status"] = "full_match"
                validation["notes"].append("Matched via local Burrow corpus (DED).")
                return validation

            # 1b. Live DSAL DED query as fallback.
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

        # 3) Fallback: search by meaning text via live DSAL, then match language+headword.
        meaning_match: Optional[LanguageAttestation] = None
        if meaning:
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
    
    def validate_batch(self, start_idx: int, end_idx: int):
        """Validate batch with progress"""
        df = self._require_data_loaded()
        batch = df.iloc[start_idx:end_idx]
        
        print(f"\nValidating {start_idx+1} to {end_idx}...")
        
        for idx, row in batch.iterrows():
            validation = self.validate_entry(row)
            self.validation_results.append(validation)
            
            status_symbol = {
                "full_match": "✓",
                "no_attestation_match": "✗",
                "ded_not_found": "?",
                "no_ded_number": "-",
                "error": "!",
            }.get(validation["match_status"], "·")
            
            print(
                f"  {status_symbol} {validation['starling_id']}: "
                f"{validation['match_status']}"
            )
    
    def validate_all(self, batch_size: int = 20):
        """Validate all entries"""
        df = self._require_data_loaded()
        total = len(df)
        
        print(f"\n{'='*70}")
        print(f"ENHANCED VALIDATION: {total} entries")
        print(f"{'='*70}")
        
        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            self.validate_batch(start, end)
        
        self.save_results()
    
    def save_results(self):
        """Save validation results"""
        results_df = pd.DataFrame(self.validation_results)
        
        csv_file = self.output_dir / "enhanced_validation_results.csv"
        results_df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        print(f"\nResults saved: {csv_file}")
        
        json_file = self.output_dir / "enhanced_validation_results.json"
        with open(json_file, "w", encoding="utf-8-sig") as f:
            json.dump(self.validation_results, f, ensure_ascii=False, indent=2)
        print(f"JSON saved: {json_file}")
        
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
    
    args = parser.parse_args()
    
    workflow = EnhancedValidationWorkflow(args.csv_file, args.output_dir)

    if args.burrow_corpus:
        workflow.load_burrow_corpus(args.burrow_corpus)

    workflow.load_data()
    
    if args.test:
        print("\n*** TEST MODE: First 10 entries only ***")
        workflow.df = workflow._require_data_loaded().head(10)
    
    workflow.validate_all(batch_size=args.batch_size)


if __name__ == "__main__":
    main()