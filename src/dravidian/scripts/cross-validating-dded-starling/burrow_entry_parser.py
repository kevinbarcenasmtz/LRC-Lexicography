"""
Enhanced Burrow entry parser.
Extracts detailed language attestations from full DED entries.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import re

from bs4 import BeautifulSoup
from bs4.element import Tag


@dataclass
class LanguageAttestation:
    """Single language form within a Burrow entry"""

    language_abbrev: str
    language_name: str
    headwords: List[str]
    gloss: str
    source_text: str


class BurrowEntryParser:
    """
    Parse detailed Burrow entries from page URLs.
    Extracts individual language attestations with headwords and glosses.
    """

    def __init__(self):
        from burrow_language_mappings import normalize_language

        self.normalize_language = normalize_language

    def clean_ded_number(self, ded_str: str) -> str:
        """
        Clean DED number: strip leading/trailing zeros.
        Examples: '045' → '45', '0047' → '47', '100' → '100'
        """
        try:
            return str(int(float(ded_str)))
        except (ValueError, TypeError):
            return str(ded_str).strip()

    def extract_page_url(
        self, search_result_html: str, ded_number: str
    ) -> Optional[str]:
        """
        Extract page URL from search results.
        Looks for the primary entry's page link.
        """
        soup = BeautifulSoup(search_result_html, "html.parser")

        raw_results = soup.find_all("div", class_="hw_result")
        results: List[Tag] = [r for r in raw_results if isinstance(r, Tag)]

        for result_div in results:
            blockquote = result_div.find("blockquote")
            if not isinstance(blockquote, Tag):
                continue

            full_text = blockquote.get_text(strip=True)

            if not full_text.startswith(str(ded_number)):
                continue

            if len(full_text) <= len(str(ded_number)) + 5:
                continue

            page_link: Optional[Tag] = None
            for link in result_div.find_all("a"):
                if not isinstance(link, Tag):
                    continue
                href_val = link.get("href")
                if isinstance(href_val, str) and "page=" in href_val:
                    page_link = link
                    break

            if page_link is not None:
                href_val = page_link.get("href")
                return href_val if isinstance(href_val, str) else None

        return None

    def parse_language_sections(
        self, html_content: str, ded_number: Optional[str] = None
    ) -> List[LanguageAttestation]:
        """
        Parse full entry HTML to extract language attestations.

        Two structures:
        1. Direct query: <div class="hw_result"><blockquote>...</blockquote></div>
        2. Page query: <div class="hw_result"><div><number>40</number>...</div><div><number>41</number>...</div></div>

        If ded_number provided, will find the specific entry within page results.
        """
        soup = BeautifulSoup(html_content, "html.parser")

        result_div_raw = soup.find("div", class_="hw_result")
        if not isinstance(result_div_raw, Tag):
            return []
        result_div: Tag = result_div_raw

        blockquote = result_div.find("blockquote")
        if isinstance(blockquote, Tag):
            entry_html = str(blockquote)
        else:
            raw_nested_divs = result_div.find_all("div", recursive=False)
            nested_divs: List[Tag] = [
                d for d in raw_nested_divs if isinstance(d, Tag)
            ]

            if not nested_divs:
                return []

            if ded_number:
                clean_ded = self.clean_ded_number(ded_number)
                for div in nested_divs:
                    number_tag = div.find("number")
                    if isinstance(number_tag, Tag) and number_tag.get_text(
                        strip=True
                    ) == clean_ded:
                        entry_html = str(div)
                        break
                else:
                    return []
            else:
                entry_html = str(nested_divs[0])

        attestations = []

        invalid_langs = {"Voc", "CDIAL", "DED", "DEDS", "Turner", "Cf", "Skt"}

        # Parse all <b><i>Lang.</i> word</b> patterns
        all_pattern = r"<b><i>([A-Z][a-zḍṇṭḷṟṅāīūṃṁṛêôĀĪŪṚḤŚṢṬḌṆṆŅṂṀḶḸṞŊñóúáíḗṓāḏṭěṣç]+\.?)</i>\s+([^<]+)</b>"

        all_matches = re.finditer(all_pattern, entry_html)

        for match in all_matches:
            lang_abbrev = match.group(1)
            headword_text = match.group(2)

            lang_clean = lang_abbrev.rstrip(".")
            if lang_clean in invalid_langs:
                continue

            headwords = [hw.strip() for hw in headword_text.split(",")]
            headwords = [
                hw for hw in headwords if hw and len(hw) > 1 and not hw.startswith("(")
            ]

            if not headwords:
                continue

            # Extract gloss for this language
            start_pos = match.end()
            # Find next language marker or end
            next_match = re.search(r"<b><i>[A-Z]", entry_html[start_pos:])
            if next_match:
                end_pos = start_pos + next_match.start()
            else:
                # Look for end markers like "DED", "CDIAL", "/"
                end_markers = [" / ", " DED", " DEDS", " <i>CDIAL"]
                end_pos = len(entry_html)
                for marker in end_markers:
                    marker_pos = entry_html.find(marker, start_pos)
                    if marker_pos > 0 and marker_pos < end_pos:
                        end_pos = marker_pos

            gloss_html = entry_html[start_pos:end_pos]
            gloss_soup = BeautifulSoup(gloss_html, "html.parser")
            gloss_text = gloss_soup.get_text(strip=True)

            lang_name = self.normalize_language(lang_abbrev)

            attestations.append(
                LanguageAttestation(
                    language_abbrev=lang_abbrev,
                    language_name=lang_name,
                    headwords=headwords,
                    gloss=gloss_text[:200],
                    source_text=f"{lang_abbrev} {', '.join(headwords)}",
                )
            )

        return attestations

    def find_matching_attestation(
        self,
        attestations: List[LanguageAttestation],
        starling_language: str,
        starling_headword: str,
    ) -> Optional[LanguageAttestation]:
        """
        Find matching attestation for a StarlingDB entry.
        Matches on language and headword (case-insensitive, flexible matching).
        """
        from burrow_language_mappings import match_language_variant

        starling_headword_clean = (
            starling_headword.replace("*", "").replace("-", "").strip().lower()
        )

        for attestation in attestations:
            if not match_language_variant(
                attestation.language_abbrev, starling_language
            ):
                continue

            for burrow_headword in attestation.headwords:
                burrow_clean = burrow_headword.strip().lower()

                if burrow_clean == starling_headword_clean:
                    return attestation

                if (
                    burrow_clean in starling_headword_clean
                    or starling_headword_clean in burrow_clean
                ):
                    return attestation

        return None


if __name__ == "__main__":
    parser = BurrowEntryParser()

    sample_html = """
    <div class='hw_result'>
    <blockquote>
    <p><number>45</number> <b><i>Ta.</i> toṉṉai</b> cup made of plantain or other leaf. 
    <i>Ma.</i> <b>donna</b> cup made out of a leaf, for brahmans to drink pepper-water, etc. 
    <i>Ka.</i> <b>donne, jonne</b> leaf-cup. 
    <i>Tu.</i> <b>donnè</b> cup made of plantain leaves, etc. 
    <i>Te.</i> <b>donne</b> cup made of leaves. 
    <i>Ga.</i> (S.²) <b>dona</b> leaf-cup. 
    <i>Go.</i> (A.) <b>ḍona</b> id. (<i>Voc.</i> 1613). 
    <i>Konḍa</i> <b>done</b> id. 
    <i>Manḍ.</i> <b>duna</b> id. 
    <i>Kui</i> <b>ḍono</b>, (P.) <b>ḍoho</b> id.; <b>ḍoo</b> balance word in <b>kali ḍoo</b> leaf-cup. 
    <i>Kuwi</i> (F) <b>dunnō</b> (Su.) <b>dono</b> id.; (Isr.) <b>ṭono</b> cup-like container made of leaves. 
    / Turner, <i>CDIAL</i>, no. 6641, <b>dróṇa-</b> (e.g. <b>H. donā,</b> Mar. <b>ḍoṇā</b> leaf-cup). 
    DED(S) 2913.</p>
    </blockquote>
    </div>
    """

    print("=" * 70)
    print("TESTING BURROW ENTRY PARSER")
    print("=" * 70)

    attestations = parser.parse_language_sections(sample_html)

    print(f"\nExtracted {len(attestations)} language attestations:\n")

    for att in attestations:
        print(f"{att.language_abbrev:<10} ({att.language_name})")
        print(f"  Headwords: {', '.join(att.headwords)}")
        print(f"  Gloss: {att.gloss[:60]}...")
        print()

    print("\n" + "=" * 70)
    print("TESTING MATCHING")
    print("=" * 70)

    test_cases = [
        ("Tamil", "toṉṉai"),
        ("Kannada", "donne"),
        ("Kuwi (Schulze)", "dunnō"),
        ("Konda", "done"),
        ("Telugu", "fake-word"),
    ]

    for lang, hw in test_cases:
        match = parser.find_matching_attestation(attestations, lang, hw)
        if match:
            print(f"✓ {lang:<20} '{hw}' → FOUND: {match.headwords}")
        else:
            print(f"✗ {lang:<20} '{hw}' → NOT FOUND")
