"""Markup-rendering helpers for the markup-preserving StarlingDB re-scrape.

The original flat scrape called ``get_text(strip=True)`` on each value cell,
which stripped the whitespace around ``<i>`` tags and destroyed the form/gloss
boundaries in Derivates / Additional forms / Dialectal forms.
The patched ``dravidian_scraper.py`` now stores the raw inner HTML of each cell
under an entry's ``_field_html`` map alongside the (unchanged) flattened text.

These helpers turn that captured HTML back into usable field values. They are
shared by ``destructuring_of_scraped_json.ipynb`` and
``build_dravidilex_import.py`` so both render the markup identically.
"""

from __future__ import annotations

import re
from bs4 import BeautifulSoup

_WS = re.compile(r"\s+")


def html_to_spaced_text(html: str) -> str:
    """Flatten captured cell HTML to plain text with the ORIGINAL spacing intact.

    ``get_text()`` (no ``strip=True``) keeps the real spaces that live between the
    ``<i>`` tags and the surrounding roman text, so run-together artifacts like
    ``aṭakku(aṭakki-) to control`` come back correctly spaced as
    ``aṭakku (aṭakki-) to control``. Whitespace runs are then collapsed to a
    single space. Italic (form vs. gloss) distinction is discarded; use
    :func:`html_to_italic_markdown` if you need to keep it.
    """
    if not html:
        return ""
    text = BeautifulSoup(html, "html.parser").get_text()
    return _WS.sub(" ", text).strip()


def html_to_italic_markdown(html: str) -> str:
    """Render captured cell HTML to Markdown, mapping ``<i>``/``<b>`` boundaries.

    Forms (originally italic) become ``*form*`` and bold becomes ``**text**``, so
    the form/gloss distinction survives into a display layer that renders
    Markdown. Spacing is preserved exactly as in :func:`html_to_spaced_text`.
    """
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["i", "em"]):
        tag.insert_before("*")
        tag.insert_after("*")
        tag.unwrap()
    for tag in soup.find_all(["b", "strong"]):
        tag.insert_before("**")
        tag.insert_after("**")
        tag.unwrap()
    text = soup.get_text()
    # Collapse whitespace but don't let it swallow the marker asterisks.
    return _WS.sub(" ", text).strip()


def render_field(entry: dict, field: str, *, markdown: bool = False) -> str:
    """Return the best rendering of ``entry[field]``.

    Prefers the markup-preserving HTML in ``entry['_field_html'][field]`` when the
    re-scrape captured it; otherwise falls back to the flat text value. Set
    ``markdown=True`` to keep italic form/gloss markers.
    """
    html = (entry.get("_field_html") or {}).get(field)
    if html:
        return html_to_italic_markdown(html) if markdown else html_to_spaced_text(html)
    return (entry.get(field) or "").strip()


if __name__ == "__main__":
    # Smoke test against the real captured shapes from the re-scrape.
    samples = [
        "<i>aṭakku</i> (<i>aṭakki-</i>) to control, repress, hide, conceal, bury; "
        "<i>aṭakkam</i> submission",
        "Also Gondi_Tr <i>askānā</i>, Gondi_Mu <i>ask-</i> to cut (meat), carve "
        "(<i>ask-</i> is pl. action of <i>acc-</i>)",
    ]
    for h in samples:
        print("HTML:    ", h)
        print("spaced:  ", html_to_spaced_text(h))
        print("markdown:", html_to_italic_markdown(h))
        print()
