# Burrow & Emeneau Language Mappings

**Purpose:** Map Burrow DED dictionary language abbreviations (as in the printed dictionary) to StarlingDB language names, and define variant groups so validation can match attestations correctly.

**Source:** Based on frontmatter §52 from the printed Burrow & Emeneau *Dravidian Etymological Dictionary*.

---

## 1. Burrow abbreviation → StarlingDB language

| Burrow (DED) | StarlingDB language |
|--------------|---------------------|
| ĀlKu. | Ālu Kuṟumba |
| Bel. | Belari |
| Br. | Brahui |
| Dr. | Proto-Dravidian |
| PDr. | Proto-Dravidian |
| Ga. | Gadba |
| Go. | Gondi |
| Ir. | Iruḷa |
| Ka. | Kannada |
| Ko. | Kota |
| Koḍ. | Kodagu |
| Kol. | Kolami |
| Kor. | Koraga |
| Kur. | Kurukh |
| Kurub. | Beṭṭa Kuruba |
| Ma. | Malayalam |
| Malt. | Malto |
| Manḍ. | Manda |
| Nk. | Naikri |
| Nk. (Ch.) | Naiki |
| Pa. | Parji |
| PālKu. | Pālu Kuṟumba |
| Pe. | Pengo |
| Ta. | Tamil |
| Te. | Telugu |
| To. | Toda |
| Tu. | Tulu |
| Konḍa | Konda |
| Kui | Kui |
| Kuwi | Kuwi (Schulze) |

*Total: 31 Burrow abbreviations.*

---

## 2. StarlingDB variant groups

These StarlingDB language names are treated as variants of a single Burrow language for matching.

### Gondi (Go.)
- Koya Gondi, Muria Gondi, Maria Gondi, Betul Gondi  
- Adilabad Gondi, Mandla Gondi (Phailbus), Maria Gondi (Mitchell)  
- Mandla Gondi (Williamson), Seoni Gondi, Gommu Gondi  
- Yeotmal Gondi, Chindwara Gondi, Durg Gondi, Chanda Gondi  
- Mandla Gondi, Maria Gondi (Lind), Maria Gondi (Smith)

### Gadba (Ga.)
- Salur Gadba, Ollari Gadba, Kondekor Gadba, Poya Gadba

### Kuwi (Kuwi)
- Kuwi (Schulze), Kuwi (Fitzgerald), Kuwi (Israel)  
- Sunkarametta Kuwi, Kuwi (Mahanti), Tekriya Kuwi, Dongriya Kuwi, Parja Kuwi

### Kolami (Kol.)
- Kinwat Kolami, Kolami (Setumadhava Rao)

### Konda (Konḍa)
- Konda (Burrow/Bhattacharya)

### Kui (Kui)
- Khuttia Kui

### Telugu (Te.)
- Telugu (Krishnamurti), Inscriptional Telugu, Merolu Telugu, Proto-Telugu

---

## 3. Matching logic

- **Exact:** Burrow normalized name equals StarlingDB name (e.g. Ta. → Tamil = “Tamil”).
- **Variant:** StarlingDB name is in the variant list for that Burrow base (e.g. Go. matches “Maria Gondi”).
- **Flexible (optional):** If not strict, we also treat as match when one name contains the other and the shorter name has at least 4 characters (e.g. “Gondi” in “Maria Gondi”). This avoids very short false positives.

**Intended behavior:**
- `Go.` ↔ `Maria Gondi` → match  
- `Kuwi` ↔ `Kuwi (Schulze)` → match  
- `Ta.` ↔ `Tamil` → match  
- `Te.` ↔ `Telugu (Krishnamurti)` → match  
- `Ka.` ↔ `Telugu` → no match  

---

## 4. Reverse mapping (for reports)

For display in validation output we show Burrow-style abbreviations (e.g. Ka., Ta.). The reverse map is:

**Full name → Burrow abbreviation** (first occurrence only when multiple Burrow codes map to same name):

| StarlingDB language | Burrow (display) |
|---------------------|------------------|
| Proto-Dravidian | Dr. |
| Ālu Kuṟumba | ĀlKu. |
| Belari | Bel. |
| Brahui | Br. |
| Gadba | Ga. |
| Gondi | Go. |
| Iruḷa | Ir. |
| Kannada | Ka. |
| Kota | Ko. |
| Kodagu | Koḍ. |
| Kolami | Kol. |
| Koraga | Kor. |
| Kurukh | Kur. |
| Beṭṭa Kuruba | Kurub. |
| Malayalam | Ma. |
| Malto | Malt. |
| Manda | Manḍ. |
| Naikri | Nk. |
| Naiki | Nk. (Ch.) |
| Parji | Pa. |
| Pālu Kuṟumba | PālKu. |
| Pengo | Pe. |
| Tamil | Ta. |
| Telugu | Te. |
| Toda | To. |
| Tulu | Tu. |
| Konda | Konḍa |
| Kui | Kui |
| Kuwi (Schulze) | Kuwi |

Variant names (e.g. Maria Gondi) use the base language’s abbreviation (e.g. Go.) for display.

---

## 5. Test cases for validation

| Burrow | StarlingDB | Expected match? |
|--------|------------|-----------------|
| Go. | Maria Gondi | ✓ Yes |
| Kuwi | Kuwi (Schulze) | ✓ Yes |
| Ta. | Tamil | ✓ Yes |
| Te. | Telugu (Krishnamurti) | ✓ Yes |
| Ka. | Telugu | ✗ No |


