# Development Guide

## Setup

1. Create a virtual environment (Python 3.11.1 required):

```bash
py -3.11 --version  # Verify version
Python 3.11.1

py -3.11 -m venv lrc_env
```

1. Activate the virtual environment:
   - Windows: `lrc_env\Scripts\activate`
   - macOS/Linux: `source lrc_env/bin/activate`

2. Install dependencies:

```bash
pip install -r src/requirements.txt
```

## Working with Individual Lexicons

Each lexicon project (nahuatl, romani, dravidian, ielex) may have additional project-specific requirements. Check for `requirements.txt` files in individual project directories.

To install project-specific dependencies:

```bash
pip install -r nahuatl/requirements.txt
```

## Core Dependencies

The main requirements file (`src/requirements.txt`) includes:

- Data processing: pandas, numpy
- Excel/CSV handling: openpyxl, xlrd, csv
- Database: sqlite3, SQLAlchemy
- Web scraping: requests, BeautifulSoup4
- Data validation: schema libraries, validators
- Encoding tools: chardet (critical for multilingual text processing)
- Development: Jupyter, pytest, black

## Project-Specific Workflows

Navigate to individual lexicon directories for specialized scripts and notebooks:

```bash
cd nahuatl/scripts/     # Run Nahuatl processing tools
cd dravidian/notebooks/ # Explore Dravidian analysis
```
