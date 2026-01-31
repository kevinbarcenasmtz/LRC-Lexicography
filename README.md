# LRC Lexicography Projects

A monorepo containing comprehensive lexical database construction projects for indigenous and historical languages, developed through the Linguistic Research Center. The work focuses on systematic web scraping, data validation, and database construction to preserve and make accessible linguistic resources scattered across academic websites and databases.

## Projects

**NahuatLEX** - Classical and contemporary Nahuatl lexical resources, connecting colonial-era sources (Molina, Karttunen, Carochi, Olmos) with modern dialects and Lockhart's historical materials.

**RomLEX** - Romani language databases, integrating resources from University of Graz's RomLex project.

**DravidianLEX** - Dravidian language family etymological resources from the StarLing database.

**IELEX** - Indo-European lexical database work.

## Repository Structure

```txt
LRC-Lexicography/
├── nahuatl/
│   ├── scripts/          # Processing and scraping tools
│   ├── notebooks/        # Analysis and exploration
│   ├── data/             # Lexical databases and exports
│   └── docs/             # Nahuatl-specific documentation
├── romani/
├── dravidian/
├── ielex/
├── shared/               # Common utilities across projects
│   ├── db_utils.py
│   ├── validators.py
│   └── scrapers.py
└── docs/                 # Cross-project methodology
```

## Methodology

All lexicon projects follow consistent workflows:

- Respectful web scraping with appropriate rate limiting
- Data normalization and validation
- HTML processing with markup preservation
- Checkpoint-based version control for processing pipelines
- Comprehensive quality control with manual review workflows
- Complete bibliographic attribution and scholarly citation preservation

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.
