# LRC Lexicography Projects

A monorepo containing comprehensive lexical database construction projects for indigenous and historical languages, developed through the Linguistic Research Center. The work focuses on systematic web scraping, data validation, and database construction to preserve and make accessible linguistic resources scattered across academic websites and databases.

## Projects

**NahuatLEX** - Classical and contemporary Nahuatl lexical resources, connecting colonial-era sources (Molina, Karttunen, Carochi, Olmos) with modern dialects and Lockhart's historical materials.

**RomLEX** - Indo-Iranian Romani language databases, integrating resources from University of Graz's RomLex project.

**DravidianLEX** - Dravidian language family etymological resources from the StarLing database.

**IELEX** - Indo-European lexical database work.

## Repository Structure

```txt
LRC-Lexicography/
├── config/                   # Project configurations
│   └── whp_config.json
├── data/                     # Lexical databases and exports
│   ├── nahuatl/
│   │   ├── scraped_db_files/
│   │   ├── data_given_to_us_db_files/
│   │   └── raw/
│   ├── dravidian/
│   ├── romani/
│   ├── iranian/
│   └── ielex/
├── src/                      # Source code for all projects
│   ├── nahuatl/
│   │   ├── scripts/          # Processing and import tools
│   │   ├── notebooks/        # Analysis and validation workflows
│   │   ├── whp_scraping/     # Wired Humanities Project scrapers
│   │   └── visualization/
│   ├── dravidian/
│   │   ├── scripts/          # StarlingDB scrapers
│   │   ├── notebooks/        # Data processing
│   │   └── docs/
│   ├── romlex/               # RomLex scraping tools
│   ├── iranian/
│   └── requirements.txt      # Shared Python dependencies
├── docs/                     # Cross-project documentation
│   ├── CHANGELOG.md
│   └── tasks/
└── logs/                     # Processing logs
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
