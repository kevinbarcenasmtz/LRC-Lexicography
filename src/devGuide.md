
# Development Guide

1. Create a virtual environment: (Python version 3.11.1 required)
py -3.11 --version
Python 3.11.1

```bash
py -3.11 -m venv nahuaLEX_env
```

1. Activate the virtual environment:
   - On Windows: `nahuaLEX_env\Scripts\activate`
   - On macOS/Linux: `source nahuaLEX_env/bin/activate`

2. Install the requirements:

```bash
pip install -r src/requirements.txt
```

This requirements file includes:

- Core data processing libraries (pandas, numpy)
- Multiple Excel processing libraries for different file types and operations
- CSV handling tools
- Data validation and schema libraries
- Character encoding tools (important for your Nahuatl text encoding issues)
- NLP libraries that might be useful for lexical work
- Visualization tools for data exploration
- Development tools for a smoother workflow

You can customize this file by removing libraries you don't need or adding specific version requirements for your project.
