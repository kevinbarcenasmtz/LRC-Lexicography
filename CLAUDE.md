# LRC-Lexicography

## Python environment

Always use the project virtual environment's interpreter for Python commands in this
repo, not a bare `python`/`py` from PATH:

```
lrc_env\Scripts\python.exe script.py ...   # PowerShell / cmd
./lrc_env/Scripts/python.exe script.py ... # Git Bash
```

`python`/`python3` are not guaranteed to exist on PATH (Git Bash in particular has
none), and a system interpreter would lack this repo's installed dependencies
(pandas, BeautifulSoup4, etc. from `src/requirements.txt`) anyway.

## Console encoding

Windows' console (cp1252) cannot print the diacritics used throughout this
repo's Dravidian-linguistics data (ḍ, ḷ, ṅ, ṭ, etc.). Scripts that print such
text should reconfigure `sys.stdout` to UTF-8 internally (see the pattern in
`src/dravidian/scripts/cross-validating-dded-starling/*.py`); for ad-hoc
`python -c` snippets, set `PYTHONIOENCODING=utf-8` or wrap stdout yourself.
