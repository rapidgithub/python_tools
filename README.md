# Python Tools

Standalone Python utilities for diagnosing Hive-on-Tez jobs and reading
HiveServer2 Web UI query information.

## Tools

### Tez log analysis

`tez-log-analysis/tez-log-analysis.py` analyzes an aggregated YARN log from a
Hive-on-Tez application. It:

- Splits the aggregated log into container and log-type files.
- Finds DAG task attempts and reports their wait and execution times.
- Identifies failed tasks and links them to their container logs.
- Reports the slowest successful task and the task with the longest wait.
- Supports analyzing an existing split-log directory without splitting again.

See [`tez-log-analysis/README.md`](tez-log-analysis/README.md) for the detailed
workflow and sample output.

### Hive Web UI reader

`hive-webui-reader/html_hive.py` connects to the HiveServer2 Web UI, reads the
running and completed query tables, follows query links, and prints extracted
query details.

Create `hive-webui-reader/config.props`:

```ini
[default]
hs2_host=hiveserver.example.com
hs2_webport=10002
log_level=INFO
```

Run it from the repository root or from the tool directory:

```bash
python hive-webui-reader/html_hive.py
```

The reader retries failed HTTP requests and uses a five-second request timeout.
It expects the Web UI page to contain at least three tables: the page header,
running queries, and completed queries.

## Requirements

- Python 3.11, 3.12, or 3.13
- `requests`
- `beautifulsoup4`
- `lxml` (required by BeautifulSoup's HTML parser)

Install the dependencies with:

```bash
python -m pip install --upgrade pip
python -m pip install pytest pylint requests beautifulsoup4 lxml
```

If using the repository's conda environment:

```bash
conda run -n pdf_to_csv_web python -m pytest -q
```

## Tez log analysis usage

Analyze an aggregated YARN log:

```bash
python tez-log-analysis/tez-log-analysis.py \
  --log /path/to/application_1631466459248_0034.log
```

When the log contains multiple DAGs, select one by its positive numeric ID:

```bash
python tez-log-analysis/tez-log-analysis.py \
  --log /path/to/application.log \
  --dagid 1
```

The default file mode creates `app_log_dir/` in the current working directory.
It refuses to overwrite an existing directory. Analyze an existing split-log
directory instead:

```bash
python tez-log-analysis/tez-log-analysis.py \
  --mode dir \
  --appdir /path/to/app_log_dir \
  --dagid 1
```

Use `--help` to see all command-line options:

```bash
python tez-log-analysis/tez-log-analysis.py --help
```

The analyzer writes diagnostic output to the console and
`tez-log-analysis.out`. Generated output directories and runtime files should
not be committed.

## Development and verification

Run the complete test suite:

```bash
pytest -q
```

Run pylint with the same threshold used by CI:

```bash
pylint $(git ls-files '*.py') --fail-under=7.5
```

The GitHub Actions workflow runs both commands on Python 3.11, 3.12, and 3.13.
Pull requests are automatically approved after the workflow completes
successfully, provided the PR has not already been approved.

## Repository layout

```text
.
├── hive-webui-reader/
│   └── html_hive.py
├── tez-log-analysis/
│   ├── README.md
│   ├── test_tez_log_analysis.py
│   └── tez-log-analysis.py
├── .github/workflows/
│   ├── pylint.yml
│   └── prApprover.yml
└── AGENTS.md
```

## Troubleshooting

- **`app_log_dir` already exists:** move or rename it, or use `--mode dir`
  with `--appdir`.
- **No DAG log found:** confirm that the supplied log is an aggregated Tez
  application log and that it contains DAG log entries.
- **Hive Web UI connection failure:** verify `hs2_host`, `hs2_webport`, network
  access, and that the Web UI is enabled.
- **BeautifulSoup parser error:** install `lxml` in the active Python
  environment.
