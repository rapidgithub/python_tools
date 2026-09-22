# Agent Guidelines & Repository Workflow

This document defines the operational rules, quality standards, and development workflows for AI agents working in the `python_tools` repository.

---

## 1. Repository Architecture & Scope

`python_tools` is a collection of standalone Python tools and utilities focused on big data ecosystem diagnostics, log analysis, and operational scraping.

### Existing Tools:
- **`tez-log-analysis/`**:
  - Entry point: `tez-log-analysis/tez-log-analysis.py`
  - Tests: `tez-log-analysis/test_tez_log_analysis.py`
  - Purpose: Analyzes aggregated YARN / Tez logs for Hive-on-Tez queries to identify long-running, waiting, and failed task attempts.
  - Runtime artifacts: Produces `tez-log-analysis.out` and split log directories (`app_log_dir/`).
- **`hive-webui-reader/`**:
  - Entry point: `hive-webui-reader/html_hive.py`
  - Purpose: Connects to HiveServer2 Web UI, extracts query execution information, and scrapes HTML status tables.
  - Configuration: `config.props` (HS2 host, port, logging level).

---

## 2. Environment & Compatibility Matrix

All code **must** maintain compatibility with the GitHub Actions matrix:

- **Supported Python Versions**: `3.11`, `3.12`, `3.13`
- **Target OS**: Linux / macOS / POSIX

### Critical Compatibility Constraints:
- **Do not use syntax or features introduced in Python 3.10+ only**:
  - ❌ Avoid PEP 604 union syntax (`int | None` or `str | float`).
  - ✔️ Use `typing.Union[int, None]` or `typing.Optional[int]`.
  - ❌ Avoid `match`/`case` pattern matching statements.
  - ❌ Avoid `typing.Self` (Python 3.11+).
- Always ensure stdlib imports and third-party APIs used are supported on Python 3.8.

---

## 3. Dependency Management

The project uses a lean dependency footprint. Dependencies are managed per workflow and tool requirements:

```bash
# Core testing & linting dependencies
pip install pylint pytest

# Tool runtime dependencies
pip install requests beautifulsoup4 lxml
```

When introducing new libraries:
- Keep third-party dependencies to a minimum.
- Prefer Python standard library (`urllib`, `argparse`, `dataclasses`, `pathlib`, `logging`, `re`) where feasible.
- If adding a dependency required across the project, update `.github/workflows/pylint.yml` accordingly.

---

## 4. GitHub Actions CI/CD Pipeline

The repository enforces strict continuous integration via two GitHub Actions workflows:

### A. Pylint & Test Suite (`.github/workflows/pylint.yml`)
Runs on every `push` and `pull_request`:
1. **Matrix Evaluation**: Executes concurrently across Python `3.8`, `3.9`, and `3.10`.
2. **Unit Tests**:
   ```bash
   pytest -q
   ```
   All test cases across the repository must pass with zero failures or errors.
3. **Static Analysis & Code Quality**:
   ```bash
   pylint $(git ls-files '*.py') --fail-under=7.5
   ```
   Every tracked Python file is evaluated. The cumulative score **must not fall below 7.5 / 10** (failing this will fail the CI run).

### B. Automated PR Approver (`.github/workflows/prApprover.yml`)
- Triggers automatically upon successful completion of the `Pylint` workflow.
- Inspects the pull request associated with the head commit.
- If all matrix checks succeed, the bot automatically grants an official `APPROVE` review:
  > *"✅ Automated approval: All checks passed successfully."*
- **Takeaway for Agents**: Any PR that breaks tests or drops the Pylint score below 7.5 will fail CI and be blocked from automated approval.

---

## 5. Agent Verification Checklist (Pre-Flight)

Before completing any task, opening a PR, or creating a commit, agents **must** execute and pass the following checks:

### 1. Run Unit Tests
```bash
pytest -q
```
- Verify existing tests continue to pass.
- Write new pytest test cases for any new functionality, edge cases, or bug fixes (e.g., in `tez-log-analysis/test_tez_log_analysis.py` or a dedicated test file).

### 2. Run Pylint Check
```bash
pylint $(git ls-files '*.py') --fail-under=7.5
```
- Aim for a score of **8.5+** (minimum acceptable is **7.5**).
- Common Pylint pitfalls to resolve:
  - Missing module, class, or function docstrings (`missing-docstring`).
  - Unused imports or variables (`unused-import`, `unused-variable`).
  - Lines exceeding column limits (`line-too-long`).
  - Broad exception catching without comment or re-raise (`broad-except`).
  - Mutable default arguments (`dangerous-default-value`).

### 3. Check Git Hygiene
- Ensure generated files (e.g. `tez-log-analysis.out`, `.pytest_cache/`, `__pycache__/`, `app_log_dir/`) are not staged or tracked.
- Respect `.gitignore`.

---

## 6. Code Style & Engineering Guidelines

1. **Docstrings & Comments**:
   - Provide clear, concise PEP 257 docstrings for every module, class, and public function.
   - Explain arguments (`Args`), return values (`Returns`), and raised exceptions (`Raises`).

2. **Type Annotations**:
   - Add type hints to function signatures and dataclasses.
   - Import types from `typing` (`List`, `Dict`, `Tuple`, `Optional`, `Union`, `Any`).

3. **Logging & Output**:
   - Use Python's standard `logging` library rather than bare `print` calls for internal state and diagnostic messages.
   - CLI user output should be cleanly formatted and deliberate.

4. **Error Handling**:
   - Never pass silently on exceptions (`bare except:` or `except Exception: pass`).
   - Catch specific exceptions (`FileNotFoundError`, `ValueError`, `requests.RequestException`).

5. **File & CLI Handling**:
   - Use `pathlib.Path` for file system interactions and path manipulation.
   - Use `argparse` for CLI commands with descriptive `--help` text and clear argument validation.

---

## 7. Structure for Adding New Tools

When adding a new tool to `python_tools`:
1. Create a dedicated directory: `<tool-name>/`
2. Include:
   - `<tool-name>.py`: Main executable logic or package.
   - `test_<tool-name>.py`: Pytest test suite covering core operations.
   - `README.md`: Tool-specific instructions, arguments, sample outputs, and use cases.
3. Update root `README.md` to list the new tool.
4. Ensure the new tool is covered by `pytest -q` and satisfies `pylint --fail-under=7.5`.
