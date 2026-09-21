import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).with_name("tez-log-analysis.py")
SPEC = importlib.util.spec_from_file_location("tez_log_analysis", MODULE_PATH)
tez_log_analysis = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(tez_log_analysis)


def make_task_line(task_id: str = "task_123", start: int = 1000, finish: int = 2000, run_time: int = 1500, status: str = "SUCCEEDED") -> str:
    return (
        f"Event:TASK_ATTEMPT_FINISHED,dagId=1,taskId={task_id},start={start},x=9,finish={finish},y=8,run={run_time},status={status}"
    )


def test_parse_task_attempt_valid_line() -> None:
    task = tez_log_analysis.parse_task_attempt(make_task_line())

    assert task.task_id == "task_123"
    assert task.status == "SUCCEEDED"
    assert task.wait_time == 1000
    assert task.run_time == 1500
    assert task.wait_time_seconds() == 1.0
    assert task.runtime_seconds() == 1.5


def test_parse_task_attempt_invalid_line_raises() -> None:
    with pytest.raises(ValueError):
        tez_log_analysis.parse_task_attempt("Event:TASK_ATTEMPT_FINISHED,taskId=bad")


def test_find_log_files_matches_task_id(tmp_path: Path) -> None:
    task_log = tmp_path / "syslog_task_123"
    task_log.write_text("LogType:syslog_task_123\n")

    other_log = tmp_path / "syslog_task_456"
    other_log.write_text("LogType:syslog_task_456\n")

    matches = tez_log_analysis.find_log_files([task_log, other_log], "task_123")

    assert matches == [task_log]
