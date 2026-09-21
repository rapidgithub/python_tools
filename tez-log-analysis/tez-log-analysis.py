import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree
from typing import List, Dict, Tuple

# Configure logging once
def setup_logging(output_file: str = 'tez-log-analysis.out'):
    """Initialize logging to file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[
            logging.FileHandler(output_file, mode='w'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# Compile regex patterns once at module level
LOCAL_AGGREGATION_REGEX = re.compile(r"LogAggregationType: LOCAL")
DAG_LOG_REGEX = re.compile(r"LogType:syslog_dag_\d+_\d+_\d+$")
TASK_ATTEMPT_REGEX = re.compile(r"Event:TASK_ATTEMPT_FINISHED")
CONTAINER_PREFIX = 'Container: '
LOGTYPE_PREFIX = "LogType:"
LOGTYPE_SEPARATOR = ":"
LOGTYPE_END = 'End of LogType:'

def build_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(description='Analyze Tez application logs')
    parser.add_argument("--mode", choices=['file', 'dir'], default='file',
                        help="Mode: analyze single file or directory")
    parser.add_argument("--dagid", type=int, help="Dag id to be analyzed")
    parser.add_argument("--log", help="Tez application log file")
    parser.add_argument("--appdir", help="Pre-split tez application log directory")
    return parser


args = argparse.Namespace(mode='file', dagid=None, log=None, appdir=None)


@dataclass
class Task:
    """Represents a Tez task attempt."""
    task_id: str
    status: str
    wait_time: int  # milliseconds
    run_time: int   # milliseconds
    
    def runtime_seconds(self) -> float:
        """Convert run time to seconds."""
        return self.run_time / 1000
    
    def wait_time_seconds(self) -> float:
        """Convert wait time to seconds."""
        return self.wait_time / 1000


def parse_task_attempt(line: str) -> Task:
    """
    Parse a TASK_ATTEMPT_FINISHED log line.
    
    Args:
        line: A log line starting with Event:TASK_ATTEMPT_FINISHED
        
    Returns:
        Task object with parsed attributes
        
    Raises:
        ValueError: If line format is invalid
    """
    try:
        parts = line.split(",")
        if len(parts) < 9:
            raise ValueError(f"Line has insufficient fields (expected 9+, got {len(parts)})")
        
        task_id = parts[2].split("=")[1]
        task_status = parts[8].split("=")[1]
        wait_time = int(parts[5].split("=")[1]) - int(parts[3].split("=")[1])
        run_time = int(parts[7].split("=")[1])
        
        return Task(
            task_id=task_id,
            status=task_status,
            wait_time=wait_time,
            run_time=run_time
        )
    except (IndexError, ValueError) as e:
        raise ValueError(f"Failed to parse task line: {line}") from e


def remove_and_create(log_dir: Path) -> None:
    """Remove directory if exists and create it fresh."""
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create directory {log_dir}: {e}")
        raise


def split_logs(log_file: Path, output_dir: Path) -> None:
    """
    Split aggregated Tez log file by container and log type.
    
    Args:
        log_file: Path to the aggregated log file
        output_dir: Directory to write split logs
    """
    output_dir = output_dir.resolve()
    containers_base = output_dir / 'containers'
    hosts_base = output_dir / 'hosts'
    
    remove_and_create(containers_base)
    remove_and_create(hosts_base)
    
    containers: Dict[str, Path] = {}
    hosts: Dict[str, Path] = {}
    split_file = None
    container_dir = None
    logtype = None
    container_header = None
    
    try:
        with open(log_file) as log_fh:
            for line in log_fh:
                if line.startswith(CONTAINER_PREFIX):
                    container_header = line
                    parts = line.split()
                    if len(parts) >= 4:
                        container = parts[1].strip()
                        host = parts[3].strip()
                        
                        if container not in containers:
                            container_dir = containers_base / container
                            container_dir.mkdir(parents=True, exist_ok=True)
                            containers[container] = container_dir
                            
                            if host not in hosts:
                                hostdir = hosts_base / host
                                hostdir.mkdir(parents=True, exist_ok=True)
                                hosts[host] = hostdir
                            
                            # Create symlink from host dir to container dir
                            symlink_path = hosts[host] / container
                            if not symlink_path.exists():
                                symlink_path.symlink_to(container_dir)
                    
                elif line.startswith(LOGTYPE_PREFIX):
                    # Close previous log file
                    if split_file:
                        split_file.close()
                    
                    logtype = line.split(LOGTYPE_SEPARATOR)[1].strip()
                    if container_dir:
                        log_path = container_dir / logtype
                        split_file = open(log_path, 'w+')
                        if container_header:
                            split_file.write(container_header)
                
                elif line.startswith(LOGTYPE_END):
                    end_logtype = line.split(LOGTYPE_SEPARATOR)[1].strip() if len(line.split(LOGTYPE_SEPARATOR)) > 1 else None
                    if end_logtype == logtype and split_file:
                        split_file.close()
                        split_file = None
                        logtype = None
                
                if split_file:
                    split_file.write(line)
    
    finally:
        if split_file:
            split_file.close()


def find_files(log_path: Path) -> List[Path]:
    """
    Recursively find all regular files (excluding symlinks) in a directory.
    
    Args:
        log_path: Root directory to search
        
    Returns:
        List of Path objects for regular files
    """
    return [f for f in log_path.rglob('*') if f.is_file()]


def grep_line(file_path: Path, regex: re.Pattern) -> List[str]:
    """
    Find all lines in a file matching a regex pattern.
    
    Args:
        file_path: File to search
        regex: Compiled regex pattern
        
    Returns:
        List of matching lines
    """
    try:
        matches = []
        with open(file_path) as f:
            for line in f:
                if regex.search(line):
                    matches.append(line)
        return matches
    except (IOError, OSError) as e:
        logger.warning(f"Failed to read {file_path}: {e}")
        return []


def find_log_files(logs_list: List[Path], task_id: str) -> List[Path]:
    """
    Find all log files matching a task ID pattern.
    
    Args:
        logs_list: List of log file paths to search
        task_id: Task ID to search for
        
    Returns:
        List of matching log files
    """
    pattern = re.compile(f"LogType:syslog_{re.escape(task_id)}")
    matching_files = []
    for file_path in logs_list:
        if grep_line(file_path, pattern):
            matching_files.append(file_path)
    return matching_files


def print_failed_tasks(tasks_failed: List[Task], logs_list: List[Path]) -> None:
    """
    Print details about failed tasks and locate their log files.
    
    Args:
        tasks_failed: List of failed Task objects
        logs_list: List of all log files
    """
    for task in tasks_failed:
        logger.info(f"\n\tFound failure for task {task.task_id}")
        log_files = find_log_files(logs_list, task.task_id)
        for file_path in log_files:
            logger.info(f"Log location: {file_path}")


def analyze_log(dag_log: Path, logs_list: List[Path]) -> None:
    """
    Analyze a DAG log file and print performance statistics.
    
    Args:
        dag_log: Path to DAG log file
        logs_list: List of all log files for cross-reference
    """
    task_lines = grep_line(dag_log, TASK_ATTEMPT_REGEX)
    
    if not task_lines:
        logger.info(f"No tasks found in dag.\nCheck below log for details.\n{dag_log}")
        return
    
    logger.info(f"Total tasks in dag = {len(task_lines)}")
    
    tasks_passed = []
    tasks_failed = []
    
    for line in task_lines:
        try:
            task = parse_task_attempt(line)
            if task.status == 'SUCCEEDED':
                tasks_passed.append(task)
            elif task.status == 'FAILED':
                tasks_failed.append(task)
        except ValueError as e:
            logger.warning(f"Skipping malformed task line: {e}")
    
    if not tasks_passed and not tasks_failed:
        logger.info(f"No failed or succeeded tasks found. Check log: {dag_log}")
        return
    
    # Find tasks with extreme times
    top_runtime = max(tasks_passed, key=lambda t: t.run_time) if tasks_passed else None
    top_wait = max(tasks_passed, key=lambda t: t.wait_time) if tasks_passed else None
    
    # Log performance details
    if tasks_passed:
        logger.info("Printing details for all tasks sorted by runtime (ms, descending):")
        logger.info("(wait_time_ms, run_time_ms, task_id, status)")
        for task in sorted(tasks_passed, key=lambda t: t.run_time):
            logger.info(f"({task.wait_time}, {task.run_time}, {task.task_id}, {task.status})")
    
    # Print slowest tasks
    if top_runtime:
        logger.info(f"Longest run time: {top_runtime.runtime_seconds():.2f}s (task {top_runtime.task_id})")
        runtime_logs = find_log_files(logs_list, top_runtime.task_id)
        if runtime_logs:
            logger.info(f"Check log for details: {runtime_logs[-1]}")
    
    if top_wait:
        logger.info(f"Longest wait time: {top_wait.wait_time_seconds():.2f}s (task {top_wait.task_id})")
        logger.info(f"Check DAG log for details: {dag_log}")
    
    # Print failed tasks (limit to first 5)
    if tasks_failed:
        limited_failed = tasks_failed[:5]
        print_failed_tasks(limited_failed, logs_list)
        if len(tasks_failed) > 5:
            logger.info(f"\n(Showing first 5 of {len(tasks_failed)} failed tasks)")


def analyze_dir(app_log: Path, dagid: int | None = None) -> None:
    """
    Analyze all DAG logs in a directory.
    
    Args:
        app_log: Directory containing split/aggregated logs
        dagid: Optional DAG ID to analyze when multiple DAG logs are present
    """
    app_log = Path(app_log)
    all_files = find_files(app_log)
    dag_files = []
    
    for filepath in all_files:
        if grep_line(filepath, DAG_LOG_REGEX):
            dag_files.append(filepath)
    
    dag_count = len(dag_files)
    
    if dag_count == 0:
        logger.error(f"No dag log found in {app_log}")
        return
    
    if dag_count == 1:
        logger.info(f"Analyzing dag log: {dag_files[0].name}")
        analyze_log(dag_files[0], all_files)
        return
    
    # Multiple DAGs: extract and sort by ID
    try:
        dag_files_with_id = []
        for filepath in dag_files:
            # Extract DAG ID from path components (format: dag_X_Y_Z)
            parts = filepath.parts
            for part in parts:
                if part.startswith('dag_'):
                    dag_id = int(part.split('_')[3])
                    dag_files_with_id.append((filepath, dag_id))
                    break
        
        dag_files_with_id.sort(key=lambda x: x[1])
        dagids = [dag_id for _, dag_id in dag_files_with_id]
        
        if dagid and 0 < dagid <= dag_count:
            selected_dag = dag_files_with_id[dagid - 1][0]
            logger.info(f"Analyzing dag id {dagid}: {selected_dag.name}")
            analyze_log(selected_dag, all_files)
        else:
            logger.warning(f"Total {dag_count} dags found.")
            logger.warning("Either --dagid option was not used or dag with given id was not found.")
            usage()
            if dag_count > 10:
                logger.info(f"Valid dag ids (showing top 10):\n{dagids[:10]}")
            else:
                logger.info(f"Valid dag ids:\n{dagids}")
    
    except (ValueError, IndexError) as e:
        logger.error(f"Error processing DAG files: {e}")


def usage() -> None:
    """Print usage instructions."""
    script_name = Path(sys.argv[0]).name
    print(f"1. To run analysis on aggregated tez log:")
    print(f"\tpython {script_name} --log <aggregated_log_file> [--dagid 1]\n")
    print(f"2. To run analysis on already split and aggregated tez log directory:")
    print(f"\tpython {script_name} --mode dir --appdir <aggregated_log_split_dir> [--dagid 1]\n")


def main(argv: List[str] | None = None) -> None:
    """Main entry point."""
    global args
    args = build_parser().parse_args(argv)

    if args.mode == 'file':
        if not args.log:
            logger.error("No options provided.")
            usage()
            return
        
        log_path = Path(args.log)
        if not log_path.is_file():
            logger.error(f"Provided file does not exist: {args.log}")
            sys.exit(1)
        
        # Check if log is complete (not LOCAL_AGGREGATION)
        if grep_line(log_path, LOCAL_AGGREGATION_REGEX):
            logger.error("Log file is not complete.")
            logger.error("Make sure yarn log contains 'LogAggregationType: AGGREGATED'")
            logger.error("Collect the yarn job log after killing the application or wait for completion.")
            return
        
        # Check for existing app_log_dir
        output_dir = Path('app_log_dir')
        if output_dir.exists():
            logger.error(f"Directory {output_dir} already exists in current location.")
            logger.error("Rename/move it and run again, or use: --mode dir --appdir <dir>")
            return
        
        logger.info("Starting log split...")
        split_logs(log_path, output_dir)
        analyze_dir(output_dir, dagid=args.dagid)
    
    else:  # mode == 'dir'
        if not args.appdir:
            logger.error("Required option --appdir is missing!")
            return
        
        appdir = Path(args.appdir)
        if not appdir.is_dir():
            logger.error(f"Path is not a directory: {args.appdir}")
            return
        
        logger.info(f"Starting analysis for {args.appdir}")
        analyze_dir(appdir, dagid=args.dagid)


if __name__ == '__main__':
    main()
