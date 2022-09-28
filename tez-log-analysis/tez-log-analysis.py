import argparse
import re
import sys
from shutil import rmtree
from os import path, makedirs, symlink, mkdir, walk

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=['file', 'dir'], default='file')
parser.add_argument("--dagid", type=int, help="Dag id to be analyzed.")
parser.add_argument("--log", help="Tez application log file.")
parser.add_argument("--appdir", help="Pre split tez application log dir.")
args = parser.parse_args()

LOCAL_AGGREGATION = "LogAggregationType: LOCAL"
DAG_LOG_REGEX = "LogType:syslog_dag_\d+_\d+_\d+$"
"""
DOT_LOG_REGEX = "LogType:dag_\d+_\d+_\d+_priority.dot"
"""
CONTAINER_PREFIX = 'Container: '
LOGTYPE_PREFIX = "LogType:"
LOGTYPE_SEPARATOR = ":"
LOGTYPE_END = 'End of LogType:'


def remove_and_create(log_dir):
    try:
        mkdir(log_dir)
    except OSError:
        rmtree(log_dir)
        mkdir(log_dir)


def split_logs(log, output_dir):
    output_dir = path.abspath(output_dir)
    try:
        makedirs(output_dir)
    except OSError:
        pass
    containers_base = path.join(output_dir, 'containers')
    hosts_base = path.join(output_dir, 'hosts')
    remove_and_create(containers_base)
    remove_and_create(hosts_base)
    containers = set()
    hosts = set()
    split_file = None
    container_dir = None
    logtype = None
    container_header = None
    with open(log) as log_file:
        for line in log_file:
            if line.startswith(CONTAINER_PREFIX):
                container_header = line
                container = line.split()[1].strip()
                if container not in containers:
                    containers.add(container)
                    container_dir = path.join(containers_base, container)
                    mkdir(container_dir)
                    host = line.split()[3].strip()
                    hostdir = path.join(hosts_base, host)
                    if host not in hosts:
                        hosts.add(host)
                        mkdir(hostdir)
                    symlink(container_dir, path.join(hostdir, container))
            elif line.startswith(LOGTYPE_PREFIX):
                logtype = line.split(LOGTYPE_SEPARATOR)[1].strip()
                split_file = open(path.join(container_dir, logtype), 'w+')
                split_file.write(container_header)
            elif line.startswith(LOGTYPE_END):
                if line.split(LOGTYPE_SEPARATOR)[1].strip() == logtype:
                    if split_file is not None:
                        split_file.close()
                    split_file = None
                else:
                    pass  # Ignore empty log type
            if split_file is not None:
                split_file.write(line)
    if split_file is not None:
        split_file.close()


def findfiles(log_path):
    res = []
    for root, dirs, fnames in walk(log_path):
        for fname in fnames:
            fname_path = path.join(root, fname)
            if path.islink(fname_path) is False:
                res.append(fname_path)
    return res


def grep_line(file_path, regex):
    res = []
    with open(file_path) as f:
        for line in f:
            if regex.search(line):
                res.append(line)
    return res


def print_failed_tasks(tasks_failed, logs_list):
    for items in tasks_failed:
        msg = "\n\tFound failure for task {}".format(items[2])
        print(msg)
        append_log(msg)
        log_regex = "LogType:syslog_{}".format(items[2])
        for file_path in logs_list:
            if grep_line(file_path, re.compile(log_regex)):
                msg = "Log location: {}".format(file_path)
                print(msg)
                append_log(msg)


def analyze_log(dag_log, logs_list):
    tasks_list = grep_line(dag_log, re.compile("Event:TASK_ATTEMPT_FINISHED"))
    if len(tasks_list) > 0:
        msg = "Total tasks in dag = {}".format(len(tasks_list))
        print(msg)
        append_log(msg)
        tasks_passed = []
        tasks_failed = []
        for task in tasks_list:
            task_id = task.split(",")[2].split("=")[1]
            task_status = task.split(",")[8].split("=")[1]
            wait_time = int(task.split(",")[5].split("=")[1]) - int(task.split(",")[3].split("=")[1])
            run_time = int(task.split(",")[7].split("=")[1])
            if task_status == 'SUCCEEDED':
                tasks_passed.append((wait_time, run_time, task_id, task_status))
            elif task_status == 'FAILED':
                tasks_failed.append((wait_time, run_time, task_id, task_status))
        top_runtime = []
        top_wait = []
        if len(tasks_passed) > 0:
            top_runtime = sorted(tasks_passed, key=lambda x: x[1])[-1]
            top_wait = sorted(tasks_passed)[-1]
            msg = "Printing details for all tasks sorted by runtime(ms) desc..\n(wait_time, run_time, task_id, task_status)"
            append_log(msg)
            for items in sorted(tasks_passed, key=lambda x: x[1]):
                append_log(str(items))
        elif len(tasks_passed) == 0 and len(tasks_failed) == 0:
            msg = "No failed or succeeded tasks found. Check below log for details.\n{}".format(dag_log)
            print(msg)
            append_log(msg)
            return
        long_log_file = []
        if len(top_wait) > 0 and len(top_runtime) > 0:
            print("Longest run time of {} seconds was taken by task id {} with status "
                  "{}".format(top_runtime[1] / 1000, top_runtime[2], top_runtime[3]))
            print("Longest wait time of {} seconds was taken by task id {} with status "
                  "{}".format(top_wait[0] / 1000, top_wait[2], top_wait[3]))
            log_regex = "LogType:syslog_{}".format(top_runtime[2])
            for file_path in logs_list:
                if grep_line(file_path, re.compile(log_regex)):
                    long_log_file.append(file_path)
        if len(tasks_failed) > 6:
            tasks_failed = tasks_failed[:5]
            print_failed_tasks(tasks_failed, logs_list)
        elif len(tasks_failed) > 0:
            print_failed_tasks(tasks_failed, logs_list)
        if len(long_log_file) > 0:
            print("\nCheck below log for details about long running "
                  "task:\n{}".format(long_log_file[-1]))
            print("\nCheck below log for details about waiting "
                              "task:\n{}".format(dag_log))
        else:
            print("\n\tDag log location:\n{}".format(dag_log))
    else:
        msg = "No task found in dag.\nCheck below log for details.\n{}".format(dag_log)
        print(msg)
        overwrite_log(msg)


def overwrite_log(msg):
    out_log = open('tez-log-analysis.out', 'w+')
    out_log.write(msg+'\n')
    out_log.close()


def append_log(msg):
    out_log = open('tez-log-analysis.out', 'a+')
    out_log.write(msg+'\n')
    out_log.close()


"""
def print_viz(dag_log_dot):
    dot_file = []
    with open(dag_log_dot, "r") as ifile:
        for line in ifile:
            dot_file.append(line)
    print(dot_file)
"""


def analyze_dir(app_log):
    all_files = findfiles(app_log)
    dag_files = []
    dag_regObj = re.compile(DAG_LOG_REGEX)
    for filepath in all_files:
        if grep_line(filepath, dag_regObj):
            dag_files.append(filepath)
    dag_count = len(dag_files)
    if dag_count == 1:
        msg = "\n\tAnalyzing dag log {}".format(dag_files[0].split('/')[3])
        print(msg)
        append_log(msg)
        analyze_log(dag_files[0], all_files)
    elif dag_count > 1:
        dag_files = map(lambda x: (x, int(x.split('/')[3].split('_')[4])), dag_files)
        dag_files = sorted(dag_files, key=lambda x: x[1])
        dagids = list(map(lambda x: x[1], dag_files))
        if args.dagid and dag_count >= args.dagid > 0:
            msg = "\nAnalyzing dag id {}".format(dag_files[args.dagid - 1][0].split('/')[3])
            print(msg)
            append_log(msg)
            analyze_log(dag_files[args.dagid - 1][0], all_files)
        else:
            print("\n\tNote: Total {} dags found.".format(dag_count))
            print("\nEither --dagid option was not used or dag with given id was not found.")
            usage()
            if dag_count > 10:
                print("Valid dag ids are(showing top 10):\n{}\n".format(dagids[:10]))
            else:
                print("Valid dag ids are:\n{}\n".format(dagids))
    else:
        print("\n\tNo dag log found in {}\n".format(app_log))


def usage():
    print("1. To run analysis on aggregated tez log.")
    print("\tpython " + sys.argv[0] + " --log <aggregated_log_file> [--dagid 1]\n")
    print("2. To run analysis on already split and aggregated tez log directory.")
    print("\tpython " + sys.argv[0] + " --mode dir --appdir <aggregated_log_split_dir> [--dagid 1]\n")


if __name__ == '__main__':
    if args.mode == 'file':
        if args.log and path.isfile(args.log):
            if grep_line(args.log, re.compile(LOCAL_AGGREGATION)):
                print("\n\tERROR : Log file is not complete.")
                print("\n\tMake sure yarn job log collected contains the line LogAggregationType: AGGREGATED ")
                print("\n\tCollect the yarn job log after killing the application or wait for it to complete!\n")
                overwrite_log('Nothing to add..')
            else:
                if path.exists('app_log_dir'):
                    print("\n\tERROR : Directory or file with name app_log_dir exists in current location.")
                    print("\tRename / move the directory or file with name app_log_dir and run again.")
                    print("\tTo run the analysis on existing directory use below syntax.")
                    print("\n\tpython " + sys.argv[0] + " --mode dir --appdir <aggregated_log_split_dir> [--dagid 1]\n")
                    overwrite_log('Analysis failed!')
                else:
                    split_logs(args.log, 'app_log_dir')
                    overwrite_log('Starting analysis for app_log_dir')
                    analyze_dir('app_log_dir')
        elif args.log and path.isfile(args.log) is False:
            print("\nERROR: Provided option \"{}\" is not valid file".format(args.log))
            overwrite_log('Analysis failed!')
            exit(1)
        else:
            print("\n\tERROR: No options provided.\n")
            overwrite_log('Analysis failed!')
            usage()
    else:
        if args.appdir:
            if path.isdir(args.appdir):
                overwrite_log('Starting analysis for {}'.format(args.appdir))
                analyze_dir(args.appdir)
            else:
                print("Path \"{}\" is not a directory!".format(args.appdir))
                overwrite_log('Analysis failed!')
        else:
            print("ERROR: Required option --appdir is missing!")
            overwrite_log('Analysis failed!')
