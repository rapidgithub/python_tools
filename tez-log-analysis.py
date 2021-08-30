import sys, re, argparse
from os import path, makedirs, symlink, chdir, mkdir, walk
from shutil import rmtree

parser = argparse.ArgumentParser()
parser.add_argument("--log", required=True,
                    help="Tez application log")
parser.add_argument("--dagid", type=int, help="Dag id to process, defaults to 1 if not specified")
args = parser.parse_args()

LOCAL_AGGREGATION = "LogAggregationType: LOCAL"
DAG_LOG_REGEX = "LogType:syslog_dag_\d+_\d+_\d+$"
DOT_LOG_REGEX = "LogType:dag_\d+_\d+_\d+_priority.dot"
CONTAINER_PREFIX = 'Container: '
LOGTYPE_PREFIX = 'LogType:'
LOGTYPE_SEPARATOR = ':'
LOGTYPE_END = 'End of LogType:'


def remove_and_create(dir):
    try:
        mkdir(dir)
    except OSError:
        rmtree(dir)
        mkdir(dir)


def split_logs(log, outputdir):
    outputdir = path.abspath(outputdir)
    try:
        makedirs(outputdir)
    except OSError:
        pass
    containers_base = path.join(outputdir, 'containers')
    hosts_base = path.join(outputdir, 'hosts')
    remove_and_create(containers_base)
    remove_and_create(hosts_base)
    containers = set()
    hosts = set()
    container = None
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
            res.append(path.join(root, fname))
    return res


def grep_line(file_path, regex):
    res = []
    with open(file_path) as f:
        for line in f:
            if regex.search(line):
                res.append(line)
    return res


def analyze_log(dag_log, logs_list):
    tasks_list = grep_line(dag_log, re.compile("TASK_ATTEMPT_FINIS"))
    task_details = []
    for task in tasks_list:
        task_id = task.split(",")[2].split("=")[1]
        task_status = task.split(",")[8].split("=")[1]
        create_time = int(task.split(",")[3].split("=")[1])
        start_time = int(task.split(",")[5].split("=")[1])
        wait_time = start_time - create_time
        run_time = int(task.split(",")[7].split("=")[1])
        task_details.append((wait_time, run_time, task_id, task_status))
    print "Total tasks in dag = {}".format(len(task_details))
    top_runtime = sorted(task_details, key=lambda x: x[1])[-1]
    top_wait = sorted(task_details)[-1]
    print "Longest run time of {} seconds was taken by {} with status " \
          "{}".format(top_runtime[1] / 1000, top_runtime[2], top_runtime[3])
    print "Longest wait time of {} seconds was taken by {} with status " \
          "{}".format(top_wait[0] / 1000, top_wait[2], top_wait[3])
    long_log_regex = "LogType:syslog_{}".format(top_runtime[2])
    long_log_file = []
    for file_path in logs_list:
        if grep_line(file_path, re.compile(long_log_regex)):
            long_log_file.append(file_path)
    print "\nCheck below log for more details:\n{}\n{}\n".format(long_log_file[-1], dag_log)


def print_viz(dag_log_dot):
    dot_file = []
    with open(dag_log_dot, "r") as ifile:
        for line in ifile:
            dot_file.append(line)
    print dot_file


if __name__ == '__main__':
    if grep_line(args.log, re.compile(LOCAL_AGGREGATION)):
        print "\n\tERROR : Log file is not complete."
        print "\n\tMake sure the provided yarn job log says LogAggregationType: AGGREGATED "
        print "\n\tCollect the yarn log after killing the application or wait for it to complete!\n"
    else:
        if path.exists('app_log_dir'):
            print "\n\tERROR : Found directory or file with name app_log_dir in current location."
            print "\tERROR : Please rename or move the directory / file and try again.\n"
        else:
            split_logs(args.log, 'app_log_dir')
            regObj = re.compile(DAG_LOG_REGEX)
            all_files = findfiles('app_log_dir')
            dag_files = []
            for filepath in all_files:
                if grep_line(filepath, regObj):
                    dag_files.append(filepath)
            dag_count = len(dag_files)
            if dag_count == 1:
                print "\tAnalyzing dag log {}".format(dag_files[0].split('/')[3])
                analyze_log(dag_files[0], all_files)
            elif dag_count > 1:
                dag_files.sort()
                if args.dagid and args.dagid <= dag_count:
                    print "\nAnalyzing dag id {}".format(dag_files[args.dagid - 1].split('/')[3])
                    analyze_log(dag_files[args.dagid - 1], all_files)
                else:
                    print "\nAnalyzing dag {}".format(dag_files[0].split('/')[3])
                    print "\n\tNote : More than one dag found and either dagid option was", \
                        "\n\t      not used or dag with given id in option not found.", \
                        "\n\tTo check specific dag rerun script with", \
                        "--dagid [1-{}]".format(dag_count)
                    print "\tExample:\n\tpython", sys.argv[0], "--logs <log_file> --dagid 1\n"
                    analyze_log(dag_files[0], all_files)
            else:
                print "No dag log found in {}".format(all_files)
                rmtree('app_log_dir')
