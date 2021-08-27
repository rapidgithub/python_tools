import sys, re
from os import path, makedirs, symlink, chdir, mkdir, walk
from shutil import rmtree

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


def grep_line(filepath, regex):
    res = []
    with open(filepath) as f:
        for line in f:
            if regex.search(line):
                res.append(line)
    return res


def analyze_log(dag_log):
    dag_id = dag_log.split("/")[3]
    completed_tasks = grep_line(dag_log, re.compile("TASK_ATTEMPT_FINIS"))
    task_dict = {}
    task_details = {}
    all_times = []
    for tasks in completed_tasks:
        vertex_name = tasks.split(",")[1].split(":")[-1].split("=")[1]
        task_id = tasks.split(",")[2].split("=")[1]
        time_taken = tasks.split(",")[7].split("=")[1]
        all_times.append(int(time_taken))
        task_details[task_id] = tasks
        task_dict[task_id] = time_taken
        print "Task {} of vertex {} took {} ms".format(task_id, vertex_name, time_taken)
    print len(task_dict)
    all_times.sort()
    index_num = task_dict.values().index(str(all_times[-1]))
    dic_keys = list(task_dict.keys())
    print "Longest time of {}ms was taken by {} ".format(all_times[-1], dic_keys[index_num])


def print_viz(dag_log_dot):
    dot_file = []
    with open(dag_log_dot, "r") as ifile:
        for line in ifile:
            dot_file.append(line)
    print dot_file


def usage():
    print '\n\tERROR: Wrong number of arguments!!'
    print '\n\tUsage: python', sys.argv[0], '<application log> [dag_id]'
    print '\nNote: dag_id only needed if application log has more than 1 query/dags.\n'


if __name__ == '__main__':
    if len(sys.argv) > 3 or len(sys.argv) < 2:
        usage()
    elif grep_line(sys.argv[1], re.compile(LOCAL_AGGREGATION)):
        print "\n\tERROR : Log file is not complete."
        print "\n\tMake sure the provided yarn job log says LogAggregationType: AGGREGATED "
        print "\n\tCollect the yarn log after killing the application or wait for it to complete!\n"
    else:
        if path.exists('app_log_dir'):
            print "\n\tERROR : Found directory or file with name app_log_dir in current location."
            print "\tERROR : Please rename or move the directory / file and try again.\n"
        else:
            split_logs(sys.argv[1], 'app_log_dir')
            regObj = re.compile(DAG_LOG_REGEX)
            filepaths = findfiles('app_log_dir')
            matched_files = {}
            for filepath in filepaths:
                if grep_line(filepath, regObj):
                    matched_files[filepath] = grep_line(filepath, regObj)
            dag_count = len(matched_files)
            if dag_count == 1:
                print "Result of grep {}".format(matched_files)
                analyze_log(matched_files.keys()[0])
            elif dag_count > 1:
                print "\n\tTotal dags / queries in log are {}".format(dag_count)
                print "\tRerun script with dag_id option ranging from 1 to {}".format(dag_count)
                print "\tExample : python", sys.argv[0], sys.argv[1], matched_files.keys()[0].split('/')[3]
                print "\n\tSample of dags found (showing max 10):"
                if len(matched_files.keys()) > 10:
                    for items in matched_files.keys()[:10]:
                        print "\t", items.split('/')[3]
                else:
                    for items in matched_files.keys():
                        print "\t", items.split('/')[3]
                print "\n"
                rmtree('app_log_dir')
            else:
                print "No dag log was found in {}".format(filepaths)
                rmtree('app_log_dir')
