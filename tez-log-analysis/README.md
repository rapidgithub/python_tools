## How to

`tez-log-analysis.py` takes 2 arguments.

`--log` followed by aggregated yarn log file of hive on tez job.

`--dagid` followed by positive integer.

`--dagid` is optional and only needed if aggregated yarn log has more than one query.

If no `--dagid` passed then code will analyze first dag or query.

This code creates app_log_dir under working directoy.

The logic of splitting the log is based on below python code.

>https://raw.githubusercontent.com/TarunParimi/yarn-log-splitter/master/yarn-log-splitter.py

If app_log_dir is already present under working directoy then code will print ERRORR and exit.

```
python ~/tez-log-analysis.py --log application_1631466459248_0034.log

        ERROR : Found directory or file with name app_log_dir in current location.
        ERROR : Please rename or move the directory / file and try again. 
```
The code first breaks the log into containers using yarn-log-splitter.py logic 

After that it analyze the log and give details on failed tasks (if present) with log file name to check.

Then it checks for longest running task in the query and prints log file path for same.

It also print the dag log path and wait time for longest waiting task which can help in identifying yarn resource issue.

### Example 
```
python ~/tez-log-analysis.py --log application_1631466459248_0034.log
        Analyzing dag log syslog_dag_1631466459248_0034_1
Total tasks in dag = 21
Longest run time of 23 seconds was taken by attempt_1631466459248_0034_1_00_000011_0 with status SUCCEEDED
Longest wait time of 4 seconds was taken by attempt_1631466459248_0034_1_00_000017_0 with status SUCCEEDED

Check below logs for details about long running and waiting tasks:
app_log_dir/containers/container_e71_1631466459248_0034_01_000003/syslog_attempt_1631466459248_0034_1_00_000011_0
app_log_dir/containers/container_e71_1631466459248_0034_01_000001/syslog_dag_1631466459248_0034_1
