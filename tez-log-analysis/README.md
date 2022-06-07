## How to

`tez-log-analysis.py` takes 2 arguments.

`--log` followed by aggregated yarn log file of hive on tez job.

`--dagid` followed by positive integer.

If no `--dagid` passed then code will analyze if only one dag is present in log.

Else `--dagid` is needed if log file contain more than one dags.

This code creates app_log_dir under working directoy.

The logic of splitting the log is based on below python code.

>https://raw.githubusercontent.com/TarunParimi/yarn-log-splitter/master/yarn-log-splitter.py

If app_log_dir is already present under working directoy then code will print usage on how to analyze pre-split logs.

```
        ERROR : Directory or file with name app_log_dir exists in current location.
        Rename / move the directory or file with name app_log_dir and run again.
        To run the analysis on existing directory use below syntax.

        python tez-log-analysis/tez-log-analysis.py --mode dir --appdir <aggregated_log_split_dir> [--dagid 1]
```
The code first breaks the log into multiple log files and group by containers. 

It will give details about failed tasks (if present) with log file name to check.

Then it checks for longest running task in the query and prints log file path for same.

It also print the dag log path which can be used to check wait time for longest waiting task.

It may help in confirming yarn resource issue.

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
```
