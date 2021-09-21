This script takes 2 arguments.

--log followed by aggregated yarn log of hive on tez job.

--dagid followed by positive integer depending on which query you want to analyze

--dagid is optional and only needed if aggregated yarn log has more than one query.

If no --dagid passed then code will analyze first dag or query.

This code create app_log_dir on working directoy which use the logic of below python code.

>https://raw.githubusercontent.com/TarunParimi/yarn-log-splitter/master/yarn-log-splitter.py

If app_log_dir is present on working directoy then code will print ERRORR and exit.

The code first breaks the log in containers using yarn-log-splitter.py logic 

After that it analyze the log and give details on failed tasks if found with log file to check.

Then it checks for longest running task in the query and prints log file path for same.

It also print the dag log path and wait time for longest waiting task which can help in identifying yarn resource issue.

```python ~/tez-log-analysis.py --log application_1631466459248_0034.log
        Analyzing dag log syslog_dag_1631466459248_0034_1
Total tasks in dag = 21
Longest run time of 23 seconds was taken by attempt_1631466459248_0034_1_00_000011_0 with status SUCCEEDED
Longest wait time of 4 seconds was taken by attempt_1631466459248_0034_1_00_000017_0 with status SUCCEEDED

Check below logs for details about long running and waiting tasks:
app_log_dir/containers/container_e71_1631466459248_0034_01_000003/syslog_attempt_1631466459248_0034_1_00_000011_0
app_log_dir/containers/container_e71_1631466459248_0034_01_000001/syslog_dag_1631466459248_0034_1