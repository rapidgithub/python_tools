import os
import sys
import re

DAG_LOG_REGEX="syslog_dag_\d+_\d+_\d+$"
#DAG_LOG_REGEX="syslog_dag_\d+_\d+_1+$"

def findfiles(path):
    res = []
    for root, dirs, fnames in os.walk(path):
        for fname in fnames:
                res.append(os.path.join(root, fname))
    return res

def grep_line(filepath, regex):
    res = []
    with open(filepath) as f:
        for line in f:
		if regex.search(line):
	           res.append(line)
    return res

def usage():
    print 'Usage: python', sys.argv[0], '<application log> <search regex>'

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
    else:
	regObj = re.compile(DAG_LOG_REGEX)
        filepaths = findfiles(sys.argv[1])
        matched_files = {}
	for filepath in filepaths:
	  if grep_line(filepath, regObj):
                matched_files[filepath]=grep_line(filepath, regObj)
        dag_count = len(matched_files)
        if dag_count == 1:
		print "Result of grep {}".format(matched_files)
                for items in list(matched_files.keys()):
			dag_id = items.split("/")[3]
			completed_tasks = grep_line(items,re.compile("TASK_ATTEMPT_FINIS"))
  			task_dict = {}
			task_details = {}
			all_times=[]
			for tasks in completed_tasks:
			  vertex_name = tasks.split(",")[1].split(":")[-1].split("=")[1]
                          task_id = tasks.split(",")[2].split("=")[1]
			  time_taken = tasks.split(",")[7].split("=")[1]
			  all_times.append(int(time_taken))
                          task_details[task_id]=tasks
			  task_dict[task_id]=time_taken
			  print "Task {} of vertex {} took {} ms".format(task_id,vertex_name,time_taken)
			print len(task_dict)
                        print task_dict.values()
			all_times.sort()
			index_num = task_dict.values().index(str(all_times[-1]))
			dic_keys = list(task_dict.keys())
			print "Longest time: {} by {} ".format(all_times[-1],dic_keys[index_num])
			#for name in task_dict.keys():
			  #print "The vertex {} detail is::: {}".format(name.split(":")[-1],task_dict[name][1])
			  #for items in task_dict[name][1]:
				#print items
			  
	elif dag_count > 1:
		print "\n\tTotal dags/queries in log are {}. Select the query you want to debug".format(dag_count)
                for items in matched_files.keys():
			print items
        else:
		print "No dag(s) logs found in {}".format(filepaths)
 
