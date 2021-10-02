import urllib2, logging, ConfigParser
from socket import timeout

config = ConfigParser.RawConfigParser()
config.read('config.props')
log_level = config.get('default','log_level')
host = config.get('default','hs2_host')
port = config.get('default','hs2_webport')
FORMAT = '%(asctime)-15s ::%(levelname)s:: %(message)s'

if log_level.upper() == 'INFO':
	logging.basicConfig(format=FORMAT,level=logging.INFO)
elif log_level.upper() == 'ERROR':
	logging.basicConfig(format=FORMAT,level=logging.ERROR)
else: 
	logging.basicConfig(format=FORMAT)
	logging.warn('Unable to find valid log level from config, using default value WARN.')
	logging.warn('Permissible log levels are INFO and ERROR.')

url_base = 'http://'+host+':'+port

def make_soup(url):
	from bs4 import BeautifulSoup #pip install beautifulsoup4
	try:
		hive_page = urllib2.urlopen(url, timeout=5).read()
	except:
		logging.error('Unable to connet using URL: %s', url_base)
		exit(1)
	return BeautifulSoup(hive_page,features="lxml")

def process_table(table_body):
	rows = table_body.find_all('tr')
	data = []
	hrefs = table_body.find_all('a')
	for row in rows:
		cols = row.find_all('td')
		cols = [ele.text.strip() for ele in cols]
		data.append([ele for ele in cols if ele]) # Get rid of empty values
	return (hrefs,data) 
		
def print_details(table_data,table_type):
	num_queries = len(table_data[0])
	result = []
	if num_queries > 0:
		if num_queries == 1:
			logging.info('Printing %s %s query.', num_queries, table_type)
		else:
			logging.info('Printing %s %s queries.', num_queries, table_type)
		for item in table_data[0]:
			query_page_url = url_base+str(item).split('"')[1]
			query_page_soup = make_soup(query_page_url)
			tab = query_page_soup.findAll('table')[0]
			table_data += process_table(tab)
		for items in table_data[3:]:
			if len(items) > 0:
				logging.info('Found %s',items[2][0]+'='+items[2][1])
				result.append(items[2][1])
	else:
		logging.info('No %s query found.',table_type)
	return result

open_queries = make_soup(url_base).findAll('table')[1]
closed_queries = make_soup(url_base).findAll('table')[2]
open_queries = print_details(process_table(open_queries),'running')
closed_queries = print_details(process_table(closed_queries),'closed')

print "List of open: {}\nList of closed: {}".format(open_queries,closed_queries)
