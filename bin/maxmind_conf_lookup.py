import sys
import csv
import os, os.path
import errno
import ConfigParser
import logging, logging.handlers
import splunk

def setup_logging():
	logger = logging.getLogger('maxmind_ip')
	SPLUNK_HOME = '/opt/splunk/'

	LOGGING_DEFAULT_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log.cfg')
	LOGGING_LOCAL_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log-local.cfg')
	LOGGING_STANZA_NAME = 'python'
	LOGGING_FILE_NAME = "maxmind.log"
	BASE_LOG_PATH = os.path.join('var', 'log', 'splunk')
	LOGGING_FORMAT = "%(asctime)s %(levelname)-s\t%(module)s:%(lineno)d - %(message)s"
	splunk_log_handler = logging.handlers.RotatingFileHandler(os.path.join(SPLUNK_HOME, BASE_LOG_PATH, LOGGING_FILE_NAME), mode='a')
	splunk_log_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
	logger.addHandler(splunk_log_handler)
	splunk.setupSplunkLogger(logger, LOGGING_DEFAULT_CONFIG_FILE, LOGGING_LOCAL_CONFIG_FILE, LOGGING_STANZA_NAME)
	return logger

# Reads the config file and turns the settings into compiled code
def read_conf(curr_path):
	logger.debug("read_conf")
	config = ConfigParser.RawConfigParser()
	config.read(os.path.abspath(os.path.join(curr_path, os.pardir, "default", "mmdb.conf")))
	sect = config.sections()
	logger.debug(sect)
	conf_dict = {}

	for s in sect:
		conf_dict[s] = {}
		for i in config.items(s):
			conf_dict[s][i[0]] = compile('out_row[k] = response.'+ i[1], '<string>', 'exec')

	return conf_dict

# readers dict.
def get_readers(curr_path, conf_dict):
	logger.debug("get_readers")
	readers = {}
	try:
		for r in conf_dict:
			reader = geoip2.database.Reader(os.path.abspath(os.path.join(curr_path, os.pardir, "data", r)))
			data_type = reader.metadata().database_type.split('-', 1)[1]
			data_type = data_type.replace("-", "_").lower()
			readers[r] = getattr(reader, data_type)
	except IOError, e:
		if e.errno == errno.ENOENT:
			print "Error: one or more database files do not exist"
			logger.error("one or more database files do not exist")
			sys.exit(1)
		else:
			raise

	return readers

# Sets up the csv library to read from stdin and write to stdout.
def csv_setup():
	logger.debug("csv_setup")

	infile = sys.stdin
	outfile = sys.stdout

	in_csv = csv.DictReader(infile)

	logger.debug(in_csv.fieldnames)
	out_csv = csv.DictWriter(outfile, fieldnames=in_csv.fieldnames)
	out_csv.writerow(dict((h, h) for h in in_csv.fieldnames))

	return (in_csv, out_csv)

# Calls the fill_row function to return the results from the database files and
# writes the results to the out_csv file (which is stdout)
def write_to_csv(conf_dict, readers, ip, in_csv, out_csv):
	logger.debug("write_to_csv")

	for in_row in in_csv:
		out_row = {"ip":in_row[ip]}

		try:
			fill_row(conf_dict, readers, in_row[ip], out_row)
		except geoip2.errors.AddressNotFoundError:
			print "Address not found"

		try:
			out_csv.writerow(out_row)
			logger.debug(out_row)
		except UnicodeEncodeError:
			utf8_row = {}
			for k, v in out_row.items():
				if isinstance(v, unicode):
					utf8_row[k] = v.encode('utf8')
				else:
					utf8_row[k] = v
			out_csv.writerow(utf8_row)

# Fills in a row using the code compiled in read_conf
def fill_row(conf_dict, readers, ip, out_row):
	logger.debug(ip)

	for s in conf_dict:
		response = readers[s](ip)
		for k,v in conf_dict[s].items():
			logger.debug(conf_dict[s])
			exec(v)
	logger.debug(out_row)

def main(logger):

	logger.debug("entered main")
	if len(sys.argv) != 2:
		print "Usage: python maxmind_conf_lookup.py [ip field]"
		logger.error("argument problem")
		sys.exit(1)

	curr_path = os.path.dirname(os.path.realpath(__file__))
	logger.debug(curr_path)

	conf_dict = read_conf(curr_path)

	readers = get_readers(curr_path, conf_dict)

	ip = sys.argv[1]

	in_csv, out_csv = csv_setup()

	write_to_csv(conf_dict, readers, ip, in_csv, out_csv)
	logger.debug("end of main")

logger = setup_logging()
logger.debug("logging initiated")

import geoip2.database
logger.debug("imported geoip2.database")
main(logger)
logger.debug("exiting")
