import sys
import json
import os
from pyspark import SparkContext

# json = list of dict of dict in this input
# hello world in spark


def time_day(json):
	time_day="N/A"
	if json.has_key('source'):
		source = json['source']
		if source.has_key('istdt') : 
			time_day = int(source['istdt'].split()[-1].split(":")[0])
	return (source['istdt'], time_day)

if __name__ == "__main__":
	file_name = sys.argv[1]
	if not os.path.isfile(file_name) or not os.access(file_name, os.R_OK):
		print >> sys.stderr, "File '%s' does not exist or isn't accessible" % (file_name)
		exit(-1)
	sc = SparkContext("local[4]", "mopub_adlogs")
	lines = sc.textFile(file_name)
	list_dict = lines.map(lambda x: json.loads(x)).map(time_day)
	list_dict.collect() # will list all the required data that you wanted to convert 
		

