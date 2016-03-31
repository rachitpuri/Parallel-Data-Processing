from subprocess import call
import os
import gzip
import re

def getTime(filename, job):
	string = filename
	occur = 2  # on which occourence you want to split
	print filename
	indices = [x.start() for x in re.finditer("/", string)]
	part1 = string[0:indices[occur-1]]
	filetime = part1 + "/syslog.gz"
	start = ""
	end = ""
	#print(filetime)
	with gzip.open(filetime, 'r') as fd:
		for line in fd:
			if "map 0%" in line:
				words = line.split(' ')
				start = words[1]
				#print(words[1])
			if "reduce 100%" in line:
				words = line.split(' ')
				end = words[1]
				#print(words[1])

	comma = 1
	index = [x.start() for x in re.finditer(",", str(start))]
	t1 = start[0:index[comma-1]]
	starttime = str(t1).split(':')

	index2 = [x.start() for x in re.finditer(",", str(end))]
	t2 = end[0:index2[comma-1]]
	endtime = str(t2).split(':')			

	#print(starttime)
	#print(endtime)
	timetaken = ((int(endtime[0]) - int(starttime[0]))*60*60 + (int(endtime[1])*60 - int(starttime[1])*60) + (int(endtime[2]) - int(starttime[2])))
	print(timetaken)
	f = open("timing.txt", "a+")
	f.write(job + " " +str(timetaken) + "\n")
	f.close()
	# with open('timing.txt', 'r') as fconsole:
	# 	for line in fconsole:
	# 		if "Mean" in line:
	# 			words = line.split(' ')
	# 			call(["echo", words[1]])


def getFileData(filename):
	#print (filename)
	with gzip.open(filename, 'r') as myfile:
		for line in myfile:
			if "jobMean.jar" in line:
				#print(filename)
				getTime(filename, "Mean")

			if "jobMedian.jar" in line:
				#print(filename)
				getTime(filename, "Median")

			if "jobFastMedian.jar" in line:
				getTime(filename, "FastMedian")	

			break

def scan_dir(dir):
	for name in os.listdir(dir):
		path = os.path.join(dir, name)
		if os.path.isfile(path):
			getFileData(path)
		else:
			scan_dir(path)

with open('clusterId.txt') as fp:
	for i, line in enumerate(fp):
		if i == 1:
			words = line.split(':')
			clusterId = words[1]
			clusterId = clusterId[2:-2]
			#print(clusterId)
			# loop
			call(["aws", "s3", "cp", "s3://adibalwani/log/"+clusterId+"/steps", "EmrData", "--recursive"])
			scan_dir("EmrData")
				


