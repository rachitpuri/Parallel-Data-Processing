from subprocess import call
import json
import sys
import time

#f = open("health.json", "w+")
with open('clusterId.txt') as fp:
	for i, line in enumerate(fp):
		if i == 1:
			words = line.split(':')
			clusterId = words[1]
			clusterId = clusterId[2:-2]
			print(clusterId)
			# loop
			while 1:
				#f.close()
				f = open("health.json", "w+")
				time.sleep(30)
				call(["aws", "emr", "describe-cluster", "--cluster-id", clusterId],  stdout=f)
				f.close()
				with open('health.json', 'r') as myfile:
				    string=myfile.read().replace('\n', '')
				data = json.loads(string)
				status = data["Cluster"]["Status"]["State"]
				if status == "TERMINATED":
					sys.exit()
				time.sleep(30)
				print ("Waiting for cluster to finish the job")
				f.close()
				#print(data["Cluster"]["Status"]["State"])
				#print (data)
				#aws emr describe-cluster --cluster-id clusterId
