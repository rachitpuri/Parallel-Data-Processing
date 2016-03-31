format:
	rm -rf /tmp/hadoop*/*
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver 

hstop:
	mr-jobhistory-daemon.sh stop historyserver 
	stop-yarn.sh
	stop-dfs.sh

adib:
	#hadoop fs -rm -r -f -skipTrash /user/adib
	#hadoop fs -rm -r -f -skipTrash /user/adib/input
	hadoop fs -mkdir -p /user/adib
	hadoop fs -mkdir -p /user/adib/input

compile:
	javac -cp "lib/*" ClusterAnalysis.java
	rm -rf job.jar
	rm -rf output
	jar cvf job.jar *.class

hdfs:
	hadoop fs -put all /user/adib/input/
	#hadoop fs -rm -r -f -skipTrash output
	#hadoop fs -rm -r -f -skipTrash /tmp
run:
	hadoop ClusterAnalysis /user/adib/input/all output
	hadoop fs -get output
	cat output/part* > finaloutput
	R < script.R --no-save
	xdg-open plot.png
	firefox report.html

pseudo: hstop format hstart adib compile hdfs run hstop

createcluster: 
	aws s3 rm s3://adibalwani/output --recursive
	aws s3 cp job.jar s3://adibalwani/
	aws emr create-cluster \
    --name "CLI Test Cluster" \
    --release-label emr-4.3.0 \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge \
--steps Type=CUSTOM_JAR,Name="CLI Test JAR Step",ActionOnFailure=CONTINUE,Jar=s3://adibalwani/job.jar,MainClass=ClusterAnalysis,Args=[s3://adibalwani/input,s3://adibalwani/output] \
    --auto-terminate \
    --log-uri s3://adibalwani/log \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a\
    --enable-debugging > clusterId.txt

waitforcompletion:
	python checkstatus.py

gets3data:
	aws s3 cp s3://adibalwani/output result --recursive
	cat result/part* > finaloutput
	R < script.R --no-save
	xdg-open plot.png
	firefox report.html

emr: compile createcluster waitforcompletion gets3data

