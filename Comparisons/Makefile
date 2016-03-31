#!/bin/bash
# Author: Adib Alwani

compile: compilecluster
	javac ThreadedAnalysis.java

jar:
	sbt package
	cp target/scala-*/job_*.jar job.jar

runscala: jar
	rm -rf output
	sbt "run 3 all output"
	#cat output/part* > finaloutput
	#R < script_scala.R --no-save
	#echo -n "MEAN,SCALA,">> benchmark.csv

run: compile header mean median fastmedian  emr r cleanup

header:
	echo "MODE,PLATFORM,TIME" > benchmark.csv

mean:
	echo -n "MEAN,SINGLE_THREADED,">> benchmark.csv
	java ThreadedAnalysis 1 -input=all > output.txt
	head -n 10 output.txt
	cat output.txt | tail -n 1 >> benchmark.csv
	echo -n "MEAN,MULTI_THREADED,">> benchmark.csv
	java ThreadedAnalysis -p 1 -input=all > output.txt
	head -n 10 output.txt
	cat output.txt | tail -n 1 >> benchmark.csv
	

median:
	echo -n "MEDIAN,SINGLE_THREADED," >> benchmark.csv
	java ThreadedAnalysis 2 -input=all > output.txt
	head -n 10 output.txt
	cat output.txt | tail -n 1 >> benchmark.csv
	echo -n "MEDIAN,MULTI_THREADED,">> benchmark.csv
	java ThreadedAnalysis -p 2 -input=all > output.txt
	head -n 10 output.txt
	cat output.txt | tail -n 1 >> benchmark.csv

fastmedian:
	echo -n "FAST_MEDIAN,SINGLE_THREADED," >> benchmark.csv
	java ThreadedAnalysis 3 -input=all > output.txt
	head -n 10 output.txt
	cat output.txt | tail -n 1 >> benchmark.csv
	echo -n "FAST_MEDIAN,MULTI_THREADED,">> benchmark.csv
	java ThreadedAnalysis -p 3 -input=all > output.txt
	head -n 10 output.txt
	cat output.txt | tail -n 1 >> benchmark.csv

r:
	R < script.R --no-save
	xdg-open Rplots.pdf

cleanup:
	rm -rf output.txt

clean:
	rm -rf *.class *.csv project target *.jar out
	make clusterclean

###########################################################################

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
	hadoop fs -mkdir -p /user/adib
	hadoop fs -mkdir -p /user/adib/input

compileFastMedian:
	javac -cp "lib/*" ClusterAnalysisFastMedian.java
	rm -rf jobFastMedian.jar
	rm -rf outputFastMedian
	jar cvf jobFastMedian.jar ClusterAnalysisFastMedian*.class

compileMedian:
	javac -cp "lib/*" ClusterAnalysisMedian.java
	rm -rf jobMedian.jar
	rm -rf outputMedian
	jar cvf jobMedian.jar ClusterAnalysisMedian*.class

compileMean:
	javac -cp "lib/*" ClusterAnalysisMean.java
	rm -rf jobMean.jar
	rm -rf outputMean
	jar cvf jobMean.jar ClusterAnalysisMean*.class

compilecluster: compileMedian compileMean compileFastMedian

hdfs:
	hadoop fs -put all /user/adib/input/

runClusterMean:
	hadoop ClusterAnalysisMean /user/adib/input/all outputMean
	hadoop fs -get outputMean
	cat outputMean/part* > finaloutputMean
	echo -n "MEAN,PSEUDO_DISTRUBUTED,">> benchmark.csv
	cat finaloutputMean | sort -r -k3 -n | head -n 10 | awk '{print $2 " " $1 " " $4}'

runClusterMedian:
	hadoop ClusterAnalysisMedian /user/adib/input/all outputMedian
	hadoop fs -get outputMedian
	cat outputMedian/part* > finaloutputMedian
	echo -n "MEDIAN,PSEUDO_DISTRUBUTED,">> benchmark.csv
	cat finaloutputMedian | sort -r -k3 -n | head -n 10 | awk '{print $2 " " $1 " " $4}'

runClusterFastMedian:
	hadoop ClusterAnalysisFastMedian /user/adib/input/all outputFastMedian
	hadoop fs -get outputFastMedian
	cat outputFastMedian/part* > finaloutputFastMedian
	echo -n "FAST_MEDIAN,PSEUDO_DISTRUBUTED,">> benchmark.csv
	cat finaloutputFastMedian | sort -r -k3 -n | head -n 10 | awk '{print $2 " " $1 " " $4}'

runlocalcluster:
	runClusterMean runClusterMedian runClusterFastMedian

pseudo: hstop format hstart adib compile hdfs run hstop

createcluster:
	aws s3 rm s3://adibalwani/outputMedian --recursive
	aws s3 rm s3://adibalwani/outputMean --recursive
	aws s3 rm s3://adibalwani/outputFastMean --recursive
	aws s3 cp jobMean.jar s3://adibalwani/
	aws s3 cp jobMedian.jar s3://adibalwani/
	aws s3 cp jobFastMedian.jar s3://adibalwani/
	aws emr create-cluster \
    --name "CLI Test Cluster" \
    --release-label emr-4.3.0 \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium \
--steps file://./jar.json \
    --auto-terminate \
    --log-uri s3://adibalwani/log \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a\
    --enable-debugging > clusterId.txt

waitforcompletion:
	python checkstatus.py

gettiming:
	python testemr.py

gets3data:
	aws s3 cp s3://adibalwani/outputMean resultMean --recursive
	aws s3 cp s3://adibalwani/outputMedian resultMedian --recursive
	aws s3 cp s3://adibalwani/outputFastMedian resultFastMedian --recursive
	cat resultMean/part* > finaloutputMean
	cat resultMedian/part* > finaloutputMedian
	cat resultFastMedian/part* > finaloutputFastMedian
	echo -n "MEAN,DISTRUBUTED,">> benchmark.csv
	cat finaloutputMean | sort -r -k3 -n | head -n 10 | awk '{print $2 " " $1 " " $4}' > output.txt
	head -n 10 output.txt
	cat timing.txt | sort -k1 | awk '{print $2}' | sed '1!d' >> benchmark.csv
	echo -n "MEDIAN,DISTRUBUTED,">> benchmark.csv
	cat finaloutputMedian | sort -r -k3 -n | head -n 10 | awk '{print $2 " " $1 " " $4}' > output.txt
	head -n 10 output.txt
	cat timing.txt | sort -k1 | awk '{print $2}' | sed '3!d' >> benchmark.csv
	echo -n "FAST_MEDIAN,DISTRUBUTED,">> benchmark.csv
	cat finaloutputFastMedian | sort -r -k3 -n | head -n 10 | awk '{print $2 " " $1 " " $4}' > output.txt
	head -n 10 output.txt
	cat timing.txt | sort -k1 | awk '{print $2}' | sed '2!d' >> benchmark.csv
	cat output.txt | tail -n 1 >> benchmark.csv
	
	
	#R < script.R --no-save
	#xdg-open plot.png

emr: createcluster waitforcompletion gettiming gets3data

clusterclean:
	rm -rf result *.class plot.png Rplots.pdf health.json final* clusterId.txt job*.jar
	rm -rf ClusterAnalysis*.jar report.html report.Rmd finaloutput* timing.txt result*
