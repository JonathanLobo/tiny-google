#! /bin/bash
clear
mkdir class
javac -verbose -classpath /opt/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar -d class src/*.java -source 1.7 -target 1.7
jar -cvf TinyGoogle.jar -C class/ .
hadoop jar TinyGoogle.jar TinyGoogle
