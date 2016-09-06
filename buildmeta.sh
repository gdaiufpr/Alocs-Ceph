#!/bin/bash
export LD_LIBRARY_PATH=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server

rm -f InterfaceJava.class
rm -f IntervalTree.class
rm -f ClusterMappingEstructure.class
rm -f Auxiliary.class

javac -cp '.:jar/zookeeper-3.4.6.jar:jar/junit-4.11.jar:jar/log4j-1.2.16.jar:jar/slf4j-api-1.6.3.jar:jar/slf4j-log4j12-1.7.0.jar' Auxiliary.java
javac -cp '.:jar/zookeeper-3.4.6.jar:jar/junit-4.11.jar:jar/log4j-1.2.16.jar:jar/slf4j-api-1.6.3.jar:jar/slf4j-log4j12-1.7.0.jar' IntervalTree.java
javac -cp '.:jar/zookeeper-3.4.6.jar:jar/junit-4.11.jar:jar/log4j-1.2.16.jar:jar/slf4j-api-1.6.3.jar:jar/slf4j-log4j12-1.7.0.jar' ClusterMappingEstructure.java
javac -cp '.:jar/zookeeper-3.4.6.jar:jar/junit-4.11.jar:jar/log4j-1.2.16.jar:jar/slf4j-api-1.6.3.jar:jar/slf4j-log4j12-1.7.0.jar' InterfaceJava.java

gcc -o imds imds.c -I/usr/lib/jvm/java-8-oracle/include -I/usr/lib/jvm/java-8-oracle/include/linux -L/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server -ljvm
