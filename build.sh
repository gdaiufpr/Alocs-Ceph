#!/bin/bash
export LD_LIBRARY_PATH=/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server

rm -f InterfaceJava.class
rm -f IntervalTree.class
rm -f ClusterMappingEstructure.class
rm -f Auxiliary.class

rm -f *.o
rm -f *.so

javac -cp '.:jar/zookeeper-3.4.6.jar:jar/junit-4.11.jar:jar/log4j-1.2.16.jar:jar/slf4j-api-1.6.3.jar:jar/slf4j-log4j12-1.7.0.jar' Auxiliary.java
javac -cp '.:jar/zookeeper-3.4.6.jar:jar/junit-4.11.jar:jar/log4j-1.2.16.jar:jar/slf4j-api-1.6.3.jar:jar/slf4j-log4j12-1.7.0.jar' IntervalTree.java
javac -cp '.:jar/zookeeper-3.4.6.jar:jar/junit-4.11.jar:jar/log4j-1.2.16.jar:jar/slf4j-api-1.6.3.jar:jar/slf4j-log4j12-1.7.0.jar' ClusterMappingEstructure.java
javac -cp '.:jar/zookeeper-3.4.6.jar:jar/junit-4.11.jar:jar/log4j-1.2.16.jar:jar/slf4j-api-1.6.3.jar:jar/slf4j-log4j12-1.7.0.jar' InterfaceJava.java

gcc -c -Werror -fpic imds.c -I/usr/lib/jvm/java-8-oracle/include -I/usr/lib/jvm/java-8-oracle/include/linux -L/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server -ljvm util.c cache.c common.c ceph_lib.c iceph.c libalocs.c
gcc -shared  -o libalocs.so common.o libalocs.o iceph.o ceph_lib.o cache.o util.o imds.o

sudo rm -f /usr/lib/libalocs.so

sudo cp *.so /usr/lib/.
sudo cp *.h /usr/include/alocs/.
