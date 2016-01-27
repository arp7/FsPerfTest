hdfs-perf
=========

Micro benchmarks for HDFS perf testing. The package includes the following two tests:

1. WriteFile: A multi-threaded write IO test that allows tweaking write parameters like replication, IO size, number of threads etc. A single thread should be able to saturate the network or single disk bandwidth (whichever is lower).
1. ReadFile: A read IO test. Work in progress.

Building
========

You will need Maven + JDK7 to build the benchmark source code. The command to build the jar is

    mvn clean package -DskipTests


Running the Tests
=================

A single-threaded write test with a 100GB file can be started as follows:

    hadoop jar HdfsPerfTest-1.0-SNAPSHOT.jar net.arp7.HdfsPerfTest.WriteFile -s 100GB 

Run without parameters to see usage.

    hadoop jar HdfsPerfTest-1.0-SNAPSHOT.jar net.arp7.HdfsPerfTest.WriteFile
