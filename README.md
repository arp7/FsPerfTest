## hdfs-perf

Micro benchmarks for HDFS/HCFS perf testing. The package includes the following two tests:

1. FsStress: A very simplistic multi-threaded client that can issue metadata requests to a Hadoop-Compatible File System (HCFS) to measure its metadata IOPS.
1. WriteFile: A multi-threaded write IO test that allows tweaking write parameters like replication, IO size, number of threads etc. A single thread should be able to saturate the network or single disk bandwidth (whichever is lower). This test is currently HDFS-specific.

## Building

You will need Maven + JDK7 to build the benchmark source code. The command to build the jar is

    mvn clean package -DskipTests


## Running the Tests

### FsStress

Usage:
```
  Usage: FsStress -t <runTime> -n NumThreads --enabledTests=....

   -t runTime     : Specify the runtime in seconds. Default is 300

   -n NumThreads  : Number of client threads. Default is 1

   --enabledThreads=... : A comma-separated list of requests that are enabled.
                          Where tests can be one or more of:
                            CREATE, DELETE, MKDIRS, RENAME, STAT, GETCONTENTSUMMARY
                            or 'ALL' for everything (default); or 'READS'
                            for all read requests, 'WRITES' for all write requests
```

Example Usage:
Run test for 5 minutes, measuring FileSystem read performance with 4 client threads.
```
hadoop jar HdfsPerfTest-1.0-SNAPSHOT.jar net.arp7.HdfsPerfTest.FsStress -t 300 -n 4 --enabledTests=reads
```


### WriteFile

A single-threaded write test with a 100GB file can be started as follows:

    hadoop jar HdfsPerfTest-1.0-SNAPSHOT.jar net.arp7.HdfsPerfTest.WriteFile -s 100GB 

Run without parameters to see usage.

    hadoop jar HdfsPerfTest-1.0-SNAPSHOT.jar net.arp7.HdfsPerfTest.WriteFile
