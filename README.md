## FS-perf

Programs to test performance of Apache Hadoop compatible filesystems (e.g. HDFS, Ozone, S3A, WASB). The package includes the following tests:

1. WriteFile: A write test that allows tweaking client parameters like replication, IO size, number of threads etc. A single thread should be able to saturate the network or single disk bandwidth.
1. ReadFile: A multi-threaded read IO test to benchmark reading files.
1. FsStress: A simplistic multi-threaded client that can issue metadata requests to a Apache Hadoop-Compatible File System (HCFS) to measure its metadata IOPS.

## Building

You will need Maven + JDK7 to build the benchmark source code. The command to build the jar is

    mvn clean package -DskipTests


## Running the Tests

### WriteFile

A single-threaded write test with a 100GB file can be started as follows:

    hadoop jar FsPerfTest-1.0-SNAPSHOT.jar net.arp7.FsPerfTest.WriteFile -s 100GB 

With two threads

    hadoop jar FsPerfTest-1.0-SNAPSHOT.jar net.arp7.FsPerfTest.WriteFile -s 100GB -t 2

Run without parameters to see full usage.

    hadoop jar FsPerfTest-1.0-SNAPSHOT.jar net.arp7.FsPerfTest.WriteFile


### ReadFile

Start 4 threads for 10 minutes to benchmark read performance by reading files under the `/myInput` directory.

    hadoop jar FsPerfTest-1.0-SNAPSHOT.jar net.arp7.FsPerfTest.ReadFile -d 10m -t 4 -i /myInput

Run without parameters to see full usage.

    hadoop jar FsPerfTest-1.0-SNAPSHOT.jar net.arp7.FsPerfTest.ReadFile
    
 ### FsStress

Usage:
```
  Usage: FsStress -t <runTime> -n NumThreads --enabledTests=....

   -t runTime     : Specify the runtime in seconds. Default is 300

   -n NumThreads  : Number of client threads. Default is 1

   --enabledTests=... : A comma-separated list of requests that are enabled.
                          Where tests can be one or more of:
                            CREATE, DELETE, MKDIRS, RENAME, STAT, GETCONTENTSUMMARY
                            or 'ALL' for everything (default); or 'READS'
                            for all read requests, 'WRITES' for all write requests
```

Example Usage:
Run test for 5 minutes, measuring FileSystem read performance with 4 client threads.
```
hadoop jar FsPerfTest-1.0-SNAPSHOT.jar net.arp7.FsPerfTest.FsStress -t 300 -n 4 --enabledTests=reads
```



*Apache®, Apache Hadoop, Hadoop®, and the yellow elephant logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.*
