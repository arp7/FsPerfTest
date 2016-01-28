/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.arp7.HdfsPerfTest;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.lang.Math.abs;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CreateFlag.*;

public class WriteFile {
  static final Logger LOG = LoggerFactory.getLogger(WriteFile.class);

  static final Random rand = new Random(System.nanoTime()/1000);

  static Optional<Long> blockSize = Optional.absent();
  static Optional<Long> replication = Optional.absent();
  static Optional<Long> fileSize = Optional.absent();
  static long numFiles = Constants.DEFAULT_NUM_FILES;
  static int ioSize = Constants.DEFAULT_IO_LENGTH;
  static long numThreads = Constants.DEFAULT_THREADS;
  static boolean lazyPersist = false;
  static boolean hsync = false;
  static boolean hflush = false;
  static boolean verbose = false;
  static boolean throttle = false;
  static Path outputDir = new Path(Constants.DEFAULT_DIR);

  public static void main (String[] args) throws Exception {
    final Configuration conf = new HdfsConfiguration();

    parseArgs(args);
    validateArgs(conf);

    final byte[] data = new byte[ioSize];
    Arrays.fill(data, (byte) 65);

    final FileIoStats stats = new FileIoStats();
    final FileSystem fs = FileSystem.get(conf);
    final AtomicLong filesLeft = new AtomicLong(numFiles);
    final long runId = abs(rand.nextLong());

    // Start the writers.
    ExecutorService executor = Executors.newFixedThreadPool((int) numThreads);
    CompletionService<Object> ecs =
        new ExecutorCompletionService<>(executor);
    LOG.info("NumFiles=" + numFiles +
        ", FileSize=" + FileUtils.byteCountToDisplaySize(fileSize.get()) +
        ", IoSize=" + FileUtils.byteCountToDisplaySize(ioSize) +
        ", BlockSize=" + FileUtils.byteCountToDisplaySize(blockSize.get()) +
        ", ReplicationFactor=" + replication.get());
    LOG.info("Starting " + numThreads + " writer thread" +
        (numThreads > 1 ? "s" : "") + ".");
    for (long t = 0; t < numThreads; ++t) {
      final long threadIndex = t; 
      Callable<Object> c = new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          long fileIndex = 0;
          while (filesLeft.addAndGet(-1) >= 0) {
            final String fileName = "WriteFile-" + runId +
                "-" + (threadIndex + 1) + "-" + (++fileIndex);
            final Path file = new Path(outputDir, fileName);
            writeOneFile(stats, fs, data, file);
          }
          return null;
        }
      };
      ecs.submit(c);
    }

    // And wait for all writers to complete.
    for (long t = 0; t < numThreads; ++t) {
      ecs.take();
    }

    executor.shutdown();
    writeStats(stats);
  }

  static void writeOneFile(final FileIoStats stats,
                           final FileSystem fs,
                           final byte[] data,
                           final Path file)
  throws IOException, InterruptedException {
    long startTime = System.nanoTime();
    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, OVERWRITE);
    if (lazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }

    LOG.info("Writing file " + file.toString());

    try (FSDataOutputStream os = fs.create(
        file, FsPermission.getFileDefault(), createFlags,
        Constants.BUFFER_SIZE,
        replication.get().shortValue(), blockSize.get(), null)) {

      long lastLoggedPercent = 0;
      long writeStartTime = System.nanoTime();
      for (long j = 0; j < fileSize.get() / ioSize; ++j) {
        os.write(data, 0, data.length);

        if (hsync) {
          os.hsync();
        } else if (hflush) {
          os.hflush();
        }

        if (throttle) {
          Thread.sleep(300);
        }

        if (LOG.isDebugEnabled()) {
          long percentWritten = (j * ioSize * 100) / fileSize.get();
          if (percentWritten > lastLoggedPercent) {
            LOG.debug("  >> Wrote " + j * ioSize + "/" +
                fileSize.get() + " [" + percentWritten + "%]");
            lastLoggedPercent = percentWritten;
          }
        }
      }

      long endTime = System.nanoTime();
      stats.totalTimeMs.addAndGet((endTime - startTime) / 1000000);
      stats.totalWriteTimeMs.addAndGet((endTime - writeStartTime) / 1000000);
      stats.filesWritten.addAndGet(1);
      stats.bytesWritten.addAndGet(fileSize.get());
    }
  }

  static private void writeStats(final FileIoStats stats) {
    LOG.info("Total files written: " + stats.filesWritten.get());
    LOG.info("Total data written: " +
        FileUtils.byteCountToDisplaySize(stats.bytesWritten.get()));
    LOG.info("Mean Time per file: " + stats.totalTimeMs.get() / numFiles + " ms");
    LOG.info("Mean Time to create file on NN: " +
        (stats.totalTimeMs.get() - stats.totalWriteTimeMs.get()) / numFiles + " ms");
    LOG.info("Mean Time to write data: " +
        (stats.totalTimeMs.get() / numFiles + "ms"));
    LOG.info("Mean throughput: " +
      (stats.totalWriteTimeMs.get() > 0 ? 
          ((numFiles * fileSize.get()) / (stats.totalWriteTimeMs.get())) : "Nan ") + "KBps");
  }

  static private void usageAndExit() {
    System.err.println(
        "\n  Usage: WriteFile -s <fileSize> [-b <blockSize>] [-r replication]" + 
        "\n                   [-n <numFiles>] [-i <ioSize>] [-t <numThreads>]" +
        "\n                   [-o OutputDir] [--lazyPersist] [--hsync|hflush]" +
        "\n                   [--throttle] [--verbose]");
    System.err.println(
        "\n   -s fileSize   : Specify the file size. Must be specified.");
    System.err.println(
        "\n   -b blockSize  : HDFS block size. Default is 'dfs.blocksize'");
    System.err.println(
        "\n   -r replication: Replication factor. Default is 'dfs.replication'");
    System.err.println(
        "\n   -n numFiles   : Specify the number of Files. Default is " + Constants.DEFAULT_NUM_FILES);
    System.err.println(
        "\n   -i ioSize     : Specify the io size. Default " + Constants.DEFAULT_IO_LENGTH);
    System.err.println(
        "\n   -t numThreads : Number of writer threads. Default " + Constants.DEFAULT_THREADS);
    System.err.println(
        "\n   -o outputDir  : Output Directory. Default " + Constants.DEFAULT_DIR);
    System.err.println(
        "\n   --lazyPersist : Sets CreateFlag.LAZY_PERSIST. Optional.");
    System.err.println(
        "\n   --hsync       : Optionally issues hsync after every write. Optional.");
    System.err.println(
        "\n                   Cannot be used with --hflush.");
    System.err.println(
        "\n   --hflush      : Optionally issues hflush after every write. Optional.");
    System.err.println(
        "\n                   Cannot be used with --hsync.");
    System.err.println(
        "\n   --throttle    : Adds artificial throttle. The rate of throttling " +
        "\n                   is not configurable. Optional.");
    System.err.println(
        "\n   --verbose     : Print verbose messages. Optional.");
    System.exit(1);
  }

  static private void parseArgs(String[] args) {
    int argIndex = 0;

    while(argIndex < args.length && args[argIndex].indexOf("-") == 0) {
      if (args[argIndex].equalsIgnoreCase("--lazyPersist")) {
        lazyPersist = true;
      } else if (args[argIndex].equalsIgnoreCase("--hsync")) {
        hsync = true;
      } else if (args[argIndex].equalsIgnoreCase("--hflush")) {
        hflush = true;
      } else if (args[argIndex].equalsIgnoreCase("--verbose")) {
        verbose = true;
      } else if (args[argIndex].equalsIgnoreCase("--throttle")) {
        throttle = true;
      } else if (args[argIndex].equalsIgnoreCase("-s")) {
        fileSize = Optional.of(Utils.parseReadableLong(args[++argIndex]));
      } else if (args[argIndex].equalsIgnoreCase("-b")) {
        blockSize = Optional.of(Utils.parseReadableLong(args[++argIndex]));
      } else if (args[argIndex].equalsIgnoreCase("-r")) {
        replication = Optional.of(Utils.parseReadableLong(args[++argIndex]));
      } else if (args[argIndex].equalsIgnoreCase("-n")) {
        numFiles = Utils.parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-i")) {
        ioSize = Utils.parseReadableLong(args[++argIndex]).intValue();
      } else if (args[argIndex].equalsIgnoreCase("-t")) {
        numThreads = Utils.parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-o")) {
        outputDir = new Path(args[++argIndex]);
      } else {
        System.err.println("  Unknown option " + args[argIndex]);
        usageAndExit();
      }
      ++argIndex;
    }
  }

  static private void validateArgs(Configuration conf) {
    if (!fileSize.isPresent()) {
      System.err.println("\n  The file size must be specified with -f." +
          "\n  All other parameters are optional.");
      usageAndExit();
    }

    if (hsync && hflush) {
      System.err.println("\n  Cannot specify both --hsync and --hflush");
      usageAndExit();
    }

    if (numThreads < 1 || numThreads > 64) {
      System.err.println("\n  numThreads must be between 1 and 64 inclusive.");
    }

    if (fileSize.get() < ioSize) {
      // Correctly handle small files.
      ioSize = fileSize.get().intValue();
    }
    
    if (!blockSize.isPresent()) {
      // No block size specified.
      blockSize = Optional.of(conf.getLong(
          DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
          DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT));
    }
    
    if (!replication.isPresent()) {
      // No replication factor specified.
      replication = Optional.of(conf.getLong(
          DFSConfigKeys.DFS_REPLICATION_KEY,
          DFSConfigKeys.DFS_REPLICATION_DEFAULT));
      
    }
    
    if (replication.get() > Short.MAX_VALUE) {
      System.err.println("\n The replication factor " +
          replication.get() + " is too high.");
      System.exit(2);
    }
  }
}
