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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;
import static java.lang.Math.abs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import static org.apache.hadoop.fs.CreateFlag.*;

public class WriteFile {
  // Valid numbers e.g. 65536, 64k, 64kb, 64K, 64KB etc.
  static final Pattern pattern = Pattern.compile("^(\\d+)([kKmMgGtT]?)[bB]?$");
  static final int BUFFER_SIZE = 64 * 1024;
  static final Random rand = new Random(System.nanoTime()/1000);

  static long blockSize = 128 * 1024 * 1024;
  static long fileSize = -1;
  static long numFiles = 1;
  static int ioSize = 64 * 1024;
  static long numThreads = 1;
  static boolean lazyPersist = false;
  static boolean hsync = false;
  static boolean hflush = false;
  static boolean verbose = false;
  static boolean throttle = false;

  private static final class Stats {
    public AtomicLong totalTime = new AtomicLong(0);
    public AtomicLong totalWriteTime = new AtomicLong(0);
  }

  public static void main (String[] args) throws Exception {
    parseArgs(args);
    validateArgs();

    final byte[] data = new byte[ioSize];
    Arrays.fill(data, (byte) 65);

    final Stats stats = new Stats();
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    final AtomicLong filesLeft = new AtomicLong(numFiles);

    // Start the writers.
    ExecutorService executor = Executors.newFixedThreadPool((int) numThreads);
    CompletionService<Object> ecs =
        new ExecutorCompletionService<>(executor);
    for (long t = 0; t < numThreads; ++t) {
      Callable<Object> c = new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          while (filesLeft.addAndGet(-1) >= 0) {
            writeOneFile(stats, conf, fs, data);
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

  static void writeOneFile(final Stats stats,
                           final Configuration conf,
                           final FileSystem fs,
                           final byte[] data)
  throws IOException, InterruptedException {
    final Path p = new Path("/WriteFile-" + abs(rand.nextInt()));

    long startTime = System.nanoTime();
    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, OVERWRITE);
    if (lazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }

    if (verbose) {
      System.out.println(" > Writing file " + p);
    }

    try (FSDataOutputStream os = fs.create(
            p, FsPermission.getFileDefault(),
            createFlags, BUFFER_SIZE, (short) 1, blockSize, null)) {

      long lastLoggedPercent = 0;
      long writeStartTime = System.nanoTime();
      for (int j = 0; j < fileSize / ioSize; ++j) {
        os.write(data, 0, data.length);

        if (hsync) {
          os.hsync();
        } else if (hflush) {
          os.hflush();
        }

        if (throttle) {
          Thread.sleep(300);
        }

        if (verbose) {
          long percentWritten = ((long) j * ioSize * 100) / fileSize;
          if (percentWritten > lastLoggedPercent) {
            System.out.println("  >> Wrote " + j * ioSize + "/" + fileSize + " [" + percentWritten + "%]");
            lastLoggedPercent = percentWritten;
          }
        }
      }

      long endTime = System.nanoTime();
      stats.totalTime.addAndGet((endTime - startTime) / (1000000));
      stats.totalWriteTime.addAndGet((endTime - writeStartTime) / 1000000);
    }
  }

  static private void writeStats(final Stats stats) {
    System.out.println("Total data written: " +
        FileUtils.byteCountToDisplaySize(fileSize));
    System.out.println("Mean Time per file: " +
        stats.totalTime.get() / numFiles + "ms");
    System.out.println("Mean Time to create file on NN: " +
        (stats.totalTime.get() - stats.totalWriteTime.get()) / numFiles + "ms");
    System.out.println("Mean Time to write data: " +
        (stats.totalTime.get() / numFiles + "ms"));
    System.out.println("Mean throughput: " +
      (stats.totalWriteTime.get() > 0 ? 
          ((numFiles * fileSize) / (stats.totalWriteTime.get())) : "Nan ") + "KBps");
  }

  static private void usageAndExit() {
    System.err.println(
        "\n  Usage: WriteFile -s <fileSize> [-b <blockSize>] [-n <numFiles>] " +
        "\n                   [-i <ioSize>] [--lazyPersist] [--hsync|hflush] " +
        "\n                   [-t <numThreads>] [--throttle] [--verbose]");
    System.err.println(
        "\n   -s fileSize   : Specify the file size. Must be specified.");
    System.err.println(
        "\n   -b blockSize  : Specify the block size. Default 128MB");
    System.err.println(
        "\n   -n numFiles   : Specify the number of Files. Default 1");
    System.err.println(
        "\n   -i ioSize     : Specify the io size. Default 64KB");
    System.err.println(
        "\n   -i numThreads : Number of writer threads. Default 1");
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
      } else if (args[argIndex].equalsIgnoreCase("-b")) {
        blockSize = parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-n")) {
        numFiles = parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-s")) {
        fileSize = parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-i")) {
        ioSize = parseReadableLong(args[++argIndex]).intValue();
      } else if (args[argIndex].equalsIgnoreCase("-t")) {
        numThreads = parseReadableLong(args[++argIndex]);
      } else {
        System.err.println("  Unknown option args[argIndex]");
        usageAndExit();
      }
      ++argIndex;
    }
  }

  static private void validateArgs() {
    if (fileSize == -1) {
      System.err.println("\n  The file size must be specified with -f");
      usageAndExit();
    }

    if (hsync && hflush) {
      System.err.println("\n  Cannot specify both --hsync and --hflush");
      usageAndExit();
    }

    if (numThreads < 1 || numThreads > 64) {
      System.err.println("\n  numThreads must be between 1 and 64 inclusive.");
    }

    if (fileSize < ioSize) {
      // Correctly handle small files.
      ioSize = (int) fileSize;
    }
  }

  // Parse a human readable long e.g. 65536, 64KB, 4MB, 4m, 1GB etc.
  static private Long parseReadableLong(String number) {
    Matcher matcher = pattern.matcher(number);
    if (matcher.find()) {
      long multiplier = 1;
      if (matcher.group(2).length() > 0) {
        switch (matcher.group(2).toLowerCase().charAt(0)) {
          case 't':           multiplier *= 1024L;
          case 'g':           multiplier *= 1024L;
          case 'm':           multiplier *= 1024L;
          case 'k':           multiplier *= 1024L;
        }
      }
      return Long.parseLong(matcher.group(1)) * multiplier;
    }
    throw new IllegalArgumentException("Unrecognized number format " + number);
  }
}

