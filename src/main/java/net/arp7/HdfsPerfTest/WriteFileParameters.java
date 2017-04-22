/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.arp7.HdfsPerfTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


/**
 * Class to parse, validate and store the command line arguments for
 * the {@link WriteFile} benchmark.
 */
public class WriteFileParameters {
  public static final Logger LOG =
      LoggerFactory.getLogger(WriteFileParameters.class);

  private long blockSize;
  private long replication;
  private long fileSize = -1;
  private Path outputDir = new Path(Constants.DEFAULT_DIR);
  private File resultCsvFile = null;
  private long numFiles = Constants.DEFAULT_NUM_FILES;
  private int ioSize = Constants.DEFAULT_IO_LENGTH;
  private long numThreads = Constants.DEFAULT_THREADS;
  private boolean lazyPersist = false;
  private boolean hsync = false;
  private boolean hflush = false;
  private boolean throttle = false;
  private String note = "";

  WriteFileParameters(Configuration conf, String[] args) {
    initDefaultsFromConfiguration(conf);
    parse(conf, args);
  }

  private void parse(Configuration conf, String[] args) {
    int argIndex = 0;

    while(argIndex < args.length && args[argIndex].indexOf("-") == 0) {
      if (args[argIndex].equalsIgnoreCase("--lazyPersist")) {
        lazyPersist = true;
      } else if (args[argIndex].equalsIgnoreCase("--hsync")) {
        hsync = true;
      } else if (args[argIndex].equalsIgnoreCase("--hflush")) {
        hflush = true;
      } else if (args[argIndex].equalsIgnoreCase("--throttle")) {
        throttle = true;
      } else if (args[argIndex].equalsIgnoreCase("-s")) {
        fileSize = Utils.parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-b")) {
        blockSize = Utils.parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-r")) {
        replication = Utils.parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-n")) {
        numFiles = Utils.parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-i")) {
        ioSize = Utils.parseReadableLong(args[++argIndex]).intValue();
      } else if (args[argIndex].equalsIgnoreCase("-t")) {
        numThreads = Utils.parseReadableLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-o")) {
        outputDir = new Path(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("--resultCsv")) {
        resultCsvFile = new File(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("--resultNote")) {
        note = args[++argIndex];
      } else {
        System.err.println("  Unknown option " + args[argIndex]);
        usage();
        System.exit(1);
      }
      ++argIndex;
    }
    validate();
  }


  private static void usage() {
    System.err.println(
        "\n  Usage: WriteFile -s <fileSize> [-b <blockSize>] [-r replication]" +
            "\n                   [-n <numFiles>] [-i <ioSize>] [-t <numThreads>]" +
            "\n                   [-o OutputDir] [--lazyPersist] [--hsync|hflush]" +
            "\n                   [--resultCsv <file>] [--resultNote <note>]" +
            "\n                   [--throttle]");
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
        "\n   --resultCsv   : Appends benchmark results to specified CSV file. Optional.");
    System.err.println(
        "\n   --resultNote  : Note to include in result CSV file. Optional.");
    System.err.println(
        "\n   --throttle    : Adds artificial throttle. The rate of throttling " +
            "\n                   is not configurable. Optional.");
  }


  private void validate() {
    if (fileSize < 0) {
      System.err.println("\n  The file size must be specified with -s." +
          "\n  All other parameters are optional.\n");
      System.exit(1);
    }

    if (hsync && hflush) {
      System.err.println("\n  Cannot specify both --hsync and --hflush");
      WriteFileParameters.usage();
      System.exit(2);
    }

    if (numThreads < 1 || numThreads > 64) {
      System.err.println("\n  numThreads must be between 1 and 64 inclusive.");
    }

    if (fileSize < ioSize) {
      // Correctly handle small files.
      ioSize = (int) fileSize;
    }

    if (replication > Short.MAX_VALUE) {
      System.err.println("\n Replication factor " + replication +
          " is too high.");
      System.exit(2);
    }
  }


  /**
   * Initialize some write parameters from the configuration.
   *
   * @param conf
   */
  private void initDefaultsFromConfiguration(Configuration conf) {
    blockSize = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);

    replication = conf.getLong(
        DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
  }

  public static Logger getLOG() {
    return LOG;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public short getReplication() {
    return (short) replication;
  }

  public long getFileSize() {
    return fileSize;
  }

  public Path getOutputDir() {
    return outputDir;
  }

  public File getResultCsvFile() {
    return resultCsvFile;
  }

  public long getNumFiles() {
    return numFiles;
  }

  public int getIoSize() {
    return ioSize;
  }

  public long getNumThreads() {
    return numThreads;
  }

  public boolean isLazyPersist() {
    return lazyPersist;
  }

  public boolean isHsync() {
    return hsync;
  }

  public boolean isHflush() {
    return hflush;
  }

  public boolean isThrottle() {
    return throttle;
  }

  public String getNote() {
    return note;
  }
}
