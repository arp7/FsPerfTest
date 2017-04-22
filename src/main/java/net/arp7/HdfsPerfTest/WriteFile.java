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
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.lang.Math.abs;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CreateFlag.*;

/**
 * A utility for benchmarking HDFS write performance with a multi-threaded
 * client. See {@link #usage} for all the supported options. 
 */
public class WriteFile {
  private static final Logger LOG = LoggerFactory.getLogger(WriteFile.class);

  private static final Random rand = new Random(System.nanoTime());
  private static long blockSize;
  private static long replication;
  private static long fileSize = -1;
  private static Path outputDir = new Path(Constants.DEFAULT_DIR);
  private static File resultCsvFile = null;
  private static long numFiles = Constants.DEFAULT_NUM_FILES;
  private static int ioSize = Constants.DEFAULT_IO_LENGTH;
  private static long numThreads = Constants.DEFAULT_THREADS;
  private static boolean lazyPersist = false;
  private static boolean hsync = false;
  private static boolean hflush = false;
  private static boolean throttle = false;
  private static String note = "";
  private static final Locale locale = Locale.getDefault();

  public static void main(String[] args) throws Exception {
    final Configuration conf = new HdfsConfiguration();

    initDefaultsFromConfiguration(conf);
    parseArgs(args);
    validateArgs();

    final FileIoStats stats = new FileIoStats();
    writeFiles(conf, stats);
    writeStats(stats);
    if (resultCsvFile != null) {
      writeCsvResult(stats);
    }
  }

  private static void writeFiles(final Configuration conf, final FileIoStats stats)
      throws InterruptedException, IOException {
    final FileSystem fs = FileSystem.get(conf);
    final AtomicLong filesLeft = new AtomicLong(numFiles);
    final long runId = abs(rand.nextLong());
    final byte[] data = new byte[ioSize];
    Arrays.fill(data, (byte) 65);
    
    // Start the writers.
    ExecutorService executor = Executors.newFixedThreadPool((int) numThreads);
    CompletionService<Object> ecs =
        new ExecutorCompletionService<>(executor);
    LOG.info("NumFiles=" + numFiles +
        ", FileSize=" + FileUtils.byteCountToDisplaySize(fileSize) +
        ", IoSize=" + FileUtils.byteCountToDisplaySize(ioSize) +
        ", BlockSize=" + FileUtils.byteCountToDisplaySize(blockSize) +
        ", ReplicationFactor=" + replication);
    LOG.info("Starting " + numThreads + " writer thread" +
        (numThreads > 1 ? "s" : "") + ".");
    final long startTime = System.nanoTime();
    for (long t = 0; t < numThreads; ++t) {
      final long threadIndex = t;
      Callable<Object> c = new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          long fileIndex = 0;
          while (filesLeft.addAndGet(-1) >= 0) {
            final String fileName = "WriteFile-" + runId +
                "-" + (threadIndex + 1) + "-" + (++fileIndex);
            writeOneFile(new Path(outputDir, fileName), fs, data, stats);
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
    final long endTime = System.nanoTime();
    stats.setElapsedTime(endTime - startTime);
    executor.shutdown();
  }


  /**
   * Write a single file to HDFS.
   *
   * @param file
   * @param fs
   * @param data
   * @param stats object to accumulate write stats.
   * @throws IOException
   * @throws InterruptedException
   */
  private static void writeOneFile(
      final Path file, final FileSystem fs, final byte[] data,
      final FileIoStats stats) throws IOException, InterruptedException {

    final long startTime = System.nanoTime();
    final EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, OVERWRITE);
    if (lazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }

    LOG.info("Writing file " + file.toString());
    final FSDataOutputStream os = fs.create(
        file, FsPermission.getFileDefault(), createFlags,
        Constants.BUFFER_SIZE,
        (short) replication, blockSize, null);
    final long createEndTime = System.nanoTime();
    stats.addCreateTime(createEndTime - startTime);
    
    try {
      long lastLoggedPercent = 0;
      long writeStartTime = System.nanoTime();
      for (long j = 0; j < fileSize / ioSize; ++j) {
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
          long percentWritten = (j * ioSize * 100) / fileSize;
          if (percentWritten > lastLoggedPercent) {
            LOG.debug("  >> Wrote " + j * ioSize + "/" +
                fileSize + " [" + percentWritten + "%]");
            lastLoggedPercent = percentWritten;
          }
        }
      }

      final long writeEndTime = System.nanoTime();
      stats.addWriteTime(writeEndTime - writeStartTime);
      stats.incrFilesWritten();
      stats.incrBytesWritten(fileSize);
    } finally {
      final long closeStartTime = System.nanoTime();
      os.close();
      final long closeEndTime = System.nanoTime();
      stats.addCloseTime(closeEndTime - closeStartTime);
    }
  }

  static private void writeStats(final FileIoStats stats) {
    LOG.info("Total files written: " + stats.getFilesWritten());
    LOG.info("Total data written: " +
        FileUtils.byteCountToDisplaySize(stats.getBytesWritten()));
    LOG.info("Mean Time to create each file on NN: " +
        String.format("%.2f", stats.getMeanCreateTimeMs()) + " ms");
    LOG.info("Mean Time to write each file: " +
        String.format("%.2f", stats.getMeanWriteTimeMs()) + " ms");
    LOG.info("Mean Time to close each file: " +
        String.format("%.2f", stats.getMeanCloseTimeMs()) + " ms");
    LOG.info("Total elapsed time: " + formatNumber(stats.getElapsedTimeMs()) +
        " ms");
    long throughput = 0;
    if (stats.getElapsedTimeMs() > 0) {
      throughput = (numFiles * fileSize) / stats.getElapsedTimeMs();
    }
    LOG.info("Aggregate throughput: " + formatNumber(throughput) + " KBps");
  }

  private static void writeCsvResult(final FileIoStats stats) {
    final Object[] results = new Object[] {
        new Date().toGMTString(),
        numFiles,
        numThreads,
        replication,
        blockSize,
        ioSize,
        stats.getFilesWritten(),
        stats.getBytesWritten(),
        stats.getMeanCreateTimeMs(),
        stats.getMeanWriteTimeMs(),
        stats.getMeanCloseTimeMs(),
        stats.getElapsedTimeMs(),
        (fileSize * 1000) / stats.getElapsedTimeMs(),
        (numFiles * fileSize * 1000) / stats.getElapsedTimeMs(),
        note
    };

    final CsvSchema schema = CsvSchema.builder()
        .setColumnSeparator(';')
        .setQuoteChar('"')
        .setUseHeader(!resultCsvFile.exists())
        .addColumn("timestamp", CsvSchema.ColumnType.STRING)
        .addColumn("number of files", CsvSchema.ColumnType.NUMBER)
        .addColumn("number of threads", CsvSchema.ColumnType.NUMBER)
        .addColumn("replication factor", CsvSchema.ColumnType.NUMBER)
        .addColumn("block size", CsvSchema.ColumnType.NUMBER)
        .addColumn("io size", CsvSchema.ColumnType.NUMBER)
        .addColumn("total files written", CsvSchema.ColumnType.NUMBER)
        .addColumn("total bytes written", CsvSchema.ColumnType.NUMBER)
        .addColumn("mean time to create file in ms", CsvSchema.ColumnType.NUMBER)
        .addColumn("mean time to write file in ms", CsvSchema.ColumnType.NUMBER)
        .addColumn("mean time to close file in ms", CsvSchema.ColumnType.NUMBER)
        .addColumn("total ms", CsvSchema.ColumnType.NUMBER)
        .addColumn("mean throughput bytes per s", CsvSchema.ColumnType.NUMBER)
        .addColumn("aggregate throughput bytes per s", CsvSchema.ColumnType.NUMBER)
        .addColumn("note", CsvSchema.ColumnType.STRING)
        .build();

    try (FileWriter fileWriter = new FileWriter(resultCsvFile, true)) {
      final CsvMapper mapper = new CsvMapper();
      final ObjectWriter writer = mapper.writer(schema);
      writer.writeValue(fileWriter, results);
    } catch (IOException e) {
      LOG.error("Could not write results to CSV file '{}': '{}'", resultCsvFile.getPath(), e.getMessage());
    }
  }

  /**
   * Pretty print the supplied number.
   *
   * @param number
   * @return
   */
  static private String formatNumber(long number) {
    return NumberFormat.getInstance(locale).format(number);
  }

  static private void usage() {
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

  static private void parseArgs(String[] args) {
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
  }

  /**
   * Initialize some write parameters from the configuration.
   *
   * @param conf
   */
  private static void initDefaultsFromConfiguration(Configuration conf) {
    blockSize = conf.getLong(
        DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);

    replication = conf.getLong(
        DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
  }
  
  static private void validateArgs() {
    if (fileSize < 0) {
      System.err.println("\n  The file size must be specified with -s." +
          "\n  All other parameters are optional.\n");
      System.exit(1);
    }

    if (hsync && hflush) {
      System.err.println("\n  Cannot specify both --hsync and --hflush");
      usage();
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
}
