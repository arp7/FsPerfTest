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

package net.arp7.FsPerfTest;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.abs;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

/**
 * A utility for benchmarking HDFS write performance with a multi-threaded
 * client. See {@link #usage} for all the supported options.
 */
public class WriteFile {
  private static final Logger LOG = LoggerFactory.getLogger(WriteFile.class);
  private static WriteFileParameters params;

  private static final Random rand = new Random(System.nanoTime());


  public static void main(String[] args) throws Exception {
    final Configuration conf = new HdfsConfiguration();
    params = new WriteFileParameters(conf, args);
    final FileIoStats stats = new FileIoStats();
    writeFiles(conf, stats);
    writeStats(stats);
    writeCsvResult(stats);
  }

  private static void writeFiles(final Configuration conf, final FileIoStats stats)
      throws InterruptedException, IOException {
    final FileSystem fs = FileSystem.get(conf);
    final AtomicLong filesLeft = new AtomicLong(params.getNumFiles());
    final long runId = abs(rand.nextLong());
    final byte[] data = new byte[params.getIoSize()];
    Arrays.fill(data, (byte) 65);

    // Start the writers.
    final ExecutorService executor = Executors.newFixedThreadPool(
        (int) params.getNumThreads());
    final CompletionService<Object> ecs =
        new ExecutorCompletionService<>(executor);
    LOG.info("NumFiles=" + params.getNumFiles() +
        ", FileSize=" + FileUtils.byteCountToDisplaySize(params.getFileSize()) +
        ", IoSize=" + FileUtils.byteCountToDisplaySize(params.getIoSize()) +
        ", BlockSize=" + FileUtils.byteCountToDisplaySize(params.getBlockSize()) +
        ", ReplicationFactor=" + params.getReplication() +
        ", isThrottled=" + (params.maxWriteBps() > 0));
    LOG.info("Starting " + params.getNumThreads() + " writer thread" +
        (params.getNumThreads() > 1 ? "s" : "") + ".");
    final long startTime = System.nanoTime();
    ensureOutputDirExists(fs, params.getOutputDir());
    for (long t = 0; t < params.getNumThreads(); ++t) {
      final long threadIndex = t;
      Callable<Object> c = new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          long fileIndex = 0;
          while (filesLeft.addAndGet(-1) >= 0) {
            final String fileName = String.format("WriteFile-%d-%04d-%08d",
                runId, (threadIndex + 1), (++fileIndex));
            writeOneFile(new Path(params.getOutputDir(), fileName),
                fs, data, stats);
          }
          return null;
        }
      };
      ecs.submit(c);
    }

    // And wait for all writers to complete.
    Utils.joinAll(ecs, (int) params.getNumThreads(), LOG);
    final long endTime = System.nanoTime();
    stats.setElapsedTime(endTime - startTime);
    executor.shutdown();
  }

  /**
   * Create the output directory for this test run.
   */
  private static void ensureOutputDirExists(FileSystem fs, Path outputDir)
      throws IOException {
    if (!fs.mkdirs(outputDir) && !fs.isDirectory(outputDir)) {
      LOG.error("Failed to create output directory {}", outputDir);
      System.exit(1);
    }
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
    if (params.isLazyPersist()) {
      createFlags.add(LAZY_PERSIST);
    }

    LOG.info("Writing file " + file.toString());
    final FSDataOutputStream os = fs.create(
        file, FsPermission.getFileDefault(), createFlags,
        Constants.BUFFER_SIZE, params.getReplication(),
        params.getBlockSize(), null);
    final long createEndTime = System.nanoTime();
    stats.addCreateTime(createEndTime - startTime);
    final boolean isThrottled = params.maxWriteBps() > 0;
    final long expectedIoTimeNs = 
        (isThrottled ? (((long) data.length * 1_000_000_000) / params.maxWriteBps())
            : 0);

    try {
      long lastLoggedPercent = 0;
      long writeStartTime = System.nanoTime();
      for (long j = 0; j < params.getFileSize() / params.getIoSize(); ++j) {
        final long ioStartTimeNs = (isThrottled ? System.nanoTime() : 0);
        os.write(data, 0, data.length);

        if (params.isHsync()) {
          os.hsync();
        } else if (params.isHflush()) {
          os.hflush();
        }

        final long ioEndTimeNs = (isThrottled ? System.nanoTime() : 0);
        Utils.enforceThrottle(ioEndTimeNs - ioStartTimeNs, expectedIoTimeNs);

        if (LOG.isDebugEnabled()) {
          long percentWritten =
              (j * params.getIoSize() * 100) / params.getFileSize();
          if (percentWritten > lastLoggedPercent) {
            LOG.debug("  >> Wrote " + j * params.getIoSize() + "/" +
                params.getFileSize() + " [" + percentWritten + "%]");
            lastLoggedPercent = percentWritten;
          }
        }
      }

      final long writeEndTime = System.nanoTime();
      stats.addWriteTime(writeEndTime - writeStartTime);
      stats.incrFilesWritten();
      stats.incrBytesWritten(params.getFileSize());
    } catch(Throwable t) {
      LOG.error("write thread got exception ", t);
      throw t;
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
    LOG.info("Total elapsed time: " + Utils.formatNumber(stats.getElapsedTimeMs()) +
        " ms");
    long throughput = 0;
    if (stats.getElapsedTimeMs() > 0) {
      throughput = (params.getNumFiles() * params.getFileSize()) /
          stats.getElapsedTimeMs();
    }
    LOG.info("Aggregate throughput: " + Utils.formatNumber(throughput) + " KBps");
  }

  private static void writeCsvResult(final FileIoStats stats) {
    if (params.getResultCsvFile() == null) {
      return;
    }

    final Object[] results = new Object[] {
        new Date().toGMTString(),
        params.getNumFiles(),
        params.getNumThreads(),
        params.getReplication(),
        params.getBlockSize(),
        params.getIoSize(),
        stats.getFilesWritten(),
        stats.getBytesWritten(),
        stats.getMeanCreateTimeMs(),
        stats.getMeanWriteTimeMs(),
        stats.getMeanCloseTimeMs(),
        stats.getElapsedTimeMs(),
        (params.getFileSize() * 1000) / stats.getElapsedTimeMs(),
        (params.getNumFiles() * params.getFileSize() * 1000) /
            stats.getElapsedTimeMs(),
        params.getNote()
    };

    final CsvSchema schema = CsvSchema.builder()
        .setColumnSeparator(';')
        .setQuoteChar('"')
        .setUseHeader(!params.getResultCsvFile().exists())
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

    try (FileWriter fileWriter = new FileWriter(params.getResultCsvFile(), true)) {
      final CsvMapper mapper = new CsvMapper();
      final ObjectWriter writer = mapper.writer(schema);
      writer.writeValue(fileWriter, results);
    } catch (IOException e) {
      LOG.error("Could not write results to CSV file '{}': '{}'",
          params.getResultCsvFile().getPath(), e.getMessage());
    }
  }
}
