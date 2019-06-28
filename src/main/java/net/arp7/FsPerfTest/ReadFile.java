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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command()
public class ReadFile implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ReadFile.class);

  @Option(names = {"-i", "--inputdir"},
      required = true,
      description = "Input directory for reading files. This data can" +
          " be generated using WriteFile.")
  String inputDir;

  @Option(names = {"-d", "--duration"},
      required = true,
      converter = ReadableTimestampConverter.class,
      description = "Runtime. Can be specified in seconds, minutes or hours " +
          "using the s, m or h suffixes respectively. Default unit is seconds.")
  long runtime;

  @Option(names = {"-t", "--threads"},
      required = false,
      defaultValue = "1",
      showDefaultValue = ALWAYS,
      converter = ThreadCountConverter.class,
      description = "Number of reader threads.")
  int numThreads;

  @Option(names = {"-s", "--iosize"},
      required = false,
      defaultValue = "64KB",
      showDefaultValue = ALWAYS,
      converter = ReadableIntegerConverter.class,
      description = "Length of each read IO.")
  int ioSize;


  @Override
  public void run() {
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      startReaders(fs);
    } catch (IOException | InterruptedException e) {
      LOG.error("Failed with exception", e);
    }
  }

  /**
   * Kick off the reader threads. Each thread will run for the specified
   * time duration, unless it it terminates due to an exception.
   *
   * @param conf
   * @param fs
   * @throws IOException
   * @throws InterruptedException
   */
  private void startReaders(final FileSystem fs)
      throws IOException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(
        numThreads);
    final CompletionService<Object> ecs =
        new ExecutorCompletionService<>(executor);
    final FileIoStats stats = new FileIoStats();

    final List<LocatedFileStatus> inputFiles =
        Utils.getInputFilesListing(fs, new Path(inputDir));
    LOG.info("Found {} files in input directory {}",
        inputFiles.size(), inputDir);

    LOG.info("Starting " + numThreads + " reader thread" +
        (numThreads > 1 ? "s" : "") + ".");

    final long startTimeNs = System.nanoTime();
    final long timeLeftInNs = runtime * 1_000_000_000;

    for (int t = 0; t < numThreads; ++t) {
      final int threadIndex = t;
      Callable<Object> c = new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          long currentTimeNs = System.nanoTime();
          byte[] buffer = new byte[ioSize];

          while (currentTimeNs - startTimeNs < timeLeftInNs) {
            final LocatedFileStatus fileToRead = inputFiles.get(
                ThreadLocalRandom.current().nextInt(inputFiles.size()));
            readOneFile(
                fileToRead.getPath(), fs, buffer, threadIndex, stats);
            currentTimeNs = System.nanoTime();
          }
          return null;
        }
      };
      ecs.submit(c);
    }

    Utils.joinAll(ecs, numThreads, LOG);
    stats.setElapsedTime(System.nanoTime() - startTimeNs);
    printStats(stats);
    executor.shutdown();
  }

  private void printStats(FileIoStats stats) {
    LOG.info("Total files read: " + stats.getFilesRead());
    LOG.info("Total data read: " +
        FileUtils.byteCountToDisplaySize(stats.getBytesRead()));
    LOG.info("Mean Time to open each file: " +
        String.format("%.2f", stats.getMeanOpenTimeMs()) + " ms");
    LOG.info("Total elapsed time: " +
        Utils.formatNumber(stats.getElapsedTimeMs()) + " ms");
    long throughput = 0;
    if (stats.getElapsedTimeMs() > 0) {
      throughput = (stats.getBytesRead()) / stats.getElapsedTimeMs();
    }
    LOG.info("Aggregate read throughput: " +
        Utils.formatNumber(throughput) + " KBps");
  }

  /**
   * Read one file completely from the target file system.
   *
   * @param lfs
   * @param fs
   * @param buffer
   * @throws IOException
   */
  private void readOneFile(
      Path path, FileSystem fs, byte[] buffer, int threadIndex,
      FileIoStats stats)
      throws IOException {
    LOG.debug("Thread {}: Reading file {}", fs);

    long openStart = System.nanoTime();
    try (final FSDataInputStream in = fs.open(path)) {
      long readStart = System.nanoTime();
      // Read the file through the end.
      long bytesRead = in.read(buffer);
      long totalBytesRead = bytesRead;
      while (bytesRead > 0) {
        bytesRead = in.read(buffer);
        totalBytesRead += bytesRead;
      }
      stats.incrFilesRead();
      long end = System.nanoTime();
      stats.addBytesRead(totalBytesRead);
      stats.addReadTime(end - readStart);
      stats.addFileOpenTime(readStart - openStart);
    }
  }

  public static void main(String... args) {
    CommandLine.run(new ReadFile(), System.err, args);
  }
}

