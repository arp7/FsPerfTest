/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.arp7.FsPerfTest;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Utils {
  // Valid numbers e.g. 65536, 64k, 64kb, 64K, 64KB etc.
  static final Pattern numberPattern = Pattern.compile("^(\\d+)([kKmMgGtT]?)[bB]?$");
  private static final Locale locale = Locale.getDefault();

  /**
   * Parse a human readable long e.g. 65536, 64KB, 4MB, 4m, 1GB, 2TB etc.
   */
  public static Long parseReadableLong(String number) {
    Matcher matcher = numberPattern.matcher(number);
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
      long baseVal = Long.parseLong(matcher.group(1));

      if (baseVal > Long.MAX_VALUE / multiplier) {
        throw new IllegalArgumentException(number + " is too large to represent as long.");
      }
      return baseVal * multiplier;
    }
    throw new IllegalArgumentException("Unrecognized number format " + number);
  }

  /**
   * Pretty print the supplied number.
   *
   * @param number
   * @return
   */
  static String formatNumber(long number) {
    return NumberFormat.getInstance(locale).format(number);
  }


  /**
   * Enforce an IO throttle by sleeping for the appropriate amount of time.
   * @param ioTimeNs time taken by an IO operation.
   * @param expectedIoTimeNs minimum expected time for the IO operation based
   *                         on the throttling rate.
   * @throws InterruptedException
   */
  static void enforceThrottle(long ioTimeNs, long expectedIoTimeNs)
      throws InterruptedException {
    if (ioTimeNs < expectedIoTimeNs) {
      // The IO completed too fast, so sleep for some time.
      long sleepTimeNs = expectedIoTimeNs - ioTimeNs;
      Thread.sleep(sleepTimeNs / 1_000_000, (int) (sleepTimeNs % 1_000_000));
    }
  }

  /**
   * Wait for all tasks in the CompletionService to finish.
   * Log an error for each task that completed exceptionally.
   * @param cs
   * @param numTasks
   */
  public static void joinAll(
      CompletionService<Object> cs, int numTasks, Logger logger)
      throws InterruptedException {
    for (long t = 0; t < numTasks; ++t) {
      try {
        cs.take().get();
      } catch(ExecutionException ee) {
        logger.error("Thread {} execution failed with exception",
            t, ee.getCause());
      }
    }
  }

  /**
   * Recursively enumerate all regular files under the given directory and
   * return a list of {@link LocatedFileStatus}, one per file.
   *
   * @param fs Hadoop compatible FileSystem object.
   * @param dir Directory to enumerate.
   * @return a list of {@link LocatedFileStatus}, one per file.
   * @throws IOException
   */
  public static List<LocatedFileStatus> getInputFilesListing(
      FileSystem fs, Path dir) throws IOException {
    final List<LocatedFileStatus> files = new ArrayList<>();
    final RemoteIterator<LocatedFileStatus> iter = fs.listFiles(dir, true);
    while (iter.hasNext()) {
      final LocatedFileStatus fileStatus = iter.next();
      if (fileStatus.isFile()) {
        files.add(fileStatus);
      }
    }
    return Collections.unmodifiableList(files);
  }
}
