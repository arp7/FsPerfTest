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
import java.util.regex.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import static org.apache.hadoop.fs.CreateFlag.*;

public class WriteFile {
  // Valid numbers e.g. 65536, 64k, 64kb, 64K, 64KB etc.
  static final Pattern pattern = Pattern.compile("^(\\d+)([kKmMgGtT]?)[bB]?$");
  static final int BUFFER_SIZE = 64 * 1024;
  static final int IO_SIZE = 4 * 1024;
  static long blockSize;
  static long fileSize;
  static long numFiles;
  static boolean lazyPersist = false;
  static boolean hsync = false;
  static boolean verbose = false;

  public static void main (String[] args) throws Exception{
    parseArgs(args);
    final byte[] data = new byte[IO_SIZE];
    Arrays.fill(data, (byte) 65);

    final Random rand = new Random(System.nanoTime()/1000);
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream os = null;

    long totalTime = 0;
    long totalWriteTime = 0;
    boolean success = false;
    long lastLoggedPercent = 0;

    try {
      // Create the requested number of files.
      for (int i = 0; i < numFiles; ++i) {
        final Path p = new Path("/WriteFile" + rand.nextInt());

        long startTime = System.nanoTime();
        EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, OVERWRITE);
        if (lazyPersist) {
          createFlags.add(LAZY_PERSIST);
        }

        if (verbose) {
          System.out.println(" > Writing file " + p);
        }

        os =
            fs.create(
                p,
                FsPermission.getFileDefault(),
                createFlags,
                BUFFER_SIZE,
                (short) 1,
                blockSize,
                null);

        long writeStartTime = System.nanoTime();
        for (int j = 0; j < fileSize / IO_SIZE; ++j) {
          os.write(data, 0, data.length);

          if (hsync) {
            os.hsync();
          }

          if (verbose) {
            long percentWritten = ((long) j * IO_SIZE * 100) / fileSize;
            if (percentWritten > lastLoggedPercent) {
              System.out.println("  >> Wrote " + j * IO_SIZE + "/" + fileSize + " [" + percentWritten + "%]");
              lastLoggedPercent = percentWritten;
            }
          }
        }

        os.close();
        os = null;
        final long endTime = System.nanoTime();
        //fs.delete(p, false);      // TODO: Make command-line flag.

        totalTime += (endTime - startTime) / (1000 * 1000);
        totalWriteTime += (endTime - writeStartTime) / (1000 * 1000);
      }

      success = true;
    } finally {
      if (os != null) {
        os.close();
      }
    }

    if (success) {
      System.out.println("Total data written: " +
          FileUtils.byteCountToDisplaySize(fileSize));
      System.out.println("Mean Time per file: " + totalTime / numFiles + "ms");
      System.out.println("Mean Time to create file on NN: " + (totalTime - totalWriteTime) / numFiles + "ms");
      System.out.println("Mean Time to write data: " + totalTime / numFiles + "ms");
      System.out.println("Mean throughput: " + ((numFiles * fileSize) / (totalWriteTime * 1024)) + "MBps");
    }
  }

  static private void usageAndExit() {
    System.err.println(
        "\n  Usage: WriteFile [--lazyPersist] [--hsync] [--verbose] <blocksize> <filesize> <numFiles>");
    System.err.println(
        "\n   --lazyPersist : Sets CreateFlag.LAZY_PERSIST");
    System.err.println(
        "\n   --hsync       : Issues an hsync after every block write");
    System.exit(1);
  }

  static private void parseArgs(String[] args) {
    if (args.length < 3 || args.length > 6) {
      usageAndExit();
    }

    int argIndex = 0;

    while(args[argIndex].indexOf("--") == 0) {
      if (args[argIndex].equalsIgnoreCase("--lazyPersist")) {
        lazyPersist = true;
      } else if (args[argIndex].equalsIgnoreCase("--hsync")) {
        hsync = true;
      } else if (args[argIndex].equalsIgnoreCase("--verbose")) {
        verbose = true;
      } else {
        usageAndExit();
      }
      ++argIndex;
    }

    blockSize = parseReadableLong(args[argIndex++]);
    fileSize = parseReadableLong(args[argIndex++]);
    numFiles = parseReadableLong(args[argIndex++]);
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

