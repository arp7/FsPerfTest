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

public class WriteFile {
  // Valid numbers e.g. 65536, 64k, 64kb, 64K, 64KB etc.
  static final Pattern pattern = Pattern.compile("^(\\d+)([kKmMgGtT]?)[bB]?$");
  static long blockSize;
  static long fileSize;
  static long numFiles;

  public static void main (String[] args) throws Exception{
    parseArgs(args);
    final byte[] data = new byte[(int) blockSize];
    Arrays.fill(data, (byte) 0);

    final Random rand = new Random(System.currentTimeMillis()/1000);
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    DataOutputStream os = null;

    long totalTime = 0;
    long totalWriteTime = 0;
    boolean success = false;

    try {
      // Create a dummy file to prime the pipeline.
      final Path dummyPath = new Path("/WriteFileDummy" + rand.nextInt());
      fs.create(dummyPath, true).close();
      fs.delete(dummyPath, false);

      // Create the requested number of files.
      for (int i = 0; i < numFiles; ++i) {
        final Path p = new Path("/WriteFile" + rand.nextInt());

        long startTime = System.currentTimeMillis();
        os = fs.create(p, true);

        long writeStartTime = System.currentTimeMillis();
        for (int j = 0; j < fileSize / blockSize; ++j) {
          os.write(data, 0, data.length);
        }


        os.close();
        os = null;
        final long endTime = System.currentTimeMillis();
        fs.delete(p, false);

        totalTime += (endTime - startTime);
        totalWriteTime += (endTime - writeStartTime);
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
      System.out.println("Mean Time to write data: " + totalWriteTime / numFiles + "ms");
      System.out.println("Mean throughput: " + ((numFiles * fileSize) / (totalWriteTime * 1024)) + "MBps");
    }
  }

  static private void parseArgs(String[] args) {
    if (args.length != 3) {
      System.err.println(
          "\n  Usage: WriteFile <blocksize> <filesize> <numFiles>");
      System.exit(1);
    }

    int argIndex = 0;
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

