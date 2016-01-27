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
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import static org.apache.hadoop.fs.CreateFlag.*;

public class ReadFile {
  static final int BUFFER_SIZE = 64 * 1024;
  static boolean lazyPersist;
  static long blockSize;
  static long ioSize;
  static long fileSize;
  static long numReads;

  public static void main (String[] args) throws Exception{
    parseArgs(args);
    final byte[] data = new byte[(int) ioSize];
    Arrays.fill(data, (byte) 0);

    final Random rand = new Random(System.currentTimeMillis()/1000);
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    DataOutputStream os = null;
    FSDataInputStream is = null;
    boolean success = false;
    long startTime;
    long timeElapsed;
    long totalBytesRead = 0;
    final Path path = new Path("/ReadFile." + rand.nextInt(100000000) + ".dat");

    try {
      EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, OVERWRITE);
      if (lazyPersist) {
        createFlags.add(LAZY_PERSIST);
      }

      os =
          fs.create(
              path,
              FsPermission.getFileDefault(),
              createFlags,
              BUFFER_SIZE,
              (short) 1,
              blockSize,
              null);

      for (long bytesWritten = 0;
           bytesWritten < fileSize;
           bytesWritten += data.length) {
        os.write(data, 0, data.length);
      }

      System.out.println(" >> Wrote file " + path + " [" + fileSize + " bytes]");

      os.close();
      os = null;
      is = fs.open(path, BUFFER_SIZE);
      startTime = System.nanoTime();

      for (int i = 0; i < numReads; ++i) {
        for (int fileBytesRead = 0;
             fileBytesRead < fileSize;
             fileBytesRead += data.length) {
          is.readFully(data, 0, data.length);
          totalBytesRead += data.length;        
        }
        is.seek(0L);
      }

      timeElapsed = System.nanoTime() - startTime;
      success = true;
    } finally {
      if (os != null) {
        os.close();
      }

      if (is != null) {
        is.close();
      }
    }

    if (success) {
      timeElapsed /= (1000L * 1000);    // Convert to milliseconds.
      System.out.println("Mean Time to read file fully: "+
          timeElapsed / numReads + "ms");
      System.out.println("Mean Read throughput: " +
          ((numReads * fileSize * 1000) / (timeElapsed * 1024 * 1024)) + "MBps");
    }
  }

  static private void parseArgs(String[] args) {
    if (args.length < 4 || args.length > 5) {
      System.err.println(
          "\n  Usage: ReadFile --lazyPersist <blocksize> <filesize> <io-size> <numReads>");
      System.exit(1);
    }

    int argIndex = 0;
    if (args[argIndex].equalsIgnoreCase("--lazyPersist")) {
      lazyPersist = true;
      ++argIndex;
    } else {
      lazyPersist = false;
    }

    blockSize = Utils.parseReadableLong(args[argIndex++]);
    fileSize = Utils.parseReadableLong(args[argIndex++]);
    ioSize =  Utils.parseReadableLong(args[argIndex++]);
    numReads = Utils.parseReadableLong(args[argIndex++]);
  }
}

