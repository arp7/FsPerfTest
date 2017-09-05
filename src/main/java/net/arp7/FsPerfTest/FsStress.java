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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Math.abs;
import static net.arp7.FsPerfTest.RequestType.CREATE;
import static net.arp7.FsPerfTest.RequestType.MKDIRS;

/**
 * A utility for benchmarking the performance of a Hadoop-Compatible
 * {@link FileSystem} with a multi-threaded client.
 *
 * This generates only metadata operations. No user data is actually written
 * or read from the FileSystem.
 *
 * See {@link FsStressParameters#usageAndExit} for all the supported options.
 */
@InterfaceAudience.Private
public class FsStress {
  static final Logger LOG = LoggerFactory.getLogger(WriteFile.class);

  /**
   * The target FileSystem that is being tested.
   */
  private final FileSystem fs;

  private final FsStressParameters params;
  private final FsRequestStats globalStats;
  private final Path outputDir;

  // Enabled requests that are not create or mkdirs. This list is
  // maintained as an optimization to quickly choose a target
  // request type for read-operations.
  private final List<RequestType> enabledRequestsNotCreateMkdirs;

  /**
   * Instantiate a new FsStress run. Threads will not be started and
   * no requests will be issued to the {@link FileSystem} until
   * {@link doFsStress} is invoked.
   *
   * @param args command-line arguments.
   * @throws IOException
   */
  private FsStress(String[] args) throws IOException {
    Configuration conf = new HdfsConfiguration();
    fs = FileSystem.get(conf);
    params = new FsStressParameters(args);
    outputDir = makeOutputDir(fs);
    globalStats = new FsRequestStats();
    LOG.info("Using output directory {}", outputDir);

    enabledRequestsNotCreateMkdirs = ImmutableList.copyOf(
        Sets.difference(
          ImmutableSet.copyOf(params.getEnabledRequestTypes()),
          ImmutableSet.of(CREATE, MKDIRS)));
  }
  
  public static void main(String[] args) throws Exception {
    FsStress stress = new FsStress(args);
    stress.doFsStress();
    stress.writeStats();
  }

  /**
   * Create the output directory under which all files and
   * directories will be created by the test threads. The output
   * directory name has the format:
   * FsStress-[unix epoch at test start]-[randomInt]
   *
   * @param runId
   * @param fs
   * @return
   * @throws IOException
   */
  private static Path makeOutputDir(FileSystem fs) throws IOException {
    Path path = new Path("/tmp",
        "FsStress" + Path.SEPARATOR + "FsStress-"
            + System.currentTimeMillis() + "-" +
            ThreadLocalRandom.current().nextInt());

    if (!fs.mkdirs(path)) {
      throw new IOException("Failed to make output directory " + path);
    }
    return path;
  }

  /**
   * Start the client threads that will generate requests against the
   * {@link FileSystem}. This method waits until all the threads have
   * finished executing. Each thread is expected to exit when its
   * execution time has elapsed.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  private void doFsStress()
      throws InterruptedException, IOException {

    // Start the threads.
    final ExecutorService executor = Executors.newFixedThreadPool(
        (int) params.getNumThreads(),
        (new ThreadFactoryBuilder().setNameFormat(
            "FsStress-Client-Thread-%d").build()));

    final CompletionService<Object> ecs =
        new ExecutorCompletionService<>(executor);

    LOG.info("Starting {} client thread{}.",
        params.getNumThreads(),
        params.getNumThreads() > 1 ? "s" : "");
    
    for (long t = 0; t < params.getNumThreads(); ++t) {
      ecs.submit(new ClientThread(t));
    }

    // And wait for all threads to complete.
    for (long t = 0; t < params.getNumThreads(); ++t) {
      ecs.take();
    }
    executor.shutdown();
  }

  /**
   * A thread that will run for the time interval specified by
   * {@link FsStressParameters#getRuntimeInSeconds()}, constantly
   * issuing metadata requests to the FileSystem.
   *
   * @param threadIndex
   */
  private class ClientThread implements Callable<Object> {
    private final long threadIndex;
    final private FsRequestStats stats;
    final private ThreadLocalRandom tlRandom;
    
    // These objects are used as the src and destination of FS
    // operations. We avoid reallocating them on each iteration.
    final FileOrDirInfo src = new FileOrDirInfo();
    final FileOrDirInfo target = new FileOrDirInfo();

    // Thread-root directory under which this thread will create all
    // its files and directories.
    final Path threadRootDir;
    
    // True if the thread is in startup mode i.e. creating initial
    // files and directories.
    private boolean inStartup = true;

    /**
     * These maps maintain an in-memory representation of the files
     * and directories created by each thread.
     * The maps let us quickly choose targets for rename/delete and
     * other metadata operations.

     * Leaf-level Directories and files currently on the system.
     * These map from Pair<d1, d2> -> FileId
     */
    Map<Pair<Integer, Integer>, Set<Long>> currentFiles;
    Map<Pair<Integer, Integer>, Set<Long>> currentDirs;

    /**
     * Number of files and directories. These can also be computed
     * by summing the entries in the currentFiles and currentDirs
     * but the counts are maintained separately as an optimization.
     */
    long numFiles;
    long numDirs;

    /**
     * Highest file/directory ID created so far. We never reuse these IDs.
     * Hence the counter is monotonically increasing.
     */
    private long highestFileId = 0;

    // This flag helps implement hysteresis for create/mkdir.
    private boolean createCatchup = false;

    /**
     * @param threadIndex numerical index of the current thread.
     */
    ClientThread(long threadIndex) {
      this.threadIndex = threadIndex;
      stats = new FsRequestStats();
      tlRandom = ThreadLocalRandom.current();
      threadRootDir = new Path(
          outputDir, StringUtils.format("Thread-%03d", this.threadIndex));
      currentFiles = new HashMap<>();
      currentDirs = new HashMap<>();
    }

    /**
     * The heart of the stress test. A single-thread that issues requests
     * to the FileSystem in a loop until its scheduled runtime has elapsed.
     *
     * @return ignored.
     * @throws IOException
     */
    @Override
    public Object call() throws IOException {
      if (!fs.mkdirs(threadRootDir)) {
        LOG.error("Thread-{}: Failed to create thread output directory {}",
            threadIndex, threadRootDir);
        return null;
      }

      LOG.info("Thread-{}: creating initial files and directories",
          threadIndex);

      if (!makeTwoLevelDirStructure()) {
        LOG.error("Thread-{}: Error creating initial directories.",
            threadIndex);
        return null;
      }

      long threadStartTimeNs = System.nanoTime();

      // Keep issuing requests in a loop.
      while (inStartup || (System.nanoTime() - threadStartTimeNs <
          params.getRuntimeInNanoseconds())) {

        // Figure out the kind of request to execute.
        final RequestType requestType = chooseRequestType(tlRandom);

        try {
          // Send the request to the FileSystem.
          final long requestLatencyNs = issueRequest(requestType);

          // Do not update stats if we are creating initial files/dirs.
          if (!inStartup) {
            stats.addStats(requestType, 1, requestLatencyNs);
          }

          // Done creating initial files/dirs?
          if (inStartup && numFiles + numDirs >=
              FsStressConstants.NUM_LEAVES_MID_WATERMARK) {
            // Reset thread start time.
            final long startupExitTimeNs = System.nanoTime();
            LOG.info("Thread-{}: Done creating initial files and " +
                "directories in {}us", threadIndex,
                NumberFormat.getInstance(Locale.US).format(
                    (startupExitTimeNs - threadStartTimeNs) / 1_000));
            inStartup = false;
            threadStartTimeNs = startupExitTimeNs;
          }
        } catch (IOException ioe) {
          // Update stats, but keep going. It may be a transient failure.
          stats.incrFailure(requestType);
        }
      }

      LOG.info("Thread-{}: exiting", threadIndex);

      // Aggregate thread-local stats into the global stats.
      globalStats.aggregateFrom(stats);
      globalStats.addElapsedTimeNs(System.nanoTime() - threadStartTimeNs);
      return null;
    }

    /**
     * Setup the 32x32 level directories, also recording the stats for
     * each operation.
     *
     * @return
     */
    private boolean makeTwoLevelDirStructure() throws IOException {
      for (int i = 0; i < FsStressConstants.NUM_DIRS_AT_L1_AND_L2; ++i) {
        for (int j = 0; j < FsStressConstants.NUM_DIRS_AT_L1_AND_L2; ++j) {
          final Path dir = new Path(threadRootDir,
              String.valueOf(i) + Path.SEPARATOR + j);

          if (!fs.mkdirs(dir)) {
            LOG.error("Thread-{}: Failed to create directory {}",
                threadIndex, dir);
            return false;
          }

          // Create the sets that will hold files/dirs under each
          // sub-directory.
          currentFiles.put(new Pair<>(i, j), new HashSet<Long>());
          currentDirs.put(new Pair<>(i, j), new HashSet<Long>());
        }
      }

      return true;
    }

    /**
     * Issue a request of the specific type against a random target.
     *
     * @param requestType type of request to issue.
     * @return time taken to execute the request in Nano-seconds.
     */
    private long issueRequest(RequestType requestType)
        throws IOException {

      long requestStartTimeNs;

      switch (requestType) {
        case CREATE: {
          getNewFileOrDirInfo(src);
          LOG.debug("Thread-{} creating file {}", threadIndex, src);
          requestStartTimeNs = System.nanoTime();
          fs.create(src.getPath()).close();
          recordFileOrDirAddition(src, true);
          break;
        }

        case MKDIRS: {
          getNewFileOrDirInfo(src);
          LOG.debug("Thread-{} creating directory {}", threadIndex, src);
          requestStartTimeNs = System.nanoTime();
          if (!fs.mkdirs(src.getPath())) {
            LOG.error("Failed to create directory {}", src);
            throw new IOException();
          }
          recordFileOrDirAddition(src, false);
          break;
        }

        case DELETE: {
          final boolean deleteFile = tlRandom.nextBoolean();
          getRandomFileOrDir(deleteFile, src);
          LOG.debug("Thread-{} deleting {} {}", threadIndex,
              deleteFile ? "file" : "directory", src);
          requestStartTimeNs = System.nanoTime();
          if (!fs.delete(src.getPath(), true)) {
            LOG.error("Failed to remove {} {}",
                deleteFile ? "file" : "directory", src.getPath());
            throw new IOException();
          }
          recordFileOrDirDeletion(src, deleteFile);
          break;
        }

        case RENAME: {
          // Just rename the first entry in the list.
          final boolean renameFile = tlRandom.nextBoolean();
          getRandomFileOrDir(renameFile, src);
          getNewFileOrDirInfo(target);
          LOG.debug("Thread-{} renaming {} {} -> {}", threadIndex,
              renameFile ? "file" : "directory", src, target);
          requestStartTimeNs = System.nanoTime();
          if (!fs.rename(src.getPath(), target.getPath())) {
            LOG.error("Failed to rename {} {} to {}",
                renameFile ? "file" : "directory", target);
            throw new IOException();
          }

          // Delete the 'src' and add the 'target' to record the effect
          // of the rename.
          recordFileOrDirDeletion(src, renameFile);
          recordFileOrDirAddition(target, renameFile);
          break;
        }

        case STAT: {
          getRandomFileOrDir(tlRandom.nextBoolean(), src);
          LOG.debug("Thread-{} Issuing getFileInfo for {}", src);
          requestStartTimeNs = System.nanoTime();
          fs.getFileStatus(src.getPath());
          break;
        }
        
        case GETCONTENTSUMMARY: {
          getRandomFileOrDir(true, src);
          LOG.debug("Thread-{} Issuing getFileInfo for {}", src);
          requestStartTimeNs = System.nanoTime();
          fs.getContentSummary(src.getPath());
          break;
        }

        default:
          // We forgot to handle a new request type.
          throw new IllegalStateException("Unrecognized request type " + requestType);
      }

      return System.nanoTime() - requestStartTimeNs;
    }

    /**
     * Update our state after a DELETE operation.
     *
     * @param fileOrDirInfo information about the deleted file/directory.
     * @param isFile true if the deleted entry is a file, false if it is
     *               a directory.
     */
    private void recordFileOrDirDeletion(
        FileOrDirInfo fileOrDirInfo, boolean isFile) {
      if (isFile) {
        currentFiles.get(fileOrDirInfo.getDirectoryPair()).remove(
            fileOrDirInfo.getFileOrDirId());
        --numFiles;
      } else {
        currentDirs.get(fileOrDirInfo.getDirectoryPair()).remove(
            fileOrDirInfo.getFileOrDirId());
        --numDirs;
      }
    }

    /**
     * Update our state after a CREATE or MKDIR operation.
     *
     * @param fileOrDirInfo information about the new file/directory.
     * @param isFile true if the newly added entry is a file, false if it is
     *               a directory.
     */
    private void recordFileOrDirAddition(
        FileOrDirInfo fileOrDirInfo, boolean isFile) {
      if (isFile) {
        currentFiles.get(fileOrDirInfo.getDirectoryPair()).add(
            fileOrDirInfo.getFileOrDirId());
        ++numFiles;
      } else {
        currentDirs.get(fileOrDirInfo.getDirectoryPair()).add(
            fileOrDirInfo.getFileOrDirId());
        ++numDirs;
      }
    }

    /**
     * Initialize the provided {@link FileOrDirInfo} object to represent
     * a random file or directory entry that currently exists in the 
     * target FileSystem.
     *
     * @param chooseFile
     * @param fileOrDir Object to be initialized.
     * @return 
     */
    private void getRandomFileOrDir(
        boolean chooseFile, FileOrDirInfo fileOrDir) {
      int d1;
      int d2;
      Map<Pair<Integer, Integer>, Set<Long>> targetMap =
          chooseFile ? currentFiles : currentDirs;
      Pair<Integer, Integer> pair;

      // This loop just chooses the first entry from a random
      // sub-directory d1/d2. It loops until we find at least one
      // such non-empty subdir. The low watermark thresholds ensure
      // there is a high probability of finding such a non-empty
      // subdir in the first one or two iterations.
      do {
        d1 = tlRandom.nextInt(FsStressConstants.NUM_DIRS_AT_L1_AND_L2);
        d2 = tlRandom.nextInt(FsStressConstants.NUM_DIRS_AT_L1_AND_L2);
        pair = new Pair<>(d1, d2);
      } while(targetMap.get(pair).isEmpty());

      final Long fileOrDirId = targetMap.get(pair).iterator().next();
      fileOrDir.reinitialize(threadRootDir, d1, d2, fileOrDirId);
    }

    /**
     * Initialize the provided {@link FileOrDirInfo} object to represent
     * a random file or directory entry that will be created in the target
     * filesystem.
     *
     * This does not create the file/dir and does not update our
     * internal state to record its existence.
     * 
     * @param fileOrDir Object to be initialized. 
     *
     * @return
     */
    private void getNewFileOrDirInfo(FileOrDirInfo fileOrDir) {
      fileOrDir.reinitialize(
          threadRootDir,
          tlRandom.nextInt(FsStressConstants.NUM_DIRS_AT_L1_AND_L2),
          tlRandom.nextInt(FsStressConstants.NUM_DIRS_AT_L1_AND_L2),
          ++highestFileId);
    }

    /**
     * Decide which kind of request to issue.
     * {@link #createCatchup} is used to implement hysteresis.
     *
     * @param r        ThreadLocalRandom object.
     * @param numFiles number of files created by this thread that exist
     *                 currently. This does not count files that were created
     *                 and then deleted.
     * @param numDirs number of leaf-level directories that exist currently.
     *                This does not count directories that were created and then
     *                deleted.
     * @return
     */
    private RequestType chooseRequestType(Random r) {
      final long numLeaves = numDirs + numFiles;
      if (numLeaves < FsStressConstants.NUM_LEAVES_LOW_WATERMARK) {
        // If the number of leaves is below a threshold
        // then just issue a create/mkdir request.
        createCatchup = true;
        return tlRandom.nextBoolean() ? CREATE : MKDIRS;
      } else if (numLeaves < FsStressConstants.NUM_LEAVES_MID_WATERMARK &&
          createCatchup) {
        // We are past the low watermark but create/mkdir some more leaves to
        // avoid getting into a create-delete-create-delete pattern of
        // requests.
        return tlRandom.nextBoolean() ? CREATE : MKDIRS;
      }

      createCatchup = false;

      // If the number of leaves is above the high watermark, then try to issue
      // something other than create/mkdir.
      if (numFiles > FsStressConstants.NUM_LEAVES_HIGH_WATERMARK) {
        return getRandomExcludingCreateOrMkdir(r);
      }

      return getRandomRequestType(tlRandom);
    }
  }

  private RequestType getRandomRequestType(Random r) {
    return params.getEnabledRequestTypes().get(
        r.nextInt(params.getEnabledRequestTypes().size()));
  }

  private RequestType getRandomExcludingCreateOrMkdir(Random r) {
    if (enabledRequestsNotCreateMkdirs.isEmpty()) {
      return getRandomRequestType(r);
    }

    return enabledRequestsNotCreateMkdirs.get(
        r.nextInt(enabledRequestsNotCreateMkdirs.size()));
  }

  private void writeStats() {
    LOG.info("Final Request Statistics:{}", globalStats);
  }
}
