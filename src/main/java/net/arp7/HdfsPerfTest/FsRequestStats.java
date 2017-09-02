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


import org.apache.commons.lang.StringUtils;

import java.text.NumberFormat;
import java.util.Locale;

/**
 * Accumulate FileSystem Request statistics.
 * 
 * This class does not provide any synchronization guarantees so the
 * caller is responsible for ensuring thread-safe usage.
 */
class FsRequestStats {
  // Total time for each request type.
  private final long[] totalTimeNs;
  
  // Number of operations for each request type.
  private final long[] numOps;
  
  // Number of failed operations for each request type.
  private final long[] numFailedOps;

  private long executionTimeNs;
  private long numThreads = 0;

  FsRequestStats() {
    totalTimeNs = new long[RequestType.values().length];
    numOps = new long[RequestType.values().length];
    numFailedOps = new long[RequestType.values().length];
  }

  /**
   * Aggregate stats from another {@link FsRequestStats} object into this one.
   * Useful to aggregate thread-local stats into a global stats object.
   * 
   * The caller is responsible for ensuring there is no concurrent invocation
   * of {@link #addStats} or {@link incrFailure}.
   *
   * @param other
   */
  synchronized void aggregateFrom(FsRequestStats other) {
    for(int i = 0; i < totalTimeNs.length; ++i) {
      totalTimeNs[i] += other.totalTimeNs[i];
      numOps[i] += other.numOps[i];
    }
  }

  /**
   * Record a failed request.
   * 
   * @param requestType
   */
  void incrFailure(RequestType requestType) {
    ++this.numFailedOps[requestType.getId()];
  }

  /**
   * Update stats after successfully executing one or more requests
   * of a specific type.
   *
   * @param requestType
   * @param timeNs
   */
  void addStats(RequestType requestType, long numOps, long timeNs) {
    this.numOps[requestType.getId()] += numOps;
    this.totalTimeNs[requestType.getId()] += timeNs;
  }

  /**
   * Total execution time across all threads.
   * @return
   */
  private long getExecutionTimeNs() {
    return executionTimeNs;
  }

  synchronized void addElapsedTimeNs(long elapsedTime) {
    this.executionTimeNs += elapsedTime;
    ++numThreads;
  }
  
  private long totalRequestCount() {
    long totalRequests = 0;
    for(int i = 0; i < totalTimeNs.length; ++i) {
      totalRequests += numOps[i];
    }
    return totalRequests;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append("\n");

    final long requestThroughput =
        (totalRequestCount() * 1_000_000_000) * numThreads / getExecutionTimeNs();
    
    final NumberFormat nf = NumberFormat.getInstance(Locale.US);

    // Add the stats for each request type.
    for (RequestType t : RequestType.values()) {
      final String s = StringUtils.capitalize(
          t.toString().toLowerCase(Locale.US));

      if (numOps[t.getId()] > 0) {
        sb.append("Num ").append(s).append(": ")
            .append(nf.format(numOps[t.getId()]))
            .append("\n");

        sb.append("Total ").append(s).append(" Latency (us): ")
            .append(nf.format(totalTimeNs[t.getId()] / 1_000))
            .append("\n");

        sb.append("Mean ").append(s).append(" Latency (us): ")
            .append(totalTimeNs[t.getId()] / (1_000 * numOps[t.getId()]))
            .append("\n");
      }

      if (numFailedOps[t.getId()] > 0) {
        sb.append("Num Failed ").append(s).append(": ")
            .append(nf.format(numFailedOps[t.getId()]))
            .append("\n");
      }

    }

    // Add the mean request throughput for all ops combined.
    sb.append("Elapsed time (seconds): ")
        .append(getExecutionTimeNs() / 1_000_000_000)
        .append("\nMean requests/second: ")
        .append(requestThroughput);

    return sb.toString();
  }  
}
