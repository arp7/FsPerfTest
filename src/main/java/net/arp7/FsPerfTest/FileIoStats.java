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

import com.google.common.util.concurrent.AtomicDouble;

import java.util.concurrent.atomic.AtomicLong;


class FileIoStats {
  // Write metrics.
  private AtomicDouble totalCreateTimeNs = new AtomicDouble(0);
  private AtomicDouble totalWriteTimeNs = new AtomicDouble(0);
  private AtomicDouble totalCloseTimeNs = new AtomicDouble(0);
  private AtomicLong bytesWritten = new AtomicLong(0);
  private AtomicLong filesWritten = new AtomicLong(0);

  // Read metrics.
  private AtomicLong bytesRead = new AtomicLong(0);
  private AtomicLong filesRead = new AtomicLong(0);
  private AtomicDouble totalFileOpenTimeNs = new AtomicDouble(0);
  private AtomicDouble totalReadTimeNs = new AtomicDouble(0);

  private AtomicLong elapsedTimeNs = new AtomicLong(0);

  double getMeanCreateTimeMs() {
    return totalCreateTimeNs.get() / (filesWritten.get() * 1_000_000);
  }

  double getMeanWriteTimeMs() {
    return totalWriteTimeNs.get() / (filesWritten.get() * 1_000_000);
  }

  double getMeanCloseTimeMs() {
    return totalCloseTimeNs.get() / (filesWritten.get() * 1_000_000);
  }

  void addCreateTime(long deltaNs) {
    totalCreateTimeNs.addAndGet(deltaNs);
  }

  void addWriteTime(long deltaNs) {
    totalWriteTimeNs.addAndGet(deltaNs);
  }

  void addCloseTime(long deltaNs) {
    totalCloseTimeNs.addAndGet(deltaNs);
  }

  void setElapsedTime(long deltaNs) {
    elapsedTimeNs.set(deltaNs);
  }

  long getElapsedTimeMs() {
    return elapsedTimeNs.get() / 1_000_000;
  }

  void incrFilesWritten() {
    filesWritten.incrementAndGet();
  }

  void incrBytesWritten(long byteCount) {
    bytesWritten.addAndGet(byteCount);
  }

  long getFilesWritten() {
    return filesWritten.get();
  }

  long getBytesWritten() {
    return bytesWritten.get();
  }

  long getBytesRead() {
    return bytesRead.get();
  }

  long getFilesRead() {
    return filesRead.get();
  }

  double getMeanOpenTimeMs() {
    return totalFileOpenTimeNs.get() / (filesRead.get() * 1_000_000);
  }

  double getMeanReadThroughputMBps() {
    return bytesRead.get() * 1000 / elapsedTimeNs.get();
  }

  long incrFilesRead() {
    return filesRead.incrementAndGet();
  }

  void addReadTime(long deltaNs) {
    totalReadTimeNs.addAndGet(deltaNs);
  }

  void addFileOpenTime(long deltaNs) {
    totalFileOpenTimeNs.addAndGet(deltaNs);
  }

  public void addBytesRead(long count) {
    bytesRead.addAndGet(count);
  }
}
