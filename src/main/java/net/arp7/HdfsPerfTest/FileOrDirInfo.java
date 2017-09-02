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

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.Path;


/**
 * Information about a leaf-level file or directory.
 * Which is stored under dir1/dir2/fileId;
 */
class FileOrDirInfo {
  private int dir1;
  private int dir2;
  private long fileOrDirId;
  private Path path;

  FileOrDirInfo() {
    reset();
  }

  private void reset() {
    dir1 = dir2 = 0;
    fileOrDirId = 0;
    path = null;
  }

  void reinitialize(Path root, int dir1, int dir2, long fileOrDirId) {
    this.dir1 = dir1;
    this.dir2 = dir2;
    this.fileOrDirId = fileOrDirId;
    final String pathStr = String.valueOf(dir1) +
        Path.SEPARATOR + dir2 + Path.SEPARATOR +
        fileOrDirId;
    this.path = new Path(root, pathStr);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FileOrDirInfo)) return false;

    FileOrDirInfo that = (FileOrDirInfo) o;

    return dir1 == that.dir1 &&
        dir2 == that.dir2 &&
        fileOrDirId == that.fileOrDirId;
  }

  @Override
  public int hashCode() {
    int result = dir1;
    result = 31 * result + dir2;
    result = 31 * result + (int) (fileOrDirId ^ (fileOrDirId >>> 32));
    return result;
  }

  Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return path.toString();
  }

  Pair<Integer,Integer> getDirectoryPair() {
    return new Pair<>(dir1, dir2);
  }

  long getFileOrDirId() {
    return fileOrDirId;
  }
}


