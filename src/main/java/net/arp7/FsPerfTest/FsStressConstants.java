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

package net.arp7.FsPerfTest;

/**
 * Constants that control the runtime behavior of {@link FsStress}.
 */
class FsStressConstants {
  /**
   * A minimum number of leaves (files/dirs) need to be created per-thread for
   * issuing other requests.
   */
  static final int NUM_LEAVES_LOW_WATERMARK = 3_000;
  static final int NUM_LEAVES_MID_WATERMARK = 6_000;

  /**
   * Above this threshold we will try not to issue create/mkdir requests. 
   */
  
  static final int NUM_LEAVES_HIGH_WATERMARK = 1_000_000;

  /**
   * We use a two-level directory structure to reduce the number of
   * files/dirs per level. Leaf files are dirs are created under the
   * following directory structure e.g. 22/17/000000005291
   * Where 22 is the l1 directory, 17 is the l2 directory and
   * 000000005291 is the leaf-level file/dir name.
   */
  static final int NUM_DIRS_AT_L1_AND_L2 = 32;
}
