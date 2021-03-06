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


enum RequestType {
  CREATE(0, false),
  DELETE(1, false),
  MKDIRS(2, false),
  RENAME(3, false),
  STAT(4, true),
  GETCONTENTSUMMARY(5, true);
  
  private final int id;
  
  // true if this is a read request, false otherwise.
  private final boolean isReadRequest;
  
  RequestType(int id, boolean isReadRequest) {
    this.id = id;
    this.isReadRequest = isReadRequest;
  }

  public int getId() {
    return id;
  }

  public boolean isReadRequest() {
    return isReadRequest;
  }
}
