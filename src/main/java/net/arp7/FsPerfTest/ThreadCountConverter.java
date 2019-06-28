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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Parse the number of input threads, limiting it to a value
 * in the range [1, MAX_THREADS].
 */
public class ThreadCountConverter implements CommandLine.ITypeConverter<Integer> {
  @Override
  public Integer convert(String s) {
    int numThreads = Math.max(1, Integer.parseInt(s));
    return Math.min(numThreads, Constants.MAX_THREADS);
  }
}
