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

import picocli.CommandLine.ITypeConverter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * String to Long for use with PicoCLI.
 *
 * Parse a human readable long e.g. 65536, 64KB, 4MB, 4m, 1GB, 2TB etc.
 */
class ReadableLongConverter implements ITypeConverter<Long> {
  static final Pattern numberPattern = Pattern.compile("^(\\d+)([kmgt]?)[bB]?$");

  @Override
  public Long convert(String number) {
    Matcher matcher = numberPattern.matcher(number.toLowerCase());
    if (matcher.find()) {
      long multiplier = 1;
      if (matcher.group(2).length() > 0) {
        switch (matcher.group(2).charAt(0)) {
          case 't':           multiplier *= 1024L;
          case 'g':           multiplier *= 1024L;
          case 'm':           multiplier *= 1024L;
          case 'k':           multiplier *= 1024L;
        }
      }
      long baseVal = Long.parseLong(matcher.group(1));

      if (baseVal > Long.MAX_VALUE / multiplier) {
        throw new IllegalArgumentException(
            number + " is too large to represent as long.");
      }
      return baseVal * multiplier;
    }
    throw new IllegalArgumentException(
        "Unrecognized number format " + number);
  }
}