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

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;


/**
 * Parameters to control the behavior of {@link FsStress}.
 * Supports the following operations:
 *   - createFile
 *   - rename
 *   - mkdir
 *   - delete
 *   
 * All operations are enabled by default.
 */
class FsStressParameters {
  private static int DEFAULT_RUNTIME = 300;
  
  // The number of client threads submitting requests to the NameNode.
  private long numThreads = Constants.DEFAULT_THREADS;

  // The runtime of the benchmark in seconds.
  // Default = 5 minutes.
  private long runtimeInSeconds = DEFAULT_RUNTIME;
  
  // An optimization to quickly pick an enabled request type at runtime.
  private final List<RequestType> enabledRequestTypes;
  
  /**
   * Construct from command-line parameters.
   *
   * @param args
   */
  FsStressParameters(String[] args) {
    final Set<RequestType> enabledRequestTypesSet = parse(args);
    enabledRequestTypes = ImmutableList.copyOf(enabledRequestTypesSet);
  }

  private Set<RequestType> parse(String[] args) {
    int argIndex = 0;
    
    Set<RequestType> enabled = Collections.emptySet();

    while(argIndex < args.length && args[argIndex].indexOf("-") == 0) {
      if (args[argIndex].equalsIgnoreCase("-t")) {
        runtimeInSeconds = Long.parseLong(args[++argIndex]);
      } else if (args[argIndex].equalsIgnoreCase("-n")) {
        numThreads = Long.parseLong(args[++argIndex]);
      } else if (args[argIndex].toLowerCase().startsWith("--enabledtests=")) {
        final String testsSpec =
            args[argIndex].replaceFirst("^--enabledTests=", "");
        enabled = parseTestsSpec(testsSpec);
      } else if (args[argIndex].equalsIgnoreCase("--help") ||
          args[argIndex].equalsIgnoreCase("-h") ||
          args[argIndex].equalsIgnoreCase("-?")) {
        usageAndExit(0);
      } else {
        System.err.println("  Unknown option " + args[argIndex]);
        usageAndExit(1);
      }
      ++argIndex;
    }

    validate(enabled);
    return enabled;
  }

  /**
   * Parse a comma-separated list of tests and initialize our state.
   *
   * @param testsSpec
   */
  private Set<RequestType> parseTestsSpec(String testsSpec) {
    Set<RequestType> enabled = new HashSet<>();

    testsSpec = testsSpec.toUpperCase(Locale.US);

    // Enable individual tests.
    final String[] specs = testsSpec.split(",");
    label:
    for (String spec : specs) {
      try {
        switch (spec) {
          case "ALL":
            // All requests are enabled.
            enabled.addAll(Arrays.asList(RequestType.values()));
            break label;
          case "READS":
            // All read requests are enabled.
            for (RequestType t : RequestType.values()) {
              if (t.isReadRequest()) {
                enabled.add(t);
              }
            }
            break;
          case "WRITES":
            // All write requests are enabled.
            for (RequestType t : RequestType.values()) {
              if (!t.isReadRequest()) {
                enabled.add(t);
              }
            }
            break;
          default:
            final RequestType type = RequestType.valueOf(spec.trim());
            enabled.add(type);
            break;
        }
        
      } catch (IllegalArgumentException iae) {
        System.out.println("Failed to parse testSpec '" + testsSpec + "'");
        usageAndExit(1);
      }
    }

    return enabled;
  }

  /**
   * Sanity-check the test parameters.
   * @return
   */
  private void validate(Set<RequestType> enabled) {
    if (enabled.size() == 0) {
      System.err.println(
          "Must enable one or more of request type.");
      System.exit(1);
    }

    if (runtimeInSeconds <= 0) {
      System.err.println("runTime must be greater than zero.");
      System.exit(1);
    }

    if (numThreads <= 0 || numThreads > Constants.MAX_THREADS) {
      System.err.println("numThreads must be between 1 and " +
          Constants.MAX_THREADS + " (inclusive)");
      System.exit(1);
    }
  }

  /**
   * Print usage to the console.
   */
  private static void usageAndExit(int exitCode) {
    System.err.println(
        "\n  Usage: FsStress -t <runTime> -n NumThreads --enabledTests=....");
    System.err.println(
        "\n   -t runTime     : Specify the runtime in seconds. Default is " + DEFAULT_RUNTIME);
    System.err.println(
        "\n   -n NumThreads  : Number of client threads. Default is " + Constants.DEFAULT_THREADS);
    System.err.println(
        "\n   --enabledThreads=... : A comma-separated list of requests that are enabled.");
    System.err.println(
        "                          Where tests can be one or more of:");
    System.err.println(
        "                            " + StringUtils.join(RequestType.values(), ", "));
    System.err.println(
        "                            or 'ALL' for everything (default); or 'READS'");
    System.err.println(
        "                            for all read requests, 'WRITES' for all write requests");

    System.exit(exitCode);
  }

  long getNumThreads() {
    return numThreads;
  }

  long getRuntimeInSeconds() {
    return runtimeInSeconds;
  }
  
  long getRuntimeInNanoseconds() {
    return runtimeInSeconds * 1_000_000_000;
  }

  /**
   * Return a random request type from all the enabled requests.
   * @param r
   * @return
   */
  List<RequestType> getEnabledRequestTypes() {
    return enabledRequestTypes;
  }
}
