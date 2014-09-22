#!/usr/bin/perl

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and


use strict;
use warnings;
use File::Basename;
use Getopt::Std;
use List::Util qw(shuffle);
use Time::HiRes qw(time sleep);

my $block_size = "256M";
my $file_size = "1GB";

sub usage {
  printf STDERR "   Benchmark to measure mean latency to read files of varying sizes in DFS.\n";
  printf STDERR "   Usage: %s -j <jar> -s <io-sizes> -n <reads-per-run>\n", basename($0);
  printf STDERR "\t\t\t\t\t-d <description> -o <output-file>\n";
  printf STDERR "     Options:\n";
  printf STDERR "       -j   : Benchmark jar file\n";
  printf STDERR "       -s   : Comma separated list of IO sizes e.g. 1M,4M,16M\n";
  printf STDERR "       -n   : Number of reads per file size for computing mean\n";
  printf STDERR "       -d   : Description for recording in results file\n";
  printf STDERR "       -o   : Results csv file, appended to if it exists\n";
  exit 1;
}

if (scalar @ARGV == 0 || $ARGV[0] eq "--help" || $ARGV[0] eq "-?") {
  usage();
}

# Parse command line options into state variables
#
my %options = ();
getopt('j:s:n:d:o:l', \%options);

foreach my $opt (qw(j n s o d)) {
  if (!exists $options{$opt}) {
    print "Missing option $opt\n";
    usage();
  }
}

my $jar_file = $options{"j"};
my $read_iterations = $options{"n"};
my @io_sizes = split(/,/, $options{"s"});
my $description = $options{"d"};
my $output_file = $options{"o"};
$description =~ s/,/;/g;

open(my $myFile, ">>$output_file") or die "Failed to open $output_file for appends";
printf $myFile "Test description, Block Size, File Size, IO Size, Lazy Persist Flag, Mean Read Latency, Mean Read Throughput\n";


# Now run the benchmark.
#
foreach my $lazy_persist (qw(true)) {
  foreach my $io_size (@io_sizes) {
    my @command = ("bin/hadoop",
                  "jar",
                  $jar_file,
                  "ReadFile");

    if ($lazy_persist eq "true") {
      push(@command, "--lazyPersist");
    }

    push(@command, $block_size, $file_size, $io_size, $read_iterations);

    my $mean_file_read_latency;
    my $mean_read_throughput;
    my @output = qx(@command);
    chomp(@output);
    for my $line (@output) {
      if ($line =~ m/^\s*Mean Time to read file fully:\s*(\w+)\s*$/i) {
        $mean_file_read_latency = $1;
      } elsif ($line =~ m/^\s*Mean Read Throughput:\s*(\w+)\s*$/i) {
        $mean_read_throughput = $1;
      }
    }

    if (!defined $mean_file_read_latency ||
        !defined $mean_read_throughput) {
      die "Failed to parse the output of the benchmark job @output";
    }
    
    # Print stats to screen.
    #
    printf "BlockSize=%s,FileSize=%s,IoSize=%s,LazyPersist=%s,MeanLatency=%s,MeanThroughput=%s\n",
          $block_size,
          $file_size,
          $io_size,
          $lazy_persist,
          $mean_file_read_latency,
          $mean_read_throughput;

    # And to output csv file.
    #
    printf $myFile "Read Perf Test - %s,%s,%s,%s,%s,%s,%s\n",
                    $description, $block_size, $file_size, $io_size,
                    $lazy_persist,
                    $mean_file_read_latency, $mean_read_throughput;
  }
}

