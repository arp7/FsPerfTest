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

# For data throughput tests it is better to work with a single large file
# instead of multiple smaller files.
my $files_per_run = 1;

sub usage {
  printf STDERR "   Benchmark to measure HDFS write throughput with varying IO sizes\n";
  printf STDERR "   Usage: %s -j <jar> -i <io-sizes> -t <total-write-size>\n", basename($0);
  printf STDERR "\t\t\t\t\t-d <description> -o <output-file>\n";
  printf STDERR "     Options:\n";
  printf STDERR "       -j   : Benchmark jar file\n";
  printf STDERR "       -i   : Comma separated list of IO sizes e.g. 4K,64K,1M\n";
  printf STDERR "       -t   : Total data per IO size workload. e.g. 10GB\n";
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
getopt('j:i:t:d:o:', \%options);

foreach my $opt (qw(j i t d o)) {
  if (!exists $options{$opt}) {
    print "Missing option $opt\n";
    usage();
  }
}

my $jar_file = $options{"j"};
my @io_sizes = shuffle(split(/,/, $options{"i"}));
my $file_size = $options{"t"};
my $description = $options{"d"};
my $output_file = $options{"o"};
$description =~ s/,/;/g;

open(my $myfile, ">>$output_file") or die "Failed to open $output_file for appends";

# Now run the benchmark.
#
foreach my $io_size (@io_sizes) {
  my @command = ("bin/hadoop",
                 "jar",
                 $jar_file,
                 "WriteFile",
                 $io_size,
                 $file_size,
                 $files_per_run);
  my $time_per_file;
  my $throughput;
  my @output = qx(@command);
  chomp(@output);
  for my $line (@output) {
    if ($line =~ m/^\s*Mean Time per file:\s*(\w+)\s*$/i) {
      $time_per_file = $1;
    }
    if ($line =~ m/^\s*Mean Throughput:\s*(\w+)\s*$/i) {
      $throughput = $1;
    }
  }

  if (!defined $time_per_file || !defined $throughput) {
    die "Failed to parse the output of the benchmark job";
  }
  
  # Print stats to screen.
  #
  printf "IoSize=%s,FileSize=%s,Time=%s,Throughput=%s\n",
         $io_size, $file_size, $time_per_file, $throughput;

  # And to output csv file.
  #
  printf $myfile "Throughput Test,%s,%s,%s,%d,%s,%s\n",
                  $description, $file_size, $io_size,
                  $files_per_run, $time_per_file, $throughput;
}

