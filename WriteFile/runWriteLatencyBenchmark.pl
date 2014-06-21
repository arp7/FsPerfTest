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

my $io_size = "64KB";

sub usage {
  printf STDERR "   Benchmark to measure mean latency to create files of different sizes in DFS.\n";
  printf STDERR "   Usage: %s -j <jar> -s <file-sizes> -n <files-per-run>\n", basename($0);
  printf STDERR "\t\t\t\t\t-d <description> -o <output-file>\n";
  printf STDERR "     Options:\n";
  printf STDERR "       -j   : Benchmark jar file\n";
  printf STDERR "       -s   : Comma separated list of file sizes e.g. 4M,256M,1G\n";
  printf STDERR "       -n   : Number of DFS files per file size, for averaging\n";
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
getopt('j:s:n:d:o:', \%options);

foreach my $opt (qw(j n s o d)) {
  if (!exists $options{$opt}) {
    print "Missing option $opt\n";
    usage();
  }
}

my $jar_file = $options{"j"};
my $files_per_run = $options{"n"};
my @file_sizes = shuffle(split(/,/, $options{"s"}));
my $description = $options{"d"};
my $output_file = $options{"o"};
$description =~ s/,/;/g;

open(my $myfile, ">>$output_file") or die "Failed to open $output_file for appends";


# Now run the benchmark.
#
foreach my $file_size (@file_sizes) {
  my @command = ("bin/hadoop",
                 "jar",
                 $jar_file,
                 "WriteFile",
                 $io_size,
                 $file_size,
                 $files_per_run);

  my $time_per_file;
  my $time_to_create_file;
  my $time_to_write_data;
  my @output = qx(@command);
  chomp(@output);
  for my $line (@output) {
    if ($line =~ m/^\s*Mean Time per file:\s*(\w+)\s*$/i) {
      $time_per_file = $1;
    } elsif ($line =~ m/^\s*Mean Time to create file on NN:\s*(\w+)\s*$/i) {
      $time_to_create_file = $1;
    } elsif ($line =~ m/^\s*Mean Time to write data:\s*(\w+)\s*$/i) {
      $time_to_write_data = $1;
    }
  }

  if (!defined $time_per_file || 
      !defined $time_to_create_file ||
      !defined $time_to_write_data) {
    die "Failed to parse the output of the benchmark job";
  }
  
  # Print stats to screen.
  #
  printf "IoSize=%s,FileSize=%s,MeanTimeE2e=%s,MeanCreateTime=%s,MeanWriteTime=%s\n",
         $io_size,
         $file_size,
         $time_per_file,
         $time_to_create_file,
         $time_to_write_data;

  # And to output csv file.
  #
  printf $myfile "Latency Test,%s,%s,%s,%s,%s,%s,%s\n",
                  $description, $files_per_run, $io_size, $file_size,
                  $time_per_file, $time_to_create_file, $time_to_write_data;
}

