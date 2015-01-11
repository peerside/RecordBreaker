RecordBreaker - Automatically learn Avro structures from your data
========================================================================================

Introduction
----------------------------------------------------------------------------------------
RecordBreaker is a project that automatically turns your text-formatted data (server logs, sensor readings, etc) into structured Avro data, without any need to write parsers or extractors.  Its goal is to dramatically reduce the time spent preparing data for analysis, enabling more time for the analysis itself.

You can (and should!) read the full RecordBreaker tutorial here: [http://cloudera.github.com/RecordBreaker/](http://cloudera.github.com/RecordBreaker/)

The RecordBreaker repository is hosted at GitHub, here:
[https://github.com/cloudera/RecordBreaker](https://github.com/cloudera/RecordBreaker)

One interesting part of RecordBreaker is the FishEye system.  It's a
web-based tool for examining and managing the diverse datasets likely
to be found in a typical HDFS installation.  It draws features from
both filesystem management and database administration tools.  Most
interestingly, it uses RecordBreaker techniques to automatically
figure out the structure of files it finds.  You can run it by typing:



    bin/learnstructure fisheye -run <portnum> <localstoragedir>



Where __portnum__ is the HTTP port where FishEye will provide data to the
user, and __localstoreagedir__ is where it will maintain information
about a target filesystem.     


Installation
----------------------------------------------------------------------------------------
$ mvn package


Dependencies
----------------------------------------------------------------------------------------
-- Java JDK 1.6
-- Maven

