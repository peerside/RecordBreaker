/*
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.recordbreaker.analyzer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;

/*********************************************************************************
 * <code>FormatAnalyzer</code> takes an arbitrary input file and generates a
 * file-appropriate data descriptor.  Depending on the filetype, that descriptor
 * can entail a large or small amount of work.  It can also yield a large or small
 * amount of metadata about the file's contents.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 **********************************************************************************/
public class FormatAnalyzer {
  private static final Log LOG = LogFactory.getLog(FormatAnalyzer.class);    
  final static int MAX_ANALYSIS_LINES = 400;
  File schemaDbDir;
  
  /**
   * Creates a new <code>FormatAnalyzer</code> instance.
   */
  public FormatAnalyzer(File schemaDbDir) {
    this.schemaDbDir = schemaDbDir;
  }

  /**
   * Create a file-appropriate DataDescriptor instance.
   *
   * Right now we just use the file ending to figure out what to do,
   * but this will become unsatisfactory pretty quickly.
   *
   * @param f a <code>File</code> value
   * @return a <code>DataDescriptor</code> value
   */
  public DataDescriptor describeData(FileSystem fs, Path p) throws IOException {
    FileStatus fstatus = fs.getFileStatus(p);
    String fname = p.getName();

    // Test to see if the file is one of a handful of known structured formats.
    if (CSVDataDescriptor.isCSV(fs, p)) {
      return new CSVDataDescriptor(p, fs);
    } else if (fname.endsWith(".xml")) {
      return new XMLDataDescriptor(p, fs);
    } else if (fname.endsWith(".avro")) {
      return new AvroDataDescriptor(p, fs);
    } else if (AvroSequenceFileDataDescriptor.isAvroSequenceFile(fs, p)) {
      return new AvroSequenceFileDataDescriptor(p, fs);
    } else if (SequenceFileDataDescriptor.isSequenceFile(fs, p)) {
      return new SequenceFileDataDescriptor(p, fs);
    } else if (ApacheDataDescriptor.isApacheLogFile(fs, p)) {
      return new ApacheDataDescriptor(p, fs);
    } else if (SyslogDataDescriptor.isSyslogFile(fs, p)) {
      return new SyslogDataDescriptor(p, fs);      
    } else {
      // It's not one of the known formats, so apply LearnStructure 
      // to obtain the structure.
      if (UnknownTextDataDescriptor.isTextData(fs, p)) {
        try {
          return new UnknownTextDataDescriptor(fs, p, schemaDbDir);
        } catch (Exception iex) {
          //iex.printStackTrace();
        }
      }
      // If that doesn't work, then give up and call it unstructured.  You
      // can't run queries on data in this format.
      return new UnstructuredFileDescriptor(fs, p);
    }
  }

  public DataDescriptor loadDataDescriptor(FileSystem fs, Path p, String identifier, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    if (AvroDataDescriptor.AVRO_TYPE.equals(identifier)) {
      return new AvroDataDescriptor(p, fs, schemaReprs, schemaDescs, schemaBlobs);
    } else if (CSVDataDescriptor.CSV_TYPE.equals(identifier)) {
      return new CSVDataDescriptor(p, fs, schemaReprs, schemaDescs, schemaBlobs);
    } else if (SequenceFileDataDescriptor.SEQFILE_TYPE.equals(identifier)) {
      return new SequenceFileDataDescriptor(p, fs, schemaReprs, schemaDescs, schemaBlobs);      
    } else if (XMLDataDescriptor.XML_TYPE.equals(identifier)) {
      return new XMLDataDescriptor(p, fs, schemaReprs, schemaDescs, schemaBlobs);
    } else if (AvroSequenceFileDataDescriptor.AVROSEQFILE_TYPE.equals(identifier)) {
      return new AvroSequenceFileDataDescriptor(p, fs, schemaReprs, schemaDescs, schemaBlobs);
    } else if (ApacheDataDescriptor.APACHE_TYPE.equals(identifier)) {
      return new ApacheDataDescriptor(p, fs, schemaReprs, schemaDescs, schemaBlobs);
    } else if (SyslogDataDescriptor.SYSLOG_TYPE.equals(identifier)) {
      return new SyslogDataDescriptor(p, fs, schemaReprs, schemaDescs, schemaBlobs);
    } else if (UnknownTextDataDescriptor.TEXTDATA_TYPE.equals(identifier)) {
      return new UnknownTextDataDescriptor(fs, p, schemaReprs, schemaDescs, schemaBlobs);
    } else {
      return new UnstructuredFileDescriptor(fs, p);
    }
  }

  /**
   * Describe <code>main</code> method here.
   *
   * @param argv[] a <code>String</code> value
   * @exception IOException if an error occurs
   */
  public static void main(String argv[]) throws IOException {
    if (argv.length < 1) {
      System.err.println("Usage: FormatAnalyzer <inputfile> <schemaDbDir>");
      return;
    }

    FileSystem fs = FileSystem.getLocal(null);
    Path inputFile = new Path(new File(argv[0]).getCanonicalPath());
    File schemaDbDir = new File(argv[1]).getCanonicalFile();
    FormatAnalyzer fa = new FormatAnalyzer(schemaDbDir);

    DataDescriptor descriptor = fa.describeData(fs, inputFile);
    System.err.println("Filename: " + descriptor.getFilename());
    System.err.println("Filetype identifier: " + descriptor.getFileTypeIdentifier());
    List<SchemaDescriptor> schemas = descriptor.getSchemaDescriptor();
    if (schemas == null) {
      System.err.println("No schema found.");
    } else {
      System.err.println("Num schemas found: " + schemas.size());
      System.err.println();
      for (SchemaDescriptor sd: schemas) {
        Schema s = sd.getSchema();
        System.err.println("Schema src desc: " + sd.getSchemaSourceDescription());
        System.err.println();
        System.err.println("Schema identifier: " + sd.getSchemaIdentifier());
        System.err.println();
        int i = 0;
        for (Iterator it = sd.getIterator(); it.hasNext(); ) {
          GenericData.Record curRow = (GenericData.Record) it.next();
          System.err.println(i + ". Elt: " + curRow);
          i++;
        }
      }
    }
  }
}