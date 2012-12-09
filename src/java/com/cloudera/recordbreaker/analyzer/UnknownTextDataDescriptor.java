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
import java.io.InputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.util.List;
import java.util.ArrayList;

import org.apache.avro.Schema;

import com.cloudera.recordbreaker.schemadict.SchemaSuggest;
import com.cloudera.recordbreaker.schemadict.DictionaryMapping;
import com.cloudera.recordbreaker.learnstructure.LearnStructure;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;

/**
 * <code>UnknownTextDataDescriptor</code> encapsulates log files with which we are unfamiliar.
 * It is the only DataDescriptor implementation to use the LearnStructure and SchemaDictionary
 * work.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 */
public class UnknownTextDataDescriptor extends GenericDataDescriptor {
  public static String TEXTDATA_TYPE = "structured-text";
  
  /**
   * Test whether the input param is a text file.
   * We do this by examining the first k bytes.  If 90% or more
   * of them are ASCII chars, then we assume it's text.
   */
  final static double asciiThreshold = 0.9;
  public static boolean isTextData(FileSystem fs, Path p) {
    try {
      BufferedInputStream in = new BufferedInputStream(fs.open(p));
      try {
        byte buf[] = new byte[1024];
        int numBytes = in.read(buf);
        if (numBytes < 0) {
          return false;
        }
        int numASCIIChars = 0;
        for (int i = 0; i < numBytes; i++) {
          if (buf[i] >= 32 && buf[i] < 128) {
            numASCIIChars++;
          }
        }
        return ((numASCIIChars / (1.0 * numBytes)) > asciiThreshold);
      } finally {
        in.close();
      }
    } catch (IOException iex) {
      return false;
    }
  }
  
  File schemaDictDir;
  List<SchemaDescriptor> schemaDescriptors = new ArrayList<SchemaDescriptor>();
  
  /**
   * Creates a new <code>UnknownTextDataDescriptor</code>.
   */
  public UnknownTextDataDescriptor(FileSystem fs, Path p, File schemaDictDir) throws IOException {
    super(p, fs, TEXTDATA_TYPE);
    
    FileSystem localFS = FileSystem.getLocal(new Configuration());
    this.schemaDictDir = schemaDictDir;
    UnknownTextSchemaDescriptor tsd = new UnknownTextSchemaDescriptor(this);
    this.schemas.add(tsd);
    
    // The most basic schema descriptor is the raw one that captures the anonymous avro file
    // schemaDescriptors.add(new UnknownTextSchemaDescriptor(localFS, new Path(workingAvroFile.getCanonicalPath())));
    // Remove schema dictionary suggestion until we're more confident it's actually useful.
    /**
     // We might be able to find more interesting descriptors by using the SchemaDictionary     
     SchemaSuggest ss = new SchemaSuggest(schemaDictDir);
     List<DictionaryMapping> schemaMappings = ss.inferSchemaMapping(workingAvroFile, 10);
     int count = 0;
     for (DictionaryMapping dm: schemaMappings) {
       // Obtaining a dictionarymapping via 'inferSchemaMapping' is like acquiring a transformation function that hasn't been applied yet.
       // We apply the function to obtain a novel Avro file, then return a schema descriptor for this transformed data file.
       schemaDescriptors.add(new UnknownTextSchemaDescriptor(dm.applyAvroTransform(workingAvroFile, File.createTempFile("textdesc-" + count, "avro", null))));
       count++;
     }
    **/
  }

  public UnknownTextDataDescriptor(FileSystem fs, Path p, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, TEXTDATA_TYPE, schemaReprs, schemaDescs, schemaBlobs);
  }

  SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    return new UnknownTextSchemaDescriptor(this, schemaRepr, blob);
  }
}