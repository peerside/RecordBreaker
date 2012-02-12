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
import java.util.ArrayList;

import org.apache.avro.Schema;

import com.cloudera.recordbreaker.schemadict.SchemaSuggest;
import com.cloudera.recordbreaker.schemadict.DictionaryMapping;
import com.cloudera.recordbreaker.learnstructure.LearnStructure;

/**
 * <code>UnknownTextDataDescriptor</code> encapsulates log files with which we are unfamiliar.
 * It is the only DataDescriptor implementation to use the LearnStructure and SchemaDictionary
 * work.
 *
 * @author "Michael Cafarella" <mjc@caen-cse-141-212-202-194.wireless.engin.umich.edu>
 * @version 1.0
 * @since 1.0
 * @see DataDescriptor
 */
public class UnknownTextDataDescriptor implements DataDescriptor {
  File f;
  File schemaDictDir;
  File workingAvroFile;
  File workingSchemaFile;
  List<SchemaDescriptor> schemaDescriptors = new ArrayList<SchemaDescriptor>();
  
  /**
   * Creates a new <code>UnknownTextDataDescriptor</code>.
   */
  public UnknownTextDataDescriptor(File f, File schemaDictDir) throws IOException {
    this.f = f;
    this.schemaDictDir = schemaDictDir;    
    this.workingAvroFile = File.createTempFile("textdesc", "avro", null);
    this.workingSchemaFile = File.createTempFile("textdesc", "schema", null);

    // 1.  We already have a synthesized Avro data, with anonymous fields.
    // 2.  Test it against the known database of types.
    // 3.  Return the top-k types/schemas that we discover, as long as they pass a threshold.
    System.err.println("--------------------------------------");
    System.err.println("About to infer structure...");
    System.err.println("--------------------------------------");    
    LearnStructure ls = new LearnStructure();
    ls.inferRecordFormat(f, workingSchemaFile, null, null, workingAvroFile, false);

    // The most basic schema descriptor is the raw one that captures the anonymous avro file
    schemaDescriptors.add(new UnknownTextSchemaDescriptor(workingAvroFile));

    // We might be able to find more interesting descriptors by using the SchemaDictionary
    //SchemaSuggest ss = new SchemaSuggest(schemaDictDir);
    //List<DictionaryMapping> schemaMappings = ss.inferSchemaMapping(workingAvroFile, 10);
    //int count = 0;
    //for (DictionaryMapping dm: schemaMappings) {
      // Obtaining a dictionarymapping via 'inferSchemaMapping' is like acquiring a transformation function that hasn't been applied yet.
      // We apply the function to obtain a novel Avro file, then return a schema descriptor for this transformed data file.
      //schemaDescriptors.add(new UnknownTextSchemaDescriptor(dm.applyAvroTransform(workingAvroFile, File.createTempFile("textdesc-" + count, "avro", null))));
    //count++;
    //}
  }

  /**
   * @return the <code>File</code> value
   */
  public File getFilename() {
    return this.f;
  }

  /**
   * @return a <code>String</code> value that describes the filetype
   */
  public String getFileTypeIdentifier() {
    return "structured-text";
  }

  /**
   * <code>getSchemaDescriptor</code> uses the results of a SchemaDictionary search.
   */
  public List<SchemaDescriptor> getSchemaDescriptor() {
    return schemaDescriptors;
  }
}