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
  File schemaDbDir;
  KnownTextFormatLibrary formatLibrary;
  
  /**
   * Creates a new <code>FormatAnalyzer</code> instance.
   */
  public FormatAnalyzer(File schemaDbDir) {
    this.schemaDbDir = schemaDbDir;
    this.formatLibrary = new KnownTextFormatLibrary();
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
  public DataDescriptor describeData(File f, int maxLines) throws IOException {
    String fname = f.getName();
    // Test to see if the file is one of a handful of known structured formats.
    if (CSVDataDescriptor.isCSV(f)) {
      return new CSVDataDescriptor(f);
    } else if (fname.endsWith(".xml")) {
      return new XMLDataDescriptor(f);
    } else if (fname.endsWith(".avro")) {
      return new AvroDataDescriptor(f);
    } else {
      // Even if it's text, it could have a regular and expected structure
      // (e.g., Apache access logs).  The formatLibrary object keeps a list
      // of these cases.  Ask the formatLibrary to test the input data and
      // see if it corresponds to one of the known formats.
      //
      DataDescriptor retval = formatLibrary.createDescriptorForKnownFormat(f);
      if (retval != null) {
        return retval;
      } else {
        // It's not one of the known formats, so apply LearnStructure (and
        // SchemaDictionary), then emit the resulting Avro data.
        try {
          return new UnknownTextDataDescriptor(f, schemaDbDir, maxLines);
        } catch (Exception iex) {
          // If that doesn't work, then give up and call it unstructured
          return new UnstructuredFileDescriptor(f);
        }
      }
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

    File inputFile = new File(argv[0]);
    File schemaDbDir = new File(argv[1]);
    FormatAnalyzer fa = new FormatAnalyzer(schemaDbDir);

    DataDescriptor descriptor = fa.describeData(inputFile, -1);
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