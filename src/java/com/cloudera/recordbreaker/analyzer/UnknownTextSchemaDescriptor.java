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

import java.util.List;
import java.util.Random;
import java.util.Iterator;
import java.util.ArrayList;

import java.io.File;
import java.io.DataOutput;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;

import com.cloudera.recordbreaker.learnstructure.InferredType;
import com.cloudera.recordbreaker.learnstructure.LearnStructure;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

/************************************************************************
 * <code>UnknownTextSchemaDescriptor</code> returns schema data that we
 * figure out from the data itself.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see SchemaDescriptor
 *************************************************************************/
public class UnknownTextSchemaDescriptor extends GenericSchemaDescriptor {
  public static String SCHEMA_ID = "recordbreaker-recovered";
  static int MAX_LINES = 1000;
  InferredType typeTree;

  public UnknownTextSchemaDescriptor(DataDescriptor dd) throws IOException {
    super(dd);
  }

  public UnknownTextSchemaDescriptor(DataDescriptor dd, String schemaRepr, byte[] miscPayload) throws IOException {
    super(dd, schemaRepr);
    this.randId = new Random().nextInt();
    
    // Deserialize the payload string into the parser
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(miscPayload));
    try {
      this.typeTree = InferredType.readType(in);
    } finally {
      in.close();
    }
  }

  int randId;
  void computeSchema() throws IOException {
    this.randId = new Random().nextInt();    
    LearnStructure ls = new LearnStructure();
    FileSystem fs = FSAnalyzer.getInstance().getFS();
    FileSystem localFS = FileSystem.getLocal(new Configuration());
    Path inputPath = dd.getFilename();

    File workingParserFile = File.createTempFile("textdesc", "typetree", null);
    File workingSchemaFile = File.createTempFile("textdesc", "schema", null);
    
    ls.inferRecordFormat(fs, inputPath, localFS, new Path(workingSchemaFile.getCanonicalPath()), new Path(workingParserFile.getCanonicalPath()), null, null, false, MAX_LINES);

    this.schema = Schema.parse(workingSchemaFile);
    DataInputStream in = new DataInputStream(localFS.open(new Path(workingParserFile.getCanonicalPath())));
    try {
      this.typeTree = InferredType.readType(in);
    } catch(IOException iex) {
      iex.printStackTrace();
      throw iex;
    } finally {
      in.close();
    }
  }

  byte[] getPayload() {
    // Serialize the parser, return the resulting string
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    try {
      this.typeTree.write(out);
      out.close();      
    } catch (IOException iex) {
      return new byte[0];
    } finally {
    }
    return baos.toByteArray();
  }

  /**
   * Iterate through Avro-encoded rows of the file
   */
  public Iterator getIterator() {
    return new Iterator() {
      BufferedReader in = null;
      Object nextElt = null;
      {
        try {
          in = new BufferedReader(new InputStreamReader(dd.getRawBytes()));
          nextElt = lookahead();
        } catch (IOException iex) {
          nextElt = null;
        }
      }
      public boolean hasNext() {
        return nextElt != null;
      }
      public synchronized Object next() {
        Object toReturn = nextElt;
        nextElt = lookahead();
        return toReturn;
      }
      public void remove() {
        throw new UnsupportedOperationException();
      }
      Object lookahead() {
        try {
          String str = null;
          while ((str = in.readLine()) != null) {
            GenericContainer resultObj = typeTree.parse(str);
            if (resultObj != null) {
              return resultObj;
            }
          }
          if (in != null) {
            in.close();
            in = null;
          }
        } catch (IOException iex) {
        }
        return null;
      }
    };
  }
  
  /**
   * @return a <code>String</code> that annotates the schema
   */
  public String getSchemaSourceDescription() {
    return SCHEMA_ID;
  }
}