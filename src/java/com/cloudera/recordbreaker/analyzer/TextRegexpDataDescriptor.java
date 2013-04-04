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
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileOutputStream;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import org.apache.avro.Schema;

import com.cloudera.recordbreaker.hive.RegExpSerDe;

/***********************************************************************
 * Describe class <code>TextRegexpDataDescriptor</code> here.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see DataDescriptor
 ***********************************************************************/
public abstract class TextRegexpDataDescriptor extends GenericDataDescriptor {
  final static int MAX_LINES = 200;
  public static boolean isTextRegexpFile(FileSystem fs, Path p, List<Pattern> regexps) throws IOException {
    int totalCounts = 0;
    int matchCounts[] = new int[regexps.size()];
    
    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(p)));
    try {
      String cur = null;
      while ((cur = in.readLine()) != null) {
        for (int i = 0; i < regexps.size(); i++) {
          Pattern patt = regexps.get(i);
          Matcher m = patt.matcher(cur);
          if (m.find()) {
            matchCounts[i]++;
          }
        }
        totalCounts++;
        if (MAX_LINES >= 0 && totalCounts >= MAX_LINES) {
          break;
        }
      }
    } finally {
      in.close();
    }

    for (int i = 0; i < matchCounts.length; i++) {
      if (((1.0 * matchCounts[i]) / totalCounts) > 0.3) {
        return true;
      }
    }
    return false;
  }
  
  List<Pattern> regexps;
  List<Schema> localschemas;

  /**
   * Creates a new <code>TextRegexpDataDescriptor</code> instance,
   * tailored to the structure described in the param list.
   *
   * @param typeIdentifier a <code>String</code> value
   * @param regexps a <code>List<Pattern></code> value
   * @param schemas a <code>List<Schema></code> value
   */
  public TextRegexpDataDescriptor(Path p, FileSystem fs, String fileType, List<Pattern> regexps, List<Schema> schemaList) throws IOException {
    super(p, fs, fileType);
    this.regexps = regexps;
    this.localschemas = schemaList;
    schemas.add(new TextRegexpSchemaDescriptor(this, fileType, regexps, localschemas));
  }

  ///////////////////////////////////
  // GenericDataDescriptor
  //////////////////////////////////
  SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    // We can wholly ignore any params here.
    return new TextRegexpSchemaDescriptor(this, schemaId, regexps, localschemas);
  }

  public Schema getHiveTargetSchema() {
    TextRegexpSchemaDescriptor sd = (TextRegexpSchemaDescriptor) this.getSchemaDescriptor().get(0);
    List<Schema> unionFreeSchemas = SchemaUtils.getUnionFreeSchemasByFrequency(sd, 100, true);
    return unionFreeSchemas.get(0);
  }
  public String getHiveCreateTableStatement(String tablename) {
    TextRegexpSchemaDescriptor sd = (TextRegexpSchemaDescriptor) this.getSchemaDescriptor().get(0);
    File workingParserPath = null;
    try {
      workingParserPath = File.createTempFile("regexp", "parser", null);
      FileOutputStream out = new FileOutputStream(workingParserPath);
      try {
        out.write(sd.getPayload());
      } finally {
        out.close();
      }
    } catch (IOException iex) {
      iex.printStackTrace();
      return null;
    }
    Schema parentS = sd.getSchema();
    List<Schema> unionFreeSchemas = SchemaUtils.getUnionFreeSchemasByFrequency(sd, 100, true);
    String escapedSchemaString = unionFreeSchemas.get(0).toString();
    escapedSchemaString = escapedSchemaString.replace("'", "\\'");
    String creatTxt = "create table " + tablename + " ROW FORMAT SERDE 'com.cloudera.recordbreaker.hive.RegExpSerDe' WITH SERDEPROPERTIES('" +
      RegExpSerDe.DESERIALIZER + "'='" + workingParserPath.toString() + "', '" +
      RegExpSerDe.TARGET_SCHEMA + "'='" + escapedSchemaString + "') " +
      "STORED AS TEXTFILE";
    return creatTxt;
  }

  public String getHiveImportDataStatement(String tablename) {
    String fname = getFilename().toString();
    String localMarker = "";
    if (fname.startsWith("file")) {
      localMarker = " local ";
    }
    String loadTxt = "load data" + localMarker + "inpath '" + getFilename() + "' overwrite into table " + tablename;
    return loadTxt;
  }
}