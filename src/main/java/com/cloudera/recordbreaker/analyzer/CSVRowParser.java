/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
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
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import au.com.bytecode.opencsv.CSVParser;

/***********************************************************************
 * <code>CSVRowParser</code> converts a single row of a CSV file into an
 * avro object with a given schema.  If the input line is empty or is the header
 * row, the parser returns a null object.
 *
 * @author "Michael Cafarella"
 ***********************************************************************/
public class CSVRowParser {
  CSVParser parser;
  Schema schema;
  List<Schema.Field> curFields;
  String headerHash;
  
  public CSVRowParser(Schema schema, String headerHash) {
    this.parser = new CSVParser();
    this.schema = schema;
    this.curFields = schema.getFields();
    this.headerHash = headerHash;
  }

  /**
   * <code>parseRow</code> returns a GenericData.Record that matches the
   * init'ed Schema and corresponds to the given row of text.
   *
   * Returns null if there's no match, or if we're looking at the header row.
   */
  public GenericData.Record parseRow(String row) {
    if (("" + row.hashCode()).compareTo(headerHash) == 0) {
      return null;
    }
    try {
      GenericData.Record cur = null;
      String parts[] = parser.parseLine(row);
      int fieldPos = 0;

      for (int i = 0; i < parts.length; i++) {
        if (cur == null) {
          cur = new GenericData.Record(schema);
        }
        String rawFieldValue = parts[i];
        if (rawFieldValue.startsWith(",")) {
          rawFieldValue = rawFieldValue.substring(1);
        }
        rawFieldValue = rawFieldValue.trim();
        if (rawFieldValue.startsWith("\"") && rawFieldValue.endsWith("\"")) {
          rawFieldValue = rawFieldValue.substring(1, rawFieldValue.length()-1);
          rawFieldValue = rawFieldValue.trim();
        }

        Schema.Field curField = curFields.get(fieldPos);
        String fieldName = curField.name();
        Schema fieldType = curField.schema();
        cur.put(fieldName, parseField(rawFieldValue, fieldType.getType()));
        fieldPos++;
      }
      return cur;
    } catch (IOException iex) {
      iex.printStackTrace();
      return null;
    } catch (NumberFormatException nfe) {
      nfe.printStackTrace();
      return null;
    }
  }

  /**
   * Parse a single CSV-separated field with the given type
   */
  Object parseField(String rawFieldValue, Schema.Type fieldType) throws IOException {
    Object fieldValue = null;
    if (fieldType == Schema.Type.INT) {
      try {
        fieldValue = Integer.parseInt(rawFieldValue);
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
        fieldValue = 0;
      }
    } else if (fieldType == Schema.Type.DOUBLE) {
      fieldValue = Double.parseDouble(rawFieldValue);
    } else if (fieldType == Schema.Type.STRING) {
      fieldValue = rawFieldValue;
    } else {
      throw new IOException("Unexpected field-level schema type: " + fieldType);
    }
    return fieldValue;
  }
}