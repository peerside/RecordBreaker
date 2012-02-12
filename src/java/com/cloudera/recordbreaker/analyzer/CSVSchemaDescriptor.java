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
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import au.com.bytecode.opencsv.CSVParser;

/**
 * <code>CSVSchemaDescriptor</code> captures the schema that we extract from a CSV file.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see SchemaDescriptor
 */
public class CSVSchemaDescriptor implements SchemaDescriptor {
  static int MAX_LINES = 1000;

  File f;
  boolean hasHeaderRow;
  Schema schema;
  String schemaIdentifier;
  String schemaSrcDesc;
  
  public CSVSchemaDescriptor(File f) throws IOException {
    this.f = f;
    computeTypes(f);
  }

  /**
   * <code>identifyType</code> returns one of a handful of type identifiers
   * for the given string value.  Possibilities include int, float, date, string,
   * etc.
   *
   * @param val a <code>String</code> value
   * @return a <code>String</code> value
   */
  private Schema.Type identifyType(String val) {
    try {
      Integer.parseInt(val);
      return Schema.Type.INT;
    } catch (NumberFormatException nfe) {
    }
    try {
      Double.parseDouble(val);
      return Schema.Type.DOUBLE;
    } catch (NumberFormatException nfe) {
    }
    return Schema.Type.STRING;
  }

  /**
   * <code>combineTypes()</code> finds the least-common-denominator type
   * between the two input types.
   */
  private Schema.Type combineTypes(Schema.Type typeA, Schema.Type typeB) {
    if (typeA == Schema.Type.NULL && typeB == Schema.Type.NULL) {
      return Schema.Type.STRING;
    }
    if (typeA == Schema.Type.NULL) {
      return typeB;
    }
    if (typeB == Schema.Type.NULL) {
      return typeA;
    }
    if (typeA == typeB) {
      return typeA;
    }
    if ((typeA == Schema.Type.INT || typeB == Schema.Type.INT) &&
        (typeA == Schema.Type.DOUBLE || typeB == Schema.Type.DOUBLE)) {
      return Schema.Type.DOUBLE;
    }
    return Schema.Type.STRING;
  }

  /**
   * <code>computeTypes</code> examines the CSV file and tries to figure out the
   * columnar data types.  It also tests if there's a CSV header that it can extract.
   */
  void computeTypes(File f) throws IOException {   
    //
    // 1.  Go through all columns in the CSV and identify cell data types
    //
    int numColumns = 0;
    List<String> firstRow = new ArrayList<String>();
    List<List<Schema.Type>> allEltTypes = new ArrayList<List<Schema.Type>>();
    CSVParser parser = new CSVParser();    
    BufferedReader in = new BufferedReader(new FileReader(f));
    try {
      int lineno = 0;
      String s = null;
      while ((s = in.readLine()) != null) {
        List<Schema.Type> schemaTypes = new ArrayList<Schema.Type>();
        String parts[] = parser.parseLine(s);

        for (int i = 0; i < parts.length; i++) {
          String elt = parts[i];
          if (elt.startsWith(",")) {
            elt = elt.substring(1);
          }
          elt = elt.trim();
          if (elt.startsWith("\"") && elt.endsWith("\"")) {
            elt = elt.substring(1, elt.length()-1);
            elt = elt.trim();
          }

          if (lineno == 0) {
            firstRow.add(elt);
          } else {
            schemaTypes.add(identifyType(elt));
          }
        }

        if (lineno == 0) {
          numColumns = firstRow.size();
        } else {
          allEltTypes.add(schemaTypes);
        }
        lineno++;
        if (lineno >= MAX_LINES) {
          break;
        }
      }
    } finally {
      in.close();
    }

    //
    // 2.  Compute a type profile for each of the CSV columns.
    // If all the cells in a column have the same type, this is easy.
    // If not, figure out a type that characterizes the entire column.
    //
    List<Schema.Type> columnTypes = new ArrayList<Schema.Type>();
    for (int curCol = 0; curCol < numColumns; curCol++) {
      Schema.Type columnType = Schema.Type.NULL;
      for (List<Schema.Type> rowTypes: allEltTypes) {
        if (curCol < rowTypes.size()) {
          Schema.Type cellType = rowTypes.get(curCol);
          columnType = combineTypes(columnType, cellType);
        }
      }
      columnTypes.add(columnType);
    }

    //
    // 3.  Figure out whether there's a header row.  We believe there's
    // a header if all of the first row are strings, and if there's a type
    // clash with the remainder of the column.
    //
    boolean headerAllStrings = true;
    boolean typeClash = false;
    for (int i = 0; i < numColumns; i++) {
      String headerValue = firstRow.get(i);
      Schema.Type headerType = identifyType(headerValue);
      if (headerType != Schema.Type.STRING) {
        headerAllStrings = false;
      }
      Schema.Type columnType = columnTypes.get(i);
      if (headerType != columnType) {
        typeClash = true;
      }
    }

    // Now reason about the types we see
    this.hasHeaderRow = false;
    if (headerAllStrings && typeClash) {
      // Definitely a header row
      hasHeaderRow = true;
    } else if (headerAllStrings && ! typeClash) {
      // Still may be a header row, but harder to say
      boolean allStringCols = true;
      for (Schema.Type columnType: columnTypes) {
        if (columnType != Schema.Type.STRING) {
          allStringCols = false;
        }
      }
      if (! allStringCols) {
        hasHeaderRow = true;
      }
    }

    //
    // 4.  Turn the extracted type and header info into a Schema.
    //
    List<Schema.Field> schemaFields = new ArrayList<Schema.Field>();
    for (int i = 0; i < numColumns; i++) {
      String fieldName = "anon_" + i;
      String fieldDoc = "csv-noheader-" + fieldName;
      Schema.Type fieldType = columnTypes.get(i);
      if (hasHeaderRow) {
        fieldName = firstRow.get(i);
        fieldName = fieldName.replaceAll(" ","_");
        fieldDoc = "csv-header-extract-" + fieldName;
      }
      schemaFields.add(new Schema.Field(fieldName, Schema.create(fieldType), fieldDoc, null));
    }
    this.schema = Schema.createRecord(schemaFields);

    //
    // 5.  Figure out the schemaIdentifier and 'description' fields.
    //
    this.schemaIdentifier = (schema == null ? "" : schema.toString());
    this.schemaSrcDesc = "Extracted data types from CSV.  " + (hasHeaderRow ? "CSV header detected; used topic-specific field names." : "No CSV headert detected; used anonymous field names.");
  }

  /**
   * Return the CSV-derived schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Return an object to iterate through all the schema-conformant rows
   * of the CSV.  The Iterator returns instances of Avro's GenericRecord.
   */
  public Iterator getIterator() {
    return new Iterator() {
      CSVParser parser;
      int rowNum;
      Object nextElt = null;
      BufferedReader in = null;
      {
        rowNum = 0;
        try {
          this.parser = new CSVParser();
          in = new BufferedReader(new FileReader(f));
          nextElt = lookahead();          
        } catch (IOException iex) {
          this.nextElt = null;
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
        String s = null;
        try {
          List<Schema.Field> curFields = schema.getFields();
          while ((s = in.readLine()) != null) {
            rowNum++;
            if (rowNum == 1 && hasHeaderRow) {
              continue;
            }
            // Parse each line in the file
            GenericData.Record cur = null;            
            String parts[] = parser.parseLine(s);
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
            if (cur != null) {
              return cur;
            }
          }
          if (s == null) {
            in.close();
          }
        } catch (IOException iex) {
          iex.printStackTrace();
        }
        return null;
      }

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
    };
  }
  
  /**
   * A string that uniquely identifies the file's schema.  Not necessarily
   * human-readable.
   */
  public String getSchemaIdentifier() {
    return schemaIdentifier;
  }

  /**
   * Human-readable string that summarizes the file's structure
   */
  public String getSchemaSourceDescription() {
    return schemaSrcDesc;
  }
}