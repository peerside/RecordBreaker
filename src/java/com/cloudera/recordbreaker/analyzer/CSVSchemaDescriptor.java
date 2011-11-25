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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;

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
  static Pattern pattern = Pattern.compile("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)");

  private static String CELL_TYPE_STRING = "string";
  private static String CELL_TYPE_INT = "int";
  private static String CELL_TYPE_DOUBLE = "double";
  
  Schema schema;
  String schemaIdentifier;
  String schemaSrcDesc;
  
  public CSVSchemaDescriptor(File f) throws IOException {
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
  private String identifyType(String val) {
    try {
      Long.parseLong(val);
      return CELL_TYPE_INT;
    } catch (NumberFormatException nfe) {
    }
    try {
      Double.parseDouble(val);
      return CELL_TYPE_DOUBLE;
    } catch (NumberFormatException nfe) {
    }
    return CELL_TYPE_STRING;
  }

  /**
   * <code>combineTypes()</code> finds the least-common-denominator type
   * between the two input types.
   */
  private String combineTypes(String typeA, String typeB) {
    if (typeA == null && typeB == null) {
      return CELL_TYPE_STRING;
    }
    if (typeA == null) {
      return typeB;
    }
    if (typeB == null) {
      return typeA;
    }
    if (typeA.equals(typeB)) {
      return typeA;
    }
    if ((typeA.equals(CELL_TYPE_INT) || typeB.equals(CELL_TYPE_INT)) &&
        (typeA.equals(CELL_TYPE_DOUBLE) || typeB.equals(CELL_TYPE_DOUBLE))) {
      return CELL_TYPE_DOUBLE;
    }
    return CELL_TYPE_STRING;
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
    List<String> firstRow = null;
    List<List<String>> allEltTypes = new ArrayList<List<String>>();    
    BufferedReader in = new BufferedReader(new FileReader(f));
    try {
      int lineno = 0;
      String s = null;
      while ((s = in.readLine()) != null) {
        Matcher m = pattern.matcher(s);
        List<String> elts = new ArrayList<String>();
        while (m.find()) {
          String elt = m.group();
          if (elt.startsWith(",")) {
            elt = elt.substring(1);
          }
          elt = elt.trim();
          if (elt.startsWith("\"") && elt.endsWith("\"")) {
            elt = elt.substring(1, elt.length()-1);
            elt = elt.trim();
          }
          if (lineno == 0) {
            elts.add(elt);
          } else {
            elts.add(identifyType(elt));
          }
        }

        if (lineno == 0) {
          firstRow = elts;
          numColumns = firstRow.size();
        } else {
          allEltTypes.add(elts);
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
    List<String> columnTypes = new ArrayList<String>();
    for (int curCol = 0; curCol < numColumns; curCol++) {
      String columnType = null;
      for (List<String> rowTypes: allEltTypes) {
        String cellType = rowTypes.get(curCol);
        columnType = combineTypes(columnType, cellType);
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
      String headerType = identifyType(headerValue);
      if (! headerType.equals(CSVSchemaDescriptor.CELL_TYPE_STRING)) {
        headerAllStrings = false;
      }
      String columnType = columnTypes.get(i);
      if (! headerType.equals(columnType)) {
        typeClash = true;
      }
    }

    // Now reason about the types we see
    boolean hasHeaderRow = false;
    if (headerAllStrings && typeClash) {
      // Definitely a header row
      hasHeaderRow = true;
    } else if (headerAllStrings && ! typeClash) {
      // Still may be a header row, but harder to say
      boolean allStringCols = true;
      for (String columnType: columnTypes) {
        if (! columnType.equals(CSVSchemaDescriptor.CELL_TYPE_STRING)) {
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
      String fieldName = "anon." + i;
      String fieldDoc = "csv-noheader-" + fieldName;
      String fieldType = columnTypes.get(i);
      if (hasHeaderRow) {
        fieldName = firstRow.get(i);
        fieldName = fieldName.replaceAll(" ","_");
        fieldDoc = "csv-header-extract-" + fieldName;
      }

      Schema basicSchemaType = null;
      if (fieldType.equals(CELL_TYPE_STRING)) {
        basicSchemaType = Schema.create(Schema.Type.STRING);
      } else if (fieldType.equals(CELL_TYPE_INT)) {
        basicSchemaType = Schema.create(Schema.Type.INT);
      } else if (fieldType.equals(CELL_TYPE_DOUBLE)) {
        basicSchemaType = Schema.create(Schema.Type.DOUBLE);
      }
      schemaFields.add(new Schema.Field(fieldName, basicSchemaType, fieldDoc, null));
    }
    this.schema = Schema.createRecord(schemaFields);

    //
    // 5.  Figure out the schemaIdentifier and 'description' fields.
    //
    this.schemaIdentifier = (schema == null ? "" : schema.toString());
    this.schemaSrcDesc = "Extracted data types from CSV.  " + (hasHeaderRow ? "CSV header detected; used topic-specific field names." : "No CSV headert detected; used anonymous field names.");
  }

  public Schema getSchema() {
    return schema;
  }

  public Iterator getIterator() {
    return null;
  }
  
  public String getSchemaIdentifier() {
    return schemaIdentifier;
  }

  public String getSchemaSourceDescription() {
    return schemaSrcDesc;
  }
}