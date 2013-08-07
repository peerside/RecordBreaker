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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Arrays;
import java.util.Random;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;

/*******************************************************
 * The extracted schemas obtained by LearnStructure are
 * not always very user-friendly.  For example, they often
 * contain a lot of hard-to-understand unions; they also
 * support all the lines in an input file, even rare ones.
 * 
 * <code>SchemaUtils</code> exists to massage and edit these
 * Schemas after they've been emitted by LearnStructure.  The
 * functions here are often useful when presenting the schema
 * to the user, or building import code for other tools (eg, Hive).
 * 
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 ********************************************************/
public class SchemaUtils {
  static Random r = new Random();
  static class SchemaPair implements Comparable {
    int schemaId;
    int count;
    public SchemaPair(int schemaId, int count) {
      this.schemaId = schemaId;
      this.count = count;
    }
    public int compareTo(Object o) {
      SchemaPair sp = (SchemaPair) o;
      int result = count - sp.count;
      if (result == 0) {
        result = schemaId - sp.schemaId;
      }
      return result;
    }
  }

  /**
   * Takes a schema that potentially contains unions and converts it into
   * a list of union-free schemas observed with the given data object.
   */
  public static List<Schema> unrollUnionsWithData(Schema schema, Object grObj, boolean topLevelOnly) {
    return unrollUnionsWithData(schema, grObj, true, topLevelOnly);
  }

  static List<Schema> unrollUnionsWithData(Schema schema, Object grObj, boolean isTopLevel, boolean topLevelOnly) {
    if (schema.getType() == Schema.Type.RECORD && grObj instanceof GenericRecord) {
      GenericRecord gr = (GenericRecord) grObj;
      List<List<Schema>> fieldSchemaLists = new ArrayList<List<Schema>>();
      int targetTotal = 1;
      for (Schema.Field sf: schema.getFields()) {
        if (gr.get(sf.name()) == null) {
          return null;
        }
        List<Schema> fieldSchemaList = unrollUnionsWithData(sf.schema(), gr.get(sf.name()), false, topLevelOnly);
        if (fieldSchemaList == null) {
          return null;
        }
        fieldSchemaLists.add(fieldSchemaList);
        targetTotal *= fieldSchemaList.size();
      }

      List<Schema> outputSchemas = new ArrayList<Schema>();
      for (int i = 0; i < targetTotal; i++) {
        List<Schema.Field> newFields = new ArrayList<Schema.Field>();

        int j = 0;
        for (Schema.Field oldField: schema.getFields()) {
          List<Schema> curFieldSchemaList = fieldSchemaLists.get(j);
          newFields.add(new Schema.Field(oldField.name(), curFieldSchemaList.get(i % curFieldSchemaList.size()), oldField.doc(), null));
          j++;
        }
        Schema s = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
        s.setFields(newFields);
        outputSchemas.add(s);
      }
      return outputSchemas;
    } else if (schema.getType() == Schema.Type.UNION) {
      List<Schema> unrolledSchemas = new ArrayList<Schema>();

      if ((! topLevelOnly) || isTopLevel) {
        for (Schema s: schema.getTypes()) {
          List<Schema> subschemas = SchemaUtils.unrollUnionsWithData(s, grObj, false, topLevelOnly);
          if (subschemas != null) {
            unrolledSchemas.addAll(subschemas);
          }
        }
      } else {
        unrolledSchemas.add(schema);
      }
      return unrolledSchemas;
    } else if (schema.getType() == Schema.Type.ARRAY) {
      // Iterate through all elements of array; call unrollUnionsWithData() on each one.
      // Then deduplicate the resulting schemas
      TreeMap<String, Schema> seenSchemas = new TreeMap<String, Schema>();
      GenericArray gra = (GenericArray) grObj;
      for (int i = 0; i < gra.size(); i++) {
        List<Schema> result = unrollUnionsWithData(schema.getElementType(), gra.get(i), false, topLevelOnly);
        if (result != null) {
          for (Schema subS: result) {
            if (seenSchemas.get(subS.toString()) == null) {
              seenSchemas.put(subS.toString(), subS);
            }
          }
        }
      }
      
      // Xform the tree into a list, and return.
      List<Schema> newSchemas = new ArrayList<Schema>();
      for (Schema s: seenSchemas.values()) {
        newSchemas.add(Schema.createArray(s));
      }
      return newSchemas;
    } else {
      // Base type
      if (grObj instanceof GenericData.Record
          || grObj instanceof GenericData.Array) {
        return null;
      }
      List<Schema> retList = new ArrayList<Schema>();
      retList.add(schema);
      return retList;
    }
  }

  /**
   * <code>getUnionFreeSchemasByFrequency</code> will transform the schema of
   * a given SchemaDescriptor into a set of union-free schemas.  It will then
   * rank them by popularity in a data sample of 'maxRows' tuples from the file.
   * 
   * The resulting schema list is returned in descending order of frequency, and
   * only includes schemas that appeared at least once in the sample.
   */
  public static List<Schema> getUnionFreeSchemasByFrequency(SchemaDescriptor sd, int maxRows, boolean topLevelOnly) {
    Schema schema = sd.getSchema();

    // 1. Enumerate all the non-union schemas that we observe in the sample
    TreeMap<String, Integer> schemaCounts = new TreeMap<String, Integer>();
    int numRows = 0;
    TreeMap<String, Schema> uniqueUnrolledSchemas = new TreeMap<String, Schema>();    
    for (Iterator it = sd.getIterator(); it.hasNext(); ) {
      GenericData.Record gr = (GenericData.Record) it.next();
      List<Schema> grSchemas = SchemaUtils.unrollUnionsWithData(schema, gr, topLevelOnly);
      if (grSchemas != null) {
        for (Schema grs: grSchemas) {
          if (uniqueUnrolledSchemas.get(grs.toString()) == null) {
            uniqueUnrolledSchemas.put(grs.toString(), grs);
          }
          Integer oldCount = schemaCounts.get(grs.toString());
          if (oldCount == null) {
            oldCount = new Integer(0);
          }
          schemaCounts.put(grs.toString(), new Integer(oldCount.intValue() + 1));
        }
      }
      if (numRows >= maxRows) {
        break;
      }
      numRows++;
    }

    List<Schema> allSchemas = new ArrayList(uniqueUnrolledSchemas.values());
    List<SchemaPair> schemaFrequency = new ArrayList<SchemaPair>();
    for (int i = 0; i < allSchemas.size(); i++) {
      Schema s1 = allSchemas.get(i);
      Integer sCount = schemaCounts.get(s1.toString());
      schemaFrequency.add(new SchemaPair(i, sCount.intValue()));
    }

    SchemaPair sortedByFreq[] = schemaFrequency.toArray(new SchemaPair[schemaFrequency.size()]);
    Arrays.sort(sortedByFreq, Collections.reverseOrder());
    List<Schema> schemasRankedByFreq = new ArrayList<Schema>();
    for (int i = 0; i < sortedByFreq.length; i++) {
      if (sortedByFreq[i].count > 0) {
        schemasRankedByFreq.add(allSchemas.get(sortedByFreq[i].schemaId));
      }
    }
    return schemasRankedByFreq;
  }

  /**
   * Takes an Avro record Schema and creates dot-separated names for each
   * leaf-level field.  The input Schema should *not* have any unions.
   */
  public static List<String> flattenNames(Schema schema) {
    if (schema.getType() == Schema.Type.RECORD) {
      List<String> schemaLabels = new ArrayList<String>();
      for (Schema.Field field: schema.getFields()) {
        Schema fieldSchema = field.schema();
        Schema.Type fieldSchemaType = fieldSchema.getType();
        List<String> subnames = SchemaUtils.flattenNames(fieldSchema);
        if (subnames == null) {
          schemaLabels.add(field.name());
        } else {
          for (String s: subnames) {
            schemaLabels.add(field.name() + "." + s);
          }
        }
      }
      return schemaLabels;
    } else if (schema.getType() == Schema.Type.UNION) {
      List<Schema> unionTypes = schema.getTypes();
      throw new UnsupportedOperationException("Cannot process UNION");
    } else if (schema.getType() == Schema.Type.ARRAY) {
      return flattenNames(schema.getElementType());
    } else {
      return null;
    }
  }

  /**
   * Grab a value from a record that is potentially deeply-nested, using
   * a dot-notation field label.
   */
  public static String getNestedValues(GenericRecord gr, String fieldname) {
    int dotIndex = fieldname.indexOf(".");
    if (dotIndex >= 0) {
      String firstComponent = fieldname.substring(0, dotIndex);
      String remainder = fieldname.substring(dotIndex+1);
      Object oobj2 = gr.get(firstComponent);
      return (oobj2 == null || (! (oobj2 instanceof GenericRecord))) ? "" : getNestedValues((GenericRecord) oobj2, remainder);
    } else {
      Object result = gr.get(fieldname);
      return (result == null ? "" : result.toString());
    }
  }
}