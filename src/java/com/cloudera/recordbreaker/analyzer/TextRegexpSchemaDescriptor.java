package com.cloudera.recordbreaker.analyzer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/*********************************************************************
 * <code>TextRegexpSchemaDescriptor</code> captures any log file that is:
 * a) Line-oriented
 * b) Can be captured via regular expressions
 * c) Has a well-known format
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see SchemaDescriptor
 **********************************************************************/
public class TextRegexpSchemaDescriptor implements SchemaDescriptor {
  FileSystem fs;
  Path p;
  String schemaLabel;
  List<Schema> schemaOptions;
  List<Pattern> patterns;

  Schema schema;
  
  /**
   * Creates a new <code>TextRegexpSchemaDescriptor</code> instance.
   *
   * @param f a <code>File</code> value
   * @param schemaLabel a <code>String</code> value
   * @param patterns a <code>List<Pattern></code> value
   * @param schemaOptions a <code>List<Schema></code> value
   * @exception Exception if an error occurs
   */
  public TextRegexpSchemaDescriptor(FileSystem fs, Path p, String schemaLabel, List<Pattern> patterns, List<Schema> schemaOptions) throws Exception {
    this.fs = fs;
    this.p = p;
    this.schemaLabel = schemaLabel;
    this.schemaOptions = schemaOptions;
    this.patterns = patterns;

    List<Schema.Field> topFields = new ArrayList<Schema.Field>();
    topFields.add(new Schema.Field("row", Schema.createUnion(schemaOptions), "One of several row formats", null));
    this.schema = Schema.createRecord(topFields);
  }

  public Schema getSchema() {
    return schema;
  }

  /**
   * Create an Iterator object that steps through the file, returning
   * Avro records.  The Avro records use the correct Schema format.
   */
  public Iterator getIterator() {
    return new Iterator() {
      Object nextElt;
      BufferedReader in;
      {
        try {
          this.in = new BufferedReader(new InputStreamReader(fs.open(p)));
          this.nextElt = lookahead();          
        } catch (IOException iex) {
          this.nextElt = null;
        }
      }
      public boolean hasNext() {
        return nextElt != null;
      }
      public synchronized Object next() {
        Object toReturn = nextElt;
        this.nextElt = lookahead();
        return toReturn;
      }
      public void remove() {
        throw new UnsupportedOperationException();
      }
      Object lookahead() {
        String s = null;
        try {
          while ((s = in.readLine()) != null) {
            for (int i = 0; i < patterns.size(); i++) {
              Pattern curPattern = patterns.get(i);
              Schema curSchema = schemaOptions.get(i);              
              Matcher curMatcher = curPattern.matcher(s);

              if (curMatcher.find()) {
                // Create Avro record here
                GenericData.Record cur = new GenericData.Record(curSchema);
                List<Schema.Field> curFields = curSchema.getFields();

                for (int j = 0; j < curMatcher.groupCount(); j++) {
                  Schema.Field curField = curFields.get(j);
                  
                  String fieldName = curField.name();
                  Schema fieldType = curField.schema();
                  String rawFieldValue = curMatcher.group(j+1);

                  Object fieldValue = null;
                  if (fieldType.getType() == Schema.Type.INT) {
                    fieldValue = Integer.parseInt(rawFieldValue);
                  } else if (fieldType.getType() == Schema.Type.FLOAT) {
                    fieldValue = Float.parseFloat(rawFieldValue);
                  } else if (fieldType.getType() == Schema.Type.STRING) {
                    fieldValue = rawFieldValue;
                  } else {
                    throw new IOException("Unexpected field-level schema type: " + fieldType);
                  }
                  cur.put(fieldName, fieldValue);
                }
                GenericData.Record row = new GenericData.Record(schema);
                row.put("row", cur);
                return row;
              }
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
    };
  }
  
  
  public String getSchemaIdentifier() {
    return schema.toString();
  }
  public String getSchemaSourceDescription() {
    return schemaLabel;
  }
}