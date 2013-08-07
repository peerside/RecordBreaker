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
package com.cloudera.recordbreaker.hive;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.io.File;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericContainer;

import com.cloudera.recordbreaker.hive.borrowed.AvroDeserializer;
import com.cloudera.recordbreaker.hive.borrowed.AvroGenericRecordWritable;
import com.cloudera.recordbreaker.hive.borrowed.AvroObjectInspectorGenerator;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

/**********************************************************************
 * <code>RegExpSerDe</code> parses a text file using a parser
 * and schema induced by Fisheye/regular-expressions.
 *
 * @author "Michael Cafarella" 
 **********************************************************************/
public class RegExpSerDe extends HiveSerDe {
  private static final Log LOG = LogFactory.getLog(RegExpSerDe.class);
  List<Schema> schemaOptions;
  List<Pattern> patterns;

  /**
   * <code>initDeserializer</code> sets up the RegExp-specific
   * parts of the SerDe.  In particular, it loads in the regular
   * expressions and corresponding schema descriptions.
   *
   * The patternPayloadJSONFile parameter is a serailized JSON
   * object that contains the schema and regexp info.
   */
  void initDeserializer(String patternPayloadJSONFile) {
    try {
      DataInputStream in = new DataInputStream(new FileInputStream(new File(patternPayloadJSONFile)));
      byte buf[] = new byte[8096];
      StringBuffer payload = new StringBuffer();
      try {
        int numBytes = in.read(buf);
        while (numBytes >= 0) {
          payload.append(new String(buf, 0, numBytes));
          numBytes = in.read(buf);
        }
      } finally {
        in.close();
      }
      String payloadStr = payload.toString();
      JSONObject jobj = new JSONObject(payloadStr);

      this.patterns = new ArrayList<Pattern>();
      JSONArray patternArray = jobj.getJSONArray("patterns");
      for (int i = 0; i < patternArray.length(); i++) {
        String patternStr = patternArray.getString(i);
        this.patterns.add(Pattern.compile(patternStr));
      }

      this.schemaOptions = new ArrayList<Schema>();
      JSONArray schemaOptionArray = jobj.getJSONArray("schemaoptions");
      for (int i = 0; i < schemaOptionArray.length(); i++) {
        String schemaStr = schemaOptionArray.getString(i);
        this.schemaOptions.add(Schema.parse(schemaStr));
      }
    } catch (UnsupportedEncodingException uee) {
      uee.printStackTrace();
    } catch (IOException iex) {
      iex.printStackTrace();
    } catch (JSONException jse) {
      jse.printStackTrace();
    }
  }

  /**
   * Deserialize a single line of text in the raw input.
   * Transform into a GenericData.Record object for Hive.
   */
  GenericData.Record deserializeRowBlob(Writable blob) {
    String rowStr = ((Text) blob).toString();
    GenericData.Record rowRecord = null;

    for (int i = 0; i < patterns.size(); i++) {
      Pattern curPattern = patterns.get(i);
      Schema curSchema = schemaOptions.get(i);              
      Matcher curMatcher = curPattern.matcher(rowStr);

      if (curMatcher.find()) {
        // Create Avro record here
        rowRecord = new GenericData.Record(curSchema);
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
          }
          if (fieldValue != null) {
            rowRecord.put(fieldName, fieldValue);
          }
        }
        return rowRecord;
      }
    }
    return null;
  }
}
