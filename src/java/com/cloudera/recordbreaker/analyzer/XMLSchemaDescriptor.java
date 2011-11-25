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
import java.util.Map;
import java.util.List;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.avro.Schema;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.parsers.ParserConfigurationException;

/*****************************************************************
 * <code>XMLSchemaDescriptor</code> builds an Avro-style schema out of the XML info.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see SchemaDescriptor
 ******************************************************************/
public class XMLSchemaDescriptor implements SchemaDescriptor {
  SAXParser parser;
  Schema rootSchema;
  String rootSchemaName;
  
  /**
   * Creates a new <code>XMLSchemaDescriptor</code> instance.
   * Processes the input XML data and creates an Avro-compatible
   * Schema representation.
   */
  public XMLSchemaDescriptor(File f) throws IOException {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    // Unfortunately, validation is usually not possible
    factory.setValidating(false);

    try {
      this.parser = factory.newSAXParser();
      this.parser.parse(f, new XMLSchemaHandler());
    } catch (SAXException saxe) {
      throw new IOException(saxe.toString());
    } catch (ParserConfigurationException pcee) {
      throw new IOException(pcee.toString());
    }
  }

  /**
   * <code>XMLSchemaHandler</code> intercepts XML open/close/data callbacks from the SAXParser.
   * Hard to believe, but many XML files don't actually contain a reference to the schema; thus,
   * we just use the actual observed XML to extract the user-observable schema.
   */
  class XMLSchemaHandler extends DefaultHandler {
    List<List<Schema.Field>> childLists = new ArrayList<List<Schema.Field>>();
    
    public void startElement(String uri, String localName, String qName, Attributes attrs) {
      List<Schema.Field> fields = new ArrayList<Schema.Field>();
      childLists.add(fields);
    }
    public void endElement(String uri, String localName, String qName) {
      qName = qName.replace("-", "_");
      List<Schema.Field> childSchemas = childLists.remove(childLists.size()-1);
      Schema.Field curSchemaField = null;

      // Is this a leaf-level schema or an "internal node"?
      if (childSchemas.size() == 0) {
        curSchemaField = new Schema.Field(qName, Schema.create(Schema.Type.STRING), "", null);
      } else {
        Map<String, Integer> childSchemaCount = new TreeMap<String, Integer>();
        for (Schema.Field childSchema: childSchemas) {
          String s = childSchema.name() + "-" + childSchema.schema().toString();
          Integer oldCount = childSchemaCount.get(s);
          if (oldCount == null) {
            oldCount = new Integer(0);
          }
          childSchemaCount.put(s, oldCount.intValue() + 1);
        }

        List<Schema.Field> newChildSchemas = new ArrayList<Schema.Field>();
        for (Schema.Field childSchema: childSchemas) {
          String s = childSchema.name() + "-" + childSchema.schema().toString();
          Integer schemaCount = childSchemaCount.get(s);
          if (schemaCount == null) {
            continue;
          }
          if (schemaCount.intValue() == 1) {
            newChildSchemas.add(childSchema);
          } else {
            newChildSchemas.add(new Schema.Field(childSchema.name(), Schema.createArray(childSchema.schema()), childSchema.doc(), childSchema.defaultValue()));
          }
          childSchemaCount.remove(s);
        }
        //System.err.println("This schema has a record of size " + newChildSchemas.size());        
        curSchemaField = new Schema.Field(qName, Schema.createRecord(newChildSchemas), "", null);
      }

      if (childLists.size() == 0) {
        List<Schema.Field> singletonList = new ArrayList<Schema.Field>();
        singletonList.add(curSchemaField);
        rootSchema = Schema.createRecord("ROOT", "", "", false);
        rootSchema.setFields(singletonList);
      } else {
        List<Schema.Field> parentChildren = childLists.get(childLists.size()-1);
        parentChildren.add(curSchemaField);
      }
    }
  }

  /**
   * @return the root <code>Schema</code> 
   */
  public Schema getSchema() {
      return rootSchema;
  }

  public Iterator getIterator() {
    return null;
  }

  /**
   * Schema ID
   */
  public String getSchemaIdentifier() {
    return rootSchema.toString(true);
  }

  /**
   * It's an XML file
   */
  public String getSchemaSourceDescription() {
    return "xml";
  }
}