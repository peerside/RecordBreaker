/*
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.recordbreaker.fisheye;

import com.cloudera.recordbreaker.analyzer.SchemaSummary;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.IOException;
import java.util.Iterator;

/**
 * Wicket Page class that describes a specific Schema
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see WebPage
 */
public class SchemaPage extends WebPage {
  static JsonFactory factory = new JsonFactory(new ObjectMapper());

  static List<List<JsonNode>> getSchemaDigest(String s) throws IOException {
    List<List<JsonNode>> listOfSchemaElts = new ArrayList<List<JsonNode>>();
    JsonParser parser = factory.createJsonParser(s);
    try {
      JsonNode root = parser.readValueAsTree();
      System.err.println("JSON ROOT: " + root);
      if (! root.isArray()) {
        // This handles default cases like CSV and XML
        // (and eventually the others when bugs are fixed)
        JsonNode fieldSet = root.get("fields");

        // Take care of strange parser case
        if (fieldSet.isArray() && fieldSet.get(0).get("name").toString().equals("\"row\"")) {
          fieldSet = fieldSet.get(0).get("type").get(0).get("fields");
        }

        // Emit results
        List<JsonNode> curListOfSchemaElts = new ArrayList<JsonNode>();
        listOfSchemaElts.add(curListOfSchemaElts);
        for (Iterator<JsonNode> it = fieldSet.getElements(); it.hasNext(); ) {
          curListOfSchemaElts.add(it.next());
        }
      } else {
        for (int i = 0; i < root.size(); i++) {
          List<JsonNode> curListOfSchemaElts = new ArrayList<JsonNode>();              
          JsonNode fieldSet = root.get(i).get("fields");
          listOfSchemaElts.add(curListOfSchemaElts);
          for (Iterator<JsonNode> it = fieldSet.getElements(); it.hasNext(); ) {
            curListOfSchemaElts.add(it.next());
          }
        }
      }
    } finally {
      parser.close();
    }
    return listOfSchemaElts;
  }
  
  final class SchemaPageDisplay extends WebMarkupContainer {
    public SchemaPageDisplay(String name, String schemaidStr) {
      super(name);
      FishEye fe = FishEye.getInstance();

      if (fe.hasFSAndCrawl()) {
        List<List<JsonNode>> listOfSchemaElts = new ArrayList<List<JsonNode>>();
        long numFilesWithSchema = -1;
        String schemaDescription = "";
        if (schemaidStr != null) {
          try {
            long schemaId = Long.parseLong(schemaidStr);
            SchemaSummary ss = new SchemaSummary(fe.getAnalyzer(), schemaId);
            if (ss.getIdentifier().length() > 0) {
              listOfSchemaElts = getSchemaDigest(ss.getIdentifier());
            } else {
              listOfSchemaElts = new ArrayList<List<JsonNode>>();
            }
            schemaDescription = ss.getDesc();
            numFilesWithSchema = fe.getAnalyzer().countFilesForSchema(schemaId);
          } catch (NumberFormatException nfe) {
            nfe.printStackTrace();
          } catch (IOException ie) {
            ie.printStackTrace();            
          }
        }

        add(new ListView<List<JsonNode>>("biglistview", listOfSchemaElts) {
          protected void populateItem(ListItem<List<JsonNode>> item) {      
            List<JsonNode> myListOfSchemaElts = item.getModelObject();
        
            ListView<JsonNode> listview = new ListView<JsonNode>("listview", myListOfSchemaElts) {
              protected void populateItem(ListItem<JsonNode> item2) {
                JsonNode jnode = item2.getModelObject();
        
                item2.add(new Label("fieldname", "" + jnode.get("name")));
                item2.add(new Label("fieldtype", "" + jnode.get("type")));
                item2.add(new Label("fielddoc", "" + jnode.get("doc")));
              }
            };
            item.add(listview);
          }
          });

        add(new Label("schemaDesc", schemaDescription));
        int numElts = 0;
        for (List<JsonNode> sublist: listOfSchemaElts) {
          for (JsonNode jn: sublist) {
            numElts++;
          }
        }
        add(new Label("numSchemaElements", "" + numElts));
        add(new Label("numFilesWithSchema", "" + numFilesWithSchema));
      }
      
      setOutputMarkupPlaceholderTag(true);
      setVisibilityAllowed(false);
    }
    public void onConfigure() {
      FishEye fe = FishEye.getInstance();
      setVisibilityAllowed(fe.hasFSAndCrawl());
    }
  }

  public SchemaPage() {
    add(new SettingsWarningBox());
    add(new CrawlWarningBox());            
    add(new SchemaPageDisplay("currentSchemaDisplay", ""));
  }

  public SchemaPage(PageParameters params) {
    add(new SettingsWarningBox());
    add(new CrawlWarningBox());            
    add(new SchemaPageDisplay("currentSchemaDisplay", params.get("schemaid").toString()));
  }  
}
