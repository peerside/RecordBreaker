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

import com.cloudera.recordbreaker.analyzer.FSAnalyzer;
import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.DataDescriptor;
import com.cloudera.recordbreaker.analyzer.FileSummaryData;
import com.cloudera.recordbreaker.analyzer.SchemaDescriptor;

import org.apache.wicket.model.Model;
import org.apache.wicket.AttributeModifier;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.IOException;
import java.io.Serializable;


/****************************************************
 * <code>FileContentsTable</code> is a panel that shows the (structured)
 * contents of a FishEye file.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 ****************************************************/
public class FileContentsTable extends WebMarkupContainer {
  long fid = -1L;
  final static int MAX_ROWS = 9;

  static List<String> flattenNames(Schema schema) {
    List<String> schemaLabels = new ArrayList<String>();
    for (Schema.Field field: schema.getFields()) {
      Schema fieldSchema = field.schema();
      Schema.Type fieldSchemaType = fieldSchema.getType();
      if (fieldSchemaType == Schema.Type.RECORD) {
        List<String> subnames = FileContentsTable.flattenNames(fieldSchema);
        for (String s: subnames) {
          schemaLabels.add(field.name() + "." + s);
        }
      } else {
        schemaLabels.add(field.name());
      }
    }
    return schemaLabels;
  }

  static String getNestedValues(Object oobj, String fieldname) {
    GenericRecord gr = (GenericRecord) oobj;
    int dotIndex = fieldname.indexOf(".");
    if (dotIndex >= 0) {
      String firstComponent = fieldname.substring(0, dotIndex);
      String remainder = fieldname.substring(dotIndex+1);
      Object oobj2 = gr.get(firstComponent);
      return getNestedValues(oobj2, remainder);
    } else {
      Object result = gr.get(fieldname);
      return (result == null ? "" : result.toString());
    }
  }
  
  public FileContentsTable() {
    super("filecontentstable");
    setOutputMarkupPlaceholderTag(true);
    setVisibilityAllowed(false);
  }

  class HeaderPair implements Serializable {
    String s;
    int count;
    boolean isBottom;
    public HeaderPair(String s, int count) {
      this.s = s;
      this.count = count;
      this.isBottom = false;
    }
    void bumpCount() {
      count += 1;
    }
    String getString() {
      return s;
    }
    int getColSpan() {
      return count;
    }
    boolean isBottom() {
      return isBottom;
    }
    void setBottom(boolean isBottom) {
      this.isBottom = isBottom;
    }
  }
  public FileContentsTable(long fid) {
    super("filecontentstable");
    this.fid = fid;
    FishEye fe = FishEye.getInstance();
    FSAnalyzer fsa = fe.getAnalyzer();
    FileSummaryData fsd = fsa.getFileSummaryData(fid);
    DataDescriptor dd = fsd.dd;
    List<SchemaDescriptor> sds = dd.getSchemaDescriptor();

    if (sds.size() > 0) {
      List<List<String>> tuplelist = new ArrayList<List<String>>();
      SchemaDescriptor sd = sds.get(0);
      Schema schema = sd.getSchema();

      //
      // Step 1.  Figure out the hierarchical labels from the Schema.
      // These are the fields we'll grab from each tuple.
      //
      List<String> schemaLabels = FileContentsTable.flattenNames(schema);

      //
      // Step 2.  Build the set of rows for display.  One row per tuple.
      //
      int numRows = 0;
      for (Iterator it = sd.getIterator(); it.hasNext(); ) {
        GenericData.Record gr = (GenericData.Record) it.next();
        List<String> tupleElts = new ArrayList<String>();

        for (String schemaHeader: schemaLabels) {
          tupleElts.add(FileContentsTable.getNestedValues(gr, schemaHeader));
        }
        tuplelist.add(tupleElts);

        numRows++;
        if (numRows >= MAX_ROWS) {
          tupleElts = new ArrayList<String>();          
          for (String schemaHeader: schemaLabels) {
            tupleElts.add("...");
          }
          tuplelist.add(tupleElts);
          break;
        }
      }

      //
      // Step 3.  Build the hierarchical set of header rows for display.
      //
      int maxDepth = 1;
      for (String s: schemaLabels) {
        int curDepth = s.split("\\.").length;
        maxDepth = Math.max(maxDepth, curDepth);
      }

      List<List<HeaderPair>> headerSet = new ArrayList<List<HeaderPair>>();
      for (int i = 0; i < maxDepth; i++) {
        headerSet.add(new ArrayList<HeaderPair>());
      }

      for (String s : schemaLabels) {
        String parts[] = s.split("\\.");
        for (int i = 0; i < parts.length; i++) {
          headerSet.get(maxDepth-i-1).add(new HeaderPair(parts[parts.length-i-1], 1));
        }
        for (int i = parts.length; i < maxDepth; i++) {
          headerSet.get(maxDepth-i-1).add(new HeaderPair("", 1));
        }
      }

      List<List<HeaderPair>> newHeaderSet = new ArrayList<List<HeaderPair>>();
      for (List<HeaderPair> singleRow: headerSet) {
        List<HeaderPair> newHeaderRow = new ArrayList<HeaderPair>();

        HeaderPair lastHp = singleRow.get(0);
        for (int i = 1; i < singleRow.size(); i++) {
          HeaderPair hp = singleRow.get(i);
          if (hp.getString().equals(lastHp.getString())) {
            lastHp.bumpCount();
          } else {
            newHeaderRow.add(lastHp);
            lastHp = hp;
          }
        }
        newHeaderRow.add(lastHp);
        newHeaderSet.add(newHeaderRow);
      }
      List<HeaderPair> bottomLine = newHeaderSet.get(newHeaderSet.size()-1);
      for (HeaderPair hp: bottomLine) {
        hp.setBottom(true);
      }

      //
      // Step 4.  Add the info to the display.
      //
      add(new ListView<List<HeaderPair>>("attributelabels", newHeaderSet) {
          protected void populateItem(ListItem<List<HeaderPair>> item) {
            List<HeaderPair> myListOfFieldLabels = item.getModelObject();
            ListView<HeaderPair> listOfFields = new ListView<HeaderPair>("fieldlist", myListOfFieldLabels) {
              protected void populateItem(ListItem<HeaderPair> item2) {
                HeaderPair displayPair = item2.getModelObject();
                item2.add(new Label("alabel", "" + displayPair.getString()));
                item2.add(new AttributeModifier("colspan", true, new Model("" + displayPair.getColSpan())));
                if (! displayPair.isBottom()) {
                  item2.add(new AttributeModifier("style", true, new Model("text-align:center")));
                }
              }
            };
            item.add(listOfFields);
          }
        });

      add(new ListView<List<String>>("schemalistview", tuplelist) {
          protected void populateItem(ListItem<List<String>> item) {
            List<String> myListOfSchemaElts = item.getModelObject();
        
            ListView<String> listofTupleFields = new ListView<String>("tupleview", myListOfSchemaElts) {
              protected void populateItem(ListItem<String> item2) {
                String displayStr = item2.getModelObject();
                item2.add(new Label("celltext", "" + displayStr));
              }
            };
            item.add(listofTupleFields);
          }
        });
    }
    setOutputMarkupPlaceholderTag(true);
    setVisibilityAllowed(false);
  }

  public void onConfigure() {
    if (fid < 0) {
      setVisibilityAllowed(false);
    } else {
      FishEye fe = FishEye.getInstance();
      AccessController accessCtrl = fe.getAccessController();    
      FSAnalyzer fsAnalyzer = fe.getAnalyzer();
      FileSummary fileSummary = new FileSummary(fsAnalyzer, fid);
      try {
        setVisibilityAllowed(fe.hasFSAndCrawl() && accessCtrl.hasReadAccess(fileSummary) && fileSummary.isDir() && fileSummary.getDataDescriptor().getSchemaDescriptor().size() > 0);
      } catch (IOException iex) {
        setVisibilityAllowed(false);
      }
    }
  }
}