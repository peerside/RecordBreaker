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

  /**
   * Takes a schema that potentially contains unions and converts it into
   * a list of all possible union-free schemas.
   */
  static List<Schema> unrollUnions(Schema schema) {
    if (schema.getType() == Schema.Type.RECORD) {
      List<List<Schema>> fieldSchemaLists = new ArrayList<List<Schema>>();
      int targetTotal = 1;
      for (Schema.Field sf: schema.getFields()) {
        List<Schema> fieldSchemaList = unrollUnions(sf.schema());
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
        outputSchemas.add(Schema.createRecord(newFields));
      }
      return outputSchemas;
    } else if (schema.getType() == Schema.Type.UNION) {
      List<Schema> unrolledSchemas = new ArrayList<Schema>();
      for (Schema s: schema.getTypes()) {
        unrolledSchemas.addAll(unrollUnions(s));
      }
      return unrolledSchemas;
    } else if (schema.getType() == Schema.Type.ARRAY) {
      List<Schema> subschemas = unrollUnions(schema.getElementType());
      List<Schema> newSchemas = new ArrayList<Schema>();
      for (Schema s: subschemas) {
        newSchemas.add(Schema.createArray(s));
      }
      return newSchemas;
    } else {
      // Base type
      List<Schema> retList = new ArrayList<Schema>();
      retList.add(schema);
      return retList;
    }
  }
  
  /**
   * Takes an Avro record Schema and creates dot-separated names for each
   * leaf-level field.  The input Schema should *not* have any unions.
   */
  static List<String> flattenNames(Schema schema) {
    if (schema.getType() == Schema.Type.RECORD) {
      List<String> schemaLabels = new ArrayList<String>();
      for (Schema.Field field: schema.getFields()) {
        Schema fieldSchema = field.schema();
        Schema.Type fieldSchemaType = fieldSchema.getType();
        List<String> subnames = FileContentsTable.flattenNames(fieldSchema);
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

    class SchemaPair implements Comparable {
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

    if (sds.size() > 0) {
      SchemaDescriptor sd = sds.get(0);
      Schema schema = sd.getSchema();

      //
      // Step 1.  Figure out the hierarchical labels from the Schema.
      // These are the fields we'll grab from each tuple.
      //
      List<List<List<String>>> perSchemaTupleLists = new ArrayList<List<List<String>>>();
      List<List<List<String>>> dataOrderTupleLists = new ArrayList<List<List<String>>>();
      List<Integer> schemaOrder = new ArrayList<Integer>();
      List<SchemaPair> schemaFrequency = new ArrayList<SchemaPair>();

      List<Schema> allSchemas = FileContentsTable.unrollUnions(schema);
      List<List<String>> schemaLabelLists = new ArrayList<List<String>>();

      for (int i = 0; i < allSchemas.size(); i++) {
        Schema s1 = allSchemas.get(i);
        schemaLabelLists.add(FileContentsTable.flattenNames(s1));
        perSchemaTupleLists.add(new ArrayList<List<String>>());
        schemaFrequency.add(new SchemaPair(i, 0));
      }

      //
      // Step 2.  Build the set of rows for display.  One row per tuple.
      //
      int numRows = 0;
      int lastBestIdx = -1;
      boolean hasMoreRows = false;
      for (Iterator it = sd.getIterator(); it.hasNext(); ) {
        GenericData.Record gr = (GenericData.Record) it.next();

        if (numRows >= MAX_ROWS) {
          hasMoreRows = true;
          break;
        }

        // OK, now the question is: which schema does the row observe?
        int maxGood = 0;
        int bestIdx = -1;
        int i = 0;
        List<String> bestSchemaLabels = null;
        for (List<String> schemaLabels: schemaLabelLists) {
          int numGood = 0;
          for (String schemaHeader: schemaLabels) {
            String result = FileContentsTable.getNestedValues(gr, schemaHeader);
            if (result.length() > 0) {
              numGood++;
            }
          }
          if (numGood >= maxGood) {
            maxGood = numGood;
            bestSchemaLabels = schemaLabels;
            bestIdx = i;
          }
          i++;
        }

        List<String> tupleElts = new ArrayList<String>();
        for (String schemaHeader: bestSchemaLabels) {
          tupleElts.add(FileContentsTable.getNestedValues(gr, schemaHeader));
        }
        perSchemaTupleLists.get(bestIdx).add(tupleElts);

        if (bestIdx != lastBestIdx) {
          dataOrderTupleLists.add(new ArrayList<List<String>>());
        }
        dataOrderTupleLists.get(dataOrderTupleLists.size()-1).add(tupleElts);
        schemaOrder.add(bestIdx);
        schemaFrequency.get(bestIdx).count += 1;

        lastBestIdx = bestIdx;
        
        numRows++;
      }

      /**
        if (numRows >= MAX_ROWS) {
          tupleElts = new ArrayList<String>();          
          for (String schemaHeader: schemaLabels) {
            tupleElts.add("...");
          }
          tuplelist.add(tupleElts);
          break;
        }
      **/

      //
      // Step 3.  Build the hierarchical set of header rows for display.
      // 
      // schemaLabelLists holds N lists of schema labels, one for each schema.
      // tupleLists holds N lists of tuples, one for each schema.
      // schemaOrder holds a list of M indexes, one for each tuple in the data
      //   to be displayed.
      //
      List<List<List<HeaderPair>>> outputHeaderSets = new ArrayList<List<List<HeaderPair>>>();
      List<List<List<String>>> outputTupleLists = null;
      boolean dataOrder = true;
      if (dataOrder) {
        // Show data in order of how it appears in the file
        for (int i = 1; i < schemaOrder.size(); i++) {
          if (schemaOrder.get(i) != schemaOrder.get(i-1)) {
            createOutputHeaderSet(schemaLabelLists.get(schemaOrder.get(i-1)), outputHeaderSets);
          }
        }
        if (schemaOrder.size() > 0) {
          createOutputHeaderSet(schemaLabelLists.get(schemaOrder.get(schemaOrder.size()-1)), outputHeaderSets);
        }
        outputTupleLists = dataOrderTupleLists;
      } else {
        // Show data in order of schema popularity
        outputTupleLists = new ArrayList<List<List<String>>>();

        // Sort schemas by descending frequency
        SchemaPair sortedByFreq[] = schemaFrequency.toArray(new SchemaPair[schemaFrequency.size()]);
        Arrays.sort(sortedByFreq);

        // Iterate through, populate lists
        for (int i = 0; i < sortedByFreq.length; i++) {
          if (sortedByFreq[i].count > 0) {
            createOutputHeaderSet(schemaLabelLists.get(sortedByFreq[i].schemaId), outputHeaderSets);
            outputTupleLists.add(perSchemaTupleLists.get(sortedByFreq[i].schemaId));
          }
        }
      }
      
      //
      // Step 4.  Add the info to the display.
      // Inputs: outputHeaderSets and pageOrderTupleLists
      //
      System.err.println();      
      System.err.println("Number of schemas: " + outputHeaderSets.size());
      System.err.println("Number of tuple sets: " + outputTupleLists.size());
      System.err.println();
      
      add(new ListView<List<HeaderPair>>("attributelabels", outputHeaderSets.get(0)) {
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

      add(new ListView<List<String>>("schemalistview", outputTupleLists.get(0)) {
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

  /**
   * Create a single schema-specific set of tuple information, which includes schema info.
   * Depending on how the user wants to view the page and the internal order of rows in
   * a table, a single file could have a large number of different ranges.
   *
   */
  void createOutputHeaderSet(List<String> schemaLabels, List<List<List<HeaderPair>>>outputHeaderSets) {
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

    outputHeaderSets.add(newHeaderSet);
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