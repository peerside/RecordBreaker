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
import com.cloudera.recordbreaker.analyzer.SchemaUtils;
import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.DataDescriptor;
import com.cloudera.recordbreaker.analyzer.FileSummaryData;
import com.cloudera.recordbreaker.analyzer.SchemaDescriptor;

import org.apache.wicket.model.Model;
import org.apache.wicket.AttributeModifier;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.repeater.RepeatingView;
import org.apache.wicket.markup.html.WebMarkupContainer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
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
  final static int MAX_ROWS = 100;

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
  class DataField implements Serializable {
    String fieldName;
    boolean isStringVal;
    String dataStr;
    String filename;
    public DataField(String fieldName, Object dataObj, String filename) {
      this.fieldName = fieldName;
      this.isStringVal = ! ((dataObj instanceof Integer) || (dataObj instanceof Double) || (dataObj instanceof Float));
      this.dataStr = "" + dataObj;
      this.filename = filename;
    }
    public String getDataFieldName() {
      return fieldName;
    }
    public boolean isStringVal() {
      return isStringVal;
    }
    public String getDataStr() {
      return dataStr;
    }
    public String getFilename() {
      return filename;
    }
  }
  class DataTablePair implements Serializable {
    List<List<HeaderPair>> headerPairs;
    List<List<DataField>> outputTupleList;
    
    public DataTablePair(List<List<HeaderPair>> headerPairs, List<List<DataField>> outputTupleList) {
      this.headerPairs = headerPairs;
      this.outputTupleList = outputTupleList;
    }
    List<List<HeaderPair>> getHeaderPairs() {
      return headerPairs;
    }
    List<List<DataField>> getTupleList() {
      return outputTupleList;
    }
  }

  void renderToPage(String label, List<DataTablePair> tablePairs, final boolean renderLinks) {
    final long localFid = this.fid;
    add(new ListView<DataTablePair>(label, tablePairs) {
        protected void populateItem(ListItem<DataTablePair> outerItem) {
          DataTablePair outerModelObj = outerItem.getModelObject();
          List<List<HeaderPair>> outputHeaderList = outerModelObj.getHeaderPairs();
          List<List<DataField>> outputTupleList = outerModelObj.getTupleList();
            
          outerItem.add(new ListView<List<HeaderPair>>("attributelabels", outputHeaderList) {
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

          outerItem.add(new ListView<List<DataField>>("schemalistview", outputTupleList) {
              protected void populateItem(ListItem<List<DataField>> item) {
                List<DataField> myListOfSchemaElts = item.getModelObject();
        
                ListView<DataField> listofTupleFields = new ListView<DataField>("tupleview", myListOfSchemaElts) {
                  protected void populateItem(ListItem<DataField> item2) {
                    DataField dataField = item2.getModelObject();


                    // SELECT * FROM DATA WHERE ATTR = 'celltext'
                    String totalHTML = "";
                    if (renderLinks && dataField.getDataStr().length() > 0) {
                      String sqlQueryText = "SELECT * FROM <i>DATA</i> WHERE " + dataField.getDataFieldName() + " = " + (dataField.isStringVal() ? "'" : "") + dataField.getDataStr() + (dataField.isStringVal() ? "'" : "");
                      String selectionClause = dataField.getDataFieldName() + "+%3D+" + (dataField.isStringVal() ? "%27" : "") + dataField.getDataStr() + (dataField.isStringVal() ? "%27" : "");
                      String sqlHyperlink = "/QueryResults?fid=" + localFid + "&projectionclause=*" + "&selectionclause=" + selectionClause + "&filename=" + dataField.getFilename();
                      totalHTML = "<a href='" + sqlHyperlink + "'>" + sqlQueryText + "</a>";
                      WebMarkupContainer popovercontent = new WebMarkupContainer("popovercontent");
                      popovercontent.add(new AttributeModifier("data-content", true, new Model(totalHTML)));
                      popovercontent.add(new Label("celltext", "" + dataField.getDataStr()));
                      item2.add(popovercontent);
                    } else {
                      item2.add(new Label("celltext", "" + dataField.getDataStr()));
                    }
                  }
                };
                item.add(listofTupleFields);
              }
            });
        }
      });
  }

  void getSchemaFromData(GenericRecord gr) {
  }
  
  public FileContentsTable(long fid) {
    super("filecontentstable");
    this.fid = fid;
    FishEye fe = FishEye.getInstance();
    FSAnalyzer fsa = fe.getAnalyzer();
    FileSummaryData fsd = fsa.getFileSummaryData(fid);
    String path = fsd.path + fsd.fname;    
    DataDescriptor dd = fsd.getDataDescriptor();
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
      // Doing so entails "unrolling" the schemas that contain unions.
      // That is, translating such schemas into a set of union-free schemas.
      //
      List<List<List<DataField>>> perSchemaTupleLists = new ArrayList<List<List<DataField>>>();
      List<List<List<DataField>>> dataOrderTupleLists = new ArrayList<List<List<DataField>>>();
      List<Integer> schemaOrder = new ArrayList<Integer>();
      List<SchemaPair> schemaFrequency = new ArrayList<SchemaPair>();

      int numRows = 0;
      TreeMap<String, Schema> uniqueUnrolledSchemas = new TreeMap<String, Schema>();
      for (Iterator it = sd.getIterator(); it.hasNext(); ) {
        GenericData.Record gr = (GenericData.Record) it.next();
        List<Schema> grSchemas = SchemaUtils.unrollUnionsWithData(schema, gr, false);
        if (grSchemas != null) {
          for (Schema grs: grSchemas) {
            if (uniqueUnrolledSchemas.get(grs.toString()) == null) {
              uniqueUnrolledSchemas.put(grs.toString(), grs);
            }
          }
        }
        if (numRows >= MAX_ROWS) {
          break;
        }
        numRows++;
      }
      List<Schema> allSchemas = new ArrayList(uniqueUnrolledSchemas.values());
      List<List<String>> schemaLabelLists = new ArrayList<List<String>>();

      for (int i = 0; i < allSchemas.size(); i++) {
        Schema s1 = allSchemas.get(i);
        schemaLabelLists.add(SchemaUtils.flattenNames(s1));
        perSchemaTupleLists.add(new ArrayList<List<DataField>>());
        schemaFrequency.add(new SchemaPair(i, 0));
      }

      //
      // Step 2.  Build the set of rows for display.  One row per tuple.
      //
      numRows = 0;
      boolean incompleteFileScan = false;
      int lastBestIdx = -1;
      boolean hasMoreRows = false;
      for (Iterator it = sd.getIterator(); it.hasNext(); ) {
        GenericData.Record gr = (GenericData.Record) it.next();
        if (numRows >= MAX_ROWS) {
          hasMoreRows = true;
          incompleteFileScan = true;
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
            Object result = SchemaUtils.getNestedValues(gr, schemaHeader);
            if (result.toString().length() > 0) {
              numGood++;
            }
          }
          if (numGood > maxGood) {
            maxGood = numGood;
            bestSchemaLabels = schemaLabels;
            bestIdx = i;
          }
          i++;
        }
        if (maxGood == 0) {
          // Some files, especially those recovered through automatic means, may have
          // lines that do not match any part of the schema; in that case, just skip
          // to the next line.
          continue;
        }
        List<DataField> tupleElts = new ArrayList<DataField>();
        for (String schemaHeader: bestSchemaLabels) {
          tupleElts.add(new DataField(schemaHeader, SchemaUtils.getNestedValues(gr, schemaHeader), path));
        }
        perSchemaTupleLists.get(bestIdx).add(tupleElts);

        if (bestIdx != lastBestIdx) {
          dataOrderTupleLists.add(new ArrayList<List<DataField>>());
        }
        dataOrderTupleLists.get(dataOrderTupleLists.size()-1).add(tupleElts);
        schemaOrder.add(bestIdx);
        schemaFrequency.get(bestIdx).count += 1;

        lastBestIdx = bestIdx;
        numRows++;
      }

      //
      // Step 3.  Build the hierarchical set of header rows for display.
      // 
      // schemaLabelLists holds N lists of schema labels, one for each schema.
      // tupleLists holds N lists of tuples, one for each schema.
      // schemaOrder holds a list of M indexes, one for each tuple in the data
      //   to be displayed.
      //
      //List<List<List<HeaderPair>>> outputHeaderSets = new ArrayList<List<List<HeaderPair>>>();
      //List<List<List<String>>> outputTupleLists = null;

      //
      // Step 4.  Build 3 different display modes.
      // There are 3 ways to view the data.  All 3 get sent to the browser, and the user
      // can toggle among them.
      //
      // "RAW" mode is just the text of the data, as closely as we can formulate it
      //
      // "DATAORDER" mode means show the structured data in tables, ordered by the
      // tuples' appearance in the datafile.
      //
      // "SCHEMAORDER" mode means show the structured data in tables, ordered by
      // most-popular tables first.
      //

      // 4a. raw mode
      List<List<List<HeaderPair>>> rawOutputHeaderSets = new ArrayList<List<List<HeaderPair>>>();
      List<List<List<DataField>>> rawOutputTupleLists = new ArrayList<List<List<DataField>>>();

      List<List<HeaderPair>> headerSet = new ArrayList<List<HeaderPair>>();
      rawOutputHeaderSets.add(headerSet);
      List<HeaderPair> header = new ArrayList<HeaderPair>();
      header.add(new HeaderPair("", 1));
      headerSet.add(header);
      List<List<DataField>> singleTable = new ArrayList<List<DataField>>();
      rawOutputTupleLists.add(singleTable);

      for (List<List<DataField>> tupleList: dataOrderTupleLists) {
        for (List<DataField> tuple: tupleList) {
          List<DataField> singleTuple = new ArrayList<DataField>();
          StringBuffer sbuf = new StringBuffer();
          for (DataField df: tuple) {
            sbuf.append(df.getDataStr());
            sbuf.append(" ");
          }
          singleTuple.add(new DataField("", sbuf.toString().trim(), path));
          singleTable.add(singleTuple);
        }
      }

      // 4b. dataorder mode
      List<List<List<HeaderPair>>> dataOutputHeaderSets = new ArrayList<List<List<HeaderPair>>>();
      List<List<List<DataField>>> dataOutputTupleLists = dataOrderTupleLists;      
      // Show data in order of how it appears in the file
      for (int i = 1; i < schemaOrder.size(); i++) {
        if (schemaOrder.get(i) != schemaOrder.get(i-1)) {
          createOutputHeaderSet(schemaLabelLists.get(schemaOrder.get(i-1)), dataOutputHeaderSets);
        }
      }
      if (schemaOrder.size() > 0) {
        createOutputHeaderSet(schemaLabelLists.get(schemaOrder.get(schemaOrder.size()-1)), dataOutputHeaderSets);
      }

      // 4c. schemaorder mode      
      List<List<List<HeaderPair>>> schemaOutputHeaderSets = new ArrayList<List<List<HeaderPair>>>();
      List<List<List<DataField>>> schemaOutputTupleLists = new ArrayList<List<List<DataField>>>();

      // Show data in order of schema popularity by descending frequency
      SchemaPair sortedByFreq[] = schemaFrequency.toArray(new SchemaPair[schemaFrequency.size()]);
      Arrays.sort(sortedByFreq, Collections.reverseOrder());

      // Iterate through, populate lists
      for (int i = 0; i < sortedByFreq.length; i++) {
        if (sortedByFreq[i].count > 0) {
          createOutputHeaderSet(schemaLabelLists.get(sortedByFreq[i].schemaId), schemaOutputHeaderSets);
          schemaOutputTupleLists.add(perSchemaTupleLists.get(sortedByFreq[i].schemaId));
        }
      }
      
      //
      // Step 5.  Add the info to the display.
      //
      final boolean hasCompletedFileScan = ! incompleteFileScan;
      final int scannedRows = numRows;
      final long fsdSize = fsd.size;
      add(new WebMarkupContainer("completeScanMessage") {
          {
            setOutputMarkupPlaceholderTag(true);
            setVisibilityAllowed(hasCompletedFileScan);
            add(new Label("numberofcompletelines", "" + scannedRows));
          }
        });
      add(new WebMarkupContainer("incompleteScanMessage") {
          {
            setOutputMarkupPlaceholderTag(true);
            setVisibilityAllowed(! hasCompletedFileScan);
            add(new Label("numberofincompletelines", "" + scannedRows));
            add(new Label("numberOfTotalBytes", "" + fsdSize));
          }
        });
      
      List<DataTablePair> rawTablePairs = new ArrayList<DataTablePair>();
      for (int i = 0; i < rawOutputHeaderSets.size(); i++) {
        rawTablePairs.add(new DataTablePair(rawOutputHeaderSets.get(i), rawOutputTupleLists.get(i)));
      }
      renderToPage("rawtables", rawTablePairs, false);
      
      List<DataTablePair> dataTablePairs = new ArrayList<DataTablePair>();
      for (int i = 0; i < dataOutputHeaderSets.size(); i++) {
        dataTablePairs.add(new DataTablePair(dataOutputHeaderSets.get(i), dataOutputTupleLists.get(i)));
      }
      renderToPage("datatables", dataTablePairs, false);
      
      List<DataTablePair> schemaTablePairs = new ArrayList<DataTablePair>();            
      for (int i = 0; i < schemaOutputHeaderSets.size(); i++) {
        schemaTablePairs.add(new DataTablePair(schemaOutputHeaderSets.get(i), schemaOutputTupleLists.get(i)));
      }
      renderToPage("schematables", schemaTablePairs, true);
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