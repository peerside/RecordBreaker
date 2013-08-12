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
import com.cloudera.recordbreaker.analyzer.FileSummaryData;
import com.cloudera.recordbreaker.analyzer.DataDescriptor;
import com.cloudera.recordbreaker.analyzer.SchemaDescriptor;
import com.cloudera.recordbreaker.analyzer.DataQuery;
import com.cloudera.recordbreaker.analyzer.TypeSummary;
import com.cloudera.recordbreaker.analyzer.SchemaSummary;
import com.cloudera.recordbreaker.analyzer.TypeGuessSummary;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.markup.html.link.DownloadLink;
import org.apache.wicket.ajax.markup.html.form.AjaxButton;
import org.apache.wicket.request.mapper.parameter.PageParameters;

import org.apache.wicket.request.Request;
import org.apache.wicket.request.Response;
import org.apache.wicket.request.http.WebResponse;
import org.apache.wicket.markup.html.link.ResourceLink;
import org.apache.wicket.request.resource.IResource;
import org.apache.wicket.request.resource.ContentDisposition;
import org.apache.wicket.request.resource.ResourceStreamResource;
import org.apache.wicket.util.resource.IResourceStream;
import org.apache.wicket.util.resource.AbstractResourceStreamWriter;

import org.apache.wicket.util.file.Files;
import org.apache.wicket.util.lang.Bytes;
import org.apache.wicket.util.time.Duration;
import org.apache.wicket.model.AbstractReadOnlyModel;
import org.apache.wicket.util.value.ValueMap;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.markup.html.form.RequiredTextField;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.ajax.AjaxRequestTarget;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.ArrayList;
import java.text.DecimalFormat;

/************************************************************************
 * <code>QueryResultsPage</code> shows the results of a single one-off
 * HIVE/SQL query against Fisheye data.
 *
 * By opening a dedicated window for each query, we can get async behavior
 * for free.
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 **************************************************************************/
public class QueryResultsPage extends WebPage {
  class DataTablePair implements Serializable {
    List<List<String>> headers;
    List<List<String>> outputTuples;
      
    public DataTablePair(List<List<String>> headers, List<List<String>> outputTuples) {
      this.headers = headers;
      this.outputTuples = outputTuples;
    }
    public List<List<String>> getHeaderPairs() {
      return headers;
    }
    public List<List<String>> getOutputTuples() {
      return outputTuples;
    }
  }
    
  class TableDisplayPanel extends WebMarkupContainer {
    String fidStr;
    String fidStr1;
    long fid;
    long fid1;
    long fid2;
    
    public TableDisplayPanel(String name, String fidStr, String filename, String fidStr1, String filename1, String fidStr2, String filename2, String projClauseStr, String selClauseStr) {
      super(name);
      this.fidStr = fidStr;
      this.fidStr1 = fidStr1;
      
      long startTime = System.currentTimeMillis();
      //System.err.println("TABLE DISPLAY: filename=" + filename);
      FishEye fe = FishEye.getInstance();
      List<List<String>> queryResults = new ArrayList<List<String>>();
      if (fe.hasFSAndCrawl()) {
        ///
        // Single table query!
        ///
        if (fidStr != null) {
          try {
            fid = Long.parseLong(fidStr);
            FileSummary fs = new FileSummary(fe.getAnalyzer(), fid);
            DataQuery dq = DataQuery.getInstance();
            FSAnalyzer fsa = fe.getAnalyzer();
            FileSummaryData fsd = fsa.getFileSummaryData(fid);
            DataDescriptor dd = fsd.getDataDescriptor();

            if (dq != null) {
              queryResults = dq.query(dd, null, projClauseStr, selClauseStr);
            }
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
        ///
        // Multi table query!
        ///
        if (fidStr1 != null) {
          try {
            fid1 = Long.parseLong(fidStr1);
            FileSummary fs1 = new FileSummary(fe.getAnalyzer(), fid1);
            fid2 = Long.parseLong(fidStr2);
            FileSummary fs2 = new FileSummary(fe.getAnalyzer(), fid2);
            DataQuery dq = DataQuery.getInstance();
            FSAnalyzer fsa = fe.getAnalyzer();
            FileSummaryData fsd1 = fsa.getFileSummaryData(fid1);
            DataDescriptor dd1 = fsd1.getDataDescriptor();
            FileSummaryData fsd2 = fsa.getFileSummaryData(fid2);
            DataDescriptor dd2 = fsd2.getDataDescriptor();

            if (dq != null) {
              queryResults = dq.query(dd1, dd2, projClauseStr, selClauseStr);
            }
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
      }

      long endTime = System.currentTimeMillis();
      double elapsedTime = (endTime - startTime) / 1000.0;
      List<String> metadata = null;
      List<List<String>> metadataList = new ArrayList<List<String>>();      
      if (queryResults.size() == 0) {
        List<String> tuple = new ArrayList<String>();
        tuple.add("No results found");
        queryResults.add(tuple);
      } else {
        metadata = queryResults.remove(0);
        metadataList.add(metadata);
      }

      // If a single file
      ExternalLink filenameLink = new ExternalLink("filenamelink", urlFor(FilePage.class, new PageParameters("fid=" + fid)).toString(), filename);
      add(filenameLink);
      if (fidStr == null) {
        filenameLink.setVisibilityAllowed(false);
      } else {
        filenameLink.setVisibilityAllowed(true);
      }

      // If multiple files
      ExternalLink filenameLink1 = new ExternalLink("filenamelink1", urlFor(FilePage.class, new PageParameters("fid=" + fid1)).toString(), filename1);
      ExternalLink filenameLink2 = new ExternalLink("filenamelink2", urlFor(FilePage.class, new PageParameters("fid=" + fid2)).toString(), filename2);
      add(filenameLink1);
      add(filenameLink2);
      if (fidStr1 == null) {
        filenameLink1.setVisibilityAllowed(false);
        filenameLink2.setVisibilityAllowed(false);
      } else {
        filenameLink1.setVisibilityAllowed(true);
        filenameLink2.setVisibilityAllowed(true);
      }
      
      add(new Label("elapsedtime", new DecimalFormat("#.##").format(elapsedTime)));
      add(new ListView<List<String>>("attributelabels", metadataList) {
          protected void populateItem(ListItem<List<String>> item) {
            List<String> myListOfFieldLabels = item.getModelObject();
            ListView<String> listOfFields = new ListView<String>("fieldlist", myListOfFieldLabels) {
              protected void populateItem(ListItem<String> item2) {
                String displayInfo = item2.getModelObject();
                item2.add(new Label("alabel", "" + displayInfo));
              }
            };
            item.add(listOfFields);
          }
        });
      add(new ListView<List<String>>("resultTable", queryResults) {
          protected void populateItem(ListItem<List<String>> item) {
            List<String> myListOfSchemaElts = item.getModelObject();
            ListView<String> listofTupleFields = new ListView<String>("resultTuple", myListOfSchemaElts) {
              protected void populateItem(ListItem<String> item2) {
                String displayStr = item2.getModelObject();
                item2.add(new Label("field", "" + displayStr));
              }
            };
            item.add(listofTupleFields);
          }
        });
    }

    public void onConfigure() {
      FishEye fe = FishEye.getInstance();
      AccessController accessCtrl = fe.getAccessController();
      if (fidStr != null) {
        FileSummary fs = new FileSummary(fe.getAnalyzer(), fid);
        setVisibilityAllowed(fe.hasFSAndCrawl() && (fs != null && accessCtrl.hasReadAccess(fs)));
      } else if (fidStr1 != null) {
        FileSummary fs1 = new FileSummary(fe.getAnalyzer(), fid1);
        FileSummary fs2 = new FileSummary(fe.getAnalyzer(), fid2);      
        setVisibilityAllowed(fe.hasFSAndCrawl() && (fs1 != null && accessCtrl.hasReadAccess(fs1)) && (fs2 != null && accessCtrl.hasReadAccess(fs2)));
      }
    }
  }
  
  public QueryResultsPage() {
    add(new CrawlWarningBox());
    add(new SettingsWarningBox());    
    add(new AccessControlWarningBox("accessControlWarningBox", null));
    add(new TableDisplayPanel("queryresultspanel", "0", "", "0", "", "0", "", null, null));
  }
  public QueryResultsPage(PageParameters params) {
    add(new CrawlWarningBox());
    add(new SettingsWarningBox());    
    add(new AccessControlWarningBox("accessControlWarningBox", null));
    add(new TableDisplayPanel("queryresultspanel", params.get("fid").toString(), params.get("filename").toString(), params.get("fid1").toString(), params.get("filename1").toString(), params.get("fid2").toString(), params.get("filename2").toString(), params.get("projectionclause").toString(), params.get("selectionclause").toString()));
  }
}