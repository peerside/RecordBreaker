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
import java.sql.SQLException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.ArrayList;

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
  class TableDisplayPanel extends WebMarkupContainer {
    long fid = -1L;
    
    public TableDisplayPanel(String name, String fidStr, String filename, String projClauseStr, String selClauseStr) {
      super(name);

      //System.err.println("TABLE DISPLAY: filename=" + filename);
      FishEye fe = FishEye.getInstance();
      List<List<String>> queryResults = null;
      if (fe.hasFSAndCrawl()) {
        if (fidStr != null) {
          try {
            this.fid = Long.parseLong(fidStr);
            FileSummary fs = new FileSummary(fe.getAnalyzer(), fid);
            DataQuery dq = DataQuery.getInstance();
            FSAnalyzer fsa = fe.getAnalyzer();
            FileSummaryData fsd = fsa.getFileSummaryData(fid);
            DataDescriptor dd = fsd.getDataDescriptor();

            if (dq != null) {
              queryResults = dq.query(dd, projClauseStr, selClauseStr);
            } else {
              queryResults = new ArrayList<List<String>>();
            }
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        }
      }

      add(new Label("filename", filename));
      add(new ListView<List<String>>("resultTable", queryResults) {
          protected void populateItem(ListItem<List<String>> tuple) {
            List<String> singleTuple = tuple.getModelObject();

            tuple.add(new ListView<String>("resultTuple", singleTuple) {
                protected void populateItem(ListItem<String> fieldItem) {
                  String fieldVal = fieldItem.getModelObject();
                  fieldItem.add(new Label("field", fieldVal));
                }
              });
          }
        });
    }
    public void onConfigure() {
      FishEye fe = FishEye.getInstance();
      FileSummary fs = new FileSummary(fe.getAnalyzer(), fid);      
      AccessController accessCtrl = fe.getAccessController();
      setVisibilityAllowed(fe.hasFSAndCrawl() && (fs != null && accessCtrl.hasReadAccess(fs)));
    }
  }
  
  public QueryResultsPage() {
    add(new CrawlWarningBox());
    add(new SettingsWarningBox());    
    add(new AccessControlWarningBox("accessControlWarningBox", null));
    add(new TableDisplayPanel("queryresultspanel", "0", "", null, null));
  }
  public QueryResultsPage(PageParameters params) {
    add(new CrawlWarningBox());
    add(new SettingsWarningBox());    
    add(new AccessControlWarningBox("accessControlWarningBox", null));
    add(new TableDisplayPanel("queryresultspanel", params.get("fid").toString(), params.get("filename").toString(), params.get("projectionclause").toString(), params.get("selectionclause").toString()));
  }
}