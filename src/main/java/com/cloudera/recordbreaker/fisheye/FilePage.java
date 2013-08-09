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
import com.cloudera.recordbreaker.analyzer.PageHistory;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.markup.html.WebMarkupContainer;
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

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

/*******************************************************
 * Wicket Page class that describes a specific File.
 * It shows metadata, shows structured file contents, and
 * allows the user to query the data.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see WebPage
 *******************************************************/
public class FilePage extends WebPage {

  class JoinPair implements Serializable {
    String fid;
    String joinPath;
    public JoinPair(String fid, String joinPath) {
      this.fid = fid;
      this.joinPath = joinPath;
    }
    public String getJoinFid() {
      return fid;
    }
    public String getJoinPath() {
      return joinPath;
    }
  }
  
  final class FilePageDisplay extends WebMarkupContainer {
    long fid = -1L;
    
    /**
     * Sets up the FilePageDisplay frame.  This is only shown
     * when there is a valid FID and a valid filesystem
     */
    public FilePageDisplay(String name, final String fidStr) {
      super(name);
      FishEye fe = FishEye.getInstance();
      final PageHistory history = PageHistory.get();
      final List<JoinPair> joinPairs = new ArrayList<JoinPair>();
      List<Long> historyFids = history.getRecentFids();
      List<String> historyPaths = history.getRecentPaths();
      for (int i = 0; i < historyFids.size(); i++) {
        joinPairs.add(new JoinPair("" + historyFids.get(i), historyPaths.get(i)));
      }

      if (fe.hasFSAndCrawl()) {
        if (fidStr != null) {
          try {
            this.fid = Long.parseLong(fidStr);
            final FileSummary fs = new FileSummary(fe.getAnalyzer(), fid);
            final long fsFid = fid;
            final String fsPath = fs.getPath().toString();
            
            FSAnalyzer fsa = fe.getAnalyzer();
            FileSummaryData fsd = fsa.getFileSummaryData(fid);
            DataDescriptor dd = fsd.getDataDescriptor();
            List<TypeGuessSummary> tgses = fs.getTypeGuesses();
            
            add(new Label("filetitle", fs.getFname()));
            add(new ExternalLink("filesubtitlelink", urlFor(FilesPage.class, new PageParameters("targetdir=" + fs.getPath().getParent().toString())).toString(), fs.getPath().getParent().toString()));
            // Set up the download file link
            IResourceStream hadoopfsStream = new AbstractResourceStreamWriter() {
                public void write(Response output) {
                  WebResponse weboutput = (WebResponse) output;
                  try {
                    BufferedInputStream in = new BufferedInputStream(new FileSummary(FishEye.getInstance().getAnalyzer(), fid).getRawBytes());
                    try {
                      byte buf[] = new byte[4096];
                      int contentLen = -1;
                      while ((contentLen = in.read(buf)) >= 0) {
                        weboutput.write(buf, 0, contentLen);
                      }
                    } finally {
                      try {
                        in.close();
                      } catch (IOException iex) {
                      }
                    }
                  } catch (IOException iex) {
                    iex.printStackTrace();
                  }
                }
                public Bytes length() {
                  return Bytes.bytes(new FileSummary(FishEye.getInstance().getAnalyzer(), fid).getSize());
                }
              };
            ResourceStreamResource resourceStream = new ResourceStreamResource(hadoopfsStream);
            resourceStream.setContentDisposition(ContentDisposition.ATTACHMENT);
            resourceStream.setFileName(fs.getFname());
            add(new ResourceLink<File>("downloadlink", resourceStream));

            // querySupported container holds queryform
            // queryUnsupported container holds an error message
            final boolean querySupported = dd.isHiveSupported() && fe.isQueryServerAvailable(false);
            final boolean hasJoins = joinPairs.size() > 0;
            add(new WebMarkupContainer("querySupported") {
                {
                  setOutputMarkupPlaceholderTag(true);
                  setVisibilityAllowed(querySupported);
                  add(new QueryForm("queryform", new ValueMap(), fid));
                  history.visitNewPage(fsFid, fsPath);

                  // Add support for join-choices here
                  add(new WebMarkupContainer("hasJoins") {
                      {
                        setOutputMarkupPlaceholderTag(true);
                        setVisibilityAllowed(hasJoins);
                        
                        add(new ListView<JoinPair>("joinlistview", joinPairs) {
                            protected void populateItem(ListItem<JoinPair> joinItem) {
                              JoinPair modelObj = joinItem.getModelObject();

                              PageParameters pps = new PageParameters();
                              pps.add("fid1", fidStr);
                              pps.add("fid2", "" + modelObj.getJoinFid());
                              joinItem.add(new ExternalLink("joinpath",
                                                            urlFor(JoinPage.class, pps).toString(),
                                                            modelObj.getJoinPath()));
                              ///joinItem.add(new Label("schemadesc", modelObj.getSchemaDesc()));
                            }
                          });
                      }
                    });
                }
              });
            add(new WebMarkupContainer("queryUnsupported") {
                {
                  setOutputMarkupPlaceholderTag(true);
                  setVisibilityAllowed(! querySupported);
                }
              });
            
            // File metadata
            add(new Label("owner", fs.getOwner()));
            add(new Label("size", "" + fs.getSize()));
            add(new Label("lastmodified", fs.getLastModified()));
            add(new Label("crawledon", fs.getCrawl().getStartedDate()));

            // Schema data
            if (tgses.size() > 0) {
              TypeGuessSummary tgs = tgses.get(0);
              TypeSummary ts = tgs.getTypeSummary();          
              SchemaSummary ss = tgs.getSchemaSummary();
              String typeUrl = urlFor(FiletypePage.class, new PageParameters("typeid=" + ts.getTypeId())).toString();
              String schemaUrl = urlFor(SchemaPage.class, new PageParameters("schemaid=" + ss.getSchemaId())).toString();
              add(new Label("typelink", "<a href=\"" + typeUrl + "\">" + ts.getLabel() + "</a>").setEscapeModelStrings(false));
              add(new Label("schemalink", "<a href=\"" + schemaUrl + "\">" + "Schema" + "</a>").setEscapeModelStrings(false));
            } else {
              add(new Label("typelink", ""));
              add(new Label("schemalink", ""));
            }
            return;            
          } catch (NumberFormatException nfe) {
          }
        }
        add(new Label("filetitle", "unknown"));
        add(new Label("filesubtitlelink", ""));
        add(new Label("downloadlink", ""));
        add(new Label("filesubtitle", ""));
        add(new Label("owner", ""));
        add(new Label("size", ""));
        add(new Label("lastmodified", ""));
        add(new Label("crawledon", ""));
        add(new QueryForm("queryform", new ValueMap(), 0L));
      }

      setOutputMarkupPlaceholderTag(true);
      setVisibilityAllowed(false);
    }
    public void onConfigure() {
      FishEye fe = FishEye.getInstance();
      FileSummary fs = new FileSummary(fe.getAnalyzer(), fid);      
      AccessController accessCtrl = fe.getAccessController();
      setVisibilityAllowed(fe.hasFSAndCrawl() && (fs != null && accessCtrl.hasReadAccess(fs)));
    }
  }

  ////////////////////////////////////////////////////////
  // User queries on the data
  ////////////////////////////////////////////////////////
  public final class QueryForm extends Form<ValueMap> {
    long fid;
    public QueryForm(final String id, ValueMap vm, long fid) {
      super(id, new CompoundPropertyModel<ValueMap>(vm));
      this.fid = fid;
      final long finalFid = fid;

      add(new TextField<String>("selectionclause").setType(String.class));
      add(new TextField<String>("projectionclause").setType(String.class));            
      add(new AjaxButton("submitquery") {
          protected void onSubmit(final AjaxRequestTarget target, final Form form) {
            //loginErrorMsgDisplay.setVisibilityAllowed(false);            
            //target.add(loginErrorMsgDisplay);
            FishEye fe = FishEye.getInstance();
            DataQuery dq = DataQuery.getInstance();
            ValueMap vals = (ValueMap) form.getModelObject();
            FSAnalyzer fsa = fe.getAnalyzer();
            FileSummaryData fsd = fsa.getFileSummaryData(finalFid);
            String path = fsd.path + fsd.fname;
            DataDescriptor dd = fsd.getDataDescriptor();
            List<List<String>> results = null;
            if (dq != null) {
              // Open a new window for the query results
              String projClause = (String) vals.get("projectionclause");
              if (projClause == null) {
                projClause = "*";
              }
              String selClause = (String) vals.get("selectionclause");
              if (selClause == null) {
                selClause = "";
              }

              PageParameters pp = new PageParameters();
              pp.add("fid", "" + finalFid);
              pp.add("projectionclause", projClause);
              pp.add("selectionclause", selClause);
              pp.add("filename", path);
              //add(new ExternalLink("queryResultsLink", urlFor(QueryResultsPage.class, pp).toString(), "queryResults"));
              //System.err.println("Got results: " + urlFor(QueryResultsPage.class, pp).toString());
              target.appendJavaScript("window.open(\"" + urlFor(QueryResultsPage.class, pp).toString() + "\")");
            }
          }
          protected void onError(final AjaxRequestTarget target, final Form form) {
            //loginErrorMsgDisplay.setVisibilityAllowed(true);            
            //target.add(loginErrorMsgDisplay);
          }
        });
    }
  }

  public FilePage() {
    add(new CrawlWarningBox());
    add(new SettingsWarningBox());    
    add(new AccessControlWarningBox("accessControlWarningBox", null));
    add(new FilePageDisplay("currentFileDisplay", ""));
    add(new FileContentsTable());
  }
  public FilePage(PageParameters params) {     
    add(new CrawlWarningBox());
    add(new SettingsWarningBox());    
    add(new AccessControlWarningBox("accessControlWarningBox", Integer.parseInt(params.get("fid").toString())));
    add(new FilePageDisplay("currentFileDisplay", params.get("fid").toString()));
    add(new FileContentsTable(Long.parseLong(params.get("fid").toString())));

    //RecentPages.addView(fs.getFname(), urlFor(FilesPage.class, inputPP).toString());    
  }
}
