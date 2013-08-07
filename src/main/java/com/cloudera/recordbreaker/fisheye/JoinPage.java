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
package com.cloudera.recordbreaker.fisheye;

import com.cloudera.recordbreaker.analyzer.DataQuery;
import com.cloudera.recordbreaker.analyzer.FSAnalyzer;
import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.FileSummaryData;
import com.cloudera.recordbreaker.analyzer.DataDescriptor;
import com.cloudera.recordbreaker.analyzer.SchemaDescriptor;
import com.cloudera.recordbreaker.analyzer.SchemaUtils;

import org.apache.avro.Schema;

import org.apache.wicket.ajax.markup.html.form.AjaxButton;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.ajax.AjaxRequestTarget;

import org.apache.wicket.util.value.ValueMap;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.markup.html.form.TextField;

import java.util.List;
import java.util.ArrayList;

public class JoinPage extends WebPage {
  final class JoinPageDisplay extends WebMarkupContainer {
    public JoinPageDisplay(String name, String fid1Str, String fid2Str) {
      super(name);
      FishEye fe = FishEye.getInstance();
      FSAnalyzer fsa = fe.getAnalyzer();
    
      if (fe.hasFSAndCrawl()) {
        if (fid1Str != null && fid2Str != null) {
          try {
            long fid1 = Long.parseLong(fid1Str);
            long fid2 = Long.parseLong(fid2Str);
            FileSummary fs1 = new FileSummary(fsa, fid1);
            FileSummary fs2 = new FileSummary(fsa, fid2);
            FileSummaryData fsd1 = fsa.getFileSummaryData(fid1);
            FileSummaryData fsd2 = fsa.getFileSummaryData(fid2);
            DataDescriptor dd1 = fsd1.getDataDescriptor();
            DataDescriptor dd2 = fsd2.getDataDescriptor();
            List<SchemaDescriptor> sds1 = dd1.getSchemaDescriptor();
            List<SchemaDescriptor> sds2 = dd2.getSchemaDescriptor();                        
            SchemaDescriptor sd1 = sds1.get(0);
            SchemaDescriptor sd2 = sds2.get(0);

            List<Schema> unionFreeSchemas1 = SchemaUtils.getUnionFreeSchemasByFrequency(sd1, 100, true);
            Schema schema1 = unionFreeSchemas1.get(0);
            List<Schema> unionFreeSchemas2 = SchemaUtils.getUnionFreeSchemasByFrequency(sd2, 100, true);
            Schema schema2 = unionFreeSchemas2.get(0);
            
            List<String> f1Attrs = new ArrayList<String>();
            List<String> f2Attrs = new ArrayList<String>();

            for (Schema.Field f: schema1.getFields()) {
              f1Attrs.add(f.name());
            }
            for (Schema.Field f: schema2.getFields()) {
              f2Attrs.add(f.name());
            }
            
            add(new ExternalLink("file1subtitlelink", urlFor(FilePage.class, new PageParameters("fid=" + fid1)).toString(), fs1.getPath().toString()));
            add(new ExternalLink("file2subtitlelink", urlFor(FilePage.class, new PageParameters("fid=" + fid2)).toString(), fs2.getPath().toString()));          
            
            add(new Label("file1title", fs1.getFname()));
            add(new Label("file2title", fs2.getFname()));

            add(new ListView<String>("fieldlist1", f1Attrs) {
                protected void populateItem(ListItem<String> listItem) {
                  String modelObj = listItem.getModelObject();
                  listItem.add(new Label("alabel", modelObj));
                }
              });
            add(new ListView<String>("fieldlist2", f2Attrs) {
                protected void populateItem(ListItem<String> listItem) {
                  String modelObj = listItem.getModelObject();
                  listItem.add(new Label("alabel", modelObj));
                }
              });

            add(new JoinQueryForm("joinqueryform", new ValueMap(), fid1, fid2));
            
            return;
          } catch (NumberFormatException nfe) {
          }
        }
      }
      add(new Label("file1title", "unknown"));
      add(new Label("file2title", "unknown"));      
    }
  }

  ////////////////////////////////////////////////////////
  // User joins on the data
  ////////////////////////////////////////////////////////
  public final class JoinQueryForm extends Form<ValueMap> {
    long fid1;
    long fid2;
    public JoinQueryForm(final String id, ValueMap vm, long fid1, long fid2) {
      super(id, new CompoundPropertyModel<ValueMap>(vm));
      this.fid1 = fid1;
      this.fid2 = fid2;
      final long finalFid1 = fid1;
      final long finalFid2 = fid2;
      add(new TextField<String>("selectionclause").setType(String.class));
      add(new TextField<String>("projectionclause").setType(String.class));            
      add(new AjaxButton("submitjoinquery") {
          protected void onSubmit(final AjaxRequestTarget target, final Form form) {
            FishEye fe = FishEye.getInstance();
            DataQuery dq = DataQuery.getInstance();
            ValueMap vals = (ValueMap) form.getModelObject();
            FSAnalyzer fsa = fe.getAnalyzer();
            FileSummaryData fsd1 = fsa.getFileSummaryData(finalFid1);
            String path1 = fsd1.path + fsd1.fname;
            DataDescriptor dd1 = fsd1.getDataDescriptor();
            FileSummaryData fsd2 = fsa.getFileSummaryData(finalFid2);
            String path2 = fsd2.path + fsd2.fname;
            DataDescriptor dd2 = fsd2.getDataDescriptor();
            
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
              pp.add("fid1", "" + finalFid1);
              pp.add("fid2", "" + finalFid2);
              pp.add("projectionclause", projClause);
              pp.add("selectionclause", selClause);
              pp.add("filename1", path1);
              pp.add("filename2", path2);
              target.appendJavaScript("window.open(\"" + urlFor(QueryResultsPage.class, pp).toString() + "\")");
            }
          }
          protected void onError(final AjaxRequestTarget target, final Form form) {
          }
        });
    }
  }

  public JoinPage() {
    add(new CrawlWarningBox());
    add(new SettingsWarningBox());    
    add(new AccessControlWarningBox("accessControlWarningBox", null));
    add(new JoinPageDisplay("joinPageDisplay", "", ""));
  }
  public JoinPage(PageParameters params) {
    add(new CrawlWarningBox());
    add(new SettingsWarningBox());    
    add(new AccessControlWarningBox("accessControlWarningBox", null));
    add(new JoinPageDisplay("joinPageDisplay", params.get("fid1").toString(), params.get("fid2").toString()));
  }
}
