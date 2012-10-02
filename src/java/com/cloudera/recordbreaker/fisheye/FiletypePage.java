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

import com.cloudera.recordbreaker.analyzer.TypeSummary;
import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.SchemaSummary;
import com.cloudera.recordbreaker.analyzer.TypeGuessSummary;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.request.mapper.parameter.PageParameters;

import java.util.List;

/**
 * Wicket Page class that describes a specific file type
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see WebPage
 */
public class FiletypePage extends WebPage {
  final class FiletypeDisplay extends WebMarkupContainer {
    public FiletypeDisplay(String name, String filetypeStr) {
      super(name);
      FishEye fe = FishEye.getInstance();
      if (fe.hasFSAndCrawl()) {
        if (filetypeStr == null) {
          add(new Label("typetitle", ""));
        } else {
          try {
            // Metadata for type
            TypeSummary ts = new TypeSummary(FishEye.getInstance().getAnalyzer(), Long.parseLong(filetypeStr));
            
            add(new Label("typetitle", ts.getLabel()));
            List<TypeGuessSummary> tgses = ts.getTypeGuesses();
            add(new Label("typecount", "" + tgses.size()));

            //
            // REMIND -- mjc -- I think this file-listing display is more trouble than its worth, at least
            // when examining filesystems of non-trivial size.  But I'm not 100% sure yet, so it will stick
            // around a bit...
            //
            /**
            ListView<TypeGuessSummary> observationList = new ListView<TypeGuessSummary>("observations", tgses) {
              protected void populateItem(ListItem<TypeGuessSummary> item) {
                TypeGuessSummary tgs = item.getModelObject();
                FileSummary fs = tgs.getFileSummary();
                SchemaSummary ss = tgs.getSchemaSummary();

                String fileUrl = urlFor(FilePage.class, new PageParameters("fid=" + fs.getFid())).toString();
                item.add(new ExternalLink("filelink", fileUrl, fs.getFname()));

                String schemaUrl = urlFor(SchemaPage.class, new PageParameters("schemaid=" + ss.getSchemaId())).toString();
                item.add(new ExternalLink("schemalink", schemaUrl, "Schema"));
              }
            };
            add(observationList);
            **/
          } catch (NumberFormatException nfe) {
          }
        }
      }
      setOutputMarkupPlaceholderTag(true);
      setVisibilityAllowed(true);
    }
  }

  public FiletypePage() {
    add(new SettingsWarningBox());
    add(new CrawlWarningBox());
    add(new FiletypeDisplay("currentFiletypeDisplay", ""));
  }
  public FiletypePage(PageParameters params) {  
    add(new SettingsWarningBox());
    add(new CrawlWarningBox());
    add(new FiletypeDisplay("currentFiletypeDisplay", params.get("typeid").toString()));
  }
}
