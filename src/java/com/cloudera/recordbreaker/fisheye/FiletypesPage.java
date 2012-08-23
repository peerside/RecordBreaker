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

import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.ExternalLink;

import java.util.List;

import com.cloudera.recordbreaker.analyzer.TypeSummary;

/**
 * The <code>SchemasPage</code> renders information about all known filetypes
 */
public class FiletypesPage extends WebPage {

  final class FiletypesListing extends WebMarkupContainer {
    public FiletypesListing(String name) {
      super(name);
      FishEye fe = FishEye.getInstance();
      List<TypeSummary> list = fe.getAnalyzer().getTypeSummaries();
      ListView<TypeSummary> listview = new ListView<TypeSummary>("listview", list) {
        protected void populateItem(ListItem<TypeSummary> item) {
          TypeSummary ts = item.getModelObject();

          String typeUrl = urlFor(FiletypePage.class, new PageParameters("typeid=" + ts.getTypeId())).toString();
          item.add(new ExternalLink("typelabel", typeUrl, ts.getLabel()));
        }
      };
      add(listview);
      add(new Label("numFisheyeTypes", "" + list.size()));

      setOutputMarkupPlaceholderTag(true);
      setVisibilityAllowed(false);
    }
    public void onConfigure() {
      FishEye fe = FishEye.getInstance();    
      setVisibilityAllowed(fe.hasFSAndCrawl());
    }
  }

  public FiletypesPage() {  
    add(new FiletypesListing("currentFiletypesListing"));
    add(new SettingsWarningBox());
    add(new CrawlWarningBox());            
  }
}
