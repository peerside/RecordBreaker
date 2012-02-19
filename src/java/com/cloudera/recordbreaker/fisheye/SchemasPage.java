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
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.link.ExternalLink;

import java.util.List;
import java.util.Arrays;

import com.cloudera.recordbreaker.analyzer.FSAnalyzer;
import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.SchemaSummary;
import com.cloudera.recordbreaker.analyzer.TypeGuessSummary;

/**
 * The <code>SchemasPage</code> renders information about all known schemas.
 */
public class SchemasPage extends WebPage {
  public SchemasPage() {
    List<SchemaSummary> list = FishEye.analyzer.getSchemaSummaries();
    ListView<SchemaSummary> listview = new ListView<SchemaSummary>("listview", list) {
      protected void populateItem(ListItem<SchemaSummary> item) {
        SchemaSummary ss = item.getModelObject();
        item.add(new Label("schemalabel", ss.getLabel()));
        item.add(new Label("schemadesc", ss.getDesc()));

        List<TypeGuessSummary> typeGuesses = ss.getTypeGuesses();
        
        for (int i = 0; i < Math.min(1, typeGuesses.size()); i++) {
          TypeGuessSummary curTGS = typeGuesses.get(i);
          FileSummary fs = curTGS.getFileSummary();

          PageParameters pars = new PageParameters();
          String fidUrl = urlFor(FilePage.class, new PageParameters("fid=" + fs.getFid())).toString();
          item.add(new ExternalLink("schemafilelink", fidUrl, fs.getFname()));
        }
      }
    };

    add(new Label("numFisheyeSchemas", "" + list.size()));    
    add(listview);
  }
}
