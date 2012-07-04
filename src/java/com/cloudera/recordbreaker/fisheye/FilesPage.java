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

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.request.mapper.parameter.PageParameters;

import java.util.List;

import com.cloudera.recordbreaker.analyzer.TypeSummary;
import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.SchemaSummary;
import com.cloudera.recordbreaker.analyzer.TypeGuessSummary;

/**
 * The <code>FilesPage</code> renders information about all known files.
 */
public class FilesPage extends WebPage {
  public FilesPage() {
    List<FileSummary> list = FishEye.getInstance().getAnalyzer().getFileSummaries();
    ListView<FileSummary> listview = new ListView<FileSummary>("listview", list) {
      protected void populateItem(ListItem<FileSummary> item) {
        FileSummary fs = item.getModelObject();

        // Fields are: 'filelink', 'sizelabel', 'typelink', and 'schemalink'
        String fileUrl = urlFor(FilePage.class, new PageParameters("fid=" + fs.getFid())).toString();
        item.add(new ExternalLink("filelink", fileUrl, fs.getFname()));

        item.add(new Label("sizelabel", "" + fs.getSize()));

        List<TypeGuessSummary> tgs = fs.getTypeGuesses();
        TypeSummary ts = tgs.get(0).getTypeSummary();
        SchemaSummary ss = tgs.get(0).getSchemaSummary();

        String typeUrl = urlFor(FiletypePage.class, new PageParameters("typeid=" + ts.getTypeId())).toString();
        item.add(new ExternalLink("typelink", typeUrl, ts.getLabel()));

        String schemaUrl = urlFor(SchemaPage.class, new PageParameters("schemaid=" + ss.getSchemaId())).toString();
        item.add(new ExternalLink("schemalink", schemaUrl, "Schema"));
      }
    };
    add(listview);
    add(new Label("numFisheyeFiles", "" + list.size()));
  }
}
