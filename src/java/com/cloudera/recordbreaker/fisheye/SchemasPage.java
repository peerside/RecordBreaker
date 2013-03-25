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
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.link.ExternalLink;

import org.codehaus.jackson.JsonNode;

import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.io.IOException;

import com.cloudera.recordbreaker.analyzer.FSAnalyzer;
import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.SchemaSummary;
import com.cloudera.recordbreaker.analyzer.TypeGuessSummary;

/**
 * The <code>SchemasPage</code> renders information about all known schemas.
 */
public class SchemasPage extends WebPage {
  final class SchemaListing extends WebMarkupContainer {
    long totalElapsed = 0;
    long totalInvocations = 0;
    public SchemaListing(String name) {
      super(name);
      System.err.println("STARTING....");
      long start = System.currentTimeMillis();
      
      FishEye fe = FishEye.getInstance();
      if (fe.hasFSAndCrawl()) {
        //List<SchemaSummary> list = fe.getAnalyzer().getSchemaSummaries();
        List<SchemaSummary> list = fe.getAnalyzer().getPrecachedSchemaSummaries();
        ListView<SchemaSummary> listview = new ListView<SchemaSummary>("listview", list) {
          protected void populateItem(ListItem<SchemaSummary> item) {
            long localStart = System.currentTimeMillis();

            SchemaSummary ss = item.getModelObject();
            item.add(new Label("schemadesc", ss.getDesc()));


            //item.add(new ExternalLink("schemalabellink", "schemaurl", "Short desc"));
            StringBuffer schemalabel = new StringBuffer();
            try {
              List<List<JsonNode>> listOfSchemaElts = SchemaPage.getSchemaDigest(ss.getIdentifier());
              for (Iterator<JsonNode> it = listOfSchemaElts.get(0).iterator(); it.hasNext(); ) {
                JsonNode curNode = it.next();
                schemalabel.append(curNode.get("name"));
                if (it.hasNext()) {
                  schemalabel.append(", ");
                }
              }
            } catch (IOException iex) {
            }
            String schemaUrl = urlFor(SchemaPage.class, new PageParameters("schemaid=" + ss.getSchemaId())).toString();
            item.add(new ExternalLink("schemalabellink", schemaUrl, schemalabel.toString()));


            //item.add(new ExternalLink("schemafilelink", "fidUrl", "fs"));
            List<TypeGuessSummary> typeGuesses = ss.getTypeGuesses();
            for (int i = 0; i < Math.min(1, typeGuesses.size()); i++) {
              TypeGuessSummary curTGS = typeGuesses.get(i);
              FileSummary fs = curTGS.getFileSummary();

              PageParameters pars = new PageParameters();
              String fidUrl = urlFor(FilePage.class, new PageParameters("fid=" + fs.getFid())).toString();
              item.add(new ExternalLink("schemafilelink", fidUrl, fs.getFname()));
            }

            long localEnd = System.currentTimeMillis();
            totalElapsed += (localEnd - localStart);
            totalInvocations++;
            System.err.println("Total populateItem: " + (totalElapsed / 1000.0) + ", total invocations: " + totalInvocations);
          }
        };
        add(new Label("numFisheyeSchemas", "" + list.size()));        
        add(listview);
      }

      setOutputMarkupPlaceholderTag(true);
      setVisibilityAllowed(false);
      long end = System.currentTimeMillis();
      System.err.println("Elapsed secs: " + ((end - start) / 1000.0));
    }
    public void onConfigure() {
      FishEye fe = FishEye.getInstance();    
      setVisibilityAllowed(fe.hasFSAndCrawl());
    }
  }

  public SchemasPage() {
    add(new SchemaListing("currentSchemaListing"));
    add(new SettingsWarningBox());
    add(new CrawlWarningBox());            
  }
}
