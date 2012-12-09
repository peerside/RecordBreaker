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

import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.TypeSummary;
import com.cloudera.recordbreaker.analyzer.SchemaSummary;
import com.cloudera.recordbreaker.analyzer.TypeGuessSummary;

import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
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

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.List;

/*******************************************************
 * Wicket Page class that describes a specific File
 *******************************************************/
public class FilePage extends WebPage {

  final class FilePageDisplay extends WebMarkupContainer {
    long fid = -1L;
    
    public FilePageDisplay(String name, String fidStr) {
      super(name);
      FishEye fe = FishEye.getInstance();

      if (fe.hasFSAndCrawl()) {
        if (fidStr != null) {
          try {
            this.fid = Long.parseLong(fidStr);
            FileSummary fs = new FileSummary(fe.getAnalyzer(), fid);
            List<TypeGuessSummary> tgses = fs.getTypeGuesses();
            
            add(new Label("filetitle", fs.getFname()));
            add(new ExternalLink("filesubtitlelink", urlFor(FilesPage.class, new PageParameters("targetdir=" + fs.getPath().getParent().toString())).toString(), fs.getPath().getParent().toString()));

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
            
            add(new Label("owner", fs.getOwner()));
            add(new Label("size", "" + fs.getSize()));
            add(new Label("lastmodified", fs.getLastModified()));
            add(new Label("crawledon", fs.getCrawl().getStartedDate()));

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
  }
}
