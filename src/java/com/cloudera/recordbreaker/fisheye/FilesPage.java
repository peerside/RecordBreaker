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
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.request.mapper.parameter.PageParameters;

import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.recordbreaker.analyzer.TypeSummary;
import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.SchemaSummary;
import com.cloudera.recordbreaker.analyzer.TypeGuessSummary;

/**
 * The <code>FilesPage</code> renders information about all known files.
 */
public class FilesPage extends WebPage {
  final class DirLabelPair {
    volatile String label;
    volatile Path dir;
    public DirLabelPair(String label, Path dir) {
      this.label = label;
      this.dir = dir;
    }
    public String getLabel() {
      return label;
    }
    public Path getDir() {
      return dir;
    }
  }

  //
  // File listing for the current directory
  //
  final class FileListing extends WebMarkupContainer {
    public FileListing(String name, String targetDir) {
      super(name);
      FishEye fe = FishEye.getInstance();
      final AccessController accessCtrl = fe.getAccessController();
      
      if (fe.hasFSAndCrawl()) {
        List<DirLabelPair> parentDirPairList = new ArrayList<DirLabelPair>();
        List<Path> dirList = fe.getDirParents(targetDir);
        dirList.add(new Path(targetDir));
        Path lastDir = null;
        if (dirList != null) {
          for (Path curDir: dirList) {
            String prefix = "";
            if (lastDir != null) {
              prefix = lastDir.toString();
            }
            String label = curDir.toString().substring(prefix.length());
            parentDirPairList.add(new DirLabelPair(label, curDir));
            lastDir = curDir;
          }
        }

        add(new Label("lastParentDirEntry", parentDirPairList.get(parentDirPairList.size()-1).getLabel()));
        parentDirPairList.remove(parentDirPairList.size()-1);

        final List<DirLabelPair> childDirPairList = new ArrayList<DirLabelPair>();
        dirList = FishEye.getInstance().getDirChildren(targetDir);
        if (dirList != null) {
          for (Path curDir: dirList) {
            String label = curDir.toString().substring(targetDir.length());
            childDirPairList.add(new DirLabelPair(label, curDir));
          }
        }

        add(new ListView<DirLabelPair>("parentdirlisting", parentDirPairList) {
            protected void populateItem(ListItem<DirLabelPair> item) {
              DirLabelPair pair = item.getModelObject();

              String dirUrl = urlFor(FilesPage.class, new PageParameters("targetdir=" + pair.getDir().toString())).toString();
              item.add(new ExternalLink("dirlink", dirUrl, pair.getLabel()));
            }
          });

        add(new WebMarkupContainer("subdirbox") {
            {
              add(new ListView<DirLabelPair>("childdirlisting", childDirPairList) {
                  protected void populateItem(ListItem<DirLabelPair> item) {
                    DirLabelPair pair = item.getModelObject();

                    String dirUrl = urlFor(FilesPage.class, new PageParameters("targetdir=" + pair.getDir().toString())).toString();
                    item.add(new ExternalLink("childdirlink", dirUrl, pair.getLabel()));
                  }
                });
              setOutputMarkupPlaceholderTag(true);
              setVisibilityAllowed(false);
            }
            public void onConfigure() {
              setVisibilityAllowed(childDirPairList.size() > 0);
            }
          });

        List<FileSummary> filelist = FishEye.getInstance().getAnalyzer().getFileSummaries(false, targetDir);
        add(new Label("numFisheyeFiles", "" + filelist.size()));
        add(new ListView<FileSummary>("filelisting", filelist) {
            protected void populateItem(ListItem<FileSummary> item) {
              FileSummary fs = item.getModelObject();

              // 1.  Filename.  Link is conditional on having read access
              if (accessCtrl.getCurrentUser() != null) {
                String fileUrl = urlFor(FilePage.class, new PageParameters("fid=" + fs.getFid())).toString();                
                item.add(new Label("filelabel", "<a href=\"" + fileUrl + "\">" + fs.getFname() + "</a>").setEscapeModelStrings(false));
              } else {
                item.add(new Label("filelabel", fs.getFname()));
              }

              // 2-5. A bunch of fields that get added no matter what the user's access rights.
              item.add(new Label("sizelabel", "" + fs.getSize()));
              item.add(new Label("ownerlabel", fs.getOwner()));
              item.add(new Label("grouplabel", fs.getGroup()));
              item.add(new Label("permissionslabel", fs.getPermissions()));

              // 6-7.  Fields that have link conditional on read access AND the existence of relevant info.
              if (accessCtrl.getCurrentUser() != null) {
                List<TypeGuessSummary> tgs = fs.getTypeGuesses();
                if (tgs.size() > 0) {
                  TypeSummary ts = tgs.get(0).getTypeSummary();
                  SchemaSummary ss = tgs.get(0).getSchemaSummary();

                  String typeUrl = urlFor(FiletypePage.class, new PageParameters("typeid=" + ts.getTypeId())).toString();
                  item.add(new Label("typelabel", "<a href=\"" + typeUrl + "\">" + ts.getLabel() + "</a>").setEscapeModelStrings(false));

                  String schemaUrl = urlFor(SchemaPage.class, new PageParameters("schemaid=" + ss.getSchemaId())).toString();
                  item.add(new Label("schemalabel", "<a href=\"" + schemaUrl + "\">" + "Schema" + "</a>").setEscapeModelStrings(false));                  
                } else {
                  item.add(new Label("typelink", ""));
                  item.add(new Label("schemalink", ""));
                }
              } else {
                item.add(new Label("typelabel", "---"));
                item.add(new Label("schemalabel", "---"));
              }
            }
          });
      }

      setOutputMarkupPlaceholderTag(true);
      setVisibilityAllowed(false);
    }

    public void onConfigure() {
      FishEye fe = FishEye.getInstance();    
      setVisibilityAllowed(fe.hasFSAndCrawl());
    }
  }
  
  public FilesPage() {
    FishEye fe = FishEye.getInstance();
    String targetDir = fe.getTopDir() != null ? fe.getTopDir() : "/";    
    add(new SettingsWarningBox());
    add(new CrawlWarningBox());
    add(new FileListing("currentDirListing", targetDir));
  }
  public FilesPage(PageParameters params) {
    String targetDir = params.get("targetdir").toString();
    add(new SettingsWarningBox());
    add(new CrawlWarningBox());
    add(new FileListing("currentDirListing", targetDir));
  }
}
