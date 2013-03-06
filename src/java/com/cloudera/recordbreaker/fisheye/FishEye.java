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

import org.apache.wicket.protocol.http.WicketFilter;
import org.apache.wicket.protocol.http.WicketServlet;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.resource.ResourceCollection;

import com.almworks.sqlite4java.SQLiteException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.*;
import java.net.URI;
import javax.servlet.*;
import javax.servlet.http.*;
import java.net.URISyntaxException;

import com.cloudera.recordbreaker.analyzer.DataQuery;
import com.cloudera.recordbreaker.analyzer.FSCrawler;
import com.cloudera.recordbreaker.analyzer.FSAnalyzer;
import com.cloudera.recordbreaker.analyzer.FileSummary;
import com.cloudera.recordbreaker.analyzer.CrawlRuntimeStatus;

/***************************************************************
 * <code>FishEye</code> is a web app that allows a user to examine
 * the semantic contents of a filesystem.  It makes extensive use of
 * LearnStructure and other format-parsing code to help the user
 * figure out the content of files.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 ***************************************************************/
public class FishEye {
  static final String FISHEYE_METADATA_STORE = "metadata";
  static final String FISHEYE_SCHEMA_REPO = "schemarepo";

  static FishEye fisheyeInstance;
  FSAnalyzer analyzer;
  FSCrawler crawler;
  AccessController accessCtrl;
  Date startTime;
  int fisheyePort;
  File fisheyeDir;
  boolean hasTestedQueryServer = false;
  boolean isQueryServerAvailable = false;

  public static FishEye getInstance() {
    return fisheyeInstance;
  }
  
  public FishEye(int port, File fisheyeDir) throws IOException, SQLiteException {
    if (! fisheyeDir.exists()) {
      if (! fisheyeDir.mkdir()) {
        throw new IOException("Cannot create directory: " + fisheyeDir.getCanonicalPath());
      }
    }
    this.startTime = new Date(System.currentTimeMillis());
    this.fisheyeDir = fisheyeDir;
    this.fisheyePort = port;

    File fisheyeStore = new File(fisheyeDir, FISHEYE_METADATA_STORE);
    File fisheyeSchemas = new File(fisheyeDir, FISHEYE_SCHEMA_REPO);
    this.analyzer = new FSAnalyzer(fisheyeStore, fisheyeSchemas);
    this.crawler = new FSCrawler(analyzer);
    this.accessCtrl = new AccessController();
    FishEye.fisheyeInstance = this;
    restartIncompleteCrawl();
  }

  /**
   * Check if the Hive query server is available
   */
  public boolean isQueryServerAvailable(boolean force) {
    if (force || (! hasTestedQueryServer)) {
      hasTestedQueryServer = true;
      isQueryServerAvailable = DataQuery.getInstance(force).testQueryServer();
    }
    return isQueryServerAvailable;
  }

  public boolean restartIncompleteCrawl() {
    URI fsURI = getFSURI();
    if (fsURI != null) {
      long fsid = analyzer.getCreateFilesystem(fsURI, true);
      long pendingCrawlId = analyzer.getCreatePendingCrawl(fsid, false);
      if (pendingCrawlId >= 0) {
        return crawler.getStartNonblockingCrawl(fsURI);
      }
    }
    return false;
  }

  public CrawlRuntimeStatus checkOngoingCrawl() {
    URI fsUri = getFSURI();
    if (fsUri != null) {
      CrawlRuntimeStatus crs = crawler.isCrawlOngoing(fsUri);
      return crs;
    }
    return null;
  }

  public boolean checkCreateCrawl() {
    URI fsUri = getFSURI();
    if (fsUri != null) {
      return crawler.getStartNonblockingCrawl(fsUri);
    }
    return false;
  }

  public boolean registerAndCrawlFilesystem(URI fsURI) throws IOException {
    // REMIND - check the filesystem before proceeding.
    analyzer.setConfigProperty("fsuri", fsURI.toString());
    analyzer.getCreateFilesystem(fsURI, true);        
    return checkCreateCrawl();
  }

  public Date getStartTime() {
    return startTime;
  }
  public int getPort() {
    return fisheyePort;
  }
  public File getFisheyeDir() {
    return fisheyeDir;
  }
  public boolean hasFSAndCrawl() {
    URI fsURI = getFSURI();
    if (fsURI != null) {
      long fsid = analyzer.getCreateFilesystem(fsURI, false);
      if (fsid >= 0) {
        return analyzer.getLatestCompleteCrawl(fsid) >= 0;
      }
    }
    return false;
  }
  public URI getFSURI() {
    try {
      String uriStr = analyzer.getConfigProperty("fsuri");
      if (uriStr == null) {
        return null;
      } else {
        return new URI(uriStr);
      }
    } catch (URISyntaxException use) {
      use.printStackTrace();
      return null;
    }
  }
  public void cancelFS() {
    URI fsUri = getFSURI();
    crawler.killOngoingCrawl(fsUri);
    analyzer.setConfigProperty("fsuri", null);
  }
  public String getTopDir() {
    URI fsUri = getFSURI();
    if (fsUri == null) {
      return null;
    }
    long fsid = analyzer.getCreateFilesystem(fsUri, false);
    if (fsid >= 0) {
      long crawlid = analyzer.getLatestCompleteCrawl(fsid);
      if (crawlid >= 0) {
        Path td = analyzer.getTopDir(crawlid);
        return td.toString();
      }
    }
    return null;
  }
  public List<FileSummary> getDirParents(String targetDir) {
    URI fsUri = getFSURI();
    if (fsUri == null) {
      return null;
    }
    long fsid = analyzer.getCreateFilesystem(fsUri, false);
    if (fsid >= 0) {
      long crawlid = analyzer.getLatestCompleteCrawl(fsid);
      if (crawlid >= 0) {
        return analyzer.getDirParents(crawlid, targetDir);
      }
    }
    return null;
  }
  public List<FileSummary> getDirChildren(String targetDir) {
    URI fsUri = getFSURI();
    if (fsUri == null) {
      return null;
    }
    long fsid = analyzer.getCreateFilesystem(fsUri, false);
    if (fsid >= 0) {
      long crawlid = analyzer.getLatestCompleteCrawl(fsid);
      if (crawlid >= 0) {
        return analyzer.getDirChildren(crawlid, targetDir);
      }
    }
    return null;
  }
  public FSAnalyzer getAnalyzer() {
    return analyzer;
  }
  public AccessController getAccessController() {
    return accessCtrl;
  }

  public void run() throws Exception {
    // Jetty object that holds the WicketServlet
    WicketServlet ws = new WicketServlet();
    ServletHolder servletHolder = new ServletHolder(ws);
    servletHolder.setInitParameter("applicationClassName", "com.cloudera.recordbreaker.fisheye.FishEyeWebApplication");
    servletHolder.setInitParameter(WicketFilter.FILTER_MAPPING_PARAM, "/*");
    servletHolder.setInitOrder(1);

    // Config the Jetty WebAppContext object
    WebAppContext context = new WebAppContext();
    context.addServlet(servletHolder, "/*");
    String jarDir = this.getClass().getClassLoader().getResource("content/library/bootstrap/1.4.0").toExternalForm();
    String htmlRoot = this.getClass().getClassLoader().getResource("web/fisheye").toExternalForm();
    context.setBaseResource(new ResourceCollection(new String[] {htmlRoot, jarDir}));

    // Start the HTTP server
    Server server = new Server();
    SocketConnector connector = new SocketConnector();
    connector.setPort(fisheyePort);
    server.setConnectors(new Connector[]{connector});
    server.setHandler(context);
    
    try {
      server.start();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Start the FishEye Server.
   */
  public static void main(String argv[]) throws Exception {
    if (argv.length < 2) {
      System.err.println("Usage: FishEye (-run <port> <fisheyeDir>) (-init <targetdir> [schemadbdir])");
      return;
    }
    int i = 0;
    String cmd = argv[i++];

    if ("-run".equals(cmd)) {
      int port = Integer.parseInt(argv[i++]);
      File fisheyeDir = new File(argv[i++]);
      FishEye fish = new FishEye(port, fisheyeDir.getCanonicalFile());
      fish.run();
    } else if ("-init".equals(cmd)) {
      File targetDir = new File(argv[i++]).getCanonicalFile();
      File schemaDbDir = null;
      if (i < argv.length) {
        schemaDbDir = new File(argv[i++]).getCanonicalFile();
      }
      if (targetDir.exists()) {
        throw new IOException("Directory already exists: " + targetDir);
      }
      if (! targetDir.mkdirs()) {
        throw new IOException("Cannot create: " + targetDir);
      }
      File metadataStore = new File(targetDir, "metadata");
      FSAnalyzer fsa = new FSAnalyzer(metadataStore, schemaDbDir);
      fsa.close();
    }
  }
}