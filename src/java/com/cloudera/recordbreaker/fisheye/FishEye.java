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

import java.io.*;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;

import com.cloudera.recordbreaker.analyzer.FSAnalyzer;
import com.cloudera.recordbreaker.analyzer.FSCrawler;
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
  Date startTime;
  int fisheyePort;
  File fisheyeDir;
  String username;

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
    this.username = null;
    this.fisheyeDir = fisheyeDir;
    this.fisheyePort = port;

    File fisheyeStore = new File(fisheyeDir, FISHEYE_METADATA_STORE);
    File fisheyeSchemas = new File(fisheyeDir, FISHEYE_SCHEMA_REPO);
    this.analyzer = new FSAnalyzer(fisheyeStore, fisheyeSchemas);
    this.crawler = new FSCrawler(analyzer);
    FishEye.fisheyeInstance = this;
    restartIncompleteCrawl();
  }

  public boolean restartIncompleteCrawl() {
    String fsUrl = analyzer.getConfigProperty("fsurl");
    if (fsUrl != null) {
      long fsid = analyzer.getCreateFilesystem(fsUrl, true);
      long pendingCrawlId = analyzer.getCreatePendingCrawl(fsid, false);
      if (pendingCrawlId >= 0) {
        return crawler.getStartNonblockingCrawl(fsUrl);
      }
    }
    return false;
  }

  public CrawlRuntimeStatus checkOngoingCrawl() {
    String fsUrl = analyzer.getConfigProperty("fsurl");
    if (fsUrl != null) {
      CrawlRuntimeStatus crs = crawler.isCrawlOngoing(fsUrl);
      return crs;
    }
    return null;
  }

  public boolean checkCreateCrawl() {
    String fsUrl = analyzer.getConfigProperty("fsurl");
    if (fsUrl != null) {
      return crawler.getStartNonblockingCrawl(fsUrl);
    }
    return false;
  }

  public boolean registerAndCrawlFilesystem(String fsUrl) throws IOException {
    // REMIND - check the filesystem before proceeding.
    analyzer.setConfigProperty("fsurl", fsUrl);
    analyzer.getCreateFilesystem(fsUrl, true);        
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
  public String getFSUrl() {
    return analyzer.getConfigProperty("fsurl");
  }
  public void cancelFS() {
    String fsUrl = analyzer.getConfigProperty("fsurl");
    crawler.killCrawl(fsUrl);
    analyzer.setConfigProperty("fsurl", null);
  }
  public String getTopDir() {
    String fsUrl = analyzer.getConfigProperty("fsurl");
    if (fsUrl == null) {
      return null;
    }
    long fsid = analyzer.getCreateFilesystem(fsUrl, false);
    if (fsid >= 0) {
      long crawlid = analyzer.getLatestCompleteCrawl(fsid);
      if (crawlid >= 0) {
        String td = analyzer.getTopDir(crawlid);
        return td;
      }
    }
    return null;
  }
  public List<File> getDirParents(String targetDir) {
    String fsUrl = analyzer.getConfigProperty("fsurl");
    if (fsUrl == null) {
      return null;
    }
    long fsid = analyzer.getCreateFilesystem(fsUrl, false);
    if (fsid >= 0) {
      long crawlid = analyzer.getLatestCompleteCrawl(fsid);
      if (crawlid >= 0) {
        return analyzer.getDirParents(crawlid, targetDir);
      }
    }
    return null;
  }
  public List<File> getDirChildren(String targetDir) {
    String fsUrl = analyzer.getConfigProperty("fsurl");
    if (fsUrl == null) {
      return null;
    }
    long fsid = analyzer.getCreateFilesystem(fsUrl, false);
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
  public String getUsername() {
    return this.username;
  }
  public void logout() {
    this.username = null;
  }
  public boolean login(String username, String password) {
    // For now, login always succeeds
    if (username.equals(password)) {
      this.username = username;
      return true;
    } else {
      return false;
    }
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