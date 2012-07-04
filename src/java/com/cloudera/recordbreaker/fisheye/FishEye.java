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

    FishEye.fisheyeInstance = this;
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
    this.username = username;
    return true;
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
      System.err.println("Usage: FishEye <port> <fisheyeDir>");
      return;
    }

    int port = Integer.parseInt(argv[0]);
    File fisheyeDir = new File(argv[1]);

    FishEye fish = new FishEye(port, fisheyeDir.getCanonicalFile());
    fish.run();
  }
}