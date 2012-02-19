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
  static FSAnalyzer analyzer;
  
  int port;
  File htmlRoot;
  File schemaDir;
  File fisheyeDir;
  
  public FishEye(int port, File htmlRoot, File schemaDir, File fisheyeDir) throws IOException, SQLiteException {
    this.port = port;
    this.htmlRoot = htmlRoot;
    this.schemaDir = schemaDir;
    this.fisheyeDir = fisheyeDir;
    FishEye.analyzer = new FSAnalyzer(fisheyeDir, schemaDir);
    System.err.println("Schema summary list: " + analyzer.getSchemaSummaries().size());
    System.err.println("Schema summary list: " + analyzer.getSchemaSummaries().size());
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
    System.err.println("JAR URL: " + jarDir);
    context.setBaseResource(new ResourceCollection(new String[] {htmlRoot.getCanonicalPath(), jarDir}));

    // Start the HTTP server
    Server server = new Server();
    SocketConnector connector = new SocketConnector();
    connector.setPort(port);
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
    if (argv.length < 4) {
      System.err.println("Usage: FishEye <port> <htmlRoot> <schemadir> <fisheyeDir>");
      return;
    }

    int port = Integer.parseInt(argv[0]);
    File htmlRoot = new File(argv[1]);
    File schemaDir = new File(argv[2]);
    File fisheyeDir = new File(argv[3]);

    FishEye fish = new FishEye(port, htmlRoot.getCanonicalFile(), schemaDir.getCanonicalFile(), fisheyeDir.getCanonicalFile());
    fish.run();
  }
}