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
package com.cloudera.recordbreaker.analyzer;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.DriverManager;

import org.apache.hadoop.fs.permission.FsPermission; 
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Random;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.avro.Schema;

/*******************************************************
 * DataQuery handles Hive data-importation and query processing.
 * Assumes Hive is running locally at port 10000.
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 ********************************************************/
public class DataQuery implements Serializable {
  private static final Log LOG = LogFactory.getLog(DataQuery.class);
  private static boolean inited = false;
  private static DataQuery dataQuery;
  private static String hiveDriverName = "org.apache.hive.jdbc.HiveDriver";
  private static String impalaDriverName = "org.apache.hive.jdbc.HiveDriver";
  private static String tmpTablesDir = "/tmp/tmptables";

  String hiveConnectString;
  String impalaConnectString;
  Configuration conf;
  Connection hiveCon;
  Connection impalaCon;
  HiveTableCache tableCache;
  Random r = new Random();
  Map<Path, String> tables;
  Set<Path> isLoaded;

  public synchronized static DataQuery getInstance() {
    return DataQuery.getInstance(false);
  }
  public synchronized static DataQuery getInstance(boolean force) {
    if (force && dataQuery != null) {
      try {
        dataQuery.close();
      } catch (SQLException sqe) {
      }
      dataQuery = null;
    }
    if (force || (!inited)) {
      try {
        dataQuery = new DataQuery();
      } catch (SQLException se) {
        se.printStackTrace();
      } finally {
        inited = true;
      }
    }
    return dataQuery;    
  }

  public DataQuery() throws SQLException {
    try {
      this.conf = new Configuration();
      Class.forName(hiveDriverName);
      Class.forName(impalaDriverName);
      this.hiveConnectString = conf.get("hive.connectstring", "jdbc:hive2://localhost:10000/default");
      this.impalaConnectString = conf.get("impala.connectstring", "jdbc:hive2://localhost:21050/;auth=noSasl");
      LOG.info("Hive connect string: " + hiveConnectString);
      LOG.info("Impala connect string: " + impalaConnectString);      

      this.tableCache = new HiveTableCache();
      
      try {
        this.hiveCon = DriverManager.getConnection(hiveConnectString, "cloudera", "cloudera");
      } catch (Exception ex) {
        ex.printStackTrace();
      } 
      this.impalaCon = DriverManager.getConnection(impalaConnectString, "cloudera", "cloudera");      
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    // Force impala to refresh metadata
    if (impalaCon != null) {
      Statement stmt = impalaCon.createStatement();
      try {
        try {
          LOG.info("Rebuilding Impala metadata...");
          stmt.execute("INVALIDATE METADATA");
        } catch (Exception iex) {
          LOG.info("Impala metadata rebuild failed: " + iex.toString());
        }
      } finally {
        stmt.close();
      }
    }

    // Misc data structures
    this.tables = new HashMap<Path, String>();
    this.isLoaded = new HashSet<Path>();
  }

  public void close() throws SQLException {
    if (hiveCon != null) {
      this.hiveCon.close();
    }
    this.hiveCon = null;

    if (impalaCon != null) {
      this.impalaCon.close();
    }
    this.impalaCon = null;
  }

  /**
   * Connection string for Hive
   */
  public String getHiveConnectionString() {
    return hiveConnectString;
  }
  
  /**
   * Run a sample set of Hive test queries to check whether the Hive server is up and active
   */
  public boolean testQueryServer() {
    if (hiveCon == null) {
      return false;
    }
    try {
      //
      // Create table
      //
      String tablename = "test_datatable" + Math.abs(r.nextInt());
      Statement stmt = hiveCon.createStatement();
      try {
        stmt.execute("CREATE TABLE " + tablename + "(a int, b int, c int)");
      } finally {
        stmt.close();
      }

      //
      // Drop table
      //
      stmt = hiveCon.createStatement();
      try {
        stmt.execute("DROP TABLE " + tablename);
      } finally {
        stmt.close();
      }
      return true;
    } catch (Exception ex) {
      ex.printStackTrace();
      return false;
    }
  }

  String grabTable(DataDescriptor desc) throws SQLException, IOException {
    // Set up Hive table
    Path p = desc.getFilename();    
    String tablename = tableCache.get(p);
    if (tablename == null) {
      tablename = "datatable" + Math.abs(r.nextInt());
      Statement stmt = hiveCon.createStatement();
      try {
        String creatTxt = desc.getHiveCreateTableStatement(tablename);
        LOG.info("Create: " + creatTxt);
        stmt.execute(creatTxt);
        tables.put(p, tablename);
      } finally {
        stmt.close();
      }

      // Copy avro version of data into secret location prior to Hive import
      FileSystem fs = FileSystem.get(conf);      
      Path tmpTables = new Path(tmpTablesDir);
      if (! fs.exists(tmpTables)) {
        fs.mkdirs(tmpTables, new FsPermission("-rwxrwxrwx"));
      }
      Path secretDst = new Path(tmpTables, "r" + r.nextInt());
      LOG.info("Preparing Avro data at " + secretDst);      
      desc.prepareAvroFile(fs, fs, secretDst, conf);
      fs.setPermission(secretDst, new FsPermission("-rwxrwxrwx"));

      // Import data
      stmt = hiveCon.createStatement();
      try {
        LOG.info("Import data into Hive: " + desc.getHiveImportDataStatement(tablename, secretDst));
        stmt.execute(desc.getHiveImportDataStatement(tablename, secretDst));
        isLoaded.add(p);
      } finally {
        stmt.close();
      }

      // Refresh impala metadata
      stmt = impalaCon.createStatement();      
      try {
        try {
          LOG.info("Rebuilding Impala metadata...");
          stmt.execute("INVALIDATE METADATA");
        } catch (Exception iex) {
          LOG.info("Impala metadata rebuild failed: " + iex.toString());
        }
      } finally {
        stmt.close();
      }
      
      // Insert into table cache
      tableCache.put(p, tablename);
    }
    return tablename;
  }
  
  public List<List<Object>> query(DataDescriptor desc1, DataDescriptor desc2, String projectionClause, String selectionClause) throws SQLException, IOException {
    String tablename1 = grabTable(desc1);
    String tablename2 = null;
    if (desc2 != null) {
      tablename2 = grabTable(desc2);
    }
        
    //
    // Build the SQL query against the table
    //
    if (projectionClause == null || projectionClause.trim().length() == 0) {
      projectionClause = "*";
    }
    if (selectionClause == null) {
      selectionClause = "";
    }
    if (tablename2 == null) {
      projectionClause = projectionClause.replaceAll("DATA", tablename1);
      selectionClause = selectionClause.replaceAll("DATA", tablename1);
    }
    projectionClause = projectionClause.trim();    
    selectionClause = selectionClause.trim();
    String query;
    if (tablename2 == null) {
      query = "SELECT " + projectionClause + " FROM " + tablename1;
    } else {
      query = "SELECT " + projectionClause + " FROM " + tablename1 + " DATA1" + ", " + tablename2 + " DATA2";
    }
      
    if (selectionClause.length() > 0) {
      query = query + " WHERE " + selectionClause;
    }

    //
    // Try to run it first with the impala connection.
    // If that fails, try hive.
    //
    List<List<Object>> result = new ArrayList<List<Object>>();
    Statement stmt = impalaCon.createStatement();
    LOG.info("Processing: " + query);
    try {
      ResultSet res = null;
      try {
        res = stmt.executeQuery(query);
        LOG.info("Ran Impala query: " + query);
      } catch (Exception iex) {
        iex.printStackTrace();
        // Fail back to Hive!
        stmt.close();
        stmt = hiveCon.createStatement();
        res = stmt.executeQuery(query);
        LOG.info("Ran Hive query: " + query);
      }

      // OK now do the real work
      ResultSetMetaData rsmd = res.getMetaData();
      List<Object> metatuple = new ArrayList<Object>();
      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        metatuple.add(rsmd.getColumnLabel(i));
      }
      result.add(metatuple);
        
      while (res.next()) {
        List<Object> tuple = new ArrayList<Object>();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
          tuple.add(res.getObject(i));
        }
        result.add(tuple);
      }
      return result;
    } finally {
      stmt.close();
    }
  }
}