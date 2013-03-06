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

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Random;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

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
  private static boolean inited = false;
  private static DataQuery dataQuery;
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static String connectString = "jdbc:hive://localhost:10000/default";

  Connection con;
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
      Class.forName(driverName);
      this.con = DriverManager.getConnection(connectString, "", "");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    this.tables = new HashMap<Path, String>();
    this.isLoaded = new HashSet<Path>();
  }

  public void close() throws SQLException {
    if (con != null) {
      this.con.close();
    }
    this.con = null;
  }

  /**
   * Connection string for Hive
   */
  public String getHiveConnectionString() {
    return connectString;
  }
  
  /**
   * Run a sample set of Hive test queries to check whether the Hive server is up and active
   */
  public boolean testQueryServer() {
    if (con == null) {
      return false;
    }
    try {
      //
      // Create table
      //
      String tablename = "test_datatable" + Math.abs(r.nextInt());
      Statement stmt = con.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("CREATE TABLE " + tablename + "(a int, b int, c int)");
      } finally {
        stmt.close();
      }

      //
      // Drop table
      //
      stmt = con.createStatement();
      try {
        ResultSet rs = stmt.executeQuery("DROP TABLE " + tablename);
      } finally {
        stmt.close();
      }
      return true;
    } catch (Exception ex) {
      ex.printStackTrace();
      return false;
    }
  }

  public List<List<String>> query(DataDescriptor desc, String projectionClause, String selectionClause) throws SQLException {
    SchemaDescriptor sd = desc.getSchemaDescriptor().get(0);
    Schema schema = sd.getSchema();
    Path p = desc.getFilename();
        
    // Set up Hive table
    String tablename = tables.get(p);
    if (tablename == null) {
      tablename = "datatable" + Math.abs(r.nextInt());
      Statement stmt = con.createStatement();
      try {
        String creatTxt = desc.getHiveCreateTableStatement(tablename);
        ResultSet res = stmt.executeQuery(creatTxt);
        tables.put(p, tablename);
      } finally {
        stmt.close();
      }
    }

    // Insert data from file, if it hasn't happened already
    if (! isLoaded.contains(p)) {
      Statement stmt = con.createStatement();
      try {            
        ResultSet res = stmt.executeQuery(desc.getHiveImportDataStatement(tablename));
        isLoaded.add(p);
      } finally {
        stmt.close();
      }
    }

    // Run the hive query against the table
    if (projectionClause == null || projectionClause.trim().length() == 0) {
      projectionClause = "*";
    }
    projectionClause = projectionClause.trim();
    if (selectionClause == null) {
      selectionClause = "";
    }
    selectionClause = selectionClause.trim();
    
    String query = "SELECT " + projectionClause + " FROM " + tablename;
    if (selectionClause.length() > 0) {
      query = query + " WHERE " + selectionClause;
    }
    List<List<String>> result = new ArrayList<List<String>>();
    Statement stmt = con.createStatement();
    try {
      ResultSet res = stmt.executeQuery(query);
      ResultSetMetaData rsmd = res.getMetaData();
      
      while (res.next()) {
        List<String> tuple = new ArrayList<String>();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
          tuple.add("" + res.getObject(i));
        }
        result.add(tuple);
      }
      return result;
    } finally {
      stmt.close();
    }
  }
}