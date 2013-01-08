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
  private static boolean inited;
  private static DataQuery dataQuery;
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

  Connection con;
  Random r = new Random();
  Map<Path, String> tables;
  Set<Path> isLoaded;

  public static DataQuery getInstance() {
    if (! inited) {
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
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    this.con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
    this.tables = new HashMap<Path, String>();
    this.isLoaded = new HashSet<Path>();
  }

  public void close() throws SQLException {
    this.con.close();
    this.con = null;
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
        ResultSet res = stmt.executeQuery(desc.getHiveCreateTableStatement(tablename));
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
      System.err.println("NOW RUNNING '" + query + "'");
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

  public void standaloneQuery() throws SQLException {
    Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");    
    Statement stmt = con.createStatement();
    stmt.executeQuery("drop table foobar");
    // 1. Add the SERDE jar

    // 2. Create the table, with SERDE configured
    ResultSet res = stmt.executeQuery("create table foobar row format serde 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' stored as inputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' outputformat 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' tblproperties ('avro.schema.url' = 'file:///Users/mjc/cloudera/repos/recordbreaker/src/samples/schemas/hr-schema.json')");

    // 3. Insert data from file
    res = stmt.executeQuery("load data local inpath '/Users/mjc/cloudera/repos/recordbreaker/src/samples/schemas/hr.avro' into table foobar");

    res = stmt.executeQuery("select * from foobar");
    ResultSetMetaData rsmd = res.getMetaData();

    List<List<String>> result = new ArrayList<List<String>>();    
    while (res.next()) {
      List<String> tuple = new ArrayList<String>();
      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        tuple.add("" + res.getObject(i));
      }
      result.add(tuple);
    }

    System.err.println("Got " + result.size() + " elements back!");
    for (List<String> tup: result) {
      for (String col: tup) {
        System.err.print(col + "\t");
      }
      System.err.println();
    }
    
    /**
    HiveConf hc = new HiveConf();
    for (Map.Entry<String, String> pair: hc) {
      System.err.println(pair.getKey() + "=" + pair.getValue());
    }
    **/
  }

  public static void main(String argv[]) throws Exception {
    try {
      Class.forName(DataQuery.driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    DataQuery dq = new DataQuery();
    dq.standaloneQuery();
  }
}