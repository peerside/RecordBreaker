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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import java.util.Date;
import java.util.Random;
import java.util.ArrayList;

import com.almworks.sqlite4java.SQLite;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteQueue;
import com.almworks.sqlite4java.SQLiteStatement;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteConnection;


/***************************************************************
 * <code>FSAnalyzer</code> crawls a filesystem and figures out
 * its schema contents. We place the results of that analysis into
 * a store for future analytics
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 ***************************************************************/
public class FSAnalyzer {
  ////////////////////////////////////////
  // All the SQL statements we need
  ////////////////////////////////////////
  static Random r = new Random();
  static SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  //
  // 1. Create the schemas
  //
  static String CREATE_TABLE_CONFIG = "CREATE TABLE Configs(propertyname varchar(128), property varchar(256));";
  static String CREATE_TABLE_CRAWL = "CREATE TABLE Crawls(crawlid integer primary key autoincrement, lastexamined text, inprogress text, fsid integer, foreign key(fsid) references Filesystems(fsid));";
  static String CREATE_TABLE_FILESYSTEM = "CREATE TABLE Filesystems(fsid integer primary key autoincrement, fsname text);";    
  static String CREATE_TABLE_FILES = "CREATE TABLE Files(fid integer primary key autoincrement, crawlid integer, fname varchar(256), owner varchar(16), size integer, modified date, path varchar(256), foreign key(crawlid) references Crawls(crawlid));";
  static String CREATE_TABLE_TYPES = "CREATE TABLE Types(typeid integer primary key autoincrement, typelabel varchar(64), typedescriptor varchar(1024));";
  static String CREATE_TABLE_SCHEMAS = "CREATE TABLE Schemas(schemaid integer primary key autoincrement, schemalabel varchar(64), schemadescriptor varchar(1024));";
  static String CREATE_TABLE_GUESSES = "CREATE TABLE TypeGuesses(fid integer, typeid integer, schemaid integer, score double, foreign key(fid) references Files(fid), foreign key(typeid) references Types(typeid), foreign key(schemaid) references Schemas(schemaid));";
  void createTables() throws SQLiteException {
    dbQueue.execute(new SQLiteJob<Object>() {
        protected Object job(SQLiteConnection db) throws SQLiteException {
          try {
            db.exec(CREATE_TABLE_CONFIG);
            db.exec(CREATE_TABLE_FILESYSTEM);                        
            db.exec(CREATE_TABLE_CRAWL);
            db.exec(CREATE_TABLE_FILES);    
            db.exec(CREATE_TABLE_TYPES);
            db.exec(CREATE_TABLE_SCHEMAS);
            db.exec(CREATE_TABLE_GUESSES);    
          } finally {
          }
          return null;
        }
      }).complete();
  }

  ///////////////////////////////////////////////
  // Manage Crawls and Filesystems
  ///////////////////////////////////////////////
  public long getCreateFilesystem(final String fsname) throws SQLiteException {
    long fsid = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT fsid FROM Filesystems WHERE fsname = ?");
          try {
            stmt.bind(1, fsname);
            if (stmt.step()) {
              long resultId = stmt.columnLong(0);
              return resultId;
            } else {
              return -1L;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
    if (fsid >= 0) {
      return fsid;
    }

    // It wasn't there, so create it!
    return dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("INSERT into Filesystems VALUES(null, ?)");
          try {
            stmt.bind(1, fsname);
            stmt.step();
            return db.getLastInsertId();
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  };
  
  /**
   * Helper fn <code>getNewOrPendingCrawl</code> returns the id of a Crawl for the specified filesystem.
   * If a crawl is pending, that one is returned.
   * If no crawl is pending, a new one is created.
   */
  public long getNewOrPendingCrawl(final long fsid) throws SQLiteException {
    long crawlid = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT crawlid from Crawls WHERE fsid = ? AND inprogress = 'True'");
          try {
            stmt.bind(1, fsid);
            if (stmt.step()) {
              return stmt.columnLong(0);
            } else {
              return -1L;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();

    if (crawlid >= 0) {
      return crawlid;
    }
    
    // Time to insert
    return dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          Date now = new Date(System.currentTimeMillis());
          String lastExamined = fileDateFormat.format(now);
          String inprogress = "True";
          SQLiteStatement stmt = db.prepare("INSERT into Crawls VALUES(null, ?, ?, ?)");
          try {
            stmt.bind(1, lastExamined).bind(2, inprogress).bind(3, fsid);
            stmt.step();
            return db.getLastInsertId();
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }
  public void completeCrawl(final long fsid) throws SQLiteException {
    dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("UPDATE Crawls SET inprogress='False' WHERE fsid = ?");
          try {
            stmt.bind(1, fsid);
            if (stmt.step()) {
              return fsid;
            } else {
              return -1L;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  ///////////////////////////////////////////////
  // Manage Types and Schemas
  ///////////////////////////////////////////////
  /**
   * Helper fn <code>getCreateType</code> returns the id of a specified Type in the Types table.
   * The row is created, if necessary.
   */
  long getCreateType(final String typeLabel, final String typeDesc) throws SQLiteException {
    long typeid = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT typeid FROM Types WHERE typelabel = ? AND typedescriptor = ?");
          try {
            stmt.bind(1, typeLabel).bind(2, typeDesc);
            if (stmt.step()) {
              long resultId = stmt.columnLong(0);
              return resultId;
            } else {
              return -1L;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();

    if (typeid >= 0) {
      return typeid;
    }
    
    // Time to insert
    return dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("INSERT into Types VALUES(null, ?, ?)");
          try {
            stmt.bind(1, typeLabel).bind(2, typeDesc);
            stmt.step();
            return db.getLastInsertId();
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  /**
   * Helper fn <code>getCreateSchema</code> returns the id of a specified Schema in the Schemas table.
   * The row is created, if necessary.
   */
  long getCreateSchema(final String schemaLabel, final String schemaDesc) throws SQLiteException {
    long schemaid = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          final SQLiteStatement stmt = db.prepare("SELECT schemaid FROM Schemas WHERE schemalabel = ? AND schemadescriptor = ?");
          try {
            stmt.bind(1, schemaLabel).bind(2, schemaDesc);
            if (stmt.step()) {
              long resultId = stmt.columnLong(0);
              return resultId;
            } else {
              return -1L;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();

    if (schemaid >= 0) {
      return schemaid;
    }
    
    // Time to insert
    return dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          final SQLiteStatement stmt = db.prepare("INSERT into Schemas VALUES(null, ?, ?)");
          try {
            stmt.bind(1, schemaLabel).bind(2, schemaDesc);
            stmt.step();
            return db.getLastInsertId();      
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }
    
  /**
   * Add a new object to the set of all known files.  This involves several tables.
   */
  long insertIntoFiles(final String fname, final String owner, final long size, final String timeDateStamp, final String path, final long crawlId, final List<TypeGuess> typeGuesses) throws SQLiteException {
    final long fileId = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("INSERT into Files VALUES(null, ?, ?, ?, ?, ?, ?)");
          try {
            stmt.bind(1, crawlId).bind(2, fname).bind(3, owner).bind(4, size).bind(5, timeDateStamp).bind(6, path);
            stmt.step();
            return db.getLastInsertId();
          } finally {
            stmt.dispose();
          }
        }
      }).complete();

    // Go through all the associated typeGuesses
    dbQueue.execute(new SQLiteJob<Object>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          for (TypeGuess tg: typeGuesses) {
            String typeLabel = tg.getTypeLabel();
            String typeDesc = tg.getTypeDesc();
            String schemaLabel = tg.getSchemaLabel();
            String schemaDesc = tg.getSchemaDesc();
            double score = tg.getScore();

            long typeId = getCreateType(typeLabel, typeDesc);
            long schemaId = getCreateSchema(schemaLabel, schemaDesc);

            SQLiteStatement stmt = db.prepare("INSERT into TypeGuesses VALUES(?, ?, ?, ?)");
            try {
              stmt.bind(1, fileId).bind(2, typeId).bind(3, schemaId).bind(4, score);
              stmt.step();
            } finally {
              stmt.dispose();
            }
          }
          return null;
        }
      }).complete();
    return fileId;
  }

  /**
   * Try to describe the contents of the given file
   */
  DataDescriptor describeData(File f) throws IOException {
    return formatAnalyzer.describeData(f);
  }
  
  ///////////////////////////////////////////////////
  // ACCESSORS FOR SETS OF OBJECTS
  ///////////////////////////////////////////////////
  /**
   * <code>getFidUnderPath</code> returns the files under the given path prefix
   */
  static String subpathFilesQuery = "SELECT fid from Files WHERE path LIKE ?";
  public List<Long> getFidUnderPath(final String pathPrefix) throws SQLiteException {
    List<Long> finalResults = dbQueue.execute(new SQLiteJob<List<Long>>() {
        protected List<Long> job(SQLiteConnection db) throws SQLiteException {
          List<Long> results = new ArrayList<Long>();          
          SQLiteStatement stmt = db.prepare(subpathFilesQuery);
          try {
            stmt.bind(1, pathPrefix + "%");
            while (stmt.step()) {
              long resultId = stmt.columnLong(0);
              results.add(resultId);
            }
            return results;
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
    return finalResults;
  }

  /**
   * <code>getSchemaSummaries</code> returns an instance of SchemaSummary
   * for each unique schema in the database.
   */
  static String schemaInfoQuery = "SELECT schemaid FROM Schemas";    
  public List<SchemaSummary> getSchemaSummaries() {
    return dbQueue.execute(new SQLiteJob<List<SchemaSummary>>() {
        protected List<SchemaSummary> job(SQLiteConnection db) throws SQLiteException {
          List<SchemaSummary> output = new ArrayList<SchemaSummary>();          
          SQLiteStatement stmt = db.prepare(schemaInfoQuery);
          
          try {
            while (stmt.step()) {
              long schemaId = stmt.columnLong(0);
              output.add(new SchemaSummary(FSAnalyzer.this, schemaId));
            }
          } catch (SQLiteException se) {
            se.printStackTrace();
          } finally {
            stmt.dispose();
          }
          return output;
        }}).complete();
  }

  /**
   * <code>getFileSummaries</code> returns an instance of FileSummary
   * for each unique schema in the database.
   */
  static String fileInfoQuery = "SELECT fid FROM Files";    
  public List<FileSummary> getFileSummaries() {
    return dbQueue.execute(new SQLiteJob<List<FileSummary>>() {
        protected List<FileSummary> job(SQLiteConnection db) throws SQLiteException {
          List<FileSummary> output = new ArrayList<FileSummary>();          
          SQLiteStatement stmt = db.prepare(fileInfoQuery);
          
          try {
            while (stmt.step()) {
              long fid = stmt.columnLong(0);
              output.add(new FileSummary(FSAnalyzer.this, fid));
            }
          } catch (SQLiteException se) {
            se.printStackTrace();
          } finally {
            stmt.dispose();
          }
          return output;
        }}).complete();
  }

  /**
   * <code>getCrawlSummaries</code> returns a list of the historical crawl info
   */
  static String crawlInfoQuery = "SELECT crawlid, lastexamined FROM Crawls";    
  public List<CrawlSummary> getCrawlSummaries() {
    return dbQueue.execute(new SQLiteJob<List<CrawlSummary>>() {
        protected List<CrawlSummary> job(SQLiteConnection db) throws SQLiteException {
          List<CrawlSummary> output = new ArrayList<CrawlSummary>();
          SQLiteStatement stmt = db.prepare(crawlInfoQuery);
          try {
            while (stmt.step()) {
              long cid = stmt.columnLong(0);
              String lexamined = stmt.columnString(1);
              output.add(new CrawlSummary(FSAnalyzer.this, cid, lexamined));
            }
          } catch (SQLiteException se) {
            se.printStackTrace();
          } finally {
            stmt.dispose();
          }
          return output;
        }}).complete();
  }
  
  /**
   * <code>getTypeSummaries</code> returns an instance of TypeSummary
   * for each unique type in the database.
   */
  static String typeInfoQuery = "SELECT typeid FROM Types";    
  public List<TypeSummary> getTypeSummaries() {
    return dbQueue.execute(new SQLiteJob<List<TypeSummary>>() {
        protected List<TypeSummary> job(SQLiteConnection db) throws SQLiteException {
          List<TypeSummary> output = new ArrayList<TypeSummary>();          
          SQLiteStatement stmt = db.prepare(typeInfoQuery);
          
          try {
            while (stmt.step()) {
              long typeid = stmt.columnLong(0);
              output.add(new TypeSummary(FSAnalyzer.this, typeid));
            }
          } catch (SQLiteException se) {
            se.printStackTrace();
          } finally {
            stmt.dispose();
          }
          return output;
        }}).complete();
  }
  
  ///////////////////////////////////////////
  // ACCESSORS FOR INDIVIDUAL OBJECTS
  ///////////////////////////////////////////
  /**
   * Read a property's value
   */
  public String getConfigProperty(final String propertyName) {
    return dbQueue.execute(new SQLiteJob<String>() {
        protected String job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT property FROM Configs WHERE propertyname=?");
          try {
            stmt.bind(1, propertyName);
            if (stmt.step()) {
              return stmt.columnString(0);
            } else {
              return null;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  /**
   * Write a property
   */
  public void setConfigProperty(final String propertyName, final String property) {
    if (property == null) {
      deleteConfigProperty(propertyName);
    } else {
      dbQueue.execute(new SQLiteJob<Object>() {
          protected Object job(SQLiteConnection db) throws SQLiteException {
            SQLiteStatement stmt = db.prepare("REPLACE into Configs VALUES(?, ?)");
            try {
              stmt.bind(1, propertyName);
              stmt.bind(2, property);            
              stmt.step();
            } finally {
              stmt.dispose();
            }
            return null;
          }
        }).complete();
    }
  }

  /**
   * Delete a property
   */
  public void deleteConfigProperty(final String propertyName) {
    dbQueue.execute(new SQLiteJob<Object>() {
        protected Object job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("DELETE from Configs WHERE propertyname=?");
          try {
            stmt.bind(1, propertyName);
            stmt.step();
          } finally {
            stmt.dispose();
          }
          return null;
        }
      }).complete();
  }
  
  /**
   * Grab details on a specific file.
   */
  public FileSummaryData getFileSummaryData(final long fid) {
    return dbQueue.execute(new SQLiteJob<FileSummaryData>() {
        protected FileSummaryData job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT crawlid, fname, owner, size, modified, path from Files WHERE fid = ?");
          try {
            stmt.bind(1, fid);
            if (stmt.step()) {
              return new FileSummaryData(fid, stmt.columnLong(0), stmt.columnString(1), stmt.columnString(2), stmt.columnLong(3), stmt.columnString(4), stmt.columnString(5));
            } else {
              return null;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  /**
   * Grab details on a schema.
   */
  public SchemaSummaryData getSchemaSummaryData(final long schemaid) {
    return dbQueue.execute(new SQLiteJob<SchemaSummaryData>() {
        protected SchemaSummaryData job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT schemalabel, schemadescriptor FROM Schemas WHERE schemaid = ?");
          try {
            stmt.bind(1, schemaid);
            if (stmt.step()) {
              return new SchemaSummaryData(schemaid, stmt.columnString(0), stmt.columnString(1));
            } else {
              return null;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  /**
   * Grab details on a type.
   */
  public TypeSummaryData getTypeSummaryData(final long typeid) {
    return dbQueue.execute(new SQLiteJob<TypeSummaryData>() {
        protected TypeSummaryData job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT typelabel, typedescriptor FROM Types WHERE typeid = ?");
          try {
            stmt.bind(1, typeid);
            if (stmt.step()) {
              return new TypeSummaryData(typeid, stmt.columnString(0), stmt.columnString(1));
            } else {
              return null;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  /**
   * Grab details on a crawl.
   */
  public String getCrawlLastExamined(final long crawlid) {
    return dbQueue.execute(new SQLiteJob<String>() {
        protected String job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT lastexamined FROM Crawls WHERE crawlid = ?");
          try {
            stmt.bind(1, crawlid);
            if (stmt.step()) {
              return stmt.columnString(0);
            } else {
              return null;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  
  ///////////////////////////////////////////
  // Get type guesses
  ///////////////////////////////////////////
  static String typeGuessQueryForFile = "SELECT fid, typeid, schemaid, score FROM TypeGuesses WHERE fid = ?";
  static String typeGuessQueryForSchema = "SELECT fid, typeid, schemaid, score FROM TypeGuesses WHERE schemaid = ?";
  static String typeGuessQueryForType = "SELECT fid, typeid, schemaid, score FROM TypeGuesses WHERE typeid = ?";    
  public List<TypeGuessSummary> getTypeGuessesForFile(final long fid) {
    return getTypeGuesses(typeGuessQueryForFile, fid);
  }
  public List<TypeGuessSummary> getTypeGuessesForSchema(final long schemaid) {
    return getTypeGuesses(typeGuessQueryForSchema, schemaid);
  }
  public List<TypeGuessSummary> getTypeGuessesForType(final long typeid) {
    return getTypeGuesses(typeGuessQueryForType, typeid);
  }
  List<TypeGuessSummary> getTypeGuesses(final String queryStr, final long idval) {
    return dbQueue.execute(new SQLiteJob<List<TypeGuessSummary>>() {
        protected List<TypeGuessSummary> job(SQLiteConnection db) throws SQLiteException {
          List<TypeGuessSummary> outputList = new ArrayList<TypeGuessSummary>();
          SQLiteStatement stmt = db.prepare(queryStr);
          try {
            stmt.bind(1, idval);
            while (stmt.step()) {
              outputList.add(new TypeGuessSummary(FSAnalyzer.this, stmt.columnLong(0), stmt.columnLong(1), stmt.columnLong(2), stmt.columnDouble(3)));
            }
          } finally {
            stmt.dispose();
          }
          return outputList;
        }
      }).complete();
  }

  ////////////////////////////////////////
  // Initialize and close an instance of FSAnalyzer
  ////////////////////////////////////////
  SQLiteConnection db;
  SQLiteQueue dbQueue;
  FormatAnalyzer formatAnalyzer;
  
  /**
   * Inits (and optionally creates) a new <code>FSAnalyzer</code> instance.
   */
  public FSAnalyzer(File metadataStore, File schemaDir) throws IOException, SQLiteException {
    boolean isNew = false;
    metadataStore = metadataStore.getCanonicalFile();
    if (! metadataStore.exists()) {
      isNew = true;
    }
    this.dbQueue = new SQLiteQueue(metadataStore);
    this.dbQueue.start();

    if (isNew) {
      createTables();
    }
    //this.formatAnalyzer = new FormatAnalyzer(schemaDir);
  }

  void close() throws IOException, SQLiteException, InterruptedException {
    this.dbQueue.stop(true).join();
  }
}