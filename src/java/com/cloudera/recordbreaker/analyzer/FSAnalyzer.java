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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

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
  static String CREATE_TABLE_CRAWL = "CREATE TABLE Crawls(crawlid integer primary key autoincrement, crawlstarted date, crawlfinished date, inprogress text, fsid integer, foreign key(fsid) references Filesystems(fsid));";
  static String CREATE_TABLE_FILESYSTEM = "CREATE TABLE Filesystems(fsid integer primary key autoincrement, fsname text);";    
  static String CREATE_TABLE_FILES = "CREATE TABLE Files(fid integer primary key autoincrement, isDir string, crawlid integer, fname varchar(256), owner varchar(16), size integer, modified date, path varchar(256), foreign key(crawlid) references Crawls(crawlid));";
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
  public long getCreateFilesystem(final String fsname, boolean canCreate) {
    // REMIND -- must check to make sure FS is valid before accepting it.
    // (E.g., for HDFS see if we can contact it)
    
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
    if (canCreate) {
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
    } else {
      return -1L;
    }
  };
  
  /**
   * Helper fn <code>getNewOrPendingCrawl</code> returns the id of a Crawl for the specified filesystem.
   * If a crawl is pending, that one is returned.
   * If no crawl is pending, a new one is created.
   */
  public long getCreatePendingCrawl(final long fsid, boolean shouldCreate)  {
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
      if (shouldCreate) {
        return dbQueue.execute(new SQLiteJob<Long>() {
            protected Long job(SQLiteConnection db) throws SQLiteException {
              Date now = new Date(System.currentTimeMillis());
              String dateCreated = fileDateFormat.format(now);
              String syntheticDateFinished = fileDateFormat.format(new Date(0));
              String inprogress = "True";
              SQLiteStatement stmt = db.prepare("INSERT into Crawls VALUES(null, ?, ?, ?, ?)");
              try {
                stmt.bind(1, dateCreated).bind(2, syntheticDateFinished).bind(3, inprogress).bind(4, fsid);
                stmt.step();
                return db.getLastInsertId();
              } finally {
                stmt.dispose();
              }
            }
          }).complete();
      }
    return -1L;
  }
  
  public void completeCrawl(final long crawlid) throws SQLiteException {
    dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("UPDATE Crawls SET inprogress='False', crawlfinished=? WHERE crawlid = ?");
          try {
            Date now = new Date(System.currentTimeMillis());
            String dateFinished = fileDateFormat.format(now);
            stmt.bind(1, dateFinished).bind(2, crawlid);
            if (stmt.step()) {
              return crawlid;
            } else {
              return -1L;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  public long getLatestCompleteCrawl(final long fsid) {
    return dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT crawlid from Crawls WHERE fsid = ? AND inprogress = 'False' ORDER BY crawlid DESC LIMIT 1");
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
  long insertIntoFiles(FileSystem fs, Path insertFile, final long crawlId, final List<TypeGuess> typeGuesses) throws SQLiteException, IOException {
    FileStatus fstatus = fs.getFileStatus(insertFile);
    final String timeDateStamp = fileDateFormat.format(new Date(fstatus.getModificationTime()));
    final String owner = fstatus.getOwner();
    final boolean isDir = fstatus.isDir();
    final long size = fstatus.getLen();
    
    final String parentPath = insertFile.getParent().toString();
    final String fName = insertFile.getName();
    final long fileId = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("INSERT into Files VALUES(null, ?, ?, ?, ?, ?, ?, ?)");
          try {
            stmt.bind(1, isDir ? "True" : "False").bind(2, crawlId).bind(3, fName).bind(4, owner).bind(5, size).bind(6, timeDateStamp).bind(7, parentPath);
            stmt.step();
            return db.getLastInsertId();
          } finally {
            stmt.dispose();
          }
        }
      }).complete();

    // Go through all the associated typeGuesses
    final List<Long> typeIds = new ArrayList<Long>();
    final List<Long> schemaIds = new ArrayList<Long>();    
    for (TypeGuess tg: typeGuesses) {
      String typeLabel = tg.getTypeLabel();
      String typeDesc = tg.getTypeDesc();
      String schemaLabel = tg.getSchemaLabel();
      String schemaDesc = tg.getSchemaDesc();
      typeIds.add(getCreateType(typeLabel, typeDesc));
      schemaIds.add(getCreateSchema(schemaLabel, schemaDesc));
    }
    dbQueue.execute(new SQLiteJob<Object>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          for (int i = 0; i < typeGuesses.size(); i++) {
            TypeGuess tg = typeGuesses.get(i);
            long typeId = typeIds.get(i);
            long schemaId = schemaIds.get(i);            
            double score = tg.getScore();

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
  DataDescriptor describeData(FileSystem fs, Path p, int maxLines) throws IOException {
    return formatAnalyzer.describeData(fs, p, maxLines);
  }
  
  ///////////////////////////////////////////////////
  // ACCESSORS FOR SCHEMAS
  ///////////////////////////////////////////////////
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

  ///////////////////////////////////////////////////
  // ACCESSORS FOR FILES
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
   * <code>getFileSummaries</code> returns an instance of FileSummary
   * for each unique schema in the database.
   */
  static String fileInfoQueryWithoutPrefix = "SELECT fid FROM Files WHERE isDir = ?";
  static String fileInfoQueryWithPrefix = "SELECT fid FROM Files WHERE isDir = ? AND path = ?";
  public List<FileSummary> getFileSummaries(final boolean isDir, final String prefix) {
    return dbQueue.execute(new SQLiteJob<List<FileSummary>>() {
        protected List<FileSummary> job(SQLiteConnection db) throws SQLiteException {
          List<FileSummary> output = new ArrayList<FileSummary>();
          SQLiteStatement stmt;
          if (prefix == null) {
            stmt = db.prepare(fileInfoQueryWithoutPrefix);
            stmt.bind(1, isDir ? "True" : "False");            
          } else {
            stmt = db.prepare(fileInfoQueryWithPrefix);
            stmt.bind(1, isDir ? "True" : "False").bind(2, prefix);            
          }
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
   * <code>getFilesForCrawl()</code> returns all the seen files for a given crawlid
   */
  public List<Path> getFilesForCrawl(final long crawlid) {
    return getFileEntriesForCrawl(crawlid, "False");
  }
  public List<Path> getDirsForCrawl(final long crawlid) {
    return getFileEntriesForCrawl(crawlid, "True");    
  }
  static String filenameForCrawlQuery = "SELECT path, fname FROM Files WHERE crawlid=? AND isDir = ?";        
  private List<Path> getFileEntriesForCrawl(final long crawlid, final String isDir) {
    return dbQueue.execute(new SQLiteJob<List<Path>>() {
        protected List<Path> job(SQLiteConnection db) throws SQLiteException {
          List<Path> output = new ArrayList<Path>();          
          SQLiteStatement stmt = db.prepare(filenameForCrawlQuery);
          try {
            stmt.bind(1, crawlid).bind(2, isDir);
            while (stmt.step()) {
              output.add(new Path(stmt.columnString(0), stmt.columnString(1)));
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
   * Get the top-level directory from a given crawl
   */
  public String getTopDir(final long crawlid)  {
    return dbQueue.execute(new SQLiteJob<String>() {
        protected String job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT path||'/'||fname FROM Files WHERE crawlid = ? AND isDir = 'True' ORDER BY length(path||'/'||fname) ASC LIMIT 1");
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

  /**
   * Get the parents for the given directory from a given crawl
   */
  public List<Path> getDirParents(final long crawlid, final String targetDir) {
    return dbQueue.execute(new SQLiteJob<List<Path>>() {
        protected List<Path> job(SQLiteConnection db) throws SQLiteException {
          List<Path> output = new ArrayList<Path>();
          SQLiteStatement stmt = db.prepare("select path||'/'||fname from Files WHERE crawlid = ? AND length(?) > length(path||'/'||fname) AND isDir = 'True' AND length(replace(?, path||'/'||fname, '')) < length(?)");
          try {
            stmt.bind(1, crawlid).bind(2, targetDir).bind(3, targetDir).bind(4, targetDir);
            while (stmt.step()) {
              output.add(new Path(stmt.columnString(0)));
            }
          } finally {
            stmt.dispose();
          }
          return output;
        }
      }).complete();
  }

  /**
   * Get the parents for the given directory from a given crawl
   */
  public List<Path> getDirChildren(final long crawlid, final String targetDir) {
    return dbQueue.execute(new SQLiteJob<List<Path>>() {
        protected List<Path> job(SQLiteConnection db) throws SQLiteException {
          List<Path> output = new ArrayList<Path>();
          SQLiteStatement stmt = db.prepare("SELECT DISTINCT path||'/'||fname AS fullpath FROM Files WHERE isDir = 'True' AND crawlid = ? AND path = ? ORDER BY fname ASC");
          try {
            stmt.bind(1, crawlid).bind(2, targetDir);
            while (stmt.step()) {
              output.add(new Path(stmt.columnString(0)));
            }
          } finally {
            stmt.dispose();
          }
          return output;
        }
      }).complete();
  }
  
  ///////////////////////////////////////////////////
  // ACCESSORS FOR CRAWLS
  ///////////////////////////////////////////////////
  /**
   * <code>getCrawlSummaries</code> returns a list of the historical crawl info
   */
  static String crawlInfoQuery = "SELECT crawlid, crawlstarted, crawlfinished, inprogress, fsid FROM Crawls";    
  public List<CrawlSummary> getCrawlSummaries() {
    return dbQueue.execute(new SQLiteJob<List<CrawlSummary>>() {
        protected List<CrawlSummary> job(SQLiteConnection db) throws SQLiteException {
          List<CrawlSummary> output = new ArrayList<CrawlSummary>();
          SQLiteStatement stmt = db.prepare(crawlInfoQuery);
          try {
            while (stmt.step()) {
              long cid = stmt.columnLong(0);
              String started = stmt.columnString(1);
              String finished = stmt.columnString(2);
              String inprogress = stmt.columnString(3);
              long fsid = stmt.columnLong(4);                            
              output.add(new CrawlSummary(FSAnalyzer.this, cid, started, finished, "True".equals(inprogress), fsid));
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
   * Grab details on a crawl.
   */
  public CrawlSummary getCrawlSummaryData(final long crawlid) {
    return dbQueue.execute(new SQLiteJob<CrawlSummary>() {
        protected CrawlSummary job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT crawlstarted, crawlfinished, inprogress, fsid FROM Crawls WHERE crawlid = ?");
          try {
            stmt.bind(1, crawlid);
            if (stmt.step()) {
              return new CrawlSummary(FSAnalyzer.this, crawlid, stmt.columnString(0), stmt.columnString(1), "True".equals(stmt.columnString(2)), stmt.columnLong(3));
            } else {
              return null;
            }
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  ///////////////////////////////////////////////////
  // ACCESSORS FOR TYPES
  ///////////////////////////////////////////////////
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
  
  ///////////////////////////////////////////
  // ACCESSORS FOR CONFIG INFO
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
    this.formatAnalyzer = new FormatAnalyzer(schemaDir);
  }

  public void close() throws IOException, SQLiteException, InterruptedException {
    this.dbQueue.stop(true).join();
  }
}