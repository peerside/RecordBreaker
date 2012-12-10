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

import java.net.URI;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import java.util.Date;
import java.util.Random;
import java.util.ArrayList;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.conf.Configuration;

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
  static FSAnalyzer fsaInstance;
  public static FSAnalyzer getInstance() {
    return fsaInstance;
  }
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
  static String CREATE_TABLE_FILES = "CREATE TABLE Files(fid integer primary key autoincrement, isDir string, crawlid integer, fname varchar(256), owner varchar(16), groupowner varchar(16), permissions varchar(32), size integer, modified date, path varchar(256), foreign key(crawlid) references Crawls(crawlid));";
  static String CREATE_TABLE_TYPES = "CREATE TABLE Types(typeid integer primary key autoincrement, typelabel varchar(64));";
  static String CREATE_TABLE_TYPE_GUESSES = "CREATE TABLE TypeGuesses(fid integer, typeid integer, foreign key(fid) references Files(fid), foreign key(typeid) references Types(typeid));";
  static String CREATE_TABLE_SCHEMAS = "CREATE TABLE Schemas(schemaid integer primary key autoincrement, schemarepr varchar(1024), schemasrcdescription varchar(32), schemapayload blob);";
  static String CREATE_TABLE_GUESSES = "CREATE TABLE SchemaGuesses(fid integer, schemaid integer, foreign key(fid) references Files(fid), foreign key(schemaid) references Schemas(schemaid));";
  void createTables() throws SQLiteException {
    dbQueue.execute(new SQLiteJob<Object>() {
        protected Object job(SQLiteConnection db) throws SQLiteException {
          try {
            db.exec(CREATE_TABLE_CONFIG);
            db.exec(CREATE_TABLE_FILESYSTEM);                        
            db.exec(CREATE_TABLE_CRAWL);
            db.exec(CREATE_TABLE_FILES);    
            db.exec(CREATE_TABLE_TYPES);
            db.exec(CREATE_TABLE_TYPE_GUESSES);
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
  public long getCreateFilesystem(final URI fsuri, boolean canCreate) {
    // REMIND -- must check to make sure FS is valid before accepting it.
    // (E.g., for HDFS see if we can contact it)
    
    long fsid = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT fsid FROM Filesystems WHERE fsname = ?");
          try {
            stmt.bind(1, fsuri.toString());
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
              stmt.bind(1, fsuri.toString());
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

  public FileSystem getFS() {
    String uriStr = getConfigProperty("fsuri");
    if (uriStr == null) {
      return null;
    }
    try {
      URI uri = new URI(uriStr);
      return FileSystem.get(uri, new Configuration());
    } catch (IOException iex) {
      return null;
    } catch (URISyntaxException use) {
      return null;
    }
  }
  
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
  long getCreateType(final String typeLabel) throws SQLiteException {
    long typeid = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT typeid FROM Types WHERE typelabel = ?");
          try {
            stmt.bind(1, typeLabel);
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
          SQLiteStatement stmt = db.prepare("INSERT into Types VALUES(null, ?)");
          try {
            stmt.bind(1, typeLabel);
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
  long getCreateSchema(SchemaDescriptor sd) throws SQLiteException {
    final String schemaIdentifier = (sd == null) ? "" : sd.getSchemaIdentifier();
    final String schemaDesc = (sd == null) ? "no schema" : sd.getSchemaSourceDescription();
    final byte[] payload = (sd == null) ? new byte[0] : ((GenericSchemaDescriptor) sd).getPayload();
    long schemaid = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          final SQLiteStatement stmt = db.prepare("SELECT schemaid FROM Schemas WHERE schemarepr = ? AND schemasrcdescription = ?");
          try {
            stmt.bind(1, schemaIdentifier).bind(2, schemaDesc);
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
          final SQLiteStatement stmt = db.prepare("INSERT into Schemas VALUES(null, ?, ?, ?)");
          try {
            stmt.bind(1, schemaIdentifier).bind(2, schemaDesc).bind(3, payload);
            stmt.step();
            return db.getLastInsertId();      
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
  }

  /**
   * Add a single brand-new file to the system.  Parse it, obtain structure, etc, if needed.
   */
  void addSingleFile(FileSystem fs, Path insertFile, long crawlId) throws IOException {
    FileStatus fstatus = fs.getFileStatus(insertFile);
    addFileMetadata(fstatus, crawlId);
    final boolean isDir = fstatus.isDir();

    if (! isDir) {
      final List<Long> typeGuesses = new ArrayList<Long>();
      DataDescriptor descriptor = formatAnalyzer.describeData(fs, insertFile);
      List<SchemaDescriptor> schemas = null;
      try {
        schemas = descriptor.getSchemaDescriptor();

        if (schemas == null || schemas.size() == 0) {
          typeGuesses.add(getCreateType(descriptor.getFileTypeIdentifier()));
          typeGuesses.add(getSingleFileSummary(descriptor.getFilename().toString()).getFid());
          typeGuesses.add(getCreateSchema(null));
        } else {
          for (SchemaDescriptor sd: schemas) {
            typeGuesses.add(getCreateType(descriptor.getFileTypeIdentifier()));
            typeGuesses.add(getSingleFileSummary(descriptor.getFilename().toString()).getFid());
            typeGuesses.add(getCreateSchema(sd));
          }
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }

      dbQueue.execute(new SQLiteJob<Object>() {
          protected Long job(SQLiteConnection db) throws SQLiteException {
            for (int i = 0; i < typeGuesses.size(); i+=3) {
              long typeId = typeGuesses.get(i);
              long fileId = typeGuesses.get(i+1);              
              long schemaId = typeGuesses.get(i+2);            

              SQLiteStatement stmt = db.prepare("INSERT into TypeGuesses VALUES(?, ?)");
              try {
                stmt.bind(1, fileId).bind(2, typeId);
                stmt.step();
              } finally {
                stmt.dispose();
              }
            }
            return null;
          }
        }).complete();

      dbQueue.execute(new SQLiteJob<Object>() {
          protected Long job(SQLiteConnection db) throws SQLiteException {
            for (int i = 0; i < typeGuesses.size(); i+=3) {
              long typeId = typeGuesses.get(i);
              long fileId = typeGuesses.get(i+1);              
              long schemaId = typeGuesses.get(i+2);            

              SQLiteStatement stmt = db.prepare("INSERT into SchemaGuesses VALUES(?, ?)");
              try {
                stmt.bind(1, fileId).bind(2, schemaId);
                stmt.step();
              } finally {
                stmt.dispose();
              }
            }
            return null;
          }
        }).complete();
    }
  }

  /**
   * <code>addFileMetadata</code> stores the pathname, size, owner, etc.
   */
  void addFileMetadata(final FileStatus fstatus, final long crawlId) {
    // Compute strings to represent file metadata
    Path insertFile = fstatus.getPath(); 
    final boolean isDir = fstatus.isDir();
    FsPermission fsp = fstatus.getPermission();
    final String permissions = (isDir ? "d" : "-") + fsp.getUserAction().SYMBOL + fsp.getGroupAction().SYMBOL + fsp.getOtherAction().SYMBOL;

    // Compute formal pathname representation
    String fnameString = null;
    String parentPathString = null;
    if (isDir && insertFile.getParent() == null) {
      parentPathString = "";
      fnameString = insertFile.toString();
    } else {
      fnameString = insertFile.getName();
      parentPathString = insertFile.getParent().toString();

      // REMIND --- mjc --- If we want to modify the Files table s.t. it does
      // not contain the filesystem prefix, then this would be the place to do it.

      if (! parentPathString.endsWith("/")) {
        parentPathString = parentPathString + "/";
      }
    }
    final String parentPath = parentPathString;
    final String fName = fnameString;
    final long fileId = dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("INSERT into Files VALUES(null, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
          try {
            stmt.bind(1, isDir ? "True" : "False").bind(2, crawlId).bind(3, fName).bind(4, fstatus.getOwner()).bind(5, fstatus.getGroup()).bind(6, permissions).bind(7, fstatus.getLen()).bind(8, fileDateFormat.format(new Date(fstatus.getModificationTime()))).bind(9, parentPath);
            stmt.step();
            return db.getLastInsertId();
          } finally {
            stmt.dispose();
          }
        }
      }).complete();
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
          SQLiteStatement stmt = db.prepare("SELECT schemarepr, schemasrcdescription FROM Schemas WHERE schemaid = ?");
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
  public List<FileSummary> getFileSummariesInDir(final boolean isDir, final String prefix) {
    return dbQueue.execute(new SQLiteJob<List<FileSummary>>() {
        protected List<FileSummary> job(SQLiteConnection db) throws SQLiteException {
          List<FileSummary> output = new ArrayList<FileSummary>();
          SQLiteStatement stmt;
          if (prefix == null) {
            stmt = db.prepare(fileInfoQueryWithoutPrefix);
            stmt.bind(1, isDir ? "True" : "False");            
          } else {
            stmt = db.prepare(fileInfoQueryWithPrefix);
            String prefixStr = prefix;
            if (! prefixStr.endsWith("/")) {
              prefixStr += "/";
            }
            stmt.bind(1, isDir ? "True" : "False").bind(2, prefixStr);            
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

  static String singletonFileInfoQuery = "SELECT fid FROM Files WHERE path||fname = ?";  
  public FileSummary getSingleFileSummary(final String fullName) {
    return dbQueue.execute(new SQLiteJob<FileSummary>() {
        protected FileSummary job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare(singletonFileInfoQuery);
          stmt.bind(1, fullName);            
          try {
            if (stmt.step()) {
              long fid = stmt.columnLong(0);
              return new FileSummary(FSAnalyzer.this, fid);
            }
          } catch (SQLiteException se) {
            se.printStackTrace();
          } finally {
            stmt.dispose();
          }
          return null;
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
    final FileSystem fs = getFS();    
    return dbQueue.execute(new SQLiteJob<FileSummaryData>() {
        protected FileSummaryData job(SQLiteConnection db) throws SQLiteException {
          FileSummaryData fsd = null;
          boolean isDir = false;
          long crawlid = 0L;
          String fname = null;
          String owner = null;
          String groupowner = null;
          String permissions = null;
          long size = 0L;
          String modified = null;
          String path = null;
          String identifier = null;          

          SQLiteStatement stmt = db.prepare("SELECT isDir, crawlid, fname, owner, groupowner, permissions, size, modified, path FROM Files WHERE Files.fid = ?");
          try {
            stmt.bind(1, fid);
            if (stmt.step()) {
              isDir = "True".equals(stmt.columnString(0));
              crawlid = stmt.columnLong(1);
              fname = stmt.columnString(2);
              owner = stmt.columnString(3);
              groupowner = stmt.columnString(4);
              permissions = stmt.columnString(5);
              size = stmt.columnLong(6);
              modified = stmt.columnString(7);
              path = stmt.columnString(8);
            }
          } finally {
            stmt.dispose();
          }

          if (! isDir) {
            stmt = db.prepare("SELECT typelabel FROM Types, TypeGuesses WHERE TypeGuesses.fid = ? AND Types.typeid = TypeGuesses.typeid");
            try {
              stmt.bind(1, fid);
              if (stmt.step()) {
                identifier = stmt.columnString(0);
              }
            } finally {
              stmt.dispose();
            }
            
            stmt = db.prepare("SELECT Schemas.schemaid, Schemas.schemarepr, Schemas.schemasrcdescription, Schemas.schemapayload FROM Schemas, SchemaGuesses WHERE SchemaGuesses.fid = ? AND SchemaGuesses.schemaid = Schemas.schemaid");
            try {
              List<String> schemaReprs = new ArrayList<String>();
              List<String> schemaDescs = new ArrayList<String>();
              List<byte[]> schemaBlobs = new ArrayList<byte[]>();
              
              stmt.bind(1, fid);
              while (stmt.step()) {
                schemaReprs.add(stmt.columnString(1));
                schemaDescs.add(stmt.columnString(2));
                schemaBlobs.add(stmt.columnBlob(3));
              }

              try {
                DataDescriptor dd = formatAnalyzer.loadDataDescriptor(fs, new Path(path + fname), identifier, schemaReprs, schemaDescs, schemaBlobs);
                fsd = new FileSummaryData(true, fid, crawlid, fname, owner, groupowner, permissions, size, modified, path, dd);
              } catch (IOException iex) {
                iex.printStackTrace();
                return null;
              }
            } finally {
              stmt.dispose();
            }
          } else {
            fsd = new FileSummaryData(false, fid, crawlid, fname, owner, groupowner, permissions, size, modified, path, null);            
          }
          return fsd;
        }
      }).complete();
  }

  /**
   * Get the top-level directory from a given crawl
   */
  public Path getTopDir(final long crawlid)  {
    return dbQueue.execute(new SQLiteJob<Path>() {
        protected Path job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare("SELECT path, fname FROM Files WHERE crawlid = ? AND isDir = 'True' ORDER BY length(path||fname) ASC LIMIT 1");
          try {
            stmt.bind(1, crawlid);
            if (stmt.step()) {
              return new Path(stmt.columnString(0) + stmt.columnString(1));
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
  public List<FileSummary> getDirParents(final long crawlid, final String targetDirStr) {
    return dbQueue.execute(new SQLiteJob<List<FileSummary>>() {
        protected List<FileSummary> job(SQLiteConnection db) throws SQLiteException {
          List<FileSummary> output = new ArrayList<FileSummary>();
          SQLiteStatement stmt = db.prepare("select fid, path, fname from Files WHERE crawlid = ? AND length(?) > length(path||fname) AND isDir = 'True' AND replace(?, path||fname, '') LIKE '/%'");
          try {
            Path targetDir = new Path(targetDirStr);
            if (targetDir.getParent() != null) {
              stmt.bind(1, crawlid).bind(2, targetDir.toString()).bind(3, targetDir.toString());
              while (stmt.step()) {
                //Path p = new Path(stmt.columnString(0) + stmt.columnString(1));
                output.add(new FileSummary(FSAnalyzer.this, stmt.columnLong(0)));
              }
            }
          } finally {
            stmt.dispose();
          }
          return output;
        }
      }).complete();
  }

  /**
   * Get the childiren dirs for the given directory from a given crawl
   */
  public List<FileSummary> getDirChildren(final long crawlid, final String targetDir) {
    return dbQueue.execute(new SQLiteJob<List<FileSummary>>() {
        protected List<FileSummary> job(SQLiteConnection db) throws SQLiteException {
          List<FileSummary> output = new ArrayList<FileSummary>();
          SQLiteStatement stmt = db.prepare("SELECT DISTINCT fid AS fullpath FROM Files WHERE isDir = 'True' AND crawlid = ? AND path = ? ORDER BY fname ASC");
          try {
            String targetDirNormalizedStr = targetDir;
            if (! targetDirNormalizedStr.endsWith("/")) {
              targetDirNormalizedStr += "/";
            }
            stmt.bind(1, crawlid).bind(2, targetDirNormalizedStr);
            while (stmt.step()) {
              output.add(new FileSummary(FSAnalyzer.this, stmt.columnLong(0)));
            }
          } finally {
            stmt.dispose();
          }
          return output;
        }
      }).complete();
  }

  public InputStream getRawBytes(Path p) throws IOException {
    return getFS().open(p);
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
          SQLiteStatement stmt = db.prepare("SELECT typelabel FROM Types WHERE typeid = ?");
          try {
            stmt.bind(1, typeid);
            if (stmt.step()) {
              return new TypeSummaryData(typeid, stmt.columnString(0));
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
  static String typeGuessQueryForFile = "SELECT SchemaGuesses.fid, TypeGuesses.typeid, SchemaGuesses.schemaid FROM TypeGuesses, SchemaGuesses WHERE TypeGuesses.fid = ? AND TypeGuesses.fid = SchemaGuesses.fid";
  static String typeGuessQueryForSchema = "SELECT SchemaGuesses.fid, TypeGuesses.typeid, SchemaGuesses.schemaid FROM TypeGuesses, SchemaGuesses WHERE SchemaGuesses.schemaid = ? AND TypeGuesses.fid = SchemaGuesses.fid";
  static String typeGuessQueryForType = "SELECT SchemaGuesses.fid, TypeGuesses.typeid, SchemaGuesses.schemaid FROM TypeGuesses, SchemaGuesses WHERE TypeGuesses.typeid = ? AND TypeGuesses.fid = SchemaGuesses.fid";
  public List<TypeGuessSummary> getTypeGuessesForFile(final long fid) {
    return getTypeGuesses(typeGuessQueryForFile, fid);
  }
  public List<TypeGuessSummary> getTypeGuessesForSchema(final long schemaid) {
    return getTypeGuesses(typeGuessQueryForSchema, schemaid);
  }
  static String countFilesQueryForSchema = "SELECT COUNT(DISTINCT fid) FROM SchemaGuesses WHERE schemaid = ?";
  public long countFilesForSchema(final long schemaid) {
    return dbQueue.execute(new SQLiteJob<Long>() {
        protected Long job(SQLiteConnection db) throws SQLiteException {
          SQLiteStatement stmt = db.prepare(countFilesQueryForSchema);
          try {
            stmt.bind(1, schemaid);
            if (stmt.step()) {
              return stmt.columnLong(0);
            }
          } finally {
            stmt.dispose();
          }
          return -1L;
        }
      }).complete();
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
              outputList.add(new TypeGuessSummary(FSAnalyzer.this, stmt.columnLong(0), stmt.columnLong(1), stmt.columnLong(2)));
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
    FSAnalyzer.fsaInstance = this;
  }

  public void close() throws IOException, SQLiteException, InterruptedException {
    this.dbQueue.stop(true).join();
  }
}