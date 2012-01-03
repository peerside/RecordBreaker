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
import java.util.List;
import java.util.Date;
import java.util.Random;
import java.util.ArrayList;

import com.almworks.sqlite4java.SQLite;
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
  static String CREATE_TABLE_CRAWL = "CREATE TABLE Crawls(crawlid integer primary key autoincrement, lastexamined text);";  
  static String CREATE_TABLE_FILES = "CREATE TABLE Files(fid integer primary key autoincrement, crawlid integer, fname varchar(256), owner varchar(16), size integer, modified date, path varchar(256), foreign key(crawlid) references Crawls(crawlid));";
  static String CREATE_TABLE_TYPES = "CREATE TABLE Types(typeid integer primary key autoincrement, typelabel varchar(64), typedescriptor varchar(1024));";
  static String CREATE_TABLE_SCHEMAS = "CREATE TABLE Schemas(schemaid integer primary key autoincrement, schemalabel varchar(64), schemadescriptor varchar(1024));";
  static String CREATE_TABLE_GUESSES = "CREATE TABLE TypeGuesses(fid integer, crawlid integer, typeid integer, schemaid integer, double score, foreign key(fid) references Files(fid), foreign key(crawlid) references Crawls(crawlid), foreign key(typeid) references Types(typeid), foreign key(schemaid) references Schemas(schemaid));";
  void createTables() throws SQLiteException {
    db.exec(CREATE_TABLE_CRAWL);
    db.exec(CREATE_TABLE_FILES);    
    db.exec(CREATE_TABLE_TYPES);
    db.exec(CREATE_TABLE_SCHEMAS);
    db.exec(CREATE_TABLE_GUESSES);    
  }

  //
  // 2. Insert data
  //
  class TypeGuess {
    String typeLabel;
    String typeDesc;
    String schemaLabel;
    String schemaDesc;
    double score;
    
    public TypeGuess(String typeLabel, String typeDesc, String schemaLabel, String schemaDesc, double score) {
      this.typeLabel = typeLabel;
      this.typeDesc = typeDesc;
      this.schemaLabel = schemaLabel;
      this.schemaDesc = schemaDesc;
      this.score = score;
    }
    String getTypeLabel() {
      return typeLabel;
    }
    String getTypeDesc() {
      return typeDesc;
    }
    String getSchemaLabel() {
      return schemaLabel;
    }
    String getSchemaDesc() {
      return schemaDesc;
    }
    double getScore() {
      return score;
    }
  }

  /**
   * Helper fn <code>getCreateCrawl</code> returns the id of a specified Crawl in the Crawls table.
   * The row is created, if necessary.
   */
  long getCreateCrawl(String lastExamined) throws SQLiteException {
    SQLiteStatement stmt = db.prepare("SELECT crawlid from Crawls WHERE lastexamined = ?");
    try {
      stmt.bind(1, lastExamined);
      if (stmt.step()) {
        long resultId = stmt.columnLong(0);
        return resultId;
      }
    } finally {
      stmt.dispose();
    }
    // Time to insert
    stmt = db.prepare("INSERT into Crawls VALUES(null, ?)");
    try {
      stmt.bind(1, lastExamined);
      stmt.step();
      return db.getLastInsertId();
    } finally {
      stmt.dispose();
    }
  }

  /**
   * Helper fn <code>getCreateType</code> returns the id of a specified Type in the Types table.
   * The row is created, if necessary.
   */
  long getCreateType(String typeLabel, String typeDesc) throws SQLiteException {
    SQLiteStatement stmt = db.prepare("SELECT typeid FROM Types WHERE typelabel = ? AND typedescriptor = ?");
    try {
      stmt.bind(1, typeLabel).bind(2, typeDesc);
      if (stmt.step()) {
        long resultId = stmt.columnLong(0);
        return resultId;
      }
    } finally {
      stmt.dispose();
    }
    // Time to insert
    stmt = db.prepare("INSERT into Types VALUES(null, ?, ?)");
    try {
      stmt.bind(1, typeLabel).bind(2, typeDesc);
      stmt.step();
      return db.getLastInsertId();
    } finally {
      stmt.dispose();
    }
  }

  /**
   * Helper fn <code>getCreateSchema</code> returns the id of a specified Schema in the Schemas table.
   * The row is created, if necessary.
   */
  long getCreateSchema(String schemaLabel, String schemaDesc) throws SQLiteException {
    SQLiteStatement stmt = db.prepare("SELECT schemaid FROM Schemas WHERE schemalabel = ? AND schemadescriptor = ?");
    try {
      stmt.bind(1, schemaLabel).bind(2, schemaDesc);
      if (stmt.step()) {
        long resultId = stmt.columnLong(0);
        return resultId;
      }
    } finally {
      stmt.dispose();
    }
    // Time to insert
    stmt = db.prepare("INSERT into Schemas VALUES(null, ?, ?)");
    try {
      stmt.bind(1, schemaLabel).bind(2, schemaDesc);
      stmt.step();
      return db.getLastInsertId();      
    } finally {
      stmt.dispose();
    }
  }
    
  /**
   * Add a new object to the set of all known files.  This involves several tables.
   */
  long insertIntoFiles(String fname, String owner, long size, String timeDateStamp, String path, String lastExamined, List<TypeGuess> typeGuesses) throws SQLiteException {
    long crawlId = getCreateCrawl(lastExamined);
    
    long fileId = -1;
    SQLiteStatement stmt = db.prepare("INSERT into Files VALUES(null, ?, ?, ?, ?, ?, ?)");
    try {
      stmt.bind(1, crawlId).bind(2, fname).bind(3, owner).bind(4, size).bind(5, timeDateStamp).bind(6, path);
      stmt.step();
      fileId = db.getLastInsertId();
    } finally {
      stmt.dispose();
    }

    // Go through all the associated typeGuesses
    for (TypeGuess tg: typeGuesses) {
      String typeLabel = tg.getTypeLabel();
      String typeDesc = tg.getTypeDesc();
      String schemaLabel = tg.getSchemaLabel();
      String schemaDesc = tg.getSchemaDesc();
      double score = tg.getScore();

      long typeId = getCreateType(typeLabel, typeDesc);
      long schemaId = getCreateSchema(schemaLabel, schemaDesc);

      stmt = db.prepare("INSERT into TypeGuesses VALUES(?, ?, ?, ?, ?)");
      try {
        stmt.bind(1, fileId).bind(2, crawlId).bind(3, typeId).bind(4, schemaId).bind(5, score);
        stmt.step();
      } finally {
        stmt.dispose();
      }
    }
    return fileId;
  }
  
  //
  // 3. Analytics queries
  //
  // For unified file listing: select F.fname, T.typelabel, S.schemadescriptor from Schemas S, Files F, Types T, TypeGuesses TG where F.fid = TG.fid AND TG.schemaid = S.schemaid AND TG.typeid = T.typeid;
  //
  // For type statistics over entire db
  // select T.typelabel, COUNT(T.typelabel) from Files F, Types T, TypeGuesses TG where F.fid = TG.fid AND TG.typeid = T.typeid GROUP BY T.typelabel;
  //
  // For schema statistics over entire db
  // select S.schemalabel, COUNT(S.schemalabel) from Files F, Schemas S, TypeGuesses TG where F.fid = TG.fid AND TG.schemaid = S.schemaid GROUP BY S.schemalabel;
  //
  //
  static String subpathFilesQuery = "SELECT fid from Files WHERE path LIKE ?";
  List<Long> getFidUnderPath(String pathPrefix) throws SQLiteException {
    List<Long> results = new ArrayList<Long>();
    SQLiteStatement stmt = db.prepare(subpathFilesQuery);
    try {
      stmt.bind(1, pathPrefix + "%");
      while (stmt.step()) {
        long resultId = stmt.columnLong(0);
        results.add(resultId);
      }
    } finally {
      stmt.dispose();
    }
    return results;
  }
  
    
  File store;
  SQLiteConnection db;
  FormatAnalyzer formatAnalyzer;
  
  /**
   * Inits (and optionally creates) a new <code>FSAnalyzer</code> instance.
   */
  public FSAnalyzer(File store, File schemaDir) throws IOException, SQLiteException {
    boolean isNew = false;
    if (! store.exists()) {
      isNew = true;
    }
    this.db = new SQLiteConnection(store);
    db.open(true);

    if (isNew) {
      createTables();
    }

    this.formatAnalyzer = new FormatAnalyzer(schemaDir);
  }

  void close() throws IOException, SQLiteException {
    db.dispose();
  }

  /**
   * <code>addFile</code> will insert a single file into the database.
   * This isn't the most efficient thing in the world; it would be better
   * to add a batch at a time.
   */
  void addFile(File f, String crawlDate) throws IOException, SQLiteException {
    List<TypeGuess> tgs = new ArrayList<TypeGuess>();    

    DataDescriptor descriptor = formatAnalyzer.describeData(f);
    List<SchemaDescriptor> schemas = descriptor.getSchemaDescriptor();

    if (schemas == null || schemas.size() == 0) {
      tgs.add(new TypeGuess(descriptor.getFileTypeIdentifier(), descriptor.getFileTypeIdentifier(),
                            "no schema", "no schema", 1.0));
    } else {
      for (SchemaDescriptor sd: schemas) {
        tgs.add(new TypeGuess(descriptor.getFileTypeIdentifier(), descriptor.getFileTypeIdentifier(),
                              sd.getSchemaIdentifier(), sd.getSchemaSourceDescription(), 1.0));
      }
    }

    Date dateModified = new Date(f.lastModified());
    long id = insertIntoFiles(f.getName(), "mjc", f.length(), fileDateFormat.format(dateModified), f.getCanonicalFile().getParent(), crawlDate, tgs);
  }
  
  /**
   * Traverse an entire region of the filesystem, analyzing files.
   * This code should:
   * a) Navigate the directory hierarchy
   * b) Run analysis code to figure out the file details
   * c) Invoke addFile() appropriately.
   */
  void crawl(File f, int subdirDepth) throws IOException, SQLiteException {
    Date now = new Date(System.currentTimeMillis());
    String crawlDate = fileDateFormat.format(now);
    
    if (f.isDirectory()) {
      for (File subfile: f.listFiles()) {
        if (subfile.isFile()) {
          addFile(subfile, crawlDate);
        }
      }
      if (subdirDepth > 0) {
        for (File subfile: f.listFiles()) {
          if (! subfile.isFile()) {
            crawl(subfile, subdirDepth-1);
          }
        }
      }
    } else {
      addFile(f, crawlDate);
    }
  }

  void query(String q) throws SQLiteException {
    SQLite.loadLibrary();
    System.err.println("SQLite version: " + SQLite.getSQLiteVersion());
  }

  public static void main(String argv[]) throws IOException, SQLiteException {
    if (argv.length < 4) {
      System.err.println("Usage: FSAnalyzer <storedir> <schemaDbDir> (--crawl <dir> <subdirdepth> | --query q)");
      return;
    }
    int i = 0;
    File storedir = new File(argv[i++]).getCanonicalFile();
    File schemadbdir = new File(argv[i++]).getCanonicalFile();
    String op = argv[i++];
    FSAnalyzer fsa = new FSAnalyzer(storedir, schemadbdir);

    try {
      if ("--crawl".equals(op)) {
        File dir = new File(argv[i++]).getCanonicalFile();
        int subdirDepth = Integer.parseInt(argv[i++]);
        fsa.crawl(dir, subdirDepth);
      } else if ("--query".equals(op)) {
        fsa.query(argv[i]);
      }
    } finally {
      fsa.close();
    }
  }
}