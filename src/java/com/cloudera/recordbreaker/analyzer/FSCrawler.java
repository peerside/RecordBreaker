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
import java.util.Date;
import java.io.IOException;
import java.util.List;
import java.util.Hashtable;
import java.util.ArrayList;
import java.text.SimpleDateFormat;

import java.io.IOException;
import com.almworks.sqlite4java.SQLiteException;

/***********************************************************
 * FSCrawler crawls a filesystem and stuffs the results into
 * an FSAnalyzer's store.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 ***********************************************************/
public class FSCrawler {
  static SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  Hashtable<Long, Thread> pendingCrawls = new Hashtable<Long, Thread>();
  FSAnalyzer analyzer;

  /**
   * Needs an analyzer to work
   */
  public FSCrawler(FSAnalyzer analyzer) {
    this.analyzer = analyzer;
  }

  /**
   * <code>addFile</code> will insert a single file into the database.
   * This isn't the most efficient thing in the world; it would be better
   * to add a batch at a time.
   */
  protected void addSingleFile(File f, long crawlid) throws IOException {
    // REMIND --- need to add support for HDFS files here
    List<TypeGuess> tgs = new ArrayList<TypeGuess>();    

    DataDescriptor descriptor = analyzer.describeData(f);
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

    // Need to grab the owner of the file somehow!!!
    String owner = "tmpowner";
    Date dateModified = new Date(f.lastModified());    
    try {
      analyzer.insertIntoFiles(f.getName(), owner, f.length(), fileDateFormat.format(dateModified), f.getCanonicalFile().getParent(), crawlid, tgs);
    } catch (SQLiteException sle) {
      throw new IOException(sle.getMessage());
    }
  }
  
  /**
   * Traverse an entire region of the filesystem, analyzing files.
   * This code should:
   * a) Navigate the directory hierarchy
   * b) Run analysis code to figure out the file details
   * c) Invoke addSingleFile() appropriately.
   */
  protected void recursiveCrawl(File f, int subdirDepth, long crawlId) throws IOException {
    // REMIND --- need to add support for HDFS files here
    if (f.isDirectory()) {
      for (File subfile: f.listFiles()) {
        if (subfile.isFile()) {
          addSingleFile(subfile, crawlId);
        }
      }
      if (subdirDepth > 0) {
        for (File subfile: f.listFiles()) {
          if (! subfile.isFile()) {
            recursiveCrawl(subfile, subdirDepth-1, crawlId);
          }
        }
      }
    } else {
      addSingleFile(f, crawlId);
    }
  }

  /**
   * <code>startNonblockingCrawl</code> goes through a given filesystem.  It returns immediately,
   * and does not wait for the crawl to complete.
   */
  public long startNonblockingCrawl(final File startDir, final int subdirDepth, final String fsname) throws IOException, SQLiteException {
    long tmpCrawlId = 0;
    synchronized (pendingCrawls) {
      long fsid = analyzer.getCreateFilesystem(fsname);
      final long crawlid = analyzer.getNewOrPendingCrawl(fsid);
      tmpCrawlId = crawlid;

      Thread pendingThread = pendingCrawls.get(crawlid);
      if (pendingThread == null) {
        Thread t = new Thread() {
            public void run() {
              try {
                recursiveCrawl(startDir, subdirDepth, crawlid);
              } catch (IOException iex) {
                iex.printStackTrace();
              }
            }
          };
        pendingCrawls.put(crawlid, t);
        t.start();
      }
    }
    return tmpCrawlId;
  }

  /**
   * waitForCrawl() will block until the given crawl is complete.  If the crawl
   * is unknown, then it will return -1.
   */
  public long waitForCrawl(long crawlid) throws IOException, SQLiteException {
    synchronized (pendingCrawls) {
      Thread t = pendingCrawls.get(crawlid);
      if (t == null) {
        return -1L;
      }
      try {
        t.join();
      } catch (InterruptedException iex) {
      }
      pendingCrawls.remove(crawlid);
    }
    analyzer.completeCrawl(crawlid);
    return crawlid;
  }

  /**
   * Kick off a crawl at the indicated directory and filesystem,
   * to the indicated depth.
   */
  public long blockingCrawl(File startDir, int subdirDepth, String fsname) throws IOException, SQLiteException {
    long crawlid = startNonblockingCrawl(startDir, subdirDepth, fsname);
    return waitForCrawl(crawlid);
  }

  ////////////////////////////////////////
  // Main()
  ////////////////////////////////////////
  public static void main(String argv[]) throws Exception {
    if (argv.length < 4) {
      System.err.println("Usage: FSCrawler <metadataStoreDir> <schemaDbDir> (--crawl <dir> <subdirdepth>)");
      return;
    }
    int i = 0;
    File metadataStoreDir = new File(argv[i++]).getCanonicalFile();
    File schemadbdir = new File(argv[i++]).getCanonicalFile();
    String op = argv[i++];
    FSAnalyzer fsa = new FSAnalyzer(metadataStoreDir, schemadbdir);

    try {
      if ("--crawl".equals(op)) {
        File dir = new File(argv[i++]).getCanonicalFile();
        int subdirDepth = Integer.parseInt(argv[i++]);
        FSCrawler crawler = new FSCrawler(fsa);
        crawler.blockingCrawl(dir, subdirDepth, "file://");
      } else if ("--test".equals(op)) {
        List<SchemaSummary> summaryList = fsa.getSchemaSummaries();
        System.err.println("Schema summary list has " + summaryList.size() + " entries");
      }
    } finally {
      fsa.close();
    }
  }
}