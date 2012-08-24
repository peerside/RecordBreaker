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
import java.util.TreeSet;
import java.util.Iterator;
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
  final static int MAX_ANALYSIS_LINES = 400;
  final static int MAX_CRAWL_DEPTH = 5;
  
  static SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  Hashtable<Long, Thread> pendingCrawls = new Hashtable<Long, Thread>();
  Hashtable<Long, CrawlRuntimeStatus> crawlStatusInfo = new Hashtable<Long, CrawlRuntimeStatus>();
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
  protected void addSingleFile(File f, boolean isDir, long crawlid) throws IOException {
    // REMIND --- need to add support for HDFS files here
    List<TypeGuess> tgs = new ArrayList<TypeGuess>();    

    if (! isDir) {
      DataDescriptor descriptor = analyzer.describeData(f, MAX_ANALYSIS_LINES);
      try {
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
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    
    // Need to grab the owner of the file somehow!!!
    String owner = "tmpowner";
    Date dateModified = new Date(f.lastModified());    
    try {
      analyzer.insertIntoFiles(f, isDir, owner, fileDateFormat.format(dateModified), crawlid, tgs);
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
  protected void recursiveCrawlBuildList(File f, int subdirDepth, long crawlId, List<File> todoFileList, List<File> todoDirList) throws IOException {
    // REMIND --- need to add support for HDFS files here
    f = f.getCanonicalFile();
    if (! f.isDirectory()) {
      todoFileList.add(f);
    } else {
      if (subdirDepth > 0) {
        todoDirList.add(f);        
        for (File subfile: f.listFiles()) {
          subfile = subfile.getCanonicalFile();
          recursiveCrawlBuildList(subfile, subdirDepth-1, crawlId, todoFileList, todoDirList);
        }
      }
    }
  }

  /**
   * <code>getStartNonblockingCrawl</code> traverses a given filesystem.  It returns immediately
   * and does not wait for the crawl to complete.
   * If the crawl is created or is already ongoing, it returns true.
   * If the crawl is not currently going and cannot start, it returns false. 
   */
  public synchronized boolean getStartNonblockingCrawl(final String fsUrl) {
    try {
      final int subdirDepth = MAX_CRAWL_DEPTH;
      long fsId = analyzer.getCreateFilesystem(fsUrl, true);    
      if (fsId < 0) {
        return false;
      }
      if (fsUrl.startsWith("file://")) {
        String fsnameRoot = fsUrl.substring("file://".length());
        if (! fsnameRoot.startsWith("/")) {
          fsnameRoot = "/" + fsnameRoot;
        }
        File fsRootDir = new File(fsnameRoot);
        final File startDir = fsRootDir.getCanonicalFile();
      
        final long crawlid = analyzer.getCreatePendingCrawl(fsId, true);

        Thread pendingThread = pendingCrawls.get(crawlid);
        if (pendingThread == null) {
          Thread t = new Thread() {
              public void run() {
                try {
                  synchronized (pendingCrawls) {
                    pendingCrawls.put(crawlid, this);
                  }
                  synchronized (crawlStatusInfo) {
                    crawlStatusInfo.put(crawlid, new CrawlRuntimeStatus("Initializing crawl"));
                  }
                  // Build the file and dir-level todo lists
                  List<File> todoFileList = new ArrayList<File>();
                  List<File> todoDirList = new ArrayList<File>();
                  recursiveCrawlBuildList(startDir, subdirDepth, crawlid, todoFileList, todoDirList);

                  // Get the files to process
                  TreeSet<String> observedFilenames = new TreeSet<String>();
                  for (File f: analyzer.getFilesForCrawl(crawlid)) {
                    observedFilenames.add(f.getCanonicalPath());
                  }
                  for (Iterator<File> it = todoFileList.iterator(); it.hasNext(); ) {
                    File f = it.next();
                    if (observedFilenames.contains(f.getCanonicalPath())) {
                      it.remove();
                    }
                  }

                  // Get the dirs to process
                  TreeSet<String> observedDirnames = new TreeSet<String>();
                  for (File f: analyzer.getDirsForCrawl(crawlid)) {
                    observedDirnames.add(f.getCanonicalPath());
                  }
                  for (Iterator<File> it = todoDirList.iterator(); it.hasNext(); ) {
                    File f = it.next();
                    if (observedDirnames.contains(f.getCanonicalPath())) {
                      it.remove();
                    }
                  }
                  
                  synchronized (crawlStatusInfo) {
                    CrawlRuntimeStatus cstatus = crawlStatusInfo.get(crawlid);
                    cstatus.setMessage("Processing files");
                    cstatus.setNumToProcess(todoFileList.size());
                    cstatus.setNumDone(0);
                  }

                  int numDone = 0;
                  for (File f: todoDirList) {
                    try {
                      addSingleFile(f, true, crawlid);
                    } catch (Exception iex) {
                      iex.printStackTrace();
                    }
                  }
                  for (File f: todoFileList) {
                    synchronized (crawlStatusInfo) {
                      CrawlRuntimeStatus cstatus = crawlStatusInfo.get(crawlid);
                      cstatus.setMessage("Processing file " + f.getPath());
                    }
                    try {
                      addSingleFile(f, false, crawlid);
                    } catch (Exception iex) {
                      iex.printStackTrace();
                    }
                    numDone++;
                    synchronized (crawlStatusInfo) {
                      CrawlRuntimeStatus cstatus = crawlStatusInfo.get(crawlid);
                      cstatus.setNumDone(numDone);
                      if (cstatus.shouldFinish()) {
                        break;
                      }
                    }
                  }
                } catch (IOException iex) {
                  iex.printStackTrace();
                } finally {
                  try {
                    synchronized (pendingCrawls) {
                      pendingCrawls.remove(crawlid);
                      analyzer.completeCrawl(crawlid);
                    }
                  } catch (SQLiteException sle) {
                  }
                }
              }
            };
          t.start();
        }
        return true;
      }
    } catch (IOException iex) {
    }
    return false;
  }

  /**
   * Is there an ongoing (running) crawl for the given filesystem?
   */
  public CrawlRuntimeStatus isCrawlOngoing(String fsUrl) {
    long fsId = analyzer.getCreateFilesystem(fsUrl, false);
    if (fsId < 0) {
      return null;
    }
    synchronized (pendingCrawls) {
      final long crawlid = analyzer.getCreatePendingCrawl(fsId, false);
      Thread pendingThread = pendingCrawls.get(crawlid);
      if (pendingThread != null && pendingThread.isAlive()) {
        synchronized (crawlStatusInfo) {
          return crawlStatusInfo.get(crawlid);
        }
      }
      return null;
    }
  }

  /**
   * waitForCrawl() will block until the given crawl is complete.  If there
   * is an ongoing crawl that completes, it will return true.
   * If there was no ongoing crawl, it will return false.
   */
  protected boolean waitForOngoingCrawl(String fsUrl, boolean shouldKill) {
    long fsId = analyzer.getCreateFilesystem(fsUrl, false);
    if (fsId < 0) {
      return false;
    }
    synchronized (pendingCrawls) {
      final long crawlid = analyzer.getCreatePendingCrawl(fsId, false);
      if (crawlid < 0) {
        return false;
      }
      if (shouldKill) {
        synchronized (crawlStatusInfo) {
          CrawlRuntimeStatus cstatus = crawlStatusInfo.get(crawlid);
          cstatus.setShouldFinish(true);
        }
      }
      Thread pendingThread = pendingCrawls.get(crawlid);
      if (pendingThread != null) {
        try {
          pendingThread.join();
        } catch (InterruptedException iex) {
        }
      }
      return true;
    }
  }

  public void killOngoingCrawl(String fsUrl) {
    long fsId = analyzer.getCreateFilesystem(fsUrl, false);
    if (fsId >= 0) {
      synchronized (pendingCrawls) {
        final long crawlid = analyzer.getCreatePendingCrawl(fsId, false);
        synchronized (crawlStatusInfo) {
          CrawlRuntimeStatus cstatus = crawlStatusInfo.get(crawlid);
          if (cstatus != null) {
            cstatus.setShouldFinish(true);
          }
        }
      }
    }
  }

  /**
   * Kick off a crawl at the indicated directory and filesystem,
   * to the indicated depth.
   */
  public boolean blockingCrawl(String fsUrl) throws IOException, SQLiteException {
    boolean crawlStarted = getStartNonblockingCrawl(fsUrl);
    if (crawlStarted) {
      waitForOngoingCrawl(fsUrl, false);
    }
    return crawlStarted;
  }

  ////////////////////////////////////////
  // Main()
  ////////////////////////////////////////
  public static void main(String argv[]) throws Exception {
    if (argv.length < 4) {
      System.err.println("Usage: FSCrawler <metadataStoreDir> <schemaDbDir> (--crawl <dir>)");
      return;
    }
    int i = 0;
    File metadataStoreDir = new File(argv[i++]).getCanonicalFile();
    File schemadbdir = new File(argv[i++]).getCanonicalFile();
    String op = argv[i++];
    FSAnalyzer fsa = new FSAnalyzer(metadataStoreDir, schemadbdir);

    try {
      if ("--crawl".equals(op)) {
        File crawlTarget = new File(argv[i++]).getCanonicalFile();
        System.err.println("About to crawl " + crawlTarget);
        FSCrawler crawler = new FSCrawler(fsa);
        crawler.blockingCrawl("file://" + crawlTarget);
      } else if ("--test".equals(op)) {
        List<SchemaSummary> summaryList = fsa.getSchemaSummaries();
        System.err.println("Schema summary list has " + summaryList.size() + " entries");
      }
    } finally {
      fsa.close();
    }
  }
}