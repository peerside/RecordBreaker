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
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.ArrayList;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import com.almworks.sqlite4java.SQLiteException;

/***********************************************************
 * FSCrawler crawls a filesystem and stuffs the results into
 * an FSAnalyzer's store.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 ***********************************************************/
public class FSCrawler {
  final static int INFINITE_CRAWL_DEPTH = -1;
  
  static SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  Hashtable<Long, Thread> pendingCrawls = new Hashtable<Long, Thread>();
  Hashtable<Long, CrawlRuntimeStatus> crawlStatusInfo = new Hashtable<Long, CrawlRuntimeStatus>();
  FSAnalyzer analyzer;
  FileSystem fs;

  /**
   * Needs an analyzer to work
   */
  public FSCrawler(FSAnalyzer analyzer) {
    this.analyzer = analyzer;
    this.fs = null;
  }

  /**
   * Traverse an entire region of the filesystem, analyzing files.
   * This code should:
   * a) Navigate the directory hierarchy
   * b) Run analysis code to figure out the file details
   * c) Invoke addSingleFile() appropriately.
   */
  protected void recursiveCrawlBuildList(FileSystem fs, Path p, int subdirDepth, long crawlId, List<Path> todoFileList, List<Path> todoDirList) throws IOException {
    FileStatus fstatus = fs.getFileStatus(p);
    if (! fstatus.isDir()) {
      todoFileList.add(p);
    } else {
      if (subdirDepth > 0 || subdirDepth < 0) {
        todoDirList.add(p);
        Path paths[] = new Path[1];
        paths[0] = p;
        for (FileStatus subfilestatus: fs.listStatus(p)) {
          Path subfile = subfilestatus.getPath();
          recursiveCrawlBuildList(fs, subfile, subdirDepth-1, crawlId, todoFileList, todoDirList);
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
  public synchronized boolean getStartNonblockingCrawl(final URI fsURI) {
    try {
      final int subdirDepth = INFINITE_CRAWL_DEPTH;
      long fsId = analyzer.getCreateFilesystem(fsURI, true);    
      if (fsId < 0) {
        return false;
      }
      final FileSystem fs = FileSystem.get(fsURI, new Configuration());
      final Path startDir = fs.makeQualified(new Path(fsURI.getPath()));

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
                List<Path> todoFileList = new ArrayList<Path>();
                List<Path> todoDirList = new ArrayList<Path>();
                recursiveCrawlBuildList(fs, startDir, subdirDepth, crawlid, todoFileList, todoDirList);

                // Get the files to process
                TreeSet<String> observedFilenames = new TreeSet<String>();
                for (Path p: analyzer.getFilesForCrawl(crawlid)) {
                  observedFilenames.add(p.toString());
                }
                for (Iterator<Path> it = todoFileList.iterator(); it.hasNext(); ) {
                  Path p = it.next();
                  if (observedFilenames.contains(p.toString())) {
                    it.remove();
                  }
                }

                // Get the dirs to process
                TreeSet<String> observedDirnames = new TreeSet<String>();
                for (Path p: analyzer.getDirsForCrawl(crawlid)) {
                  observedDirnames.add(p.toString());
                }
                for (Iterator<Path> it = todoDirList.iterator(); it.hasNext(); ) {
                  Path p = it.next();
                  if (observedDirnames.contains(p.toString())) {
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
                for (Path p: todoDirList) {
                  try {
                    analyzer.addSingleFile(fs, p, crawlid);
                  } catch (IOException iex) {
                    iex.printStackTrace();
                  }
                }
                for (Path p: todoFileList) {
                  synchronized (crawlStatusInfo) {
                    CrawlRuntimeStatus cstatus = crawlStatusInfo.get(crawlid);
                    cstatus.setMessage("Processing file " + p.toString());
                  }
                  try {
                    analyzer.addSingleFile(fs, p, crawlid);
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
    } catch (Exception iex) {
      iex.printStackTrace();
    }
    return false;
  }

  /**
   * Is there an ongoing (running) crawl for the given filesystem?
   */
  public CrawlRuntimeStatus isCrawlOngoing(URI fsURI) {
    long fsId = analyzer.getCreateFilesystem(fsURI, false);
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
  protected boolean waitForOngoingCrawl(URI fsURI, boolean shouldKill) {
    long fsId = analyzer.getCreateFilesystem(fsURI, false);
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

  public void killOngoingCrawl(URI fsURI) {
    long fsId = analyzer.getCreateFilesystem(fsURI, false);
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
  public boolean blockingCrawl(URI fsURI) throws IOException, SQLiteException {
    boolean crawlStarted = getStartNonblockingCrawl(fsURI);
    if (crawlStarted) {
      waitForOngoingCrawl(fsURI, false);
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
        crawler.blockingCrawl(new URI("file://" + crawlTarget));
      } else if ("--test".equals(op)) {
        List<SchemaSummary> summaryList = fsa.getSchemaSummaries();
        System.err.println("Schema summary list has " + summaryList.size() + " entries");
      }
    } finally {
      fsa.close();
    }
  }
}