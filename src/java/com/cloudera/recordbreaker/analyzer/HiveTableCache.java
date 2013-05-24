package com.cloudera.recordbreaker.analyzer;

import com.cloudera.recordbreaker.fisheye.FishEye;
import com.cloudera.recordbreaker.analyzer.FSAnalyzer;

import org.apache.hadoop.fs.Path;

/*************************************************************
 * <code>HiveTableCache</code> stores whether we've seen a
 * file in the Hive universe before.  If so, we tell what the
 * tablename is.
 *
 *************************************************************/
public class HiveTableCache {
  FSAnalyzer fsa;
  
  public HiveTableCache() {
    FishEye fe = FishEye.getInstance();
    this.fsa = fe.getAnalyzer();
  }

  public String get(Path p) {
    return fsa.checkHiveSupport(p);
  }

  public void put(Path p, String tablename) {
    fsa.addHiveSupport(p, tablename);
  }
}