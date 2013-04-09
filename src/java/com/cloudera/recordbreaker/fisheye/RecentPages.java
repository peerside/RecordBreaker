/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.recordbreaker.fisheye;

import java.util.List;
import java.util.ArrayList;

/*****************************************************************
 * Class that serves as singleton for storing recent page views.
 * The page-specific classes log requests here; we can then
 * return listings of the most recently-seen items
 *
 * @author Mike Cafarella 
 *****************************************************************/
public class RecentPages {
  static final int MAX_RECENT = 10;
  static List<String> recentLabels = new ArrayList<String>();
  static List<String> recentURLs = new ArrayList<String>();
  
  public static void addView(String label, String url) {
    recentLabels.add(0, label);
    recentURLs.add(0, url);
    if (recentLabels.size() > MAX_RECENT) {
      recentLabels.remove(recentLabels.size()-1);
      recentURLs.remove(recentURLs.size()-1);      
    }
  }
  public static List<String> getRecentK(int k) {
    List<String> results = new ArrayList<String>();
    for (int i = 0; i < k; i++) {
      results.add(recentLabels.get(i));
      results.add(recentURLs.get(i));      
    }
    return results;
  }
}
