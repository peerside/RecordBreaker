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
package com.cloudera.recordbreaker.analyzer;

import java.util.*;

/****************************************************
 * Describe class <code>PageHistory</code> here.
 *
 * @author "Michael Cafarella" <mjc@lofie.local>
 * @version 1.0
 * @since 1.0
 *****************************************************/
public class PageHistory {
  static PageHistory ph;
  public static PageHistory get() {
    if (ph == null) {
      ph = new PageHistory(5);
    }
    return ph;
  }

  int queueSize;
  List<FileSummary> history;
  public PageHistory(int qs) {
    this.queueSize = qs;
    this.history = new ArrayList<FileSummary>();
  }
  public List<FileSummary> getRecentHistory() {
    return history;
  }
  public void visitNewPage(FileSummary file) {
    if (history.size() > queueSize) {
      history.remove(history.size()-1);
    }
    history.add(0, file);
  }
}