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

import java.util.*;

/***********************************************************
 * Describes what a live crawl is doing
 *************************************************************/
public class CrawlRuntimeStatus {
  public String msg;
  public int numDone;
  public int numToProcess;
  boolean shouldFinish;
  
  public CrawlRuntimeStatus(String msg) {
    this.msg = msg;
    this.numToProcess = 0;
    this.numDone = 0;
    this.shouldFinish = false;
  }
  public void setMessage(String msg) {
    this.msg = msg;
  }
  public String getMessage() {
    return this.msg;
  }
  public int getNumToProcess() {
    return numToProcess;
  }
  public int getNumDone() {
    return numDone;
  }
  public boolean shouldFinish() {
    return shouldFinish;
  }
  public void setShouldFinish(boolean shouldFinish) {
    this.shouldFinish = shouldFinish;
  }
  public void setNumToProcess(int numToProcess) {
    this.numToProcess = numToProcess;
  }
  public void setNumDone(int numDone) {
    this.numDone = numDone;
  }
  
}