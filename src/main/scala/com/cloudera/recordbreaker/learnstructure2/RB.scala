/*
 * Copyright (c) 2013-2014, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.recordbreaker.learnstructure2;

import scala.io.Source
import scala.math._
import scala.collection.mutable._
import com.cloudera.recordbreaker.learnstructure2.RBTypes._
import com.cloudera.recordbreaker.learnstructure2.Parse._
import com.cloudera.recordbreaker.learnstructure2.Infer._
import com.cloudera.recordbreaker.learnstructure2.Rewrite._

object RB {
  /***************************************************
   * generateTypeDesc() takes a file as input and generates a high-level type
   * description object.  This high-level description can be further edited by hand.
   * 
   * When applied to a raw file, the high-level type description can be used
   * to generate schema-conforming example tuples.
   *
   * The maxLinesToRead parameter allows the caller to limit the time spent
   * in this method.  
   * *************************************************/
  def generateTypeDesc(fname: String, maxLinesToRead: Int): HigherType = {
    return null
  }
}


