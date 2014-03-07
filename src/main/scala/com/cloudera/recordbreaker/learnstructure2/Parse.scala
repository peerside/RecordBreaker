/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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
import java.util.regex.Pattern
import RBTypes._

/***********************************
 * Parse text file
 ***********************************/
object Parse {
  def parseFile(fname: String): Chunks = {
    process(Source.fromFile(fname, "UTF-8"))
  }
  def parseString(str: String): Chunks = {
    process(Source.fromString(str))
  }
  def process(src: Source): Chunks = {
    def findMeta(lhs:POther with ParsedValue[String], rhs:POther with ParsedValue[String], l:List[BaseType]):List[BaseType] = {
      val (left,toprocess) = l.span(x => x != lhs)
      val (center, rest) = toprocess.slice(1,toprocess.length).span(x => x != rhs)
      if (center.length > 0) {
        left ++ List(PMetaToken(lhs, center, rhs)) ++ findMeta(lhs, rhs, rest.slice(1,rest.length))
      } else {
        l
      }
    }
    def findMetaTokens(a:List[BaseType]) = {
      findMeta(new POther() with ParsedValue[String] {val parsedValue="("},
               new POther() with ParsedValue[String] {val parsedValue=")"},
               findMeta(new POther() with ParsedValue[String] {val parsedValue="["},
                        new POther() with ParsedValue[String] {val parsedValue="]"},
                        findMeta(new POther() with ParsedValue[String] {val parsedValue="<"},
                                 new POther() with ParsedValue[String] {val parsedValue=">"},
                                 a)))
    }
    val strset = src.getLines()
    var css = List[List[BaseType]]()
    for (l <- strset.filter(x=>x.trim().length > 0)) {
      var m = l.replaceAllLiterally("<", " < ").replaceAllLiterally(">", " > ").replaceAllLiterally("(", " ( ").replaceAllLiterally(")", " ) ").replaceAllLiterally("[", " [ ").replaceAllLiterally("]", " ] ")
      val tokens = m.split(" ").map(_.trim()).filter(x=>x.length>0)
      var cs = List[BaseType]()

      for (t <- tokens) {
        val c = t match {
            case x if t.forall(_.isDigit) => new PInt() with ParsedValue[Int] {val parsedValue=x.toInt}
            case y if {try{Some(y.toDouble); true} catch {case _:Throwable => false}} => new PFloat() with ParsedValue[Double] {val parsedValue=y.toDouble}
            case a if (a.length() == 1 && Pattern.matches("\\p{Punct}", a)) => new POther() with ParsedValue[String] {val parsedValue=a}
            case u => new PAlphanum() with ParsedValue[String] {val parsedValue=u}
          }
        cs = cs:+c
      }
      cs = findMetaTokens(cs)
      css = css:+cs
    }
    return css
  }
}


