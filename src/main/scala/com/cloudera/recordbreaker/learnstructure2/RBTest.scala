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
import RBTypes._
import Parse._
import Infer._
import Rewrite._

object RBTest {
  def flattenAndName(ht: HigherType, parentName: String): List[(HigherType, String)] = {
    ht match {
      case a: HTStruct => a.value.foldLeft(List[(HigherType,String)]())((lhs, rhs) => lhs ++ flattenAndName(rhs, parentName + "." + a.getClass.getName))
      case b: HTUnion => b.value.foldLeft(List[(HigherType,String)]())((lhs, rhs) => lhs ++ flattenAndName(rhs, parentName + "." + b.getClass.getName))
      case c: HTBaseType => List((c, parentName + "." + c.value.getClass.getName))
    }
  }

  /**
   *  Test the basic file parsing features
   */
  def test1(): Unit = {
    val testFileDir = new java.io.File("src/samples/textdata")
    val testFiles = testFileDir.listFiles
    for (tf <- testFiles) {
      println(tf)
      val chunks = Parse.parseFile(tf.getCanonicalPath())
    }
  }

  /**
  def testOld(): Unit = {
    val css = parseFile("test3.txt")
    val testMode = true
    if (testMode) {
      val testCaseOrig = discover(css)
      val testCase1 = HTStruct(List(HTBaseType(PInt()), HTStruct(List())))
      val testCase2 = HTUnion(List(HTBaseType(PInt()), HTUnion(List())))
      val testCase3 = HTUnion(List(HTStruct(List(HTBaseType(PFloat()), HTBaseType(PInt()))),
                                   HTStruct(List(HTBaseType(PAlphanum()), HTBaseType(PInt())))))
      val testCase4 = HTStruct(List(HTBaseType(PStringConst("foo")),
                                    HTBaseType(PStringConst("foo")),
                                    HTBaseType(PInt())))
      val testCase = testCaseOrig

      println("Test case: " + testCase)
      val improvedCase = refineAll(testCase, css)
      println("Improved case: " + improvedCase)
      val flatlist = flattenAndName(improvedCase, "root")
      for (f <- flatlist) {
        val (ht, nme) = f
        println("Flat: " + nme, ht)
      }
    } else {
      val htOrig = discover(css)
      val htImproved = refineAll(htOrig, css)

      println("Original inferred structure: " + htOrig)
      println("Refined structure: " + htImproved)
    }
  }
   */


  /*********************************************
   * Test the structure rewrite rules
   ********************************************/
  /**
  def testRewriteRules(): Unit = {
    //
    // rewriteSingletons
    //
    val testRewriteRules1 = List(HTStruct(List(HTBaseType(PInt(10)))), // 1
                                 HTStruct(List()), // 2
                                 HTUnion(List(HTBaseType(PInt(10)))), // 3
                                 HTUnion(List())) // 4

    val testRewriteRules2 = List(HTStruct(List(HTBaseType(PInt(10)), HTBaseType(PFloat(4.0)), HTBaseType(PVoid()))), // 1
                                 HTStruct(List(HTBaseType(PInt(10)), HTBaseType(PFloat(4.0)), HTBaseType(PEmpty()))), // 2
                                 HTUnion(List(HTBaseType(PInt(10)), HTBaseType(PFloat(4.0)), HTBaseType(PVoid())))) // 3

    //
    // transformUniformStruct
    //
    val testRewriteRules3 = List(HTStruct(List(HTBaseType(PInt(1)), HTBaseType(PInt(1)), HTBaseType(PInt(1)))))

    //
    // commonUnionPrefix
    //
    val testRewriteRules4 = List(HTUnion(List(HTStruct(List(HTBaseType(PInt(1)), HTBaseType(PInt(10)))),
                                              HTStruct(List(HTBaseType(PString("foo")), HTBaseType(PInt(10)))))), // 1
                                 HTUnion(List(HTStruct(List(HTBaseType(PInt(1)), HTBaseType(PInt(10)))),
                                              HTBaseType(PInt(10)))),                                            // 2
                                 HTUnion(List(HTBaseType(PInt(10)),
                                              HTStruct(List(HTBaseType(PInt(1)), HTBaseType(PInt(10)))))))       // 3

    //
    // combineAdjacentStringConstants
    //
    val testRewriteRules5 = List(HTStruct(List(HTBaseType(PStringConst("foo")),
                                               HTBaseType(PStringConst("foo")),
                                               HTBaseType(PInt(10)))))
  }
   */
}
