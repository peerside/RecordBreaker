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
import RBTypes._

/***********************************
 * Information theoretic score and rewrite
 ***********************************/
object Rewrite {
  ///////////////////////////////////////////
  // Code to refine initial schema estimate
  ///////////////////////////////////////////

  /** refineAll() applies refinement rules to a given HigherType hierarchy and some Chunks
   *  It is the only public method in this package
   */
  def refineAll(orig: HigherType, input: Chunks) = {
    val dataIndependentRules = List(rewriteSingletons _, cleanupStructUnion _, transformUniformStruct _, commonUnionPostfix _, combineAdjacentStringConstants _)
    val round1 = refine(orig, dataIndependentRules, costEncoding)
    //val round2 = refine(round1, dataDependentRules, totalCost(input))
    //val round3 = refine(round2, dataIndependentRules)
    round1
  }

  /**
   * A single step in the refinement process
   */
   private def refine(orig: HigherType, rewriteRules:List[HigherType=>HigherType], costFn:HigherType=>Double): HigherType = {
    //println("Refining " + orig)
    val newVersion = orig match {
        case a: HTBaseType => a
        case b: HTStruct => HTStruct(b.value.map(x=> refine(x, rewriteRules, costFn)))
        case c: HTArray => HTArray(refine(c.value, rewriteRules, costFn))
        case d: HTUnion => HTUnion(d.value.map(x=> refine(x, rewriteRules, costFn)))
        case _ => orig
      }
    oneStep(newVersion, rewriteRules, costFn)
  }
  
  /** Take one step in the schema refinement process
   */
  private def oneStep(orig: HigherType, rewriteRules:List[HigherType=>HigherType], costFn:HigherType=>Double): HigherType = {
    val allRewrites = rewriteRules.map(r=> r(orig)).map(s=> (costFn(s), s))
    val bestRewrite = allRewrites.reduceLeft((x,y)=>if (x._1 < y._1) x else y)

    //println("Orig is " + orig + " with cost " + costFn(orig))
    //println("Best rewrite is " +bestRewrite._2 + " with cost " + bestRewrite._1)
    if (bestRewrite._1 < costFn(orig)) {
      oneStep(bestRewrite._2, rewriteRules, costFn)
    } else {
      orig
    }
  }

  ///////////////////////////////////////////
  // Schema cost estimation methods
  ///////////////////////////////////////////

  /** totalCost of applying an encoding to a dataset
   */
  private def totalCost(input: Chunks)(encoding: HigherType): Double = costEncoding(encoding) + costData(encoding, input)

  /** costData() computes a total cost value for an encoding and a dataset
   */
  private def costData(encoding:HigherType, input:Chunks):Double = {
    def costSingleChunk(encodingIn:HigherType, inputIn:Chunk):Double = {
      (encodingIn, inputIn) match {
        case (a:HTStruct,_) => {if (a.value.length == 0) (if (inputIn.length == 0) 0 else 999999) else a.value.zip(inputIn).map(x=>costSingleChunk(x._1, List(x._2))).sum}
        case (b:HTUnion,_) => {if (b.value.length == 0) (if (inputIn.length == 0) 0 else 999999) else b.value.map(x => costSingleChunk(x, inputIn)).min}
        // Eventually add in support for HTEnum and HTSwitch
        case (HTBaseType(bt), inputIn2) if (inputIn2.length == 1) => (bt, inputIn2.head) match {
          case (a1: PInt, elt1:PInt) => 32
          case (b1: PIntConst, elt1:PInt) => 0
          case (c1: PFloat, elt1:PFloat) => 32
          case (d1: PAlphanum, elt1:PAlphanum with ParsedValue[String]) => elt1.getValue().length
          case (e1: PString, elt1:PString with ParsedValue[String]) => elt1.getValue().length
          case (f1: PStringConst, elt1:PString) => 0
          case (g1: POther, elt1:POther with ParsedValue[String]) => elt1.getValue().length
          case _ => 999999
        }
        case (HTBaseType(bt), inList) if (inList.length == 0) => bt match {
          case h1: PEmpty => 0
          case h1: PVoid => 0
          case _ => 999999
        }
        case _ => 999999
      }
    }
    input.map(chunk => costSingleChunk(encoding, chunk)).sum
  }

  /** costEncoding costs just the encoding itself; is independent of the dataset
   */
  private def costEncoding(encoding: HigherType): Double = {
    encoding match {
      case a: HTStruct => log(24) + 1 + log(a.value.length+1) + a.value.map(costEncoding).sum
      case b: HTUnion => log(24) + 1 + log(b.value.length+1) + b.value.map(costEncoding).sum
      case c: HTArray => log(24) + costEncoding(c.value)
      case d: HTArrayFW => log(24) + log(d.size) + costEncoding(d.value)
      case _ => log(24) + 2
    }
  }


  ///////////////////////////////////////////
  // Data-independent rewrite rules
  ///////////////////////////////////////////
  private def rewriteSingletons(in: HigherType): HigherType = {
    in match {
      case a: HTStruct if (a.value.length == 1) => a.value.head
      case b: HTStruct if (b.value.length == 0) => HTBaseType(PEmpty())
      case c: HTUnion if (c.value.length == 1) => c.value.head
      case d: HTUnion if (d.value.length == 0) => HTBaseType(PVoid())
      case _ => in
    }
  }
  private def cleanupStructUnion(in: HigherType): HigherType = {
    in match {
      case a: HTStruct if (a.value.contains(PVoid())) => HTBaseType(PVoid())
      case b: HTStruct if (b.value.contains(HTBaseType(PEmpty()))) => HTStruct(b.value.filterNot(x => x == HTBaseType(PEmpty())))
      case c: HTUnion if (c.value.contains(HTBaseType(PVoid()))) => HTUnion(c.value.filterNot(x => x == HTBaseType(PVoid())))
      case _ => in
    }
  }
  private def transformUniformStruct(in: HigherType): HigherType = {
    in match {
      case a: HTStruct if ((a.value.length >= 3) && ((Set() ++ a.value).size == 1)) => HTArrayFW(a.value.head, a.value.length)
      case _ => in
    }
  }
  private def commonUnionPostfix(in: HigherType): HigherType = {
    in match {
      case a: HTUnion if (a.value.length == 2) => {
        val retval = (a.value(0), a.value(1)) match {
            case (l:HTStruct, r:HTStruct) if (l.value.last == r.value.last) => {println("YES!!!!"); HTStruct(List(HTUnion(List(HTStruct(l.value.dropRight(1)),
                                                                                                                               HTStruct(r.value.dropRight(1)))),
                                                                                                                  l.value.last))}
            case (l: HTStruct, r: HigherType) if (l.value.last == r) => HTStruct(List(HTOption(HTStruct(l.value.dropRight(1))),
                                                                                      l.value.last))
            case (l: HigherType, r: HTStruct) if (r.value.last == l) => HTStruct(List(HTOption(HTStruct(r.value.dropRight(1))),
                                                                                      r.value.last))
            case _ => a
          }
        retval
      }
      case _ => in
    }
  }

  private def combineAdjacentStringConstants(in: HigherType): HigherType = {
    def findAdjacent(soFar: List[HigherType], remainder: List[HigherType]): List[HigherType] = {
      if (remainder.length == 0) {
        soFar
      } else {
        remainder match {
          case HTBaseType(c1) :: HTBaseType(c2) :: rest if (c1.isInstanceOf[PStringConst] && c2.isInstanceOf[PStringConst]) =>
            findAdjacent(soFar :+ HTBaseType(PStringConst(c1.asInstanceOf[PStringConst].cval + c2.asInstanceOf[PStringConst].cval)), rest)
          case first :: rest => findAdjacent(soFar :+ first, rest)
          // REMIND -- mjc -- this last line seems like a bug. What about 'soFar'?
          case _ => remainder
        }
      }
    }

    in match {
      case a: HTStruct => HTStruct(findAdjacent(List[HigherType](), a.value))
      case _ => in
    }
  }

  ///////////////////////////////////////////
  // Data-dependent rewrite rules
  ///////////////////////////////////////////
  //def rewriteWithConstants(in:HigherType, input: Chunks):HigherType = {
  //  input.map(chunk => LabeledHigherType root = in.parse(chunk))
  //}
}

