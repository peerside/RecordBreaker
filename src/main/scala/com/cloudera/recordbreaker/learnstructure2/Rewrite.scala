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

  /**
   * refineAll() applies refinement rules to a given HigherType hierarchy and some Chunks.
   * 
   * It is not optional: many inferred HigherType structures will translate to illegal Avro
   * structures before they go through the refinment process
   */
  val dataIndependentRules = List(removeNestedUnions _, rewriteSingletons _, cleanupStructUnion _, transformUniformStruct _, commonUnionPostfix _, filterNoops _, combineAdjacentStringConstants _, combineHomogeneousArrays _)

  def refineAll(orig: HigherType, input: Chunks) = {
    val dataDependentRules = List()
    val round1 = refine(orig, dataIndependentRules, costEncoding)
    val round2 = refine(round1, dataDependentRules, totalCost(input))
    val round3 = refine(round2, dataIndependentRules, costEncoding)

    val round4 = ensureRootIsStruct(round3)
    HigherType.resetUsageStatistics(round4)
    round4
  }

  /**
   * dropComponent() removes a targeted UNION branch from a given HigherType structure.
   * The intention here is that the user picks a UNION branch that processes only a small
   * and unimportant portion of the overall input file; its removal simplifies the structure
   * but has minimal impact on the parser's actual operation
   */
  def dropComponent(orig: HigherType, labels: List[String]): HigherType = {
    val dropComponents = List(dropComponent(labels) _)
    val origScore = costEncoding(orig)
    val round1 = refine(orig, dropComponents, costEncoding)
    val newScore = costEncoding(round1)
    if (newScore >= origScore) {
      throw CustomException("No valid UNION-child found for removal")
    }
    val round2 = refine(round1, dataIndependentRules, costEncoding)
    HigherType.resetUsageStatistics(round2)
    round2
  }

  /**
   * automaticDropComponent() uses an algorithm to figure out how many and which
   * UNION branches to remove.  It repeatedly removes the lowest-impact branches
   * that still permit it to satisfy the user-given parse fraction requirement
   */
  def automaticDropComponent(orig: HigherType, input: ParsedChunks, minParseFraction: Double): HigherType = {
    def computeMissingFraction(ht: HigherType, inc: ParsedChunks): Double = {
      HigherType.resetUsageStatistics(ht)
      Processor.process(inc, ht)
      HigherType.missingCount / HigherType.denomCount.toDouble
    }

    // Very un-scala-like.  What is the scala idiom for the below case?
    var curStructure = orig    
    HigherType.resetUsageStatistics(curStructure)
    Processor.process(input, curStructure)
    while (true) {
      val bestTupleToRemove = HigherType.getLowestCountUnionBranch(curStructure)
      if (bestTupleToRemove._2.isEmpty) {
        return curStructure
      }
      val refinedStructure = Rewrite.dropComponent(curStructure, List(bestTupleToRemove._2.get))
      if ((1-computeMissingFraction(refinedStructure, input)) < minParseFraction) {
        return curStructure
      }
      curStructure = refinedStructure
    }
    return curStructure
  }

  /**
   * A single step in the refinement process
   */
   private def refine(orig: HigherType, rewriteRules:List[HigherType=>HigherType], costFn:HigherType=>Double): HigherType = {
    //println("Refining " + orig)
    val newVersion = orig match {
        case a: HTBaseType => a
        case b: HTStruct => {
          val n = HTStruct(b.value.map(x=> refine(x, rewriteRules, costFn)))
          n.fc = b.fc
          n
        }
        case c: HTArray => {
          val n = HTArray(refine(c.value, rewriteRules, costFn))
          n.fc = c.fc
          n
        }
        case d: HTUnion => {
          val n = HTUnion(d.value.map(x=> refine(x, rewriteRules, costFn)))
          n.fc = d.fc
          n
        }
        case e: HTOption => {
          val n = HTOption(refine(e.value, rewriteRules, costFn))
          n.fc = e.fc
          n
        }
        case _ => orig
      }
    oneStep(newVersion, rewriteRules, costFn)
   }
  
  /** Take one step in the schema refinement process
   */
  private def oneStep(orig: HigherType, rewriteRules:List[HigherType=>HigherType], costFn:HigherType=>Double): HigherType = {
    if (rewriteRules.length == 0) {
      orig
    } else {
      val allRewrites = rewriteRules.map(r=> r(orig)).map(s=> (costFn(s), s))
      val bestRewrite = allRewrites.reduceLeft((x,y)=>if (x._1 < y._1) x else y)

      /**
       println()
       println("Orig is " + orig + " with cost " + costFn(orig))
       for (rw <- allRewrites) {
       println("  Rewrite " + rw._2 + " costs " + rw._1)
       }
       println()
       println("Best rewrite is " +bestRewrite._2 + " with cost " + bestRewrite._1)
       */
      if (bestRewrite._1 < costFn(orig)) {
        oneStep(bestRewrite._2, rewriteRules, costFn)
      } else {
        orig
      }
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
    val numHTs = 8
    val numPVs = 10
    encoding match {
      case a: HTStruct => log(numHTs) + log(numPVs) + 1 + log(1 + a.value.length) + a.value.map(costEncoding).sum
      case b: HTUnion => log(numHTs) + log(numPVs) + 1 + log(1 + b.value.length) + b.value.map(costEncoding).sum
      case c: HTArray => log(numHTs) + log(numPVs) + costEncoding(c.value)
      case d: HTArrayFW => log(numHTs) + log(numPVs) + 1 + log(1 + d.size) + costEncoding(d.value)
      case e: HTBaseType => log(numHTs) + log(numPVs)
      case f: HTOption => log(numHTs) + log(numPVs) + costEncoding(f.value)
      case _ => 999999
    }
  }


  ///////////////////////////////////////////
  // Data-independent rewrite rules
  ///////////////////////////////////////////
  private def filterNoops(in: HigherType): HigherType = {
    in match {
      case a: HTStruct if (a.value.exists(elt => elt match {
                                            case xa: HTNoop => true
                                            case _ => false
                                          })) => HTStruct(a.value.filter(elt => elt match {
                                                                           case ya: HTNoop => false
                                                                           case _ => true
                                                                         }))
      case b: HTUnion if (b.value.exists(elt => elt match {
                                           case xb: HTNoop => true
                                           case _ => false
                                         })) => HTOption(HTUnion(b.value.filter(elt => elt match {
                                                                                   case yb: HTNoop => false
                                                                                   case _ => true
                                                                                 })))
      case _ => in
    }
  }

  private def dropComponent(labels: List[String])(in: HigherType): HigherType = {
    in match {
      case a: HTUnion if (a.value.exists(x=>labels.contains(x.name())) &&
                            a.value.length > 1) => {
        HTUnion(a.value.filterNot(x=>labels.contains(x.name())))
      }
      case _ => in
    }
  }

  private def removeNestedUnions(in: HigherType): HigherType = {
    in match {
      // If the union has all unions has children, then merge them.
      case a: HTUnion if (a.value.exists(x=> x match {
                                           case xa: HTUnion => true
                                           case _ => false
                                         })) => HTUnion(a.value.foldLeft(List[HigherType]())((listSoFar, topLevelUnionBranch) =>
                                                          topLevelUnionBranch match {
                                                            case xa: HTUnion => listSoFar ++ xa.value
                                                            case _ => listSoFar :+ topLevelUnionBranch
                                                          }))
      case _ => in
    }
  }

  private def ensureRootIsStruct(root: HigherType): HigherType = {
    root match {
      case a: HTStruct => a
      case _ => HTStruct(List(root))
    }
  }

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
            case (l:HTStruct, r:HTStruct) if (l.value.last == r.value.last) => {HTStruct(List(HTUnion(List(HTStruct(l.value.dropRight(1)),
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
  /**
  private def unionStringRedundancy(in: HigherType): HigherType = {
    // There are multiple internal types that get mapped to an Avro String.
    // We cannot permit a Union to contain multiple such type-branches, or else the branches
    // cannot be distinguished after converting to an Avro schema.
    in match {
      case a: HTUnion => {
        def strTest(bt: BaseType): Boolean = {
          bt match {
            case a: POther => true
            case b: PAlphanum => true
            case c: PString => true
            case d: PStringConst => true
            case _ => false
          }
        }
        if (a.value.count(strTest(_)) > 1) {
          HTUnion(a.value.filter(strTest(_))
        }
      }
    }
  }
   **/
  private def combineAdjacentStringConstants(in: HigherType): HigherType = {
    def findAdjacent(soFar: List[HigherType], remainder: List[HigherType]): List[HigherType] = {
      if (remainder.length == 0) {
        soFar
      } else {
        remainder match {
          case HTBaseType(c1) :: HTBaseType(c2) :: rest if (c1.isInstanceOf[PStringConst] && c2.isInstanceOf[PStringConst]) => 
            findAdjacent(soFar :+ HTBaseType(new PStringConst(c1.asInstanceOf[PStringConst].cval + c2.asInstanceOf[PStringConst].cval)), rest)
          case first :: rest => findAdjacent(soFar :+ first, rest)
          case _ => throw new RuntimeException("This is an impossible merge step situation")
        }
      }
    }

    in match {
      case a: HTStruct => HTStruct(findAdjacent(List[HigherType](), a.value))
      case _ => in
    }
  }

  private def combineHomogeneousArrays(in: HigherType): HigherType = {
    def findAdjacent(soFar: List[HigherType], remainder: List[HigherType], willProcessAll: Boolean): List[HigherType] = {
      if (remainder.length == 0) {
        soFar
      } else {
        remainder match {
          case HTArrayFW(c1val, c1len) :: HTArrayFW(c2val, c2len) :: rest if (c1val == c2val) => {
            if (willProcessAll) {
              findAdjacent(soFar :+ HTArrayFW(c1val, c1len+c2len), rest, willProcessAll)
            } else {
              findAdjacent(soFar :+ HTArray(c1val, min(c1len, c2len)), rest, willProcessAll)
            }
          }
          case first :: rest => findAdjacent(soFar :+ first, rest, willProcessAll)
          case _ => throw new RuntimeException("This is an impossible array-combination step situation")
        }
      }
    }

    in match {
      case a: HTUnion => HTUnion(findAdjacent(List[HigherType](), a.value, false))
      case b: HTStruct => HTStruct(findAdjacent(List[HigherType](), b.value, true))
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

