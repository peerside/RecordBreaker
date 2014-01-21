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
import scala.io.Source
import scala.math._
import scala.collection.mutable._

package com.cloudera.recordbreaker.learnstructure

package structuretypes {
  /** The BaseType is an atomic type (int, str, etc) that can be either
   *  parsed or inferred post-parsing.
   */
  abstract class BaseType
  trait ParsedValue[T] extends BaseType {
    val parsedValue: T
    def getValue(): T = {parsedValue}
  }

  /** Basic integer. Parsed from input */
  case class PInt() extends BaseType

  /** Basic float.  Parsed from input */
  case class PFloat() extends BaseType

  /** Basic string.  Parsed from input */
  case class PAlphanum() extends BaseType

  /** Single-char string.  Created during metatoken parsing */
  case class POther() extends BaseType

  /** An option of Union that doesn't do anything.  Side effect of union refinement */
  case class PVoid() extends BaseType

  /** An option of Struct that doesn't do anything.  Side effect of struct refinement */
  case class PEmpty() extends BaseType

  /** A particular kind of string that has a known terminating character.  Inferred.
   *  @param terminator Terminating char for string
   */
  case class PString(terminator: String) extends BaseType with ParsedValue[String] {val parsedValue=terminator}

  /** An int that is always the same.  Inferred.
   *  @param cval Constant value
   */
  case class PIntConst(cval: Int) extends BaseType with ParsedValue[Int] {val parsedValue=cval}

  /** A string that is always the same.  Inferred.
   * @param cval Constant value
   */
  case class PStringConst(cval: String) extends BaseType with ParsedValue[String] {val parsedValue=cval}

  /** A meta token that has a list of BaseTypes with single-char POther on either side.
   * @param lval Left-hand char
   * @param center List of BaseTypes that makes up the MetaToken
   * @param rval Right-hand char
   */
  case class PMetaToken(lval:POther, center:List[BaseType], rval:POther) extends BaseType

  abstract class HigherType
  case class HTStruct(value: List[HigherType]) extends HigherType
  case class HTArray(value: HigherType) extends HigherType
  case class HTUnion(value: List[HigherType]) extends HigherType
  case class HTBaseType(value: BaseType) extends HigherType {
    def getParser(): Parser[HigherType] = {
      new Parser[HigherType] {
        def apply(in: Input)
      }
    }
  }
  case class HTNamedBaseType(name: String, value: BaseType) extends HigherType
  case class HTArrayFW(value: HigherType, size: Int) extends HigherType
  case class HTOption(value: HigherType) extends HigherType

  type Chunk = List[BaseType]
  type Chunks = List[Chunk]

  abstract class Prophecy
  case class BaseProphecy(value: BaseType) extends Prophecy
  case class StructProphecy(css: List[Chunks]) extends Prophecy
  case class ArrayProphecy(prefix: Chunks, middle:Chunks, postfix:Chunks) extends Prophecy
  case class UnionProphecy(css: List[Chunks]) extends Prophecy
}


////////////////////////////////////
// Structure prediction
////////////////////////////////////
package inferstructure {
  def oracle(input:Chunks): Prophecy = {
    val minCoverage = 1
    val maxMass = 2

    //
    // Compute histograms over input chunks
    //
    val numUniqueVals = Set() ++ input size
    def histMap(filterChunk: PartialFunction[BaseType, Int]) = {
      Map() ++ input.map(chunk => chunk collect(x=>filterChunk(x)) sum).flatten.groupBy(x=>x).map(x=> (x._1, x._2.length))
    }
    val intCounts = histMap({case x: PInt => 1})
    val floatCounts = histMap({case x: PFloat => 1})
    val alphaCounts = histMap({case x: PAlphanum => 1})
    val strCounts = histMap({case x: PString => 1})
    val otherCounts = histMap({case x: POther => 1})
    val voidCounts = histMap({case x: PVoid => 1})
    val emptyCounts = histMap({case x: PEmpty => 1})
    val metaCounts = histMap({case x: PMetaToken => 1})

    val nonMetaMaps = Map(("int"->intCounts),("float"->floatCounts),("alpha"->alphaCounts)("str"->strCounts),("other"->otherCounts),("void"->voidCounts),("empty"->emptyCounts))
    val nonMetaHistograms = nonMetaMaps.toList.map((l,x)=>Histogram(l,x))
    val metaHistogram = Histogram("meta", metaCounts)    
    val allMaps = nonMetaMaps + ("meta"->metaCounts)
    val allHistograms = allMaps.toList.map((l,x)=>Histogram(l,x))


    /** Histogram class exists just for oracle() statistics
     *  @param inM A map of observed counts to unique values.
     */
    class Histogram[T](val label:String, inM: Map[Int, T]) = {
      val m:List[(Int,T)] = inM.toList.sorted
      val normalForm:List[(Int,T)] = List((0, m(0))) ++ (m-0).toList.sortWith((x,y) => x._2 > y._2)

      def width() = normalForm.length-1
      def mass(i: Int) = normalForm(i)._2
      def rmass(i: Int) = normalForm(0)._2 + normalForm.slice(i+1,normalForm.length).map(x=>x._2).sum
      def coverage() = normalForm.slice(1,normalForm.length).map(x=>x._2).sum

      def symEntropy(otherHist:Histogram[T]): Double = {
        def average(otherHist: Histogram[T]):List[(Int,T)] = {
          def innerAverage(in:List[(Int,T)], accum:List[(Int,T)]): List[(Int,T)] = {
            in match {
              case a :: b :: rest if (a._1 == b._1) => innerAverage(rest, (a._1,(a._2+b._2)/2.0)::accum)
              case a :: rest                        => innerAverage(rest, (a._1,a._2/2.0)::accum)
              case Nil                              => accum
            }
          }
          innerAverage((normalForm ++ otherHist.normalForm).sorted, Nil).reverse
        }
        def relEntropy(otherHist: Histogram[T]): Double = {
          var total:Double = 0
          for (j <- 1 to width()) {
            total += this.mass(j) * math.log(this.mass(j)/otherHist.mass(j))
          }
          total
        }

        val averageHist = this.average(otherHist)
        0.5 * this.relEntropy(averageHist) + 0.5 * h2.relEntropy(averageHist)
      }
    }

    //////////////////////////////////////
    // Go through the 5 cases for the oracle.  Use the histogram data to make the correct prophecy
    //////////////////////////////////////

    //
    // Case 1.  If the ONLY token is a MetaToken, then crack it open with a StructProphecy.
    // If the ONLY token is a base type, then return a BaseProphecy.
    //
    def case1(): Option[Prophecy] = {
      input match {
        case x:List[List[BaseType]] if ((metaHistogram.width() > 0) &&
                                          (metaHistogram.rmass(1) == 0) &&
                                          nonMetaHistograms.forall(h => (h.width() == 0))) =>
          Some(StructProphecy(List[Chunks](x.map(v => List(v.head.asInstanceOf[PMetaToken].lval)),
                                           x.map(v => v.head.asInstanceOf[PMetaToken].center),
                                           x.map(v => List(v.head.asInstanceOf[PMetaToken].rval)))))
        case y:List[List[BaseType]] if (nonMetaHistograms.exists(h => h.coverage() == y.head.length)) =>
          Some(BaseProphecy(y.head.head))
        case _: None
      }
    }

    //
    // Build the histogram clusters as described in "case 2"
    //
    def clusterHistogramsIntoGroups(clusterTolerance: Double, histograms: List[Histogram[Int]]) = {
      val filteredHistograms = histograms.filter(h => h.coverage() > 0)
      val goodPairs = filteredHistograms.combinations(2).filter(hPair => (hPair(0).symEntropy(hPair(1)) <= clusterTolerance)).map(hPair=>(hPair(0), hPair(1)))
      val startingGroups = filteredHistograms.map(List(h=>h.label))

      def processPairs(remainingGoodPairs, currentGroups: List[List[String]]): List[List[String]] = {
        remainingGoodPairs match {
          case firstPair :: remainderPairs => {
            val grpsToMerge, grpsToRetain = currentGroups.partition(grp => grp.contains(firstPair._1.label) || grp.contains(firstPair._2.label))
            processPairs(remainderPairs, grpsToRetain :+ grpsToMerge.flatten)
          }
          case _ => currentGroups
        }
      }
      processPairs(goodPairs, startingGroups)
    }
    val groups = clusterHistogramsIntoGroups(0.01, allHistograms)

    //
    // Now utilize the clusters.  This is "case 3" in the paper
    //
    def case3(): Option[Prophecy] = {
      val orderedGroups = groups.sortBy(hGroup=>hGroup.map(h => h.rmass(1)).min)
      val chosenGroup = orderedGroups.find(hGroup=> hGroup.forall(h=> ((h.rmass(1) < maxMass) &&
                                                                         (h.coverage() > minCoverage))))
      // Build the appropriate struct, if any
      chosenGroup match {
        case Some(xlist) => {
          var totalPieces = List[Chunks]()
          for (chunk:List[BaseType] <- input) {
            val brokenPieces = chunk.foldLeft(List(List[BaseType]()))((x:List[List[BaseType]], y:BaseType) => {
                                                                        val shouldBreak = y match {
                                                                            case a: PInt => xlist contains "int"
                                                                            case b: PFloat => xlist contains "float"
                                                                            case c: PAlphanum => xlist contains "alpha"
                                                                            case d: PString => xlist contains "str"
                                                                            case e: POther => xlist contains "other"
                                                                            case g: PVoid => xlist contains "void"
                                                                            case h: PEmpty => xlist contains "empty"
                                                                            case x: PMetaToken => xlist contains "meta"
                                                                            case _ => false
                                                                          }
                                                                        if (shouldBreak) {
                                                                          x :+ List(y) :+ List()
                                                                        } else {
                                                                          x.slice(0,x.length-1) :+ (x.last :+ y)
                                                                        }
                                                                      }).filter(x=>x.length>0)

            if (totalPieces.length==0) {
              totalPieces = (1 to brokenPieces.length).foldLeft(List[List[List[BaseType]]]())((a,b)=>a:+List[List[BaseType]]())
            }
            totalPieces = totalPieces.zip(brokenPieces).map(listPair=>listPair._1 :+ listPair._2)
          }
          // REMIND -- mjc -- there's some missing code here to handle the Union case.
          // println("PROPHECY STRUCT? " + input)
          return Some(StructProphecy(totalPieces))
        }
        case _ => None
      }
    }

    //
    // Case 4
    //
    def case4(): Option[Prophecy] = {
      val c4OrderedGroups = groups.sortBy(hGrp=>hGrp.map(h => h.coverage).max)(Ordering[Double].reverse)
      val c4ChosenGroup = c4OrderedGroups.find(hGrp=> hGrp.forall(h=>((h.width() > 3) && (h.coverage() > minCoverage))))

      c4ChosenGroup match {
        case Some(xlist) => {
          val knownClusterElts = HashSet() ++ xlist
          var preambles:Chunks = List[Chunk]()
          var middles:Chunks = List[Chunk]()
          var postambles:Chunks = List[Chunk]()

          for (chunk:List[BaseType] <- input) {
            def processPiece(answerSoFar: List[Chunk], partialChunk:List[BaseType]):List[Chunk] = {
              if (partialChunk.length == 0) {
                answerSoFar
              } else {
                val seenSets = partialChunk.scanLeft(HashSet[String]())((seenSoFar:HashSet[String], elt:BaseType) => (seenSoFar +
                                                                                                                        (elt match {
                                                                                                                           case a: PInt => "int"
                                                                                                                           case b: PFloat => "float"
                                                                                                                           case c: PAlphanum => "alpha"
                                                                                                                           case d: PString => "str"
                                                                                                                           case e: POther => "other"
                                                                                                                           case g: PVoid => "void"
                                                                                                                           case h: PEmpty => "empty"
                                                                                                                           case x: PMetaToken => "meta"
                                                                                                                           case _ => ""
                                                                                                                         })))
                val endSequence = seenSets.slice(1,seenSets.size).indexWhere(observedSet=> (observedSet & knownClusterElts).size == knownClusterElts.size)
                processPiece(answerSoFar :+ partialChunk.slice(0,endSequence+1), partialChunk.slice(endSequence+1, partialChunk.length))
              }
            }
            val breakdown = processPiece(List[Chunk](), chunk)
            preambles = preambles :+ breakdown.head
            middles = middles :+ breakdown(1)
            postambles = postambles :+ breakdown.last
          }
          //println("PROPHECY ARRAY: " + input)
          return Some(ArrayProphecy(preambles, middles, postambles))
        }
        case _ => None
      }
    }

    //
    // Case 5
    //
    def case5(): Prophecy = {
      var unionPartition: Map[BaseType, List[Chunk]] = HashMap().withDefaultValue(List[Chunk]())
      for (chunk <- input) {
        unionPartition.updated(chunk(0), unionPartition(chunk(0)) :+ chunk)
      }
      // println("PROPHECY UNION: " + input)
      Some(UnionProphecy(List() ++ unionPartition.values))
    }

    return case1() orElse case2() orElse case3() orElse() case4() getOrElse case5()
  }

  def discover(cs:Chunks): HigherType = {
    return oracle(cs) match {
      case a: BaseProphecy => HTBaseType(a.value)
      case b: StructProphecy => HTStruct(b.css.map(discover))
      case c: ArrayProphecy => HTStruct(List(discover(c.prefix), HTArray(discover(c.middle)), discover(c.postfix)))
      case d: UnionProphecy => HTUnion(d.css.map(discover))
    }
  }
}


/***********************************
 * Information theoretic score and rewrite
 ***********************************/
package rewritestructure {
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
      case _ => log(24)
    }
  }

  /** totalCost of applying an encoding to a dataset
   */
  private def totalCost(input: Chunks)(encoding: HigherType): Double = costEncoding(encoding) + costData(encoding, input)


  /** Take one step in the schema refinement process
   */
  private def oneStep(orig: HigherType, rewriteRules:List[HigherType=>HigherType], costFn:HigherType=>Double): HigherType = {
    val bestRewrite = rewriteRules.map(r=> r(orig)).map(s=> (costFn(s), s)).reduceLeft((x,y)=>if (x._1 < y._1) x else y)
    if (bestRewrite._1 < costFn(orig)) {
      oneStep(bestRewrite._2, rewriteRules, costFn)
    } else {
      orig
    }
  }

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
            case (l:HTStruct, r:HTStruct) if (l.value.last == r.value.last) => HTStruct(List(HTUnion(List(HTStruct(l.value.dropRight(1)),
                                                                                                          HTStruct(r.value.dropRight(1)),
                                                                                                          l.value.last))))
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


  ///////////////////////////////////////////
  // Apply refinement rules
  ///////////////////////////////////////////
  def refineAll(orig: HigherType, input: Chunks) = {
    val dataIndependentRules = List(rewriteSingletons _, cleanupStructUnion _, transformUniformStruct _, commonUnionPostfix _, combineAdjacentStringConstants _)
    val round1 = refine(orig, dataIndependentRules, costEncoding)
    //val round2 = refine(round1, dataDependentRules, totalCost(input))
    //val round3 = refine(round2, dataIndependentRules)
    round1
  }
}

/***********************************
 * Parse text file
 ***********************************/
package parsing {
  def parseFile(fname: String): Chunks = {
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

    val src = Source.fromFile(fname, "UTF-8")
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
            case a if a.length() == 1 => new POther() with ParsedValue[String] {val parsedValue=a}
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


package test {
  def flattenAndName(ht: HigherType, parentName: String): List[(HigherType, String)] = {
    ht match {
      case a: HTStruct => a.value.foldLeft(List[(HigherType,String)]())((lhs, rhs) => lhs ++ flattenAndName(rhs, parentName + "." + a.getClass.getName))
      case b: HTUnion => b.value.foldLeft(List[(HigherType,String)]())((lhs, rhs) => lhs ++ flattenAndName(rhs, parentName + "." + b.getClass.getName))
      case c: HTBaseType => List((c, parentName + "." + c.value.getClass.getName))
    }
  }


  def test(): Unit = {
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

      val score1 = totalCost(css)(htOrig)
      val score2 = totalCost(css)(htImproved)

      println("Score 1:  " + score1 + " from " + htOrig)
      println("Score 2:  " + score2 + " from " + htImproved)
    }
  }
}



