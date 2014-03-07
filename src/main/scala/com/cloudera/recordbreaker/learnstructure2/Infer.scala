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

////////////////////////////////////
// Structure prediction
////////////////////////////////////
object Infer {
  /** discover() transforms Chunks into an HTBaseType hierarchy
   * This is the only public function in the inferstructure package.
   */
  def discover(cs:Chunks): HigherType = {
    oracle(cs) match {
      case a: BaseProphecy => HTBaseType(a.value)
      case b: StructProphecy => HTStruct(b.css.map(discover))
      case c: ArrayProphecy => HTStruct(List(discover(c.prefix), HTArray(discover(c.middle)), discover(c.postfix)))
      case d: UnionProphecy => HTUnion(d.css.map(discover))
    }
  }

  /** oracle() creates a Prophecy out of Chunks.
   * Used internally by discover()
   */
  private def oracle(input:Chunks): Prophecy = {
    /** Histogram class exists just for oracle() statistics
     *  @param inM A map of observed counts to unique values.
     */
    class Histogram[T](val label:String, inM: Map[Int, T])(implicit inN:Numeric[T]) {
      val n = inN
      val normalForm:List[(Int,T)] = List((0, inM.getOrElse(0, n.zero))) ++ (inM-0).toList.sortBy(_._2).reverse

      def width() = normalForm.length-1
      def mass(i: Int):Double = n.toDouble(normalForm(i)._2)
      def rmass(i: Int):Double = n.toDouble(n.plus(normalForm(0)._2, (normalForm.slice(i+1,normalForm.length).map(z => z._2).foldLeft(n.zero)((a1,a2)=>n.plus(a1, a2)))))
      def coverage():Double = n.toDouble(normalForm.slice(1,normalForm.length).map(x=>x._2).foldLeft(n.zero)((a1,a2)=>n.plus(a1, a2)))

      def symEntropy(otherHist:Histogram[T]): Double = {
        def average(otherHist: List[(Int,T)]):List[(Int,Double)] = {
          def innerAverage(in:List[(Int,T)], accum:List[(Int,Double)]): List[(Int,Double)] = {
            in match {
              case a :: b :: rest if (a._1 == b._1) =>
                innerAverage(rest, (a._1, n.toDouble(a._2) + n.toDouble(b._2) / 2.0)::accum)
              case a :: rest => innerAverage(rest, (a._1,n.toDouble(a._2) / 2.0)::accum)
              case Nil       => accum
            }
          }
          innerAverage((normalForm ++ otherHist).sorted, Nil).reverse
        }

        def relEntropy(srcHist: List[(Int, T)], otherHist: List[(Int, Double)]): Double = {
          val combinedPairs = srcHist.slice(1,srcHist.length-1).zip(otherHist.slice(1, srcHist.length-1))
          return combinedPairs.map(combinedElt => inN.toDouble(combinedElt._1._2) * math.log(inN.toDouble(combinedElt._1._2) / combinedElt._2._2)).sum
        }
        val averageHist = average(otherHist.normalForm)
        0.5 * relEntropy(this.normalForm, averageHist) + 0.5 * relEntropy(otherHist.normalForm, averageHist)
      }
    }

    val minCoverage = 1
    val maxMass = 2

    //
    // Compute histograms over input chunks
    //
    val numUniqueVals = Set() ++ input size
    def histMap(filterChunk: PartialFunction[BaseType, Int]):Map[Int,Int] = {
      Map() ++ input.map(chunk => chunk.collect(filterChunk).sum).groupBy(x=>x).map(z=> (z._1, z._2.length))
    }
    val intCounts = histMap({case x: PInt => 1})
    val floatCounts = histMap({case x: PFloat => 1})
    val alphaCounts = histMap({case x: PAlphanum => 1})
    val strCounts = histMap({case x: PString => 1})
    val otherCounts = histMap({case x: POther => 1})
    val voidCounts = histMap({case x: PVoid => 1})
    val emptyCounts = histMap({case x: PEmpty => 1})
    val metaCounts = histMap({case x: PMetaToken => 1})

    val nonMetaMaps = List(("int"->intCounts),("float"->floatCounts),("alpha"->alphaCounts),("str"->strCounts),("other"->otherCounts),("void"->voidCounts),("empty"->emptyCounts)).toMap
    val nonMetaHistograms = nonMetaMaps.toList.map(x => new Histogram(x._1,x._2))
    val metaHistogram = new Histogram("meta", metaCounts)    
    val allMaps = nonMetaMaps + ("meta"->metaCounts)
    val allHistograms = allMaps.toList.map(t=>new Histogram(t._1,t._2))


    //////////////////////////////////////
    // Go through the 5 cases for the oracle.  Use the histogram data to make the correct prophecy
    //////////////////////////////////////
    /**
    println("-------")
    println("histograms!")
    println("Meta histogram width: " + metaHistogram.width())
    println("Input head length: " + input.head.length)
    for (h <- nonMetaHistograms) {
      println(" sub h label: " + h.label)
      println(" sub h coverage: " + h.coverage())
      println(" sub h width: " + h.width())
      println()
    }
    println("-------")
    println("---------------- ENTER")     
    */

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
        case y:List[List[BaseType]] if (nonMetaHistograms.exists(h => h.coverage() == y.length) && (nonMetaHistograms.map(h=>h.coverage()).sum == y.length)) =>
          Some(BaseProphecy(y.head.head))
        case _ => None
      }
    }

    //
    // Build the histogram clusters as described in "case 2"
    //
    def clusterHistogramsIntoGroups[X](clusterTolerance: Double, histograms: List[Histogram[X]]): List[List[Histogram[X]]] = {
      val filteredHistograms = histograms.filter(h => h.coverage() > 0)
      val goodPairs = filteredHistograms.combinations(2).filter(hPair => (hPair(0).symEntropy(hPair(1)) <= clusterTolerance)).map(hPair=>(hPair(0), hPair(1))).toList
      val startingGroups = filteredHistograms.map(h=>List(h))

      def processPairs(remainingGoodPairs:List[(Histogram[X],Histogram[X])], currentGroups: List[List[Histogram[X]]]): List[List[Histogram[X]]] = {
        remainingGoodPairs match {
          case firstPair :: remainderPairs => {
            val (grpToMerge1, remainingGrps) = currentGroups.partition(grp => grp.contains(firstPair._1))
            val (grpToMerge2, rumpGrps) = remainingGrps.partition(grp => grp.contains(firstPair._2))
            processPairs(remainderPairs, (grpToMerge1.flatten ++ grpToMerge2.flatten) +: rumpGrps)
          }
          case _ => currentGroups
        }
      }
      return processPairs(goodPairs, startingGroups)
    }
    val groups = clusterHistogramsIntoGroups(0.01, allHistograms)
    /**
    println("All input histograms:")
    for (h <- allHistograms) {
      println("  " + h.label)
    }
    println()
    println("All histogram groups")
    for (glist <- groups) {
      print("Group: ")
      for (h <- glist) {
        print(h.label + "  ")
      }
      println()
    }
     */

    //
    // Now utilize the clusters.  This is "case 3" in the paper
    //
    def case3[X](groupsIn:List[List[Histogram[X]]]): Option[Prophecy] = {
      val orderedGroups:List[List[Histogram[X]]] = groupsIn.sortBy(hGroup=>hGroup.map(h => h.rmass(1)).min)
      val chosenGroup = orderedGroups.find(hGroup=> hGroup.forall(h=> ((h.rmass(1) < maxMass) &&
                                                                         (h.coverage() > minCoverage))))
      // Build the appropriate struct, if any
      chosenGroup match {
        case Some(groupOfHistograms) => {
          val xlist = groupOfHistograms.map(x=>x.label)
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
          return Some(StructProphecy(totalPieces))
        }
        case _ => None
      }
    }

    //
    // Case 4
    //
    def case4[X](groupsIn:List[List[Histogram[X]]]): Option[Prophecy] = {
      val c4OrderedGroups:List[List[Histogram[X]]] = groupsIn.sortBy(hGrp=>hGrp.map(h => h.coverage).max)(Ordering[Double].reverse)
      val c4ChosenGroup:Option[List[Histogram[X]]] = c4OrderedGroups.find(hGrp=> hGrp.forall(h=>((h.width() > 3) && (h.coverage() > minCoverage))))

      c4ChosenGroup match {
        case Some(xlist) => {
          /// REMIND -- THIS IS PROBABLY NOT CORRECT.  WHAT IS xList?
          val knownClusterElts = HashSet() ++ xlist.map(histogram => histogram.label)
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
        unionPartition = unionPartition.updated(chunk(0), unionPartition(chunk(0)) :+ chunk)
      }
      UnionProphecy(List() ++ unionPartition.values)
    }

    return case1() orElse case3(groups) orElse case4(groups) getOrElse case5()
  }
}


