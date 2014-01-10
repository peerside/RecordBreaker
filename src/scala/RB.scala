import scala.io.Source
import scala.math._
import scala.collection.mutable._


abstract class BaseType
trait ParsedValue[T] extends BaseType {
  val parsedValue: T
  def getValue(): T = {parsedValue}
}
case class PInt() extends BaseType
case class PFloat() extends BaseType
case class PAlphanum() extends BaseType
case class POther() extends BaseType
case class PVoid() extends BaseType
case class PEmpty() extends BaseType
case class PString(terminator: String) extends BaseType with ParsedValue[String] {val parsedValue=terminator}
case class PIntConst(cval: Int) extends BaseType with ParsedValue[Int] {val parsedValue=cval}
case class PStringConst(cval: String) extends BaseType with ParsedValue[String] {val parsedValue=cval}
case class PMetaToken(lval:POther, center:List[BaseType], rval:POther) extends BaseType

abstract class HigherType
case class HTStruct(value: List[HigherType]) extends HigherType
case class HTArray(value: HigherType) extends HigherType
case class HTUnion(value: List[HigherType]) extends HigherType
case class HTBaseType(value: BaseType) extends HigherType
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


def oracle(input:Chunks): Prophecy = {
  val minCoverage = 1
  val maxMass = 2

  //
  // Compute histograms over input chunks
  //
  var intCounts, floatCounts, alphaCounts, strCounts, otherCounts, voidCounts, emptyCounts, metaCounts, charCounts = new HashMap[Int, Int]().withDefaultValue(0)
  var tokenHistograms = new HashMap[(String, Int), Int]().withDefaultValue(0)
  var uniqueBaseTypes = new HashSet[BaseType]()
  var numUniqueVals = 0
  var numChunks = 0
  for (chunk <- input) {
    var intCount, floatCount, alphaCount, strCount, otherCount, voidCount, emptyCount, metaCount, charCount = 0
    //var charCounts = new HashMap[String, Int]().withDefaultValue(0)
    for (t <- chunk) {
      t match {
        case a: PInt => intCount+=1
        case b: PFloat => floatCount+=1
        case c: PAlphanum => alphaCount+=1
        case d: PString => strCount+=1
        case e: POther => otherCount+=1
        case g: PVoid => voidCount+=1
        case h: PEmpty => emptyCount+=1
        case x: PMetaToken => metaCount+=1
      }
      if (numUniqueVals < 2) {
        uniqueBaseTypes = uniqueBaseTypes + t
        numUniqueVals = uniqueBaseTypes.size
      }
      numChunks+=1;
    }

    intCounts(intCount)+=1
    floatCounts(floatCount)+=1
    alphaCounts(alphaCount)+=1
    strCounts(strCount)+=1
    otherCounts(otherCount)+=1
    voidCounts(voidCount)+=1
    emptyCounts(emptyCount)+=1
    metaCounts(metaCount)+=1
    charCounts(charCount)+=1

    // charCounts has all the per-tokens counts for this chunk.
    //charCounts.foreach(tokenHistograms(_) += 1)
  }
  val nonMetaMaps = Map(("int"->intCounts),("float"->floatCounts),("str"->strCounts),("other"->otherCounts),("void"->voidCounts),("empty"->emptyCounts),("alpha"->alphaCounts),("char"->charCounts))

  //
  // Some necessary histogram functions
  //
  def histogram(m:Map[Int,Int]) = {m.toList.sorted}
  def normalForm(m:Map[Int,Int]) = {List((0, m(0))) ++ (m-0).toList.sortWith((x,y) => x._2 > y._2)}
  implicit def intHist2DoubleHist(ih:List[(Int,Int)]): List[(Int,Double)] = {ih.map(x=> (x._1, x._2.toDouble))}
  def width(l:List[(Int,Double)]) = {l.length-1}
  def mass(l:List[(Int,Double)], i:Int) = {l(i)._2}
  def rmass(l:List[(Int,Double)], i:Int) = {l(0)._2 + l.slice(i+1,l.length).map(x=>x._2).sum}
  def coverage(l:List[(Int,Double)]) = {l.slice(1,l.length).map(x=>x._2).sum}
  def average(h1:List[(Int,Double)], h2:List[(Int,Double)]) = {
    def innerAverage(in:List[(Int,Double)], accum:List[(Int,Double)]): List[(Int,Double)] = {
      in match {
        case a :: b :: rest if (a._1 == b._1) => innerAverage(rest, (a._1,(a._2+b._2)/2.0)::accum)
        case a :: rest                        => innerAverage(rest, (a._1,a._2/2.0)::accum)
        case Nil                              => accum  
      }
    }
    innerAverage((h1++h2).sorted, Nil).reverse
  }

  //
  // I bet I can get this down to one line (instead of 5).  Maybe this?
  //h1.slice(1,h1.length).zip(h2.slice(1,h1.length)).zipWithIndex.map(((h1elt,h2elt),i)=>mass(h1elt,i+1) * math.log(mass(h1elt,i+1)/mass(h2elt.i+1))).sum
  //
  def relEntropy(h1:List[(Int,Double)], h2:List[(Int,Double)]) = {
    var total:Double = 0
    for (j <- 1 to width(h1)) {
      total += mass(h1,j) * math.log(mass(h1, j)/mass(h2,j))
    }
    total
  }
  def symEntropy(h1:List[(Int,Double)], h2:List[(Int,Double)]) = {
    0.5 * relEntropy(h1,average(h1,h2)) + 0.5 * relEntropy(h2,average(h1,h2))
  }

  //////////////////////////////////////
  // Go through the 5 cases for the oracle.  Use the histogram data to make the correct prophecy
  //////////////////////////////////////

  //
  // Case 1.  If the ONLY token is a MetaToken, then crack it open with a StructProphecy.
  // If the ONLY token is a base type, then return a BaseProphecy.
  //
  input match {
    case x:List[List[BaseType]] if (nonMetaMaps.toList.map(x=>x._2).forall(v => width(normalForm(v)) == 0) && width(normalForm(metaCounts)) > 0 && rmass(normalForm(metaCounts), 1) == 0) => return StructProphecy(List[Chunks](x.map(v => List(v.head.asInstanceOf[PMetaToken].lval)), x.map(v => v.head.asInstanceOf[PMetaToken].center), x.map(v => List(v.head.asInstanceOf[PMetaToken].rval))))
    case _ if nonMetaMaps.toList.map(x=>x._2).exists(v => coverage(normalForm(v)) == numChunks) => return BaseProphecy(uniqueBaseTypes.head)
    case _ => Nil
  }

  //
  // Case 2
  //
  def clusterHistogramsIntoGroups(clusterTolerance: Double, allMaps: Map[String,Map[Int,Int]]) = {
    var normHistograms:List[(String,List[(Int,Int)])] = allMaps.filter(kv => coverage(normalForm(kv._2)) > 0).toList.map(ab=>(ab._1,normalForm(ab._2)))

    var groups = normHistograms.map(x=>List(x._1))
    val goodPairs = normHistograms.combinations(2).map(pair => (pair(0)._1, pair(1)._1, symEntropy(pair(0)._2,pair(1)._2))).filter(_._3<=clusterTolerance)
    for (gp <- goodPairs) {
      val grp1 = groups.indexWhere(group => group.contains(gp._1))
      val grp2 = groups.indexWhere(group => group.contains(gp._2))
      val newgrp = groups(grp1) ++ groups(grp2)
      groups = groups.filter(group => ! group.contains(gp._1)).filter(group => ! group.contains(gp._2)) :+ newgrp
    }
    groups
  }
  val allMaps = nonMetaMaps + ("meta"->metaCounts)
  val groups = clusterHistogramsIntoGroups(0.01, allMaps)

  //
  // Case 3
  //
  val orderedGroups = groups.sortBy(y=>y.map(z => rmass(normalForm(allMaps(z)),1)).min)
  val chosenGroup = orderedGroups.find(x=> x.forall(y=>(rmass(normalForm(allMaps(y)),1) < maxMass) &&
                                                      (coverage(normalForm(allMaps(y))) > minCoverage)))

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
      return StructProphecy(totalPieces)
    }
    case _ => Nil
  }

  //
  // Case 4
  //
  val c4OrderedGroups = groups.sortBy(y=>y.map(z => coverage(normalForm(allMaps(z)))).max)(Ordering[Double].reverse)
  val c4ChosenGroup = c4OrderedGroups.find(x=> x.forall(v=>((width(normalForm(allMaps(v))) > 3) &&
                                                              (coverage(normalForm(allMaps(v))) > minCoverage))))
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
      return ArrayProphecy(preambles, middles, postambles)
    }
    case _ => Nil
  }

  //
  // Case 5
  //
  var unionPartition: Map[BaseType, List[Chunk]] = HashMap().withDefaultValue(List[Chunk]())
  for (chunk <- input) {
    unionPartition.updated(chunk(0), unionPartition(chunk(0)) :+ chunk)
  }
  // println("PROPHECY UNION: " + input)
  return UnionProphecy(List() ++ unionPartition.values)
}

def discover(cs:Chunks): HigherType = {
  return oracle(cs) match {
    case a: BaseProphecy => HTBaseType(a.value)
    case b: StructProphecy => HTStruct(b.css.map(discover))
    case c: ArrayProphecy => HTStruct(List(discover(c.prefix), HTArray(discover(c.middle)), discover(c.postfix)))
    case d: UnionProphecy => HTUnion(d.css.map(discover))
  }
}


/***********************************
 * Information theoretic score and rewrite
 ***********************************/
def costData(encoding:HigherType, input:Chunks):Double = {
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

def costEncoding(encoding: HigherType): Double = {
  val result = encoding match {
    case a: HTStruct => log(24) + 1 + log(a.value.length+1) + a.value.map(costEncoding).sum
    case b: HTUnion => log(24) + 1 + log(b.value.length+1) + b.value.map(costEncoding).sum
    case c: HTArray => log(24) + costEncoding(c.value)
    case d: HTArrayFW => log(24) + log(d.size) + costEncoding(d.value)
    case _ => log(24)
  }
  //println("Cost of " + encoding + " is " + result)
  result
}

def totalCost(input: Chunks)(encoding: HigherType): Double = {
  costEncoding(encoding) + costData(encoding, input)
}

def oneStep(orig: HigherType, rewriteRules:List[HigherType=>HigherType], costFn:HigherType=>Double): HigherType = {
  val origScore = costFn(orig)
  for (rw <- rewriteRules) {
    val newForm = rw(orig)
    if (newForm != orig) {
      val newCost = costFn(newForm)
      //println("Trying " + newForm + "(" + newCost + ") from " + orig + " (" + origScore + ") on input " + input)
    }
  }
  val bestRewrite = rewriteRules.map(r=> r(orig)).map(s=> (costFn(s), s)).reduceLeft((x,y)=>if (x._1 < y._1) x else y)
  if (bestRewrite._1 < origScore) {
    oneStep(bestRewrite._2, rewriteRules, costFn)
  } else {
    orig
  }
}

def refine(orig: HigherType, rewriteRules:List[HigherType=>HigherType], costFn:HigherType=>Double): HigherType = {
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
def rewriteSingletons(in: HigherType): HigherType = {
  in match {
    case a: HTStruct if (a.value.length == 1) => a.value.head
    case b: HTStruct if (b.value.length == 0) => HTBaseType(PEmpty())
    case c: HTUnion if (c.value.length == 1) => c.value.head
    case d: HTUnion if (d.value.length == 0) => HTBaseType(PVoid())
    case _ => in
  }
}
def cleanupStructUnion(in: HigherType): HigherType = {
  in match {
    case a: HTStruct if (a.value.contains(PVoid())) => HTBaseType(PVoid())
    case b: HTStruct if (b.value.contains(HTBaseType(PEmpty()))) => HTStruct(b.value.filterNot(x => x == HTBaseType(PEmpty())))
    case c: HTUnion if (c.value.contains(HTBaseType(PVoid()))) => HTUnion(c.value.filterNot(x => x == HTBaseType(PVoid())))
    case _ => in
  }
}
def transformUniformStruct(in: HigherType): HigherType = {
  in match {
    case a: HTStruct if ((a.value.length >= 3) && ((Set() ++ a.value).size == 1)) => HTArrayFW(a.value.head, a.value.length)
    case _ => in
  }
}
def commonUnionPostfix(in: HigherType): HigherType = {
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

def combineAdjacentStringConstants(in: HigherType): HigherType = {
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

/***********************************
 * Parse text file
 ***********************************/
def parseFile(fname: String): Chunks = {
  def findMeta(lhs:POther with ParsedValue[String], rhs:POther with ParsedValue[String], l:List[BaseType]):List[BaseType] = {
    var (left,toprocess) = l.span(x => x != lhs)
    var (center, rest) = toprocess.slice(1,toprocess.length).span(x => x != rhs)
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
} else {
  val htOrig = discover(css)
  val htImproved = refineAll(htOrig, css)

  val score1 = totalCost(css)(htOrig)
  val score2 = totalCost(css)(htImproved)

  println("Score 1:  " + score1 + " from " + htOrig)
  println("Score 2:  " + score2 + " from " + htImproved)
}



