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
import scala.util.control._
import scala.collection.JavaConversions._
import scala.collection.mutable._
import scala.util.Marshal

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.codehaus.jackson.JsonNode;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData.Record;

object RBTypes {
  type Chunk = List[BaseType]
  type Chunks = List[Chunk]
  type ParsedChunk = List[ParsedValue[Any]]
  type ParsedChunks = List[ParsedChunk]

  /** The BaseType is an atomic type (int, str, etc) that can be either
   *  parsed or inferred post-parsing.
   */
  abstract class BaseType {
    def getAvroSchema(): Schema
  }
  trait ParsedValue[+T] extends BaseType {
    val parsedValue: T
    def getValue(): T = {parsedValue}
  }

  /** Basic integer. Parsed from input */
  case class PInt() extends BaseType {
    def getAvroSchema(): Schema = {
      return Schema.create(Schema.Type.INT);
    }
  }

  /** Basic float.  Parsed from input */
  case class PFloat() extends BaseType {
    def getAvroSchema(): Schema = {
      return Schema.create(Schema.Type.DOUBLE);
    }
  }

  /** Basic string.  Parsed from input */
  case class PAlphanum() extends BaseType {
    def getAvroSchema(): Schema = {
      return Schema.create(Schema.Type.STRING);
    }
  }

  /** Single-char string.  Created during metatoken parsing */
  case class POther() extends BaseType {
    def getAvroSchema(): Schema = {
      return Schema.create(Schema.Type.STRING);
    }
  }

  /** An option of Union that doesn't do anything.  Side effect of union refinement */
  case class PVoid() extends BaseType {
    def getAvroSchema(): Schema = {
      throw new RuntimeException("Cannot convert PVoid to Avro schema.  This should have been removed prior to Avro translation")
    }
  }

  /** An option of Struct that doesn't do anything.  Side effect of struct refinement */
  case class PEmpty() extends BaseType {
    def getAvroSchema(): Schema = {
      throw new RuntimeException("Cannot convert PEmpty to Avro schema.  This should have been removed prior to Avro translation")
    }
  }

  /** A particular kind of string that has a known terminating character.  Inferred.
   *  @param terminator Terminating char for string
   */
  case class PString(terminator: String) extends BaseType with ParsedValue[String] {
    val parsedValue=terminator
    def getAvroSchema(): Schema = {
      return Schema.create(Schema.Type.STRING);
    }
  }

  /** An int that is always the same.  Inferred.
   *  @param cval Constant value
   */
  case class PIntConst(cval: Int) extends BaseType with ParsedValue[Int] {
    val parsedValue=cval
    def getAvroSchema(): Schema = {
      return Schema.create(Schema.Type.INT);
    }
  }

  /** A string that is always the same.  Inferred.
   * @param cval Constant value
   */
  case class PStringConst(cval: String) extends BaseType with ParsedValue[String] {
    val parsedValue=cval
    def getAvroSchema(): Schema = {
      return Schema.create(Schema.Type.STRING);
    }
  }

  /** A meta token that has a list of BaseTypes with single-char POther on either side.
   * @param lval Left-hand char
   * @param center List of BaseTypes that makes up the MetaToken
   * @param rval Right-hand char
   */
  case class PMetaToken(lval:POther, center:List[BaseType], rval:POther) extends BaseType {
    def getAvroSchema(): Schema = {
      throw new RuntimeException("Cannot convert PMetaToken to Avro schema.  This should have been removed prior to Avro translation")
    }
  }

  object HigherType {
    var fieldCount = 0
    var denomCount = 0
    var missingCount = 0
    def resetFieldCount(): Unit = {
      fieldCount = 0
    }
    def resetUsageStatistics(ht: HigherType): Unit = {
      denomCount = 0
      missingCount = 0
      ht.resetUsageStatistics()
    }
    def getAvroSchema(ht: HigherType): Schema = {
      ht match {
        case a: HTBaseType => throw new RuntimeException("Cannot generate Avro schema when root of HigherType tree is HTBaseType")
        case _ => ht.getAvroSchema()
      }
    }
    def dumpToBytes(ht: HigherType): Array[Byte] = {
      Marshal.dump(ht)
    }
    def loadFromBytes(b: Array[Byte]): HigherType = {
      Marshal.load[HigherType](b)
    }
    def processChunk(ht: HigherType, chunk: ParsedChunk): Option[GenericRecord] = {
      val pcResults = ht.processChunk(chunk).filter(x=>x._1.length == 0)  // We only want the parses that consume entire input
      denomCount += 1

      if (pcResults.length == 0) {
        missingCount += 1
        return None
      }
      pcResults.head._4 match {
        case a: GenericRecord =>  {
          // uniquify based on ht.name()
          pcResults.head._2.foldLeft(HashSet[String]())((seenSoFar, curHt) => {
                                                          if (seenSoFar.contains(curHt.name())) {
                                                            seenSoFar
                                                          } else {
                                                            curHt.incrementUsage()
                                                            seenSoFar + curHt.name()
                                                          }
                                                        })
          Some(a)
        }
        case _ => {
          missingCount += 1
          None
        }
      }
    }
    def getFieldCount(): Int = {
      fieldCount += 1
      fieldCount-1
    }
    def prettyprint(ht: HigherType):Unit = {
      if (denomCount > 0) {
        println("Failed to parse " + missingCount + " of " + denomCount + " input lines (" + (100 * (missingCount/denomCount.toFloat)) + "%)")
      } else {
        println("No test statistics available.")
      }
      println()
      ht.prettyprint(0, true, denomCount)
    }
  }
  abstract class HigherType {
    var fc = HigherType.getFieldCount()
    var linesProcessed = 0
    def getAvroSchema(): Schema
    def name(): String = {
      return namePrefix() + fc
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, List[HigherType], Schema, Any)]
    def resetUsageStatistics(): Unit
    def incrementUsage(): Unit = {
      linesProcessed += 1
    }
    def namePrefix(): String
    def getDocString(): String = {
      return ""
    }
    def getDefaultValue(): JsonNode = {
      return null
    }
    def prettyprint(offset: Int, showStatistics: Boolean, denom: Int)
  }

  case class HTStruct(value: List[HigherType]) extends HigherType {
    def namePrefix(): String = "record_"
    def getAvroSchema(): Schema = {
      val fields: List[Schema.Field] = value.map(v => new Schema.Field(v.name(), v.getAvroSchema(), v.getDocString(), v.getDefaultValue()))
      val s: Schema = Schema.createRecord(name(), "RECORD", "", false)

      val fieldsJ: java.util.List[Schema.Field] = ListBuffer(fields: _*)
      s.setFields(fieldsJ)
      return s
    }
    def resetUsageStatistics(): Unit = {
      linesProcessed = 0
      value.foreach(_.resetUsageStatistics())
    }
    def prettyprint(offset: Int, showStatistics: Boolean, denom: Int) {
      print(" " * offset)
      print("HTStruct " + name())
      if (showStatistics && denom > 0) {
        print("  (" + (100 * (linesProcessed / denom.toFloat)) + "%)")
      }
      println()
      for (v <- value) {
        v.prettyprint(offset+1, showStatistics, denom)
      }
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, List[HigherType], Schema, Any)] = {
      def processChildren(childrenToGo: List[HigherType], inChunk:ParsedChunk): List[(ParsedChunk, List[HigherType], List[Schema], List[Any])] = {
        childrenToGo match {
          case curChild::rest => {
            val childLists = curChild.processChunk(inChunk)
            if (childLists.length == 0) {
              List()
            } else {
              if (rest.length == 0) {
                childLists.map(x => (x._1, x._2, List(x._3), List(x._4)))
              } else {
                childLists.flatMap(cl => processChildren(rest, cl._1).map(sr => (sr._1, cl._2 ++ sr._2, cl._3 +: sr._3, cl._4 +: sr._4)))
              }
            }
          }
          case _ => List()
        }
      }
      // Each result from processChildren() is a different possible parse
      val results = processChildren(value, chunk)
      results.map(result => {
                    val recordSchema = Schema.createRecord(name(), "RECORD", "", false)
                    val fieldMetadata = value.map(v => (v.name(), v.getDocString(), v.getDefaultValue()))
                    val schemaFields =
                      for ((reportedSchema, reportedMetadata) <- result._3.zip(fieldMetadata)) yield
                        new Schema.Field(reportedMetadata._1, reportedSchema, reportedMetadata._2, reportedMetadata._3)
                    recordSchema.setFields(ListBuffer(schemaFields: _*))
                    val gdr = new GenericData.Record(recordSchema)
                    for (i <- 0 to result._3.length-1) {
                      gdr.put(i, result._4(i))
                    }
                    (result._1, result._2 :+ this, recordSchema, gdr)
                  })
    }
  }

  case class HTArray(value: HigherType, minsize: Int = 1) extends HigherType {
    def namePrefix(): String = "array_"    
    def getAvroSchema(): Schema = {
      return Schema.createArray(value.getAvroSchema())
    }
    def resetUsageStatistics(): Unit = {
      linesProcessed = 0
      value.resetUsageStatistics()
    }
    def prettyprint(offset: Int, showStatistics: Boolean, denom: Int) {
      print(" " * offset)
      print("HTArray " + name())
      if (showStatistics && denom > 0) {
        print("  (" + (100 * (linesProcessed / denom.toFloat)) + "%)")
      }
      println()

      value.prettyprint(offset+1, showStatistics, denom)
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, List[HigherType], Schema, Any)] = {
      def processChildren(minChildrenToGo: Int, inChunk: ParsedChunk): List[(ParsedChunk, List[HigherType], List[Schema], List[Any])] = {
          val childList = value.processChunk(inChunk)
          if (childList.length == 0) {
            List()
          } else {
            val futureResults = childList.flatMap(childTuple => processChildren(minChildrenToGo-1, childTuple._1).map(rht => (rht._1,
                                                                                                                              childTuple._2 ++ rht._2,
                                                                                                                              List(childTuple._3) ++ rht._3,
                                                                                                                              List(childTuple._4) ++ rht._4)))
            if (minChildrenToGo > 1) {
              futureResults
            } else {
              childList.map(x => (x._1, x._2, List(x._3), List(x._4))) ++ futureResults
            }
          }
      }
      val results = processChildren(minsize, chunk)
      results.map(result => {
                    val gda = new GenericData.Array[Any](result._4.length, getAvroSchema())
                    result._4.foreach(gda.add(_))
                    (result._1, result._2 :+ this, getAvroSchema(), gda)
                  })
    }
  }
  case class HTUnion(value: List[HigherType]) extends HigherType {
    // Note: Avro can't handle directly nested union structures.  We remove
    // such structures in the Rewrite module.
    def namePrefix(): String = "union_"    
    def getAvroSchema(): Schema = {
      try {
        return Schema.createUnion(value.map(v => v.getAvroSchema()).distinct)
      } catch {
        case NonFatal(exc) => {
          Schema.createUnion(value.map(v => v.getAvroSchema()).distinct)
        }
      }
    }
    def resetUsageStatistics(): Unit = {
      linesProcessed = 0
      value.foreach(_.resetUsageStatistics())
    }
    def prettyprint(offset: Int, showStatistics: Boolean, denom: Int) {
      print(" " * offset)
      print("HTUnion " + name())
      if (showStatistics && denom > 0) {
        print("  (" + (100 * (linesProcessed / denom.toFloat)) + "%)")
      }
      println()

      for (v <- value) {
        v.prettyprint(offset+1, showStatistics, denom)
      }
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, List[HigherType], Schema, Any)] = {
      val results = value.flatMap(_.processChunk(chunk))
      results.map(r => (r._1, r._2 :+ this, r._3, r._4))
    }
  }

  case class HTBaseType(value: BaseType) extends HigherType {
    def namePrefix(): String = "base_"        
    def getAvroSchema(): Schema = {
      return value.getAvroSchema()
    }
    def resetUsageStatistics(): Unit = {
      linesProcessed = 0
    }
    def prettyprint(offset: Int, showStatistics: Boolean, denom: Int) {
      print(" " * offset)
      print("HTBaseType(" + value + ") " + name())
      if (showStatistics && denom > 0) {
        print("  (" + (100 * (linesProcessed / denom.toFloat)) + "%)")
      }
      println()      
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, List[HigherType], Schema, Any)] = {
      if (chunk.length > 0 && (value == chunk(0))) {
        val inputElt = chunk(0).getValue()
        List((chunk.slice(1, chunk.length), List(this), getAvroSchema(), chunk(0).getValue()))
      } else {
        List()
      }
    }
  }
  case class HTNoop() extends HigherType {
    def namePrefix(): String = "noop_"
    def getAvroSchema(): Schema = {
      throw new RuntimeException("Avro schema undefined for noop")
    }
    def resetUsageStatistics(): Unit = {
      linesProcessed = 0
    }
    def prettyprint(offset: Int, showStatistics: Boolean, denom: Int) {
      print(" " * offset)
      println("HTNoop")      
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, List[HigherType], Schema, Any)] = {
      throw new RuntimeException("processChunk() undefined for noop")
    }
  }
  case class HTArrayFW(value: HigherType, size: Int) extends HigherType {
    def namePrefix(): String = "array_"            
    def getAvroSchema(): Schema = {
      return Schema.createArray(value.getAvroSchema())
    }
    def resetUsageStatistics(): Unit = {
      linesProcessed = 0
      value.resetUsageStatistics()
    }
    def prettyprint(offset: Int, showStatistics: Boolean, denom: Int) {
      print(" " * offset)
      print("HTArrayFW " + name() + " size=" + size )
      if (showStatistics && denom > 0) {
        print("  (" + (100 * (linesProcessed / denom.toFloat)) + "%)")
      }
      println()      
      value.prettyprint(offset+1, showStatistics, denom)
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, List[HigherType], Schema, Any)] = {
      def processChildren(numChildrenToGo: Int, inChunk: ParsedChunk): List[(ParsedChunk, List[HigherType], List[Schema], List[Any])] = {
        val childList = value.processChunk(inChunk)
        if (childList.length == 0) {
          List()
        } else {
          if (numChildrenToGo == 1) {
            childList.map(x => (x._1, x._2, List(x._3), List(x._4)))
          } else {
            childList.flatMap(childTuple => processChildren(numChildrenToGo-1, childTuple._1).map(rht => (rht._1,
                                                                                                          childTuple._2 ++ rht._2,
                                                                                                          List(childTuple._3) ++ rht._3,
                                                                                                          List(childTuple._4) ++ rht._4)))
          }
        }
      }
      val results = processChildren(size, chunk)
      results.map(result => {
                    val gda = new GenericData.Array[Any](size, getAvroSchema())
                    result._4.foreach(gda.add(_))
                    (result._1, result._2 :+ this, getAvroSchema(), gda)
                  })
    }
  }

  case class HTOption(value: HigherType) extends HigherType {
    def namePrefix(): String = "option_"                
    def getAvroSchema(): Schema = {
      return value.getAvroSchema()
    }
    def resetUsageStatistics(): Unit = {
      linesProcessed = 0
      value.resetUsageStatistics()
    }
    def prettyprint(offset: Int, showStatistics: Boolean, denom: Int) {
      print(" " * offset)
      print("HTOption " + name())
      if (showStatistics && denom > 0) {
        print("  (" + (100 * (linesProcessed / denom.toFloat)) + "%)")
      }
      println()      
      value.prettyprint(offset+1, showStatistics, denom)
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, List[HigherType], Schema, Any)] = {
      val results = value.processChunk(chunk).map(t=>(t._1, t._2 :+ this, t._3, t._4)) :+ (chunk, List(this), getAvroSchema(), getDefaultValue())
      results
    }
  }

  abstract class Prophecy
  case class EmptyProphecy() extends Prophecy
  case class BaseProphecy(value: BaseType) extends Prophecy
  case class StructProphecy(css: List[Chunks]) extends Prophecy
  case class ArrayProphecy(prefix: Chunks, middle:Chunks, postfix:Chunks) extends Prophecy
  case class UnionProphecy(css: List[Chunks]) extends Prophecy

}
