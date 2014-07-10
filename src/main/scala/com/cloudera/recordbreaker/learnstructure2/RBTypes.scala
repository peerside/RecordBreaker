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
    def resetFieldCount(): Unit = {
      fieldCount = 0
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
    def processChunk(ht: HigherType, chunk: ParsedChunk): GenericRecord = {
      val pcResults = ht.processChunk(chunk).filter(x=>x._1.length == 0)  // We only want the parses that consume entire input
      pcResults.head._3 match {
        case a: GenericRecord => a
        case _ => throw new RuntimeException("Call to processChunk() yielded a non-GenericContainer result, indicating that source was not a record")
      }
    }
    def getFieldCount(): Int = {
      fieldCount += 1
      fieldCount-1
    }
  }
  abstract class HigherType {
    val fc = HigherType.getFieldCount()
    def getAvroSchema(): Schema
    def name(): String = {
      return namePrefix() + fc
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, Schema, Any)]
    def namePrefix(): String
    def getDocString(): String = {
      return ""
    }
    def getDefaultValue(): JsonNode = {
      return null
    }
    def prettyprint(offset: Int = 0)
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
    def prettyprint(offset: Int = 0) {
      print(" " * offset)
      println("HTStruct")
      for (v <- value) {
        v.prettyprint(offset+1)
      }
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, Schema, Any)] = {
      def processChildren(childrenToGo: List[HigherType], inChunk:ParsedChunk): List[(ParsedChunk, List[Schema], List[Any])] = {
        childrenToGo match {
          case curChild::rest => {
            val childLists = curChild.processChunk(inChunk)
            if (childLists.length == 0) {
              List()
            } else {
              if (rest.length == 0) {
                childLists.map(x => (x._1, List(x._2), List(x._3)))
              } else {
                childLists.flatMap(cl => processChildren(rest, cl._1).map(sr => (sr._1, cl._2 +: sr._2, cl._3 +: sr._3)))
              }
            }
          }
          case _ => List()
        }
      }
      val results = processChildren(value, chunk)
      results.map(result => {
                    val recordSchema = Schema.createRecord(name(), "RECORD", "", false)
                    val fieldMetadata = value.map(v => (v.name(), v.getDocString(), v.getDefaultValue()))
                    val schemaFields =
                      for ((reportedSchema, reportedMetadata) <- result._2.zip(fieldMetadata)) yield
                        new Schema.Field(reportedMetadata._1, reportedSchema, reportedMetadata._2, reportedMetadata._3)
                    recordSchema.setFields(ListBuffer(schemaFields: _*))
                    val gdr = new GenericData.Record(recordSchema)
                    for (i <- 0 to result._3.length-1) {
                      gdr.put(i, result._3(i))
                    }
                    (result._1, recordSchema, gdr)
                  })
    }
  }

  case class HTArray(value: HigherType, minsize: Int = 1) extends HigherType {
    def namePrefix(): String = "array_"    
    def getAvroSchema(): Schema = {
      return Schema.createArray(value.getAvroSchema())
    }
    def prettyprint(offset: Int = 0) {
      print(" " * offset)
      println("HTArray")
      value.prettyprint(offset+1)
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, Schema, Any)] = {
      def processChildren(minChildrenToGo: Int, inChunk: ParsedChunk): List[(ParsedChunk, List[Schema], List[Any])] = {
          val childList = value.processChunk(inChunk)
          if (childList.length == 0) {
            List()
          } else {
            val futureResults = childList.flatMap(childTuple => processChildren(minChildrenToGo-1, childTuple._1).map(rht => (rht._1,
                                                                                                                              List(childTuple._2) ++ rht._2,
                                                                                                                              List(childTuple._3) ++ rht._3)))
            if (minChildrenToGo > 1) {
              futureResults
            } else {
              childList.map(x => (x._1, List(x._2), List(x._3))) ++ futureResults
            }
          }
      }
      val results = processChildren(minsize, chunk)
      results.map(result => {
                    val gda = new GenericData.Array[Any](result._3.length, getAvroSchema())
                    result._3.foreach(gda.add(_))
                    (result._1, getAvroSchema(), gda)
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
          println("ERRPR ON " + value)
          Schema.createUnion(value.map(v => v.getAvroSchema()).distinct)
        }
      }
    }
    def prettyprint(offset: Int = 0) {
      print(" " * offset)
      println("HTUnion")
      for (v <- value) {
        v.prettyprint(offset+1)
      }
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, Schema, Any)] = {
      val results = value.flatMap(_.processChunk(chunk))
      results
    }
  }

  case class HTBaseType(value: BaseType) extends HigherType {
    def namePrefix(): String = "base_"        
    def getAvroSchema(): Schema = {
      return value.getAvroSchema()
    }
    def prettyprint(offset: Int = 0) {
      print(" " * offset)
      println("HTBaseType(" + value + ")")
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, Schema, Any)] = {
      if (chunk.length > 0 && (value == chunk(0))) {
        val inputElt = chunk(0).getValue()
        List((chunk.slice(1, chunk.length), getAvroSchema(), chunk(0).getValue()))
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
    def prettyprint(offset: Int = 0) {
      print(" " * offset)
      println("HTNoop")
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, Schema, Any)] = {
      throw new RuntimeException("processChunk() undefined for noop")
    }
  }
  case class HTArrayFW(value: HigherType, size: Int) extends HigherType {
    def namePrefix(): String = "array_"            
    def getAvroSchema(): Schema = {
      return Schema.createArray(value.getAvroSchema())
    }
    def prettyprint(offset: Int = 0) {
      print(" " * offset)
      println("HTArrayFW  size=" + size)
      value.prettyprint(offset+1)
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, Schema, Any)] = {
      def processChildren(numChildrenToGo: Int, inChunk: ParsedChunk): List[(ParsedChunk, List[Schema], List[Any])] = {
        val childList = value.processChunk(inChunk)
        if (childList.length == 0) {
          List()
        } else {
          if (numChildrenToGo == 1) {
            childList.map(x => (x._1, List(x._2), List(x._3)))
          } else {
            childList.flatMap(childTuple => processChildren(numChildrenToGo-1, childTuple._1).map(rht => (rht._1,
                                                                                                          List(childTuple._2) ++ rht._2,
                                                                                                          List(childTuple._3) ++ rht._3)))
          }
        }
      }
      val results = processChildren(size, chunk)
      results.map(result => {
                    val gda = new GenericData.Array[Any](size, getAvroSchema())
                    result._3.foreach(gda.add(_))
                    (result._1, getAvroSchema(), gda)
                  })
    }
  }

  case class HTOption(value: HigherType) extends HigherType {
    def namePrefix(): String = "option_"                
    def getAvroSchema(): Schema = {
      return value.getAvroSchema()
    }
    def prettyprint(offset: Int = 0) {
      print(" " * offset)
      println("HTOption")
      value.prettyprint(offset+1)
    }
    def processChunk(chunk: ParsedChunk): List[(ParsedChunk, Schema, Any)] = {
      val results = value.processChunk(chunk) :+ (chunk, getAvroSchema(), getDefaultValue())
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
