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
import scala.collection.JavaConversions._
import scala.collection.mutable._
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.codehaus.jackson.JsonNode;

object RBTypes {
  type Chunk = List[BaseType]
  type Chunks = List[Chunk]

  /** The BaseType is an atomic type (int, str, etc) that can be either
   *  parsed or inferred post-parsing.
   */
  abstract class BaseType {
    def getAvroSchema(): Schema
  }
  trait ParsedValue[T] extends BaseType {
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
  }
  abstract class HigherType {
    import HigherType.fieldCount
    val selfCount = fieldCount
    fieldCount += 1
    def getAvroSchema(): Schema
    def name(): String = {
      return namePrefix() + selfCount
    }
    def namePrefix(): String
    def getDocString(): String = {
      return ""
    }
    def getDefaultValue(): JsonNode = {
      return null
    }
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
  }
  case class HTArray(value: HigherType) extends HigherType {
    def namePrefix(): String = "array_"    
    def getAvroSchema(): Schema = {
      return Schema.createArray(value.getAvroSchema())
    }
  }
  case class HTUnion(value: List[HigherType]) extends HigherType {
    // Note: this won't handle doubled union structures.  We should ensure
    // that such structures don't exist prior to processing with getAvroSchema
    def namePrefix(): String = "union_"    
    def getAvroSchema(): Schema = {
      return Schema.createUnion(value.map(v => getAvroSchema()))
    }
  }

  case class HTBaseType(value: BaseType) extends HigherType {
    def namePrefix(): String = "base_"        
    def getAvroSchema(): Schema = {
      return value.getAvroSchema()
    }
  }
  case class HTArrayFW(value: HigherType, size: Int) extends HigherType {
    def namePrefix(): String = "array_"            
    def getAvroSchema(): Schema = {
      return Schema.createArray(value.getAvroSchema())
    }
  }
  case class HTOption(value: HigherType) extends HigherType {
    def namePrefix(): String = "option_"                
    def getAvroSchema(): Schema = {
      return value.getAvroSchema()
    }
  }

  abstract class Prophecy
  case class BaseProphecy(value: BaseType) extends Prophecy
  case class StructProphecy(css: List[Chunks]) extends Prophecy
  case class ArrayProphecy(prefix: Chunks, middle:Chunks, postfix:Chunks) extends Prophecy
  case class UnionProphecy(css: List[Chunks]) extends Prophecy

}
