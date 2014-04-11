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
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData.Record;
import org.codehaus.jackson.JsonNode;
import Parse._
import RBTypes._

/*************************************************
 * Convert a parsed text file into an example of an
 * inferred schema.
 ************************************************/
object Processor {
  def parseFile(fname: String, ht: HigherType): List[GenericRecord] = {
    val chunks = Parse.parseFileWithoutMeta(fname)
    process(chunks, ht)
  }
  def parse(str: String, ht: HigherType): List[GenericRecord] = {
    val chunks = Parse.parseStringWithoutMeta(str)
    process(chunks, ht)
  }
  def process(chunks: ParsedChunks, ht: HigherType): List[GenericRecord] = {
    val schema = HigherType.getAvroSchema(ht)
    chunks.map(HigherType.processChunk(ht, _))
  }
}
