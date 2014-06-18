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

import org.scalatest._

class Test extends FlatSpec with BeforeAndAfter with Matchers {
  before {
  }

  /*************************
   * Basic Parser
   ************************/
  val basicFileA = "12.1 10\n12.1 10"
  val basicFileB = "12.1 10\nfoo 10\n12.1 10\nfoo 10\n"
  val basicFileC = "100 Hello A 0.32\n999 There X 3.147\n"
  val basicFileD = "Foo [10] blah 99.0\nFoo [20] bloop 22.1\nFooBar [333] bleep 35.5\n"

  "Raw Data Parser" should "parse basic file A" in {
    Parse.parseString(basicFileA) should be (List(List(PFloat(), PInt()), List(PFloat(), PInt())))
  }

  it should "parse basic file B" in {
    Parse.parseString(basicFileB) should be (List(List(PFloat(), PInt()),
                                                  List(PAlphanum(), PInt()),
                                                  List(PFloat(), PInt()),
                                                  List(PAlphanum(), PInt())))
  }

  it should "parse basic file C" in {
    Parse.parseString(basicFileC) should be (List(List(PInt(), PAlphanum(), PAlphanum(), PFloat()),
                                                  List(PInt(), PAlphanum(), PAlphanum(), PFloat())))
  }

  it should "parse basic file D" in {
    Parse.parseString(basicFileD) should be (List(List(PAlphanum(), PMetaToken(POther(), List(PInt()), POther()), PAlphanum(), PFloat()),
                                                  List(PAlphanum(), PMetaToken(POther(), List(PInt()), POther()), PAlphanum(), PFloat()),
                                                  List(PAlphanum(), PMetaToken(POther(), List(PInt()), POther()), PAlphanum(), PFloat())))
  }

  /*************************
   * Structure Inference
   ************************/
  "Structure Inference" should "infer structure for basic file A" in {
    Infer.discover(Parse.parseString(basicFileA)) should be (HTStruct(List(HTBaseType(PFloat()), HTBaseType(PInt()))))
  }

  it should "infer structure for basic file B" in {
    Infer.discover(Parse.parseString(basicFileB)) should be (HTStruct(List(HTUnion(List(HTStruct(List(HTBaseType(PAlphanum()), HTBaseType(PInt()))),
                                                                                        HTStruct(List(HTBaseType(PFloat()), HTBaseType(PInt()))))))))
  }

  it should "infer structure in this temporary file" in {
    Infer.discover(Parse.parseString("10\n5.0\n")) should be (HTStruct(List(HTUnion(List(HTBaseType(PInt()), HTBaseType(PFloat()))))))
  }

  it should "infer structure for basic file C" in {
    Infer.discover(Parse.parseString(basicFileC)) should be (HTStruct(List(HTBaseType(PInt()), HTBaseType(PAlphanum()), HTBaseType(PAlphanum()), HTBaseType(PFloat()))))
  }

  it should "infer structure for basic file D" in {
    Infer.discover(Parse.parseString(basicFileD)) should be (HTStruct(List(HTBaseType(PAlphanum()), HTStruct(List(HTBaseType(POther()), HTBaseType(PInt()), HTBaseType(POther()))), HTBaseType(PAlphanum()), HTBaseType(PFloat()))))
  }

  /*************************
   * Rewrite Rules
   ************************/
  "Rewrite Rules" should "rewrite singleton structs" in {
    val parsed = Parse.parseString("10")
    val shouldBeInferred = HTStruct(List(HTBaseType(PInt())))
    Rewrite.refineAll(shouldBeInferred, parsed) should be (HTBaseType(PInt()))
  }

  it should "cleanup empty structs" in {
    val parsed = Parse.parseString("")
    val shouldBeInferred = HTStruct(List())
    Rewrite.refineAll(shouldBeInferred, parsed) should be (HTBaseType(PEmpty()))
  }

  it should "rewrite singleton unions" in {
    val parsed = Parse.parseString("10")
    val shouldBeInferred = HTUnion(List(HTBaseType(PInt())))
    Rewrite.refineAll(shouldBeInferred, parsed) should be (HTBaseType(PInt()))
  }

  it should "cleanup empty unions" in {
    val parsed = Parse.parseString("")
    val shouldBeInferred = HTUnion(List())
    Rewrite.refineAll(shouldBeInferred, parsed) should be (HTBaseType(PVoid()))
  }

  it should "clean up uniform structs" in {
    val parsed = Parse.parseString("1 1 1")
    val shouldBeInferred = HTStruct(List(HTBaseType(PInt()), HTBaseType(PInt()), HTBaseType(PInt())))
    Rewrite.refineAll(shouldBeInferred, parsed) should be (HTArrayFW(HTBaseType(PInt()), 3))
  }

  it should "eliminate common union postfix type A" in {
    val parsed = Parse.parseString("1 10\n5.5 10\n")
    val shouldBeInferred = HTUnion(List(HTStruct(List(HTBaseType(PInt()), HTStruct(List(HTBaseType(PAlphanum()), HTBaseType(PInt()))))),
                                    HTStruct(List(HTBaseType(PFloat()), HTStruct(List(HTBaseType(PAlphanum()), HTBaseType(PInt())))))))
    Rewrite.refineAll(shouldBeInferred, parsed) should be (HTStruct(List(HTUnion(List(HTBaseType(PInt()),
                                                                                      HTBaseType(PFloat()))),
                                                                         HTStruct(List(HTBaseType(PAlphanum()), HTBaseType(PInt()))))))
  }

  it should "eliminate common union postfix type B" in {
    val parsed = Parse.parseString("1.0 1\n1\n1.0 1\n1\n")
    val shouldBeInferred = HTUnion(List(HTStruct(List(HTBaseType(PFloat()), HTBaseType(PInt()))),
                                        HTBaseType(PInt())))
    Rewrite.refineAll(shouldBeInferred, parsed) should be (HTStruct(List(HTOption(HTStruct(List(HTBaseType(PFloat())))),
                                                                         HTBaseType(PInt()))))
  }

  it should "combine adjacent string constants" in {
    val parsed = Parse.parseString("foo bar 10\n")
    val shouldBeInferred = HTStruct(List(HTBaseType(PStringConst("foo")),
                                     HTBaseType(PStringConst("bar")),
                                     HTBaseType(PInt())))
    Rewrite.refineAll(shouldBeInferred, parsed) should be (HTStruct(List(HTBaseType(PStringConst("foobar")),
                                                                         HTBaseType(PInt()))))
  }

  /*************************
   * Avro Schema Generation
   ************************/
  "Avro Schema Generator" should "generate a correct schema for structure A" in {
    val targetSchema = Schema.createRecord("record_1", "RECORD", "", false)
    val slist0 = new java.util.ArrayList[Schema.Field]()
    slist0.add(new Schema.Field("base_0", Schema.create(Schema.Type.INT), "", null))
    targetSchema.setFields(slist0)

    HigherType.resetFieldCount()
    HigherType.getAvroSchema(HTStruct(List(HTBaseType(PInt())))) should be (targetSchema)
  }

  it should "generate a correct schema for structure B" in {
    val targetSchema = Schema.createRecord("record_1", "RECORD", "", false)
    val slist1 = new java.util.ArrayList[Schema.Field]()
    slist1.add(new Schema.Field("base_0", Schema.create(Schema.Type.DOUBLE), "", null))
    targetSchema.setFields(slist1)

    HigherType.resetFieldCount()    
    HigherType.getAvroSchema(HTStruct(List(HTBaseType(PFloat())))) should be (targetSchema)
  }

  it should "generate a correct schema for structure C" in {
    val targetSchema = Schema.createRecord("record_1", "RECORD", "", false)
    val slist2 = new java.util.ArrayList[Schema.Field]()
    slist2.add(new Schema.Field("base_0", Schema.create(Schema.Type.STRING), "", null))
    targetSchema.setFields(slist2)

    HigherType.resetFieldCount()        
    HigherType.getAvroSchema(HTStruct(List(HTBaseType(PAlphanum())))) should be (targetSchema)
  }

  it should "generate a correct schema for structure D" in {
    val targetSchema = Schema.createRecord("record_1", "RECORD", "", false)
    val slist3 = new java.util.ArrayList[Schema.Field]()
    slist3.add(new Schema.Field("base_0", Schema.create(Schema.Type.STRING), "", null))
    targetSchema.setFields(slist3)

    HigherType.resetFieldCount()        
    HigherType.getAvroSchema(HTStruct(List(HTBaseType(PStringConst("foo"))))) should be (targetSchema)
  }

  it should "generate a correct schema for structure E" in {
    val targetSchema = Schema.createRecord("record_3", "RECORD", "", false)
    val slist4 = new java.util.ArrayList[Schema.Field]()
    slist4.add(new Schema.Field("base_0", Schema.create(Schema.Type.INT), "", null))
    slist4.add(new Schema.Field("base_1", Schema.create(Schema.Type.DOUBLE), "", null))
    slist4.add(new Schema.Field("base_2", Schema.create(Schema.Type.STRING), "", null))    
    targetSchema.setFields(slist4)

    HigherType.resetFieldCount()        
    HigherType.getAvroSchema(HTStruct(List(HTBaseType(PInt()), HTBaseType(PFloat()), HTBaseType(PAlphanum())))) should be (targetSchema)
  }

  it should "generate a correct schema for structure F" in {
    val targetSchema = Schema.createRecord("record_5", "RECORD", "", false)
    val reclist5 = new java.util.ArrayList[Schema.Field]()
    reclist5.add(new Schema.Field("base_0", Schema.create(Schema.Type.INT), "", null))
    reclist5.add(new Schema.Field("base_1", Schema.create(Schema.Type.DOUBLE), "", null))
    val unionlist5 = new java.util.ArrayList[Schema]()
    unionlist5.add(Schema.create(Schema.Type.DOUBLE))
    unionlist5.add(Schema.create(Schema.Type.STRING))
    reclist5.add(new Schema.Field("union_4", Schema.createUnion(unionlist5), "", null))
    targetSchema.setFields(reclist5)

    HigherType.resetFieldCount()        
    HigherType.getAvroSchema(HTStruct(List(HTBaseType(PInt()), HTBaseType(PFloat()), HTUnion(List(HTBaseType(PFloat()), HTBaseType(PAlphanum())))))) should be (targetSchema)
  }

  /*************************
   * Avro Record Construction
   ************************/
  "Record Parser" should "obtain correct tuples for basic input A" in {
    val testData = "12.1 10\n12.1 10"
    val ht = Infer.discover(Parse.parseString(testData))
    val parsedTuples = Processor.parse(testData, ht)
    parsedTuples(0).get(0) should be (12.1)
    parsedTuples(0).get(1) should be (10)
    parsedTuples(1).get(0) should be (12.1)
    parsedTuples(1).get(1) should be (10)
  }

  it should  "obtain correct tuples for basic input B" in {
    val testData = "10\n12.1"
    val ht = Infer.discover(Parse.parseString(testData))
    val parsedTuples = Processor.parse(testData, ht)
    parsedTuples(0).get(0) should be (10)
    parsedTuples(1).get(0) should be (12.1)    
  }

  /*************************
   * Parser Serialization
   ************************/
  "Parser serializer" should "serialize a faithful parser for basic input A" in {
    val testData = "12.1 10\n12.1 10"
    val ht = Infer.discover(Parse.parseString(testData))
    val reconHt = HigherType.loadFromBytes(HigherType.dumpToBytes(ht))
    ht should be (reconHt)
  }

  it should "serialize a faithful parser for basic input B" in {
    val testData = "10\n12.1"
    val ht = Infer.discover(Parse.parseString(testData))
    val reconHt = HigherType.loadFromBytes(HigherType.dumpToBytes(ht))
    ht should be (reconHt)
  }

  /**************************
   * End-to-end tests on large files
   *************************/
  def testfile(fname: String): List[GenericRecord] = {
    println("Got to this point? 1")
    val filechunks = Parse.parseFileWithoutMeta(fname)
    println("Got to this point? 2")
    println("# of chunks: " + filechunks.length)
    println("Got to this point? 2.2")    
    val it = Infer.discover(filechunks.slice(0, 1))
    println("Got to this point?  2.5")
    for (i <- 1 to 11) {
      val inferredType = Infer.discover(filechunks.slice(0, i))
      println("Tested up to " + i)
    }
    val inferredType = Infer.discover(filechunks)
    println("Got to this point? 3")
    val avroSchema = HigherType.getAvroSchema(inferredType)
    println("Got to this point? 4")     
    return filechunks.map(fc => HigherType.processChunk(inferredType, fc))
  }
  val fname1 = "angioplasty.txt"

  //"End-to-end test" should "yield a parser that can process all of file " + fname1 in {
//    val gdrs = testfile("src/samples/textdata/" + fname1)
//    gdrs.length should be (10)
//  }
  
  after {
  }
}
