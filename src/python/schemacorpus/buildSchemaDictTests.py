#!/usr/bin/python
import string, sys, os, bisect, heapq, random, time, httplib
import urllib, urllib2, simplejson, pickle
import freebaseUtils as fb
from avro import schema, datafile, io

##################################################################
# This program is part of the support infrastructure for
# building and testing the Schema Dictionary tool.  It takes
# as input databases acquired by obtainDatabase.py.  It transforms
# these databases into a format that is suitable for insertion
# into the Schema Dictionary.
#
# This tool can be used in two modes:
# a) Create a set of databases that populates the SD as
#    exhaustively as possible
# b) Create two different database corpora for SD testing.  The
#    first set is inserted into the SD.  The second set is
#    used as input to the SD, to see if SD finds the correct
#    match.
#
##################################################################

#
# Grab property type info from Freebase
#
def populateSchemaTypesFile(schemaElts, schemaNamesFile, schemaTypesFile, credentials):
  schemaNames = []
  schemaTypes = []

  # Iterate through all the schema elements.  These are all identifiers for objects of type "/type/property"
  for schemaElt in schemaElts:
    if not schemaElt.startswith("/"):
      schemaNames.append(schemaElt)
      schemaTypes.append("string")
      continue

    # Grab type info for the given named property.  We're interested in the human-readable 'name'
    # and the 'expected_type' structure
    query = [{"type": "/type/property",
              "id": schemaElt,
              "name": None,
              "expected_type": [{}]
              }]
    results = []
    def resultBuilder(resultList):
      for result in resultList:
        results.append(result)
    fb.readBatch(query, resultBuilder, credentials, maxResults=1)

    if len(results) > 0:
      r = results[0]
      schemaNames.append(r["name"])
      stName = "string"

      if len(r["expected_type"]) > 0:
        propertyDataType = r["expected_type"][0]["id"]
        if propertyDataType.startswith("/type/"):
          stName = propertyDataType
      schemaTypes.append(stName)
    else:
      schemaNames.append("")
      schemaTypes.append("")

  # Now record the data so future runs can benefit
  f = open(schemaNamesFile, "w")
  try:
    f.write("\t".join(schemaNames))
  finally:
    f.close()

  f = open(schemaTypesFile, "w")
  try:
    f.write("\t".join(schemaTypes))
  finally:
    f.close()

  # All done
  return schemaNames, schemaTypes
  
#
# Load all the datasets from the downloaded Freebase tree
#
def loadDatabases(inputDataDir, credentials):
  # A bunch of file-load utility functions
  def loadDataElts(inFile):
    allData = []
    f = open(inFile)
    try:
      for r in f:
        rowElts = r.split("\t")
        rowElts = [x.strip() for x in rowElts]
        allData.append(rowElts)
    finally:
      f.close()
    return allData

  def loadTypeLabel(inFile):
    f = open(inFile)
    try:
      typeLabel = "\n".join(f.readlines()).strip()
      return typeLabel
    finally:
      f.close()
    return None

  def loadTabSeparatedLine(inFile):
    f = open(inFile)
    try:
      return [x.strip() for x in f.readline().split("\t")]
    finally:
      f.close()
    return None

  # Iterate through every known type.  There's one for every database we intend to build.
  # So for the current version of Freebase there's probably roughly 1800 of these.
  allResults = []
  totalCount = len(os.listdir(inputDataDir))
  curCount = 0
  for subdirname in os.listdir(inputDataDir):
    if subdirname.startswith("d-"):
      subdirPath = os.path.join(inputDataDir, subdirname)

      dataFile = os.path.join(subdirPath, "data.txt")
      typeLabelFile = os.path.join(subdirPath, "typeLabel.txt")
      schemaFile = os.path.join(subdirPath, "schema.txt")
      schemaNamesFile = os.path.join(subdirPath, "schemanames.txt")
      schemaTypesFile = os.path.join(subdirPath, "schematypes.txt")

      # Load in the data that's definitely available from running obtainDatabase.py
      dataElts = loadDataElts(dataFile)
      typeLabel = loadTypeLabel(typeLabelFile)
      schemaElts = loadTabSeparatedLine(schemaFile)

      #
      # We now want to further annotate the information with info on each schema attribute.
      # In particular, we want the human-readable name (not the Freebase technical name) and
      # we want the basic type: int, string, date, etc.
      #
      # If the info is available from disk, great: we load it.  If not, we grab it from Freebase
      # and write it to disk for future reference.
      #
      if os.path.exists(schemaNamesFile) and os.path.exists(schemaTypesFile):
        schemaEltNames = loadTabSeparatedLine(schemaNamesFile)
        schemaEltTypes = loadTabSeparatedLine(schemaTypesFile)
      else:
        schemaEltNames, schemaEltTypes = populateSchemaTypesFile(schemaElts, schemaNamesFile, schemaTypesFile, credentials)

      if dataElts and schemaElts:
        allResults.append((typeLabel, schemaElts, schemaEltNames, schemaEltTypes, dataElts))

    # Progress counter
    curCount += 1
    print "Processed", curCount, "of", totalCount

  return allResults

#
# We need to translate data types between the Freebase universe and Avro.
# Some types don't have a good match (e.g., there's no date/time value within Avro)
# and others match imperfectly (e.g., the two different enums).  The translations
# here are subject to revision
#
global avroTypeTranslator
avroTypeTranslator = {}
avroTypeTranslator["/type/boolean"] = "boolean"
avroTypeTranslator["/type/content"] = "bytes"
avroTypeTranslator["/type/datetime"] = "string"
avroTypeTranslator["/type/enumeration"] = "string"
avroTypeTranslator["/type/float"] = "float"
avroTypeTranslator["/type/int"] = "long"
avroTypeTranslator["/type/lang"] = "string"
avroTypeTranslator["/type/media_type"] = "string"
avroTypeTranslator["/type/object"] = "string"
avroTypeTranslator["/type/rawstring"] = "bytes"
avroTypeTranslator["/type/text"] = "string"
avroTypeTranslator["/type/text_encoding"] = "string"
avroTypeTranslator["/type/type"] = "string"
avroTypeTranslator["/type/uri"] = "string"
avroTypeTranslator["/type/user"] = "string"


#
# Take as input all the entire corpus databases, and output in an Avro format that
# can be processed by SchemaDictionary.  This is useful when assembling a Schema
# Dictionary that's part of a genuine application.
#
def emitAllDatabases(knownDatabases, outputDir):
  emitData(knownDatabases, outputDir, None, 1.0)

#
# Take as input all the entire corpus databases, and divide into two parts.  The first
# part is the "known schema set" for SchemaDictionary.  The second part is a "test set"
# of databases.  We create these sets by taking the original database corpus and cutting
# each table into two parts.
#
# By start by inserting everything in the "known schema set" into a Schema Dictionary
# instance.  We then query SchemaDictionary with every database from the "test set".
# Of course, we use only the raw data (not the metadata) during each test; the whole
# point of SD is that it synthesizes new metadata for novel databases.
#
# If SchemaDictionary is doing its job, then every item in the test set should be correctly
# matched to its known counterpart in the known-schema-set.
#
def emitTestDatabases(knownDatabases, outputDir1, outputDir2):
  emitData(knownDatabases, outputDir1, outputDir2, 0.5)

#
# Utility method that implements both of the above publicly-usable functions
#
def emitData(knownDatabases, outputDir1, outputDir2, frac1):
  for db in knownDatabases:                                  
    typeLabel, schemaElts, schemaEltNames, schemaEltTypes, dataElts = db
    schemaEltNames = map(lambda x: x.replace("'", "").replace('"', ''), schemaEltNames)
    outputFilename1 = os.path.join(outputDir1, typeLabel[1:].replace("/", "-").replace(" ", "_") + ".avro")
    outputFilename2 = os.path.join(outputDir2, typeLabel[1:].replace("/", "-").replace(" ", "_") + ".avro")

    # Use the input schema to Build the Avro schema description 
    schemaStr = '''{
        "type": "record",
        "name": "''' + typeLabel + '''",
        "namespace": "AVRO",
        "fields": ['''

    seenFields = set()
    for idx, schemaPair in enumerate(zip(schemaEltNames, schemaEltTypes)):
      eltName, eltType = schemaPair
      if eltName in seenFields:
        continue

      if len(seenFields) > 0:
        schemaStr += ","
        schemaStr += "\n"
      seenFields.add(eltName)
      schemaStr += '\t{ "name": "' + eltName + '", "type": "' + avroTypeTranslator.get(eltType, "string") + '"}'

    schemaStr += '''\n\t]
    }'''

    # Build the Avro data writer
    avroSchema = schema.parse(schemaStr)    
    record_writer1 = io.DatumWriter(avroSchema)
    avroWriter1 = datafile.DataFileWriter(open(outputFilename1, 'wb'),
                                          record_writer1,
                                          writers_schema=avroSchema,
                                          codec = 'null')
    avroWriter2 = None
    if frac1 < 1.0:
      record_writer2 = io.DatumWriter(avroSchema)
      avroWriter2 = datafile.DataFileWriter(open(outputFilename2, 'wb'),
                                            record_writer2,
                                            writers_schema=avroSchema,
                                            codec = 'null')
      
    # Emit the data, close down the stream
    curDataList = []
    for dataElt in dataElts:
      data = {}
      seenFields = set()
      for eltName, eltVal, eltType in zip(schemaEltNames, dataElt, schemaEltTypes):
        if eltName in seenFields:
          continue
        seenFields.add(eltName)
        eltType = avroTypeTranslator.get(eltType, eltType)
        if eltType == "string":
          data[eltName] = eltVal
        elif eltType == "long":
          if len(eltVal) == 0:
            data[eltName] = 0
          else:
            data[eltName] = long(eltVal)
        elif eltType == "float":
          if len(eltVal) == 0:
            data[eltName] = 0.0
          else:
            data[eltName] = float(eltVal)
        elif eltType == "boolean":
          data[eltName] = bool(eltVal)
        elif eltType == "bytes":
          data[eltName] = bytes(eltVal)
        else:
          data[eltName] = eltVal
      curDataList.append(data)

    numDataIn1 = int(round(frac1 * len(curDataList)))

    for d in curDataList[0:numDataIn1]:
      avroWriter1.append(d)
    if len(curDataList) - numDataIn1 > 0:
      for d in curDataList[numDataIn1:]:
        avroWriter2.append(d)

    avroWriter1.close()
    if not avroWriter2 is None:
      avroWriter2.close()


##################################################################
# main
##################################################################
if __name__ == "__main__":
  if len(sys.argv) < 3:
    print "Usage: buildSchemaDictTests.py <inputDataDir> (-entireCorpus <outputDir> | -testCorpus <outputDir1> <outputDir2>) username password"
    sys.exit(0)

  inputDir = sys.argv[1]
  directive = sys.argv[2]

  if not os.path.exists(inputDir):
    raise "Input file does not exist:", inputDir

  if "-entireCorpus" == directive:
    outputDir = sys.argv[3]
    username = sys.argv[4]
    password = sys.argv[5]
    credentials = fb.login(username, password)

    knownDatabases = loadDatabases(inputDir, credentials)
    if not os.path.exists(outputDir):
      os.mkdir(outputDir)
      
    emitAllDatabases(knownDatabases, outputDir)
  elif "-testCorpus" == directive:
    outputDir1 = sys.argv[3]
    outputDir2 = sys.argv[4]
    username = sys.argv[5]
    password = sys.argv[6]
    credentials = fb.login(username, password)
    
    knownDatabases = loadDatabases(inputDir, credentials)
    if not os.path.exists(outputDir1):
      os.mkdir(outputDir1)
    if not os.path.exists(outputDir2):
      os.mkdir(outputDir2)

    emitTestDatabases(knownDatabases, outputDir1, outputDir2)
  else:
    print "Must indicate -entireCorpus or -testCorpus"
    sys.exit(0)

  
