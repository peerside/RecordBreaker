#!/usr/bin/python
import string, sys, os, bisect, heapq, random, time, httplib
import urllib, urllib2, simplejson, pickle
import freebaseUtils as fb

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
      propertyDataType = r["expected_type"][0]["id"]
      if propertyDataType.startswith("/type/"):
        schemaTypes.append(propertyDataType)
      else:
        schemaTypes.append("string")
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
# Take as input all the entire corpus databases, and output in an Avro format that
# can be processed by SchemaDictionary.  This is useful when assembling a Schema
# Dictionary that's part of a genuine application.
#
def emitAllDatabases(knownDatabases, outputDir):
  print "Not yet implemented"

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
  print "Not yet implemented"

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
    emitAllDatabases(knownDatabases, outputDir)
  elif "-testCorpus" == directive:
    outputDir1 = sys.argv[3]
    outputDir2 = sys.argv[4]
    username = sys.argv[5]
    password = sys.argv[6]
    credentials = fb.login(username, password)
    
    knownDatabases = loadDatabases(inputDir, credentials)
    emitTestDatabases(knownDatabases, outputDir1, outputDir2)
  else:
    print "Must indicate -entireCorpus or -testCorpus"
    sys.exit(0)

  
