#!/usr/bin/python
import string, sys, os, bisect, heapq, random, time, httplib
import urllib, urllib2, simplejson, pickle
import freebaseUtils as fb

############################################################
# We want to test the Schema Dictionary tool that synthesizes labels
# for collections of anonymous data.  The SD tool examines a novel
# anonymous dataset and compares its characteristics (number of columns,
# column types, data distributions, etc) against a large set of
# known databases.
#
# After finding a close match between an input dataset and a
# known database, Schema Dictionary will find a mapping between
# the two sets of attributes.  This mapping yields names
# for the anonymous columns that we believe will be topically-relevant.
#
# However, for this whole mechanism to work we need a large collection
# of known schemas to populate the Schema Dictionary.  We also need
# a lot of known databases in order to test whether Schema Dictionary
# is doing a good job.
#
# The goal of this program is to collect enough databases to both
# populate and evaluate the Schema Dictionary tool.  Its current
# approach is to use data available via Freebase.  The code grabs a
# bunch of Freebase data, storing locally a large number of small
# topic-specific databases.  A database consists of a schema
# (some named columns) plus a bunch of data tuples.  We construct
# one database out of each Freebase type we can find.  We filter
# out a bunch of Freebase metatypes that don't yield useful
# real-world databases.
#
# In late September 2011, we used this code to obtain roughly 1860
# good small databases from Freebase.
#
############################################################

#
# A utility function to get year from a MQL datetime value
#
def getYear(date): 
  if not date: return ''
  return "[%s]" % date[0:4]


#
# Grab all possible types!  Feed these to the obtainDatabase() function
#
def obtainTypeLabels(credentials=None, maxLabels=10):
  query = [{ 'type': '/type/type',
             'key': [{'namespace': [{'key': [{'namespace':'/'}]}]}],
             'id': [{}],
             'name': [{}],
             'limit': min(100, maxLabels)}]

  #
  # Ask Freebase for metadata about the indicated type
  #
  typeLabels = []
  def typeLabelBuilder(resultList):
    for result in resultList:
      typeLabels.append(result["id"][0]["value"])
  fb.readBatch(query, typeLabelBuilder, credentials, maxResults=maxLabels)

  typeLabels = filter(lambda x: not x.startswith("/type"), typeLabels)
  typeLabels = filter(lambda x: not x.startswith("/freebase"), typeLabels)
  typeLabels = filter(lambda x: not x.startswith("/measurement_unit"), typeLabels)
  typeLabels = filter(lambda x: not x.startswith("/common"), typeLabels)

  return typeLabels
  
#
# obtainDatabase() grabs a topic-specific database from Freebase.
# As input it takes a topic, output dirname, and user credentials.
# It contacts Freebase, obtains the relevant schema, and then grabs
# a sample of items from the topic.
# It then outputs the schema and the sample database to the indicated
#  directory.
#
def obtainDatabase(typeLabel, credentials=None):
  #
  # Using a MQL query, grab the schema for the given typeLabel
  #
  startTime = time.time()
  typeDetailQuery = [{ 'type': '/type/type',
                       'id': typeLabel,
                       'name': [],
                       '/type/type/properties': [{'id': None, 'type': [], 'name': None, 'expected_type': None}],
                       'guid': None,
                       'limit': 1}]

  #
  # Ask Freebase for metadata about the indicated type
  #
  typeProfiles = []
  def typeProfileBuilder(resultList):
    typeProfiles.extend(resultList)
  fb.readBatch(typeDetailQuery, typeProfileBuilder, credentials, maxResults=1)

  #
  # Iterate through all discovered types (probably just 1)
  #
  schema = []
  tuples = []
  for discoveredType in typeProfiles[0:1]:
    #
    # Create a query based on the type-specific profile and
    # grab the type-appropriate data
    #
    typeSpecificQuery = { 'type': typeLabel,
                          'name': None,
                          'id': None,
                          'guid': None,
                          'limit': 1}

    schema = ["name"]
    for discoveredProperty in discoveredType["/type/type/properties"]:
      schema.append(discoveredProperty["id"])
      typeSpecificQuery[discoveredProperty["id"]] = [{}]
      print "DP", discoveredProperty
      
    #
    # Send the query to Freebase
    #
    typeSpecificQuery = [typeSpecificQuery]
    typeInstances = []
    def typeInstanceBuilder(resultList):
      typeInstances.extend(resultList)

    fb.readBatch(typeSpecificQuery, typeInstanceBuilder, credentials, maxResults=10)

    #
    # Dump the sample data for this type.  Equivalent to a relation.
    #
    for elt in typeInstances:
      newTuple = []
      try:
        newTuple.append(str(elt["name"]))
      except UnicodeEncodeError:
        newTuple.append("")
        
      for p in discoveredType["/type/type/properties"]:
        valueList = elt[p["id"]]
        if len(valueList) > 0:
          if valueList[0].has_key("name"):
            v = valueList[0]["name"]
            if v is None:
              newTuple.append("")
            else:
              try:
                newTuple.append(str(v))
              except UnicodeEncodeError:
                newTuple.append("")
          else:
            v = valueList[0]["value"]
            if v is None:
              newTuple.append("")
            else:
              try:
                newTuple.append(str(v))
              except UnicodeEncodeError:
                newTuple.append("")
        else:
          newTuple.append("")
      tuples.append(tuple(newTuple))

  #
  # Return schema info and the sample results
  #
  endTime = time.time()
  return typeLabel, schema, tuples

##################################################################
# main
##################################################################
if (__name__ == "__main__"):
  if (len(sys.argv) < 4):
    print "Usage: obtainDatabase.py (-getlabels <outfile>|-getdatabases <inputTypeList> <outdir>) <username> <password>"
    sys.exit(0)

  directive = sys.argv[1]
  if "-getlabels" == directive:
    outputFname = sys.argv[2]
    username = sys.argv[3]
    password = sys.argv[4]

    if os.path.exists(outputFname):
      raise "Output file already exists:", outputFname

    credentials = fb.login(username, password)
    typeLabels = obtainTypeLabels(credentials, maxLabels=10000)
    print "Obtained", len(typeLabels), "type labels"

    f = open(outputFname, "w")
    try:
      for typeLabel in typeLabels:
        f.write(typeLabel + "\n")
    finally:
      f.close()

  elif "-getdatabases" == directive:
    inputTypeFile = sys.argv[2]
    outputDirname = sys.argv[3]
    username = sys.argv[4]
    password = sys.argv[5]

    if not os.path.exists(outputDirname):
      os.mkdir(outputDirname)

    credentials = fb.login(username, password);
    f = open(inputTypeFile)
    startTime = time.time()
    try:
      typeCounter = 0
      numProcessed = 0
      allLabels = [x for x in f.readlines()]
      for typeLabel in allLabels:
        typeLabel = typeLabel.strip()
        curTime = time.time()
        deltaTime = int(round(curTime - startTime))
        print "Grabbing", typeLabel, "(", typeCounter, "of", len(allLabels), ")", " (", numProcessed, "processed on this execution so far, with", deltaTime, "seconds elapsed)"
        typeOutputDir = os.path.join(outputDirname, "d" + typeLabel.replace("/","-"))
        if not os.path.exists(typeOutputDir):

          try:
            processedType, schema, sampleData = obtainDatabase(typeLabel, credentials)
          except urllib2.HTTPError:
            print "Skipping", typeLabel, "due to HTTP error"
            time.sleep(10)
            continue
          except fb.MQLError:
            print "Skipping", typeLabel, "due to MQLError"
            time.sleep(10)
            continue

          typeLabelFile = os.path.join(typeOutputDir, "typeLabel.txt")
          schemaFile = os.path.join(typeOutputDir, "schema.txt")
          dataFile = os.path.join(typeOutputDir, "data.txt")
          os.mkdir(typeOutputDir)

          f = open(typeLabelFile, "w")
          try:
            f.write(typeLabel + "\n")
          finally:
            f.close()

          f = open(schemaFile, "w")
          try:
            f.write("\t".join(schema))
            f.write("\n")
          finally:
            f.close()

          f = open(dataFile, "w")
          try:
            for sampleTuple in sampleData:
              f.write("\t".join(sampleTuple))
              f.write("\n")
          finally:
            f.close()

          numProcessed += 1
        typeCounter += 1
    finally:
      f.close()

  else:
    print "Illegal directive:", directive
