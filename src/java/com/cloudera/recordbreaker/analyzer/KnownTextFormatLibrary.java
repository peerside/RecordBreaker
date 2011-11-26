/*
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.recordbreaker.analyzer;

import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

/***************************************************************************
 * <code>KnownTextFormatLibrary</code> maintains a set of known important
 * textual  * data formats.  Examples include Apache server logs, the output
 * of syslogd, etc.
 * These are textual formats that are so common that it's a bit silly to run
 * LearnStructure over them.  We usually don't know whether a file corresponds
 * to one of these formats without examining the file contents in some depth.
 *
 * The FormatAnalyzer tool calls createDescriptorForKnownFormat() and passes in
 * a candidate File.  That fn will then either return an appropriate
 * DataDescriptor, or will return null.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 * @see DataDescriptor
 ***************************************************************************/
public class KnownTextFormatLibrary {
  List<TextRegexpDataDescriptor> possibleDescriptors = new ArrayList<TextRegexpDataDescriptor>();
  
  /**
   * Creates a new <code>KnownTextFormatLibrary</code> instance.  It just
   * populates the array of known descriptors.
   */
  public KnownTextFormatLibrary() {
    possibleDescriptors.add(createApacheDescriptor());
    possibleDescriptors.add(createSyslogDescriptor());
  }

  /**
   * Creates the descriptor for Apache server logs.
   */
  private TextRegexpDataDescriptor createApacheDescriptor() {
    Pattern apacheCombinedLogPattern = Pattern.compile("^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([A-Z]+)([^\"]*)HTTP/(\\S+)\" (\\d+) (\\d+) \"([^\"]*)\" \"([^\"]*)\"");
    List<Schema.Field> logFields = new ArrayList<Schema.Field>();
    logFields.add(new Schema.Field("ipaddr", Schema.create(Schema.Type.STRING), "Apache combined log ip address", null));
    logFields.add(new Schema.Field("client", Schema.create(Schema.Type.STRING), "Apache combined log client string", null));
    logFields.add(new Schema.Field("userid", Schema.create(Schema.Type.STRING), "Apache combined log user id", null));
    logFields.add(new Schema.Field("timestamp", Schema.create(Schema.Type.STRING), "Apache combined log access timestamp", null));
    logFields.add(new Schema.Field("method", Schema.create(Schema.Type.STRING), "Apache combined log HTTP method", null));
    logFields.add(new Schema.Field("arg", Schema.create(Schema.Type.STRING), "Apache combined log HTTP method arg", null));
    logFields.add(new Schema.Field("version", Schema.create(Schema.Type.FLOAT), "Apache combined log HTTP version", null));
    logFields.add(new Schema.Field("resultcode", Schema.create(Schema.Type.INT), "Apache combined log HTTP result code", null));
    logFields.add(new Schema.Field("size", Schema.create(Schema.Type.INT), "Apache combined log file size", null));
    logFields.add(new Schema.Field("referrer", Schema.create(Schema.Type.STRING), "Apache combined log HTTP referrer", null));
    logFields.add(new Schema.Field("useragent", Schema.create(Schema.Type.STRING), "Apache combined log user agent", null));
    Schema combinedLogSchema = Schema.createRecord("combined", "Apache combined log format", "", false);
    combinedLogSchema.setFields(logFields);

    Pattern apacheCommonLogPattern = Pattern.compile("^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([A-Z]+)([^\"]*)HTTP/(\\S+)\" (\\d+) (\\d+)");
    logFields = new ArrayList<Schema.Field>();    
    logFields.add(new Schema.Field("ipaddr", Schema.create(Schema.Type.STRING), "Apache common log ip address", null));
    logFields.add(new Schema.Field("client", Schema.create(Schema.Type.STRING), "Apache common log client string", null));
    logFields.add(new Schema.Field("userid", Schema.create(Schema.Type.STRING), "Apache common log user id", null));
    logFields.add(new Schema.Field("timestamp", Schema.create(Schema.Type.STRING), "Apache common log access timestamp", null));
    logFields.add(new Schema.Field("method", Schema.create(Schema.Type.STRING), "Apache common log HTTP method", null));
    logFields.add(new Schema.Field("arg", Schema.create(Schema.Type.STRING), "Apache common log HTTP method arg", null));
    logFields.add(new Schema.Field("version", Schema.create(Schema.Type.FLOAT), "Apache common log HTTP version", null));
    logFields.add(new Schema.Field("resultcode", Schema.create(Schema.Type.INT), "Apache common log HTTP result code", null));
    logFields.add(new Schema.Field("size", Schema.create(Schema.Type.INT), "Apache common log file size", null));
    Schema commonLogSchema = Schema.createRecord("common", "Apache common log format", "", false);
    commonLogSchema.setFields(logFields);

    List<Pattern> apacheRegexps = new ArrayList<Pattern>();
    apacheRegexps.add(apacheCombinedLogPattern);
    apacheRegexps.add(apacheCommonLogPattern);

    List<Schema> apacheSchemas = new ArrayList<Schema>();
    apacheSchemas.add(combinedLogSchema);
    apacheSchemas.add(commonLogSchema);    
    
    return new TextRegexpDataDescriptor("apachelog", apacheRegexps, apacheSchemas);
  }

  /**
   * Creates the descriptor for syslogd.
   */
  private TextRegexpDataDescriptor createSyslogDescriptor() {
    Pattern stdFormatPattern = Pattern.compile("^(\\S+) (\\d+) (\\d+):(\\d+):(\\d+) (\\S+) ([^\\[]+)\\[(\\d+)\\]: ([^\\n]+)$");
    List<Schema.Field> logFields = new ArrayList<Schema.Field>(); 
    logFields.add(new Schema.Field("month", Schema.create(Schema.Type.STRING), "Syslog timestamp month", null));
    logFields.add(new Schema.Field("day", Schema.create(Schema.Type.INT), "Syslog timestamp day", null));
    logFields.add(new Schema.Field("hour", Schema.create(Schema.Type.INT), "Syslog timestamp hour", null));
    logFields.add(new Schema.Field("min", Schema.create(Schema.Type.INT), "Syslog timestamp min", null));
    logFields.add(new Schema.Field("sec", Schema.create(Schema.Type.INT), "Syslog timestamp second", null));
    logFields.add(new Schema.Field("host", Schema.create(Schema.Type.STRING), "Syslog host", null));
    logFields.add(new Schema.Field("logger", Schema.create(Schema.Type.STRING), "Syslog logger", null));
    logFields.add(new Schema.Field("pri", Schema.create(Schema.Type.INT), "Syslog msg priority level", null));
    logFields.add(new Schema.Field("msg", Schema.create(Schema.Type.STRING), "Syslog msg", null));
    Schema stdFormatSchema = Schema.createRecord("std", "Standard syslog format", "", false);
    stdFormatSchema.setFields(logFields);

    List<Pattern> syslogRegexps = new ArrayList<Pattern>();
    syslogRegexps.add(stdFormatPattern);

    List<Schema> syslogSchemas = new ArrayList<Schema>();
    syslogSchemas.add(stdFormatSchema);

    return new TextRegexpDataDescriptor("syslog", syslogRegexps, syslogSchemas);
  }

  /**
   * Analyze the input file and figure out if it's one of the formats in the
   * library list.
   */
  public DataDescriptor createDescriptorForKnownFormat(File f) throws IOException {
    for (TextRegexpDataDescriptor candidateDescriptor: possibleDescriptors) {
      if (candidateDescriptor.testData(f)) {
        return candidateDescriptor.cloneWithFile(f);
      }
    }
    return null;
  }
}