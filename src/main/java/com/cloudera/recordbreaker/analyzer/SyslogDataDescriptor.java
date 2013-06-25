package com.cloudera.recordbreaker.analyzer;

import org.apache.avro.Schema;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/********************************************************************
 * <code>SyslogDataDescriptor</code> is for Unix syslog files
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 ********************************************************************/
public class SyslogDataDescriptor extends TextRegexpDataDescriptor {
  public static String SYSLOG_TYPE = "syslog";
  
  final static String stdFormatPatternString = "^(\\S+) (\\d+) (\\d+):(\\d+):(\\d+) (\\S+) ([^\\[]+)\\[(\\d+)\\]: ([^\\n]+)$";
  static List<Pattern> syslogRegexps = new ArrayList<Pattern>();
  static List<Schema> syslogSchemas = new ArrayList<Schema>();
  
  static {
    Pattern stdFormatPattern = Pattern.compile(stdFormatPatternString);
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
    
    syslogRegexps.add(stdFormatPattern);
    syslogSchemas.add(stdFormatSchema);
  }

  public static boolean isSyslogFile(FileSystem fs, Path p) throws IOException {
    return TextRegexpDataDescriptor.isTextRegexpFile(fs, p, syslogRegexps);
  }
  
  public SyslogDataDescriptor(Path p, FileSystem fs) throws IOException {
    super(p, fs, SYSLOG_TYPE, syslogRegexps, syslogSchemas);
  }

  public SyslogDataDescriptor(Path p, FileSystem fs, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, SYSLOG_TYPE, syslogRegexps, syslogSchemas);
  }
}