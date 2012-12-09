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
 * <code>ApacheDataDescriptor</code> is for Apache log files.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 ********************************************************************/
public class ApacheDataDescriptor extends TextRegexpDataDescriptor {
  public static String APACHE_TYPE = "apachelog";
  
  final static String apacheCombinedLogPatternString = "^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([A-Z]+)([^\"]*)HTTP/(\\S+)\" (\\d+) (\\d+) \"([^\"]*)\" \"([^\"]*)\"";
  final static String apacheCommonLogPatternString = "^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([A-Z]+)([^\"]*)HTTP/(\\S+)\" (\\d+) (\\d+)";
  static List<Pattern> apacheRegexps = new ArrayList<Pattern>();
  static List<Schema> apacheSchemas = new ArrayList<Schema>();
  
  static {
    Pattern apacheCombinedLogPattern = Pattern.compile(apacheCombinedLogPatternString);
    Pattern apacheCommonLogPattern = Pattern.compile(apacheCommonLogPatternString);

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

    apacheRegexps.add(apacheCombinedLogPattern);
    apacheRegexps.add(apacheCommonLogPattern);

    apacheSchemas.add(combinedLogSchema);
    apacheSchemas.add(commonLogSchema);
  }

  public static boolean isApacheLogFile(FileSystem fs, Path p) {
    try {
      return TextRegexpDataDescriptor.isTextRegexpFile(fs, p, apacheRegexps);
    } catch (IOException iex) {
      return false;
    }
  }
  
  public ApacheDataDescriptor(Path p, FileSystem fs) throws IOException {
    super(p, fs, APACHE_TYPE, apacheRegexps, apacheSchemas);
  }

  public ApacheDataDescriptor(Path p, FileSystem fs, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, APACHE_TYPE, apacheRegexps, apacheSchemas);
  }
}