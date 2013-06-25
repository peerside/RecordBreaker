package com.cloudera.recordbreaker.analyzer;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.recordbreaker.hive.HiveSerDe;

/*****************************************************
 * <code>AvroDataDescriptor</code> describes data that
 * was found in Avro format in the wild.  That is, we have not
 * had to translate the data into Avro (as we do for other filetypes
 * before most processing); someone wrote it to HDFS in Avro.
 *
 * @author Michael Cafarella
 *****************************************************/
public class AvroDataDescriptor extends GenericDataDescriptor {
  final public static String AVRO_TYPE = "avro";
  
  public AvroDataDescriptor(Path p, FileSystem fs) throws IOException {
    super(p, fs, AVRO_TYPE);
    schemas.add(new AvroSchemaDescriptor(this));
  }

  public AvroDataDescriptor(Path p, FileSystem fs, List<String> schemaReprs, List<String> schemaDescs, List<byte[]> schemaBlobs) throws IOException {
    super(p, fs, AVRO_TYPE, schemaReprs, schemaDescs, schemaBlobs);
  }

  SchemaDescriptor loadSchemaDescriptor(String schemaRepr, String schemaId, byte[] blob) throws IOException {
    // We can ignore the schemaid and blob here.
    return new AvroSchemaDescriptor(this, schemaRepr);
  }

  ///////////////////////////////////
  // GenericDataDescriptor
  //////////////////////////////////
  public boolean isHiveSupported() {
    return true;
  }
  public String getStorageFormatString(Schema targetSchema) {
    String escapedSchemaString = targetSchema.toString().replace("'", "\\'");
    return "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
      "TBLPROPERTIES('avro.schema.literal'='" + escapedSchemaString + "')";
  }
  public String getHiveSerDeClassName() {
    return "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
  }
}