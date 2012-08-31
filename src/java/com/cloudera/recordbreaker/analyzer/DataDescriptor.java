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

import org.apache.hadoop.fs.Path;
import java.util.List;

/***************************************************************************************
 * <code>DataDescriptor</code> is a generic interface for providing data about a given
 * file-format.  A "format" is something like CSV, XML, Avro, HTML, etc.  Some formats
 * have structured schema information available, others don't.  Some (such as text) may or
 * may not, depending on the text file.
 *
 * A DataDescriptor doesn't tell you anything about the actual contents of a schema, but
 * will generate SchemaDescriptor(s) for you if appropriate.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 *****************************************************************************************/
public interface DataDescriptor {
  /**
   * <code>getFilename</code> just gives the name of the file being described.
   *
   * @return a <code>Path</code> value
   */
  public Path getFilename();

  /**
   * <code>getFileTypeIdentifier</code> returns a single-string descriptor for the file type.
   * E.g., "text", "avro", "html", etc.
   *
   * @return a <code>String</code> value
   */
  public String getFileTypeIdentifier();

  /**
   * <code>getSchemaDescriptor</code> yields zero or more SchemaDescriptor objects.
   * If there's zero, it means there's no known structure to the file.
   * If there's more than one, it means the system is making several guesses as to the
   *   file's true structure.
   *
   * @return a <code>List<SchemaDescriptor></code> value
   */
  public List<SchemaDescriptor> getSchemaDescriptor();
}