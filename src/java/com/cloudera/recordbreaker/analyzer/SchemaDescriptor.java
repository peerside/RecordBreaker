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

import java.util.Iterator;

import org.apache.avro.Schema;


/*****************************************************************************
 * <code>SchemaDescriptor</code> is a container for schema-specific information.  It
 * can be implemented in several different ways, depending on whether the underlying
 * file is a CSV, XML, a known Avro file, a synthetic Avro file, etc.
 *
 * @author "Michael Cafarella"
 * @version 1.0
 * @since 1.0
 *******************************************************************************/
public interface SchemaDescriptor {
  /**
   * <code>getSchema</code> returns a description of topic-specific metadata for this dataset.
   *
   * @return a <code>Schema</code> value
   */
  public Schema getSchema();

  /**
   * <code>getIterator</code> returns an iterator over the data.  It returns a series of objects
   * that fit the Schema returned by <code>getSchema()</code>.
   *
   * @return an <code>Iterator</code> value
   */
  public Iterator getIterator();

  /**
   * A String that uniquely identifies the topic-specific metadata.  Really, it's a String representation
   * of what's returned by <code>getSchema()</code>
   *
   * @return a <code>String</code> value
   */
  public String getSchemaIdentifier();

  /**
   * Returns a human-readable comment about how this metadata was found.  Was it extracted from a CSV header,
   * found directly from an Avro file, or recovered from a SchemaDictionary operation?
   *
   * @return a <code>String</code> value
   */
  public String getSchemaSourceDescription();
}