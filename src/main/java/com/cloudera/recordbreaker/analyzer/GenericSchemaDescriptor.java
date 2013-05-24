/*
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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

import org.apache.avro.Schema;

import java.util.Iterator;
import java.io.IOException;

/*********************************************************
 * <code>GenericSchemaDescriptor</code> is an abstract superclass
 * for type-specific SchemaDescriptors.
 *
 * @author "Michael Cafarella" <mjc@cloudera.com>
 **********************************************************/
public abstract class GenericSchemaDescriptor implements SchemaDescriptor {
  DataDescriptor dd;
  Schema schema;  

  public GenericSchemaDescriptor(DataDescriptor dd) throws IOException {
    this.dd = dd;
    computeSchema();
  }
  public GenericSchemaDescriptor(DataDescriptor dd, String schemaRepr) {
    this.dd = dd;
    this.schema = Schema.parse(schemaRepr);
  }

  abstract void computeSchema() throws IOException;
  public byte[] getPayload() {
    return null;
  }
  abstract public Iterator getIterator();

  public Schema getSchema() {
    return this.schema;
  }
  abstract public String getSchemaSourceDescription();

  public String getSchemaIdentifier() {
    return schema.toString();
  }    
}
