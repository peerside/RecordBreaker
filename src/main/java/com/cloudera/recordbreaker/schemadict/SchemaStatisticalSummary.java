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
package com.cloudera.recordbreaker.schemadict;

import java.io.File;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import java.util.Iterator;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.Hashtable;
import java.util.Map;
import java.lang.Math;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Text;

/********************************************
 * The SchemaStatistical Summary object is designed to mirror the structure of an input Schema.
 * In addition to the name and type information associated with a Schema object, it keeps statistical data
 * about observed actual data values that correspond to each Schema element.  
 *
 * This class is intended to be used in the following way:
 * 1) Instantiate a SchemaStatisticalSummary object with a preexisting Schema.
 * 2) For each GenericData item that exhibits the Schema, call SchemaStatisticalSummary.addData(GenericData).  This is
 *    designed to be called multiple times.
 * 3) Once all the desired data has been added, call finalizeStatistics().
 * 4) The resulting finalized SchemaStatisticalSummary object can then be compared to other SchemaStatisticalSummary objects with the measureDistance() function.
 *
 ********************************************/
public class SchemaStatisticalSummary implements Writable {
  final static byte MAGIC = (byte) 0xa1;
  final static byte VERSION = (byte) 1;

  final static int MAX_SUMMARY_SAMPLES = 50;

  final static double MATCHCOST_TYPE_CLASH = 1 * 10 * 1000;
  final static double MATCHCOST_CREATE = 1 * 1000;
  final static double MATCHCOST_DELETE = 1 * 1000;

  final static short ARRAY_NODE = 1;
  final static short BOOLEAN_NODE = 2;
  final static short BYTES_NODE = 3;
  final static short DOUBLE_NODE = 4;
  final static short ENUM_NODE = 5;
  final static short FIXED_NODE = 6;
  final static short FLOAT_NODE = 7;
  final static short INT_NODE = 8;
  final static short LONG_NODE = 9;
  final static short MAP_NODE = 10;
  final static short NULL_NODE = 11;
  final static short RECORD_NODE = 12;
  final static short STRING_NODE = 13;
  final static short UNION_NODE = 14;

  /////////////////////////////////////////////////
  // Inner classes
  /////////////////////////////////////////////////
  /*****************************************************
   * SummaryNode is a generic statistical summary object for a given elt in the
   * hierarchy.  A single tuple in the source data may yield a number of nested
   * SummaryNodes, all rooted at a GenericRecord.
   *
   * The hierarchy is instantiated by examining the schema.  Each new data item
   * results in a call to SummaryNode.addData(), in which the data item is passed in.
   ******************************************************/
  abstract class SummaryNode implements Cloneable {
    SummaryNode parent = null;
    int preorderIdx;
    int numData;
    String docStr = "";

    public SummaryNode() {
    }
    public SummaryNode(String docStr) {
      this.docStr = docStr;
    }
    //////////////////////////////////////////
    // Methods for constructing the summary-node tree
    //////////////////////////////////////////
    public void addData(Object obj) {
      if (obj instanceof Boolean) {
        this.addData((Boolean) obj);
      } else if (obj instanceof GenericArray) {
        this.addData((GenericArray) obj);
      } else if (obj instanceof Double) {
        this.addData((Double) obj);
      } else if (obj instanceof Float) {
        this.addData((Float) obj);
      } else if (obj instanceof GenericFixed) {
        this.addData((GenericFixed) obj);
      } else if (obj instanceof Integer) {
        this.addData((Integer) obj);
      } else if (obj instanceof Long) {
        this.addData((Long) obj);
      } else if (obj instanceof Map) {
        this.addData((Map) obj);
      } else if (obj instanceof ByteBuffer) {
        this.addData((ByteBuffer) obj);
      } else if (obj instanceof GenericRecord) {
        this.addData((GenericRecord) obj);
      } else if (obj instanceof Utf8) {
        this.addData((Utf8) obj);
      } else if (obj instanceof String) {
        this.addData((String) obj);
      }
    }
    // Overridden on per-subclass basis.
    public void addData(Boolean b) {};
    public void addData(GenericArray g) {};
    public void addData(Double d) {};
    public void addData(Float f) {};
    public void addData(Integer i) {};
    public void addData(Long l) {};
    public void addData(Map m) {};
    public void addData(ByteBuffer bb) {};
    public void addData(GenericRecord g) {};
    public void addData(Utf8 u) {};
    public void addData(String s) {};

    ///////////////////////////////////////////////
    // Tree-manipulation and info methods
    ///////////////////////////////////////////////
    /**
     * How many nodes in this subtree?
     */
    public int size() {
      int total = 0;
      for (SummaryNode child: children()) {
        total += child.size();
      }
      return total + 1;
    }    

    /**
     * Setters/getters
     */
    SummaryNode getParent() {
      return parent;
    }
    void setParent(SummaryNode parent) {
      this.parent = parent;
    }
    public List<SummaryNode> children() {
      return new ArrayList<SummaryNode>();
    }
    public int preorderCount() {
      return preorderIdx;
    }
    public SummaryNode parent() {
      return parent;
    }

    /**
     * Dealing with paths and node orderings
     */
    public int computePreorder(int lastIdx) {
      lastIdx++;
      this.preorderIdx = lastIdx;
      for (SummaryNode child: children()) {
        lastIdx = child.computePreorder(lastIdx);
        child.setParent(this);
      }
      return lastIdx;
    }
    void preorder(List<SummaryNode> soFar) {
      soFar.add(this);
      for (SummaryNode child: children()) {
        child.preorder(soFar);
      }
    }
    public List<SummaryNode> preorder() {
      List<SummaryNode> l = new ArrayList<SummaryNode>();
      preorder(l);
      return l;
    }
    public List<SummaryNode> pathToRoot() {
      List<SummaryNode> path = new ArrayList<SummaryNode>();
      SummaryNode cur = this;
      while (cur != null) {
        path.add(cur);
        cur = cur.getParent();
      }
      return path;
    }
    public List<SummaryNode> getLastNodeOnPath() {
      List<SummaryNode> path = new ArrayList<SummaryNode>();
      SummaryNode cur = this;
      while (cur != null) {
        path.add(cur);
        cur = cur.getParent();
      }
      return path;
    }

    /**
     * Useful in testing whether two fields are referring to the same thing.
     * Levenshtein edit distance is great, but we would like a value that ranges 0..1.
     *
     * To compute this, note that the LD is at least abs(len(s1)-len(s2)).  It is also at
     * most max(len(s1), len(s2)).  So we normalize LD by that range.
     */
    double normalizedLevenshteinDistance(String s1, String s2) {
      int rawLD = computeLevenshteinDistance(s1, s2);
      int range = Math.abs(Math.max(s1.length(), s2.length()) - Math.abs(s1.length() - s2.length()));
      return (rawLD / (1.0 * range));
    }

    /**
     * The classic string edit distance algorithm rides again.
     */
    int computeLevenshteinDistance(String s1, String s2) {
      int s1Length = s1.length();
      int s2Length = s2.length();
      int s1pos;
      int s2pos;

      if (s1Length == 0) {
        return s2Length;
      }
      if (s2Length == 0) {
        return s1Length;
      }

      int d[][] = new int[s1Length + 1][];
      for (int i = 0; i <= s1Length; i++) {
        d[i] = new int[s2Length + 1];
      }
      for (int i = 0; i <= s1Length; i++) {
        d[i][0] = i;
      }
      for (int j = 0; j <= s2Length; j++) {
        d[0][j] = j;
      }

      for (int i = 1; i <= s1Length; i++) {
        char s1Char = s1.charAt(i-1);
        for (int j = 1; j <= s2Length; j++) {
          char s2Char = s2.charAt(j-1);

          int cost = 0;
          if (s1Char != s2Char) {
            cost = 1;
          }
          d[i][j] = Math.min(d[i-1][j]+1,
                             Math.min(d[i][j-1]+1, d[i-1][j-1] + cost));
        }
      }
      return d[s1Length][s2Length];
    }
    
    ///////////////////////////////////////////////
    // Methods for string representation
    ///////////////////////////////////////////////
    /**
     * Helper method for rendering a string version of the data
     */
    String prefixString(int prefix) {
      StringBuffer buf = new StringBuffer();
      for (int i = 0; i < prefix; i++) {
        buf.append(" ");
      }
      return buf.toString();
    }
    /**
     * Render a string version of the data
     */
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + "\n";
    }
    public abstract String getTypeDesc();
    /**
     * Find the right node and obtain a description of it.
     */
    public abstract String getDesc(boolean verbose);
    public String getDesc(int nodeid) {
      if (nodeid == preorderIdx) {
        return getDesc(false);
      } else {
        for (SummaryNode child: children()) {
          String desc = child.getDesc(nodeid);
          if (desc != null) {
            return desc;
          }
        }
      }
      return null;
    }
    public String getLabel(int nodeid) {
      if (nodeid == preorderIdx) {
        return getLabel();
      } else {
        for (SummaryNode child: children()) {
          String label = child.getLabel(nodeid);
          if (label != null) {
            return label;
          }
        }
      }
      return null;
    }
    public String getTypeDesc(int nodeid) {
      if (nodeid == preorderIdx) {
        return getTypeDesc();
      } else {
        for (SummaryNode child: children()) {
          String typedesc = child.getTypeDesc(nodeid);
          if (typedesc != null) {
            return typedesc;
          }
        }
      }
      return null;
    }
    public String getDocStr(int nodeid) {
      if (nodeid == preorderIdx) {
        return docStr;
      } else {
        for (SummaryNode child: children()) {
          String docstr = child.getDocStr(nodeid);
          if (docstr != null) {
            return docstr;
          }
        }
      }
      return null;
    }
    /**
     * Find the "label" for the current node.  Since the top-level element in the
     * NodeSummary hierarchy is a record, we know that every element has a label.
     * The getLabel() function goes up the tree to the root, constructing the 
     * dotted label sequence all the way.
     */
    public String getLabel() {
      if (parent != null) {
        return parent.getLabel("", this);
      } else {
        return "<root>";
      }
    }

    public String getLabel(String labelSoFar, SummaryNode src) {
      if (parent != null) {
        return parent.getLabel(labelSoFar, this);
      } else {
        return labelSoFar;
      }
    }

    ///////////////////////////////////////////////
    // Cost functions for schema matching
    ///////////////////////////////////////////////
    /**
     * Figure out basic normalized string edit distance to
     * see if the schema labels match.  If 'useAttributeLabels'
     * is set to false, then this distance is always zero.
     */
    double computeSchemaLabelDistance(String l1, String l2) {
      if (! useAttributeLabels) {
        return 0;
      } else {
        if (l1.indexOf(".") >= 0) {
          l1 = l1.substring(l1.lastIndexOf(".")+1);
        }
        if (l2.indexOf(".") >= 0) {
          l2 = l2.substring(l2.lastIndexOf(".")+1);
        }
        return normalizedLevenshteinDistance(l1, l2);
      }
    }
    /**
     * The default non-type-specific way of performing schema matching is to
     * just compare the attribute labels.  We can also examine data distributions,
     * but this is only possible in the subclasses' overriding transformCost() methods.
     */
    public double transformCost(SummaryNode other) {
      if (this.getClass() == other.getClass()) {
        // Examine the field name for a schema-label distance
        return computeSchemaLabelDistance(this.getLabel(), other.getLabel());
      } else {
        return MATCHCOST_TYPE_CLASH;
      }
    }
    public double deleteCost() {
      return MATCHCOST_DELETE;
    }
    public double createCost() {
      return MATCHCOST_CREATE;
    }

    ///////////////////////////////////////////////
    // Serialization/deserialization
    ///////////////////////////////////////////////
    public abstract void write(DataOutput out) throws IOException;
    public abstract void readFields(DataInput in) throws IOException;
  }

  /*****************************************************
   * Store statistical summary of observed arrays.  Basically, store length information and # times seen.
   ****************************************************/
  class ArraySummaryNode extends SummaryNode {
    int totalSize;
    SummaryNode eltSummary;
    public ArraySummaryNode() {
    }
    public ArraySummaryNode(SummaryNode eltSummary, String docStr) {
      super(docStr);
      this.eltSummary = eltSummary;
    }

    /**
     */
    public void addData(GenericArray data) {
      numData++;
      totalSize += data.size();
      for (Iterator it = data.iterator(); it.hasNext(); ) {
        eltSummary.addData(it.next());
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avgSize: " + (totalSize / (1.0 * numData)) + "\n" + eltSummary.dumpSummary(prefix+2);
    }
    public String getTypeDesc() {
      return "ARRAY";
    }
    public String getDesc(boolean verbose) {
      String desc = "ARRAY";
      if (verbose) {
        desc += "(numData: " + numData + ", avgSize: " + (totalSize / (1.0 * numData)) + ")";
      }
      return getLabel()  + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(ARRAY_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(totalSize);
      eltSummary.write(out);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.totalSize = in.readInt();
      this.eltSummary = readAndCreate(in);
    }
  }

  /*****************************************************
   * Store statistical summary of observed boolean field.  Store # times seen and distribution true vs false
   ****************************************************/
  class BooleanSummaryNode extends SummaryNode {
    int numTrue;
    int numFalse;
    public BooleanSummaryNode() {
    }
    public BooleanSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Boolean b) {
      numData++;
      if (b.booleanValue()) {
        numTrue++;
      } else {
        numFalse++;
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", numTrue: " + numTrue + ", numFalse: " + numFalse + "\n";
    }
    public String getTypeDesc() {
      return "BOOLEAN";
    }
    public String getDesc(boolean verbose) {
      String desc = "BOOLEAN";
      if (verbose) {
        desc += "(numData: " + numData + ", numTrue: " + numTrue + ", numFalse: " + numFalse + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(BOOLEAN_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(numTrue);
      out.writeInt(numFalse);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.numTrue = in.readInt();
      this.numFalse = in.readInt();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Bytes field.  Store # times seen and # bytes seen.
   ****************************************************/
  class BytesSummaryNode extends SummaryNode {
    int totalSize = 0;
    public BytesSummaryNode() {
    }
    public BytesSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(ByteBuffer bb) {
      numData++;
      totalSize += bb.remaining();
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", totalSize: " + totalSize + "\n";
    }
    public String getTypeDesc() {
      return "BYTES";
    }
    public String getDesc(boolean verbose) {
      String desc = "BYTES";
      if (verbose) {
        desc += "(numData: " + numData + ", totalSize: " + totalSize + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(BYTES_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(totalSize);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.totalSize = in.readInt();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Double field.  Store # times seen and total value
   ****************************************************/
  class DoubleSummaryNode extends SummaryNode {
    double total;
    public DoubleSummaryNode() {
    }
    public DoubleSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Double d) {
      numData++;
      total += d.doubleValue();
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg: " + (total / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "DOUBLE";
    }
    public String getDesc(boolean verbose) {
      String desc = "DOUBLE";
      if (verbose) {
        desc += "(numData: " + numData + ", avg: " + (total / (1.0 * numData)) + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(DOUBLE_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeDouble(total);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.total = in.readDouble();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Enumerated Type field.  Store # times seen and statistics on how often 
   * each enum-value is seen.
   ****************************************************/
  class EnumSummaryNode extends SummaryNode {
    String name;
    Map<String, Integer> symbolCounts = new HashMap<String, Integer>();
    public EnumSummaryNode() {
    }
    public EnumSummaryNode(String name, List<String> symbols, String docStr) {
      super(docStr);
      this.name = name;
      for (String symbol: symbols) {
        this.symbolCounts.put(symbol, 1);
      }
    }
    public void addData(String s) {
      this.symbolCounts.put(s, symbolCounts.get(s) + 1);
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      StringBuffer buf = new StringBuffer();
      buf.append(prefixString(prefix) + "numData: " + numData + " =>\n");
      for (String symbol: symbolCounts.keySet()) {
        buf.append(prefixString(prefix+2) + symbol + ": " + symbolCounts.get(symbol) + "\n");
      }
      return buf.toString();
    }
    public String getTypeDesc() {
      return "ENUM";
    }
    public String getDesc(boolean verbose) {
      String desc = "ENUM";
      if (verbose) {
        desc += "(numData: " + numData + ", numSymbols: " + symbolCounts.size() + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(ENUM_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(symbolCounts.size());
      for (String symbol: symbolCounts.keySet()) {
        new Text(symbol).write(out);
        out.writeInt(symbolCounts.get(symbol));
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      symbolCounts = new HashMap<String, Integer>();
      int numElts = in.readInt();
      for (int i = 0; i < numElts; i++) {
        Text symbol = new Text();
        symbol.readFields(in);
        Integer count = in.readInt();
        symbolCounts.put(symbol.toString(), count);
      }
    }    
  }

  /*****************************************************
   * Store statistical summary of observed GenericFixed field.  Store # times seen and byte length information.  Eventually,
   * store info on the byte content, too.
   ****************************************************/
  class FixedSummaryNode extends SummaryNode {
    String name;
    int size;
    int total;
    public FixedSummaryNode() {
    }
    public FixedSummaryNode(String name, int size, String docStr) {
      super(docStr);
      this.name = name;
      this.size = size;
      this.total = 0;
    }
    public void addData(GenericFixed data) {
      byte d[] = data.bytes();
      total += d.length;
      numData++;
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "size: " + size + ", total: " + total + ", numData: " + numData;
    }
    public String getTypeDesc() {
      return "FIXED";
    }
    public String getDesc(boolean verbose) {
      String desc = "FIXED";
      if (verbose) {
        desc += "(numData: " + numData + ", size: " + size + ", total: " + total + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(FIXED_NODE);
      new Text(name).write(out);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(size);
      out.writeInt(total);
    }
    public void readFields(DataInput in) throws IOException {
      this.name = Text.readString(in);
      this.docStr = UTF8.readString(in);
      this.size = in.readInt();
      this.total = in.readInt();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Float field.  Store # times seen and total value
   ****************************************************/
  class FloatSummaryNode extends SummaryNode {
    float total;
    public FloatSummaryNode() {
    }
    public FloatSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Float f) {
      numData++;
      total += f.floatValue();
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg: " + (total / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "FLOAT";
    }
    public String getDesc(boolean verbose) {
      String desc = "FLOAT";
      if (verbose) {
        desc += "(numData: " + numData + ", avg: " + (total / (1.0 * numData)) + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(FLOAT_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeFloat(total);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.total = in.readFloat();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Integer field.
   * Store total value, num data elements, and a sample of actual data elts
   ****************************************************/
  class IntegerSummaryNode extends SummaryNode {
    int total;
    List<Integer> samples = new ArrayList<Integer>();
    public IntegerSummaryNode() {
    }
    public IntegerSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Integer i) {
      numData++;
      total += i.intValue();
      if (samples.size() < MAX_SUMMARY_SAMPLES) {
        samples.add(i);
      }
    }

    ///////////////////////////////////////////////
    // Cost functions for schema matching
    ///////////////////////////////////////////////
    public double transformCost(SummaryNode other) {
      if (this.getClass() == other.getClass()) {
        double schemaLabelDistance = computeSchemaLabelDistance(this.getLabel(), other.getLabel());
        double klDivergence = computeSampleKLDivergence((IntegerSummaryNode) other);

        return schemaLabelDistance + klDivergence;
      } else {
        return MATCHCOST_TYPE_CLASH;
      }
    }

    /**
     * This computes the Kullback-Leibler divergence between two int distributions.  It
     * measures how much the two integer distributions differ.  Useful for testing whether
     * they should be matched.
     * 
     * Assumes the two distributions are gaussians.
     */
    public double computeSampleKLDivergence(IntegerSummaryNode other) {
      double mean1 = total / (1.0 * numData);
      double mean2 = other.total / (1.0 * other.numData);
      double stddev1 = computeStddev();
      double stddev2 = other.computeStddev();
      double variance1 = Math.pow(stddev1, 2);
      double variance2 = Math.pow(stddev2, 2);
      return Math.log(stddev2 / stddev1) + ((variance1 + Math.pow(mean1 - mean2, 2)) / (2 * Math.pow(variance2, 2))) - 0.5;
    }

    /**
     * Compute the standard deviation of the distribution of integers in this summary node.
     * Note that if the sample is smaller than the genuine data, we take the
     * "sample standard deviation", not the true stddev.
     */
    public double computeStddev() {
      double mean = total / (1.0 * numData);
      double total = 0;
      for (Integer sample: samples) {
        total += Math.pow(sample.intValue() - mean, 2);
      }
      double normalizer = 1 / (1.0 * numData);
      if (samples.size() < numData) {
        // This here's what makes the "sample std deviation" in case we're not
        // looking at the full dataset.
        normalizer = 1 / (1.0 * (numData-1));
      }
      double variance = normalizer * total;
      return Math.sqrt(variance);
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg: " + (total / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "INT";
    }
    public String getDesc(boolean verbose) {
      String desc = "INT";
      if (verbose) {
        desc += "(numData: " + numData + ", avg: " + (total / (1.0 * numData)) + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(INT_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, (docStr == null) ? "" : docStr);
      out.writeInt(total);
      out.writeInt(samples.size());
      for (Integer sample: samples) {
        out.writeInt(sample.intValue());
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.total = in.readInt();
      this.samples.clear();
      int numSamples = in.readInt();
      for (int i = 0; i < numSamples; i++) {
        this.samples.add(in.readInt());
      }
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Long field.  Store # times seen and total value
   ****************************************************/
  class LongSummaryNode extends SummaryNode {
    long total;
    public LongSummaryNode() {
    }
    public LongSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Long l) {
      numData++;
      total += l.longValue();
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg: " + (total / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "LONG";
    }
    public String getDesc(boolean verbose) {
      String desc = "LONG";
      if (verbose) {
        desc += "(numData: " + numData + ", avg: " + (total / (1.0 * numData)) + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(LONG_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeLong(total);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.total = in.readLong();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Map field.  Store # times seen and track data for each labeled key-pair.
   ****************************************************/
  class MapSummaryNode extends SummaryNode {
    Schema modelS;
    HashMap<Utf8, SummaryNode> stats = new HashMap<Utf8, SummaryNode>();

    public MapSummaryNode() {
    }
    public MapSummaryNode(Schema modelS, String docStr) {
      super(docStr);
      this.modelS = modelS;
    }
    public void addData(Map m) {
      numData++;
      Iterator it = m.keySet().iterator();
      while (it.hasNext()) {
        Utf8 key = (Utf8) it.next();
        SummaryNode s = stats.get(key);
        if (s == null) {
          s = buildStructure(modelS, modelS.getDoc());
          stats.put(key, s);
        }
        s.addData(m.get(key));
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      StringBuffer buf = new StringBuffer();
      buf.append(prefixString(prefix) + "+------------------------------------------+\n");
      buf.append(prefixString(prefix) + "numData: " + numData + "\n");
      for (Utf8 key: stats.keySet()) {
        SummaryNode s = stats.get(key);
        buf.append(prefixString(prefix) + key + " =>\n" + s.dumpSummary(prefix+2));
      }
      buf.append(prefixString(prefix) + "+------------------------------------------+\n");
      return buf.toString();
    }
    public String getTypeDesc() {
      return "MAP";
    }
    public String getDesc(boolean verbose) {
      String desc = "MAP";
      if (verbose) {
        desc += "(numData: " + numData + ", numSymbols: " + stats.size() + ")";
      }
      return getLabel() + ": " + desc;
    }
    public String getLabel(String labelSoFar, SummaryNode src) {
      for (Utf8 fname: stats.keySet()) {
        SummaryNode candidate = stats.get(fname);
        if (src == candidate) {
          if (parent != null) {
            labelSoFar = (labelSoFar.length() > 0) ? fname.toString() + "." + labelSoFar : fname.toString();
            return parent.getLabel(labelSoFar, this);
          }
        }
      }
      return labelSoFar;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(MAP_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(stats.size());
      for (Utf8 key: stats.keySet()) {
        new Text(key.toString()).write(out);
        stats.get(key).write(out);
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      int numElts = in.readInt();
      for (int i = 0; i < numElts; i++) {
        Text key = new Text();
        key.readFields(in);
        SummaryNode sn = readAndCreate(in);
        stats.put(new Utf8(key.toString()), sn);
      }
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Null field.  Just store # times seen.
   ****************************************************/
  class NullSummaryNode extends SummaryNode {
    public NullSummaryNode() {
    }
    public NullSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData() {
      numData++;
    }

    public String getDesc(boolean verbose) {
      String desc = "NULL";
      if (verbose) {
        desc += "(numData: " + numData + ")";
      }
      return getLabel() + ": " + desc;
    }
    public String getTypeDesc() {
      return "NULL";
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(NULL_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Record field.  Store # times seen and then data about sub-elements.
   ****************************************************/
  class RecordSummaryNode extends SummaryNode {
    String name;
    Map<String, SummaryNode> recordSummary = new HashMap<String, SummaryNode>();
    public RecordSummaryNode() {
    }
    public RecordSummaryNode(String name, String docStr) {
      super(docStr);
      this.name = name;
    }
    public List<SummaryNode> children() {
      List<SummaryNode> l = new ArrayList<SummaryNode>();
      for (String key: recordSummary.keySet()) {
        l.add(recordSummary.get(key));
      }
      return l;
    }
    public void addField(String fname, SummaryNode fn) {
      recordSummary.put(fname, fn);
    }
    public void addData(GenericRecord data) {
      numData++;
      for (String fname: recordSummary.keySet()) {
        recordSummary.get(fname).addData(data.get(fname));
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      StringBuffer buf = new StringBuffer();
      buf.append(prefixString(prefix) + "+------------------------------------------+\n");
      buf.append(prefixString(prefix) + "numData: " + numData + "\n");
      for (String fname: recordSummary.keySet()) {
        buf.append(prefixString(prefix) + fname + " =>\n" + recordSummary.get(fname).dumpSummary(prefix+2));
      }
      buf.append(prefixString(prefix) + "+------------------------------------------+\n");
      return buf.toString();
    }
    public String getTypeDesc() {
      return "RECORD";
    }
    public String getDesc(boolean verbose) {
      String desc = "RECORD";
      if (verbose) {
        desc += "(numData: " + numData + ", fields: " + recordSummary.size() + ")";
      }
      return getLabel() + ": " + desc;
    }
    public String getLabel(String labelSoFar, SummaryNode src) {
      for (String fname: recordSummary.keySet()) {
        SummaryNode candidate = recordSummary.get(fname);
        if (src == candidate) {
          labelSoFar = (labelSoFar.length() > 0) ? fname + "." + labelSoFar : fname;
          if (parent != null) {
            return parent.getLabel(labelSoFar, this);
          } else {
            return "<root>" + "." + labelSoFar;
          }
        }
      }
      return "<root>" + "." + labelSoFar;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(RECORD_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(recordSummary.size());
      for (String fname: recordSummary.keySet()) {
        new Text(fname).write(out);
        recordSummary.get(fname).write(out);
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      int numRecs = in.readInt();
      for (int i = 0; i < numRecs; i++) {
        Text fname = new Text();
        fname.readFields(in);
        SummaryNode sn = readAndCreate(in);
        recordSummary.put(fname.toString(), sn);
      }
    }    
  }

  /*****************************************************
   * Store statistical summary of observed String field.  Store # times seen and total length of the strings (for now).
   * Eventually, store info on the String content, too.
   ****************************************************/
  class StringSummaryNode extends SummaryNode {
    int totalLength;
    Set<Utf8> observedStrings = new TreeSet<Utf8>();
    public StringSummaryNode() {
    }
    public StringSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Utf8 s) {
      numData++;
      totalLength += s.getLength();
      observedStrings.add(s);
    }

    ///////////////////////////////////////////////
    // Cost functions for schema matching
    ///////////////////////////////////////////////
    public double transformCost(SummaryNode other) {
      if (this.getClass() == other.getClass()) {
        double schemaLabelDistance = computeSchemaLabelDistance(this.getLabel(), other.getLabel());
        double jaccardSimilarity = computeJaccardSimilarity((StringSummaryNode) other);
        double jaccardDistance = 1 - jaccardSimilarity;

        return schemaLabelDistance + jaccardDistance;
      } else {
        return MATCHCOST_TYPE_CLASH;
      }
    }

    /**
     * This is a useful score for determining whether two sets of objects are similar
     */
    public double computeJaccardSimilarity(StringSummaryNode other) {
      Set<Utf8> larger = (this.numData >= other.numData ? this.observedStrings : other.observedStrings);
      Set<Utf8> smaller = (this.numData < other.numData ? this.observedStrings : other.observedStrings);

      int unionSize = larger.size();
      if (larger.contains(new Utf8(""))) {
        unionSize -= 1;
      }
      int intersectionSize = 0;
      for (Utf8 smallElt: smaller) {
        if (smallElt.length() == 0) {
          continue;
        }
        if (larger.contains(smallElt)) {
          intersectionSize++;
        } else {
          unionSize++;
        }
      }
      if (unionSize == 0) {
        return 0;
      } else {
        return intersectionSize / (1.0 * unionSize);
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg-len: " + (totalLength / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "STRING";
    }
    public String getDesc(boolean verbose) {
      String desc = "STRING";
      if (verbose) {
        desc += "(numData: " + numData + ", avglen: " + (totalLength / (1.0 * numData)) + ")";
      }
      return getLabel()  + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(STRING_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(totalLength);

      out.writeInt(observedStrings.size());
      for (Utf8 s: observedStrings) {
        UTF8.writeString(out, s.toString());
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.totalLength = in.readInt();

      observedStrings.clear();
      int numInts = in.readInt();
      for (int i = 0; i < numInts; i++) {
        observedStrings.add(new Utf8(UTF8.readString(in)));
      }
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Union field.  Actually, a Union is not observed directly - we just know
   * it's a union from the schema.  Store # times seen, data on the particular type observed, and statistics on how 
   * often each subtype is seen.
   ****************************************************/
  class UnionSummaryNode extends SummaryNode {
    Map<Schema.Type, SummaryNode> unionTypes = new HashMap<Schema.Type, SummaryNode>();
    Map<Schema.Type, Integer> unionTypeCounts = new HashMap<Schema.Type, Integer>();
    public UnionSummaryNode() {
    }
    public UnionSummaryNode(String docStr) {
      super(docStr);
    }
    public void addType(Schema.Type t, SummaryNode sn) {
      if (unionTypes.get(t) == null) {
        unionTypes.put(t, sn);
        unionTypeCounts.put(t, 0);
      }
    }

    /**
     * We need to dispatch the object to the right element stored in 'unionTypes'
     */
    public void addData(Object obj) {
      Schema.Type t = Schema.Type.ARRAY;
      if (obj instanceof GenericArray) {
        t = Schema.Type.ARRAY;
      } else if (obj instanceof Boolean) {
        t = Schema.Type.BOOLEAN;
      } else if (obj instanceof ByteBuffer) {
        t = Schema.Type.BYTES;
      } else if (obj instanceof Double) {
        t = Schema.Type.DOUBLE;
      } else if (obj instanceof String) {
        t = Schema.Type.ENUM;
      } else if (obj instanceof GenericFixed) {
        t = Schema.Type.FIXED;
      } else if (obj instanceof Float) {
        t = Schema.Type.FLOAT;
      } else if (obj instanceof Integer) {
        t = Schema.Type.INT;
      } else if (obj instanceof Long) {
        t = Schema.Type.LONG;
      } else if (obj instanceof Map) {
        t = Schema.Type.MAP;
      } else if (obj instanceof GenericRecord) {
        t = Schema.Type.RECORD;
      } else if (obj instanceof Utf8) {
        t = Schema.Type.STRING;
      }
      unionTypes.get(t).addData(obj);
      Integer c = unionTypeCounts.get(t);
      if (c == null) {
        unionTypeCounts.put(t, 1);
      } else {
        unionTypeCounts.put(t, c.intValue() + 1);
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      StringBuffer buf = new StringBuffer();
      for (Schema.Type t: unionTypes.keySet()) {
        buf.append(prefixString(prefix) + "unionType: " + t + " =>\n");
        buf.append(unionTypes.get(t).dumpSummary(prefix+2));
      }
      return buf.toString();
    }
    public String getTypeDesc() {
      return "UNION";
    }
    public String getDesc(boolean verbose) {
      String desc = "UNION";
      if (verbose) {
        desc += "(numData: " + numData + ", numtypes: " + unionTypes.size() + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(UNION_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(unionTypes.size());
      for (Schema.Type t: unionTypes.keySet()) {
        new Text(t.toString()).write(out);
        out.writeInt(unionTypeCounts.get(t));
        unionTypes.get(t).write(out);
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      int numTypes = in.readInt();
      for (int i = 0; i < numTypes; i++) {
        Text tLabel = new Text();
        tLabel.readFields(in);
        Schema.Type t = Schema.Type.valueOf(tLabel.toString());
        int typeCount = in.readInt();
        SummaryNode sn = readAndCreate(in);
        unionTypes.put(t, sn);
        unionTypeCounts.put(t, typeCount);
      }
    }    
  }
  /***************************************
   * Op is used to track mapping results
   ***************************************/
  class PreviousChoice extends SchemaMappingOp {
    Hashtable<String, List<SchemaMappingOp>> h;
    String label;
    public PreviousChoice(Hashtable<String, List<SchemaMappingOp>> h, int i, int j) {
      this.h = h;
      this.label = "" + i + "-" + j;
    }
    public PreviousChoice(Hashtable<String, List<SchemaMappingOp>> h, int p1, int p2, int p3, int p4, int p5, int p6) {
      this.h = h;
      this.label = "" + p1 + "-" + p2 + "-" + p3 + "-" + p4 + "-" + p5 + "-" + p6;
    }
    public List<SchemaMappingOp> getOps() {
      List<SchemaMappingOp> ops = h.get(label);
      if (ops == null) {
        ops = new ArrayList<SchemaMappingOp>();
      }
      return ops;
    }
    public String toString() {
      return "Previous! " + label;
    }
  }


  /////////////////////////////////////////////////
  // Members
  /////////////////////////////////////////////////
  SummaryNode root = null;
  boolean useAttributeLabels = true;
  String datasetLabel = "";

  /////////////////////////////////////////////////
  // Constructors, initializers
  /////////////////////////////////////////////////
  public SchemaStatisticalSummary() throws IOException {
  }
  public SchemaStatisticalSummary(String datasetLabel) throws IOException {
    this.datasetLabel = datasetLabel;
  }
  public void setUseAttributeLabels(boolean useAttributeLabels) {
    this.useAttributeLabels = useAttributeLabels;
  }
  /**
   * Create the statistical summary object from data.
   */
  public Schema createSummaryFromData(File f) throws IOException {
    DataFileReader in = new DataFileReader(f, new GenericDatumReader());
    try {
      Schema s = in.getSchema();

      //
      // There has to be at least one data element for us to infer anything meaningful
      //
      Iterator it = in.iterator();
      if (! it.hasNext()) {
        throw new IOException("No contents");
      }

      //
      // We can only infer schemas from top-level records, not Fixeds or Arrays.
      //
      Object firstRecord = it.next();
      if (firstRecord instanceof GenericFixed ||
          firstRecord instanceof GenericArray) {
        throw new IOException("Not a top-level record");
      }

      // We assume the passed-in top-level Schema always represents a Record.
      if (s.getType() != Schema.Type.RECORD) {
        throw new IOException("Passed-in top-level Schema instance must be of type Schema.Type.RECORD");
      }
      this.root = buildStructure(s, "ROOT");

      //
      // Iterate through all records and collect statistics on each Schema field.
      //
      List<Schema.Field> fields = s.getFields();
      GenericRecord cur = (GenericRecord) firstRecord;
      int counter = 0;
      do {
        this.root.addData(cur);
        counter++;
        if (it.hasNext()) {
          cur = (GenericRecord) it.next();
        } else {
          cur = null;
        }
      } while (cur != null);

      this.root.computePreorder(-1);
      return s;
    } finally {
      in.close();
    }
  }

  /**
   * This function reads in data and instantiates the SummaryNode hierarchy.
   */
  public SummaryNode readAndCreate(DataInput in) throws IOException {
    short nodeType = in.readShort();
    SummaryNode sn = null;

    switch (nodeType) {
    case ARRAY_NODE: {
      sn = new ArraySummaryNode();
      break;
    }
    case BOOLEAN_NODE: {
      sn = new BooleanSummaryNode();
      break;
    }
    case BYTES_NODE: {
      sn = new BytesSummaryNode();
      break;
    }
    case DOUBLE_NODE: {
      sn = new DoubleSummaryNode();
      break;
    }
    case ENUM_NODE: {
      sn = new EnumSummaryNode();
      break;
    }
    case FIXED_NODE: {
      sn = new FixedSummaryNode();
      break;
    }
    case FLOAT_NODE: {
      sn = new FloatSummaryNode();
      break;
    }
    case INT_NODE: {
      sn = new IntegerSummaryNode();
      break;
    }
    case LONG_NODE: {
      sn = new LongSummaryNode();
      break;
    }
    case MAP_NODE: {
      sn = new MapSummaryNode();
      break;
    }
    case NULL_NODE: {
      sn = new NullSummaryNode();
      break;
    }
    case RECORD_NODE: {
      sn = new RecordSummaryNode();
      break;
    }
    case STRING_NODE: {
      sn = new StringSummaryNode();
      break;
    }
    case UNION_NODE: {
      sn = new UnionSummaryNode();
      break;
    }
    default:
      throw new IOException("Unknown node type: " + nodeType);
    }

    sn.readFields(in);
    return sn;
  }

  /**
   * Build a Summary structure out of the given schema.  Helper method.
   */ 
  SummaryNode buildStructure(Schema s, String docStr) {
    Schema.Type stype = s.getType();
    if (stype == Schema.Type.ARRAY) {
      return new ArraySummaryNode(buildStructure(s.getElementType(), s.getDoc()), docStr);
    } else if (stype == Schema.Type.BOOLEAN) {
      return new BooleanSummaryNode(docStr);
    } else if (stype == Schema.Type.BYTES) {
      return new BytesSummaryNode(docStr);
    } else if (stype == Schema.Type.DOUBLE) {
      return new DoubleSummaryNode(docStr);
    } else if (stype == Schema.Type.ENUM) {
      return new EnumSummaryNode(s.getFullName(), s.getEnumSymbols(), docStr);
    } else if (stype == Schema.Type.FIXED) {
      return new FixedSummaryNode(s.getFullName(), s.getFixedSize(), docStr);
    } else if (stype == Schema.Type.FLOAT) {
      return new FloatSummaryNode(docStr);
    } else if (stype == Schema.Type.INT) {
      return new IntegerSummaryNode(docStr);
    } else if (stype == Schema.Type.LONG) {
      return new LongSummaryNode(docStr);
    } else if (stype == Schema.Type.MAP) {
      return new MapSummaryNode(s.getValueType(), docStr);
    } else if (stype == Schema.Type.NULL) {
      return new NullSummaryNode(docStr);
    } else if (stype == Schema.Type.RECORD) {
      RecordSummaryNode rsn = new RecordSummaryNode(s.getFullName(), docStr);
      for (Field f: s.getFields()) {
        rsn.addField(f.name(), buildStructure(f.schema(), f.doc()));
      }
      return rsn;
    } else if (stype == Schema.Type.STRING) {
      return new StringSummaryNode(docStr);
    } else if (stype == Schema.Type.UNION) {
      UnionSummaryNode usn = new UnionSummaryNode(docStr);
      for (Schema subschema: s.getTypes()) {
        usn.addType(subschema.getType(), buildStructure(subschema, subschema.getDoc()));
      }
    }
    return null;
  }

  /////////////////////////////////////////////////////////
  // Schema distance computation
  /////////////////////////////////////////////////////////
  /**
   * Get the minimum mapping cost from a schema of size k to one of size m.
   * This helps us avoid mapping computations that couldn't possibly produce
   * a low-distance mapping.
   */
  public static double getMinimumMappingCost(int k, int m) {
    return Math.abs(k - m) * Math.min(MATCHCOST_CREATE, MATCHCOST_DELETE);
  }

  /**
   * Find the best mapping between the current schema summary and the one provided
   * by the parameter.
   */
  public SchemaMapping getBestMapping(SchemaStatisticalSummary other) {
    SummaryNode t1 = root;
    SummaryNode t2 = other.root;
    TreeMap<Integer, SummaryNode> t1NonLeafs = new TreeMap<Integer, SummaryNode>();
    TreeMap<Integer, SummaryNode> t1Leafs = new TreeMap<Integer, SummaryNode>();
    TreeMap<Integer, SummaryNode> t2NonLeafs = new TreeMap<Integer, SummaryNode>();
    TreeMap<Integer, SummaryNode> t2Leafs = new TreeMap<Integer, SummaryNode>();

    //
    // Find all the non-leaf nodes
    //
    for (SummaryNode iNode: t1.preorder()) {
      if (iNode.children().size() > 0) {
        t1NonLeafs.put(iNode.preorderCount(), iNode);
      } else {
        t1Leafs.put(iNode.preorderCount(), iNode);
      }
    }
    for (SummaryNode jNode: t2.preorder()) {
      if (jNode.children().size() > 0) {
        t2NonLeafs.put(jNode.preorderCount(), jNode);
      } else {
        t2Leafs.put(jNode.preorderCount(), jNode);
      }
    }

    //
    // Start by computing all the potential 1:1 leaf-level match costs.
    //
    List<DistancePair[]> allCosts = new ArrayList<DistancePair[]>();
    Set<DistancePair> allKnownCostPairs = new TreeSet<DistancePair>();

    for (SummaryNode iNode: t1.preorder()) {
      int iIdx = iNode.preorderCount();
      DistancePair fromI[] = null;
      if (t1NonLeafs.get(iIdx) == null) {
        List<DistancePair> costs = new ArrayList<DistancePair>();
        for (SummaryNode jNode: t2.preorder()) {
          int jIdx = jNode.preorderCount();
          if (t2NonLeafs.get(jIdx) == null) {
            DistancePair dp = new DistancePair(iNode.transformCost(jNode), iNode, jNode);
            costs.add(dp);
            allKnownCostPairs.add(dp);
          }
        }
        costs.add(new DistancePair(iNode.deleteCost(), iNode, null));
        fromI = costs.toArray(new DistancePair[costs.size()]);
        Arrays.sort(fromI);
      }
      allCosts.add(fromI);
    }


    //
    // Now pass those costs to the mapping algorithm.
    // Select which mapping algorithm we want to use.  For now, it's 'greedy'.
    //
    return findGreedyMapping(other, t1, t2, t1Leafs, t2Leafs, t1NonLeafs, t2NonLeafs, allKnownCostPairs);
    /**
    boolean performTraditionalMapping = false;
    if (performTraditionalMapping) {
      return findTraditionalMapping(other, t1, t2, t1Leafs, t2Leafs, t1NonLeafs, t2NonLeafs, allCosts);
    } else {
      return findGreedyMapping(other, t1, t2, t1Leafs, t2Leafs, t1NonLeafs, t2NonLeafs, allKnownCostPairs);      
    }
    **/
  }

  /**
   * findTraditionalMapping() tries the best k permutations of matches and returns the best one.
   * The number of permutations can grow rapidly as the sizes of the two schemas grow, so this method
   * can be very time-consuming.
   */
  /**
  SchemaMapping findTraditionalMapping(SchemaStatisticalSummary other, SummaryNode t1, SummaryNode t2, Map<Integer, SummaryNode> t1Leafs, Map<Integer, SummaryNode> t2Leafs, Map<Integer, SummaryNode> t1NonLeafs, Map<Integer, SummaryNode> t2NonLeafs, List<DistancePair[]> allCosts) {
    //
    // Figure out how far down into each attr's match-list we can go while only evaluating the
    // estimated top-k-scoring schema matches.  (Estimated by combining independent 1:1 match scores;
    // no enforcement of the pigeonhole constraint.)
    //
    int MAX_CANDIDATES = 100000;
    int numToPeek[] = new int[allCosts.size()];
    for (int i = 0; i < numToPeek.length; i++) {
      if (allCosts.get(i) == null) {
        numToPeek[i] = 0;
      } else {
        numToPeek[i] = 1;
      }
    }
    int numCandidates = 1;
    System.err.println("Num elts: " + numToPeek.length);
    do {
      int peekIndex = -1;
      double cheapestPeek = Double.MAX_VALUE;
      for (int i = 0; i < numToPeek.length; i++) {      
        if (allCosts.get(i) != null &&
            numToPeek[i] < allCosts.get(i).length) {
          double candidatePeekValue = allCosts.get(i)[numToPeek[i]].getCost();
          if (candidatePeekValue < cheapestPeek) {
            cheapestPeek = candidatePeekValue;
            peekIndex = i;
          }
        }
      }
      if (peekIndex >= 0) {
        numToPeek[peekIndex]++;
      } else {
        break;
      }
      numCandidates = 1;
      for (int i = 0; i < numToPeek.length; i++) {
        if (numToPeek[i] >= 1) {
          numCandidates *= numToPeek[i];
        }
      }
    } while (numCandidates < MAX_CANDIDATES);

    System.err.println("All cost size: " + allCosts.size() + ", number of candidates examined: " + numCandidates);
    System.err.println();
    numCandidates = Math.max(MAX_CANDIDATES, numCandidates);

    //
    // Now the numToPeek vector tells us how many steps down to go in each attr's
    // ranked list of preferred matches.  The product of all of these determines the # of candidates.
    //
    int curPeek[] = new int[numToPeek.length];
    for (int i = 0; i < curPeek.length; i++) {
      if (numToPeek[i] == 0) {
        curPeek[i] = 0;
      } else {
        curPeek[i] = 1;
      }
    }

    //
    // Now go through all the possible configurations of top-k mappings.
    // 
    // We optimize for the common case in which we have two near-flat hierarchies
    //
    DistancePair bestMatchConfig[] = new DistancePair[curPeek.length];
    DistancePair matchConfig[] = new DistancePair[curPeek.length];
    double bestCost = Double.MAX_VALUE;
    boolean peeksRemain = numCandidates > 0;
    long startTime = System.currentTimeMillis();
    int numIters = 0;
    while (peeksRemain) {
      numIters++;

      ////////////////////////////////////////
      // Evaluate this configuration ("peek")
      ////////////////////////////////////////
      //
      // 1. Build a proper 'match configuration' out of the leaf-level 1:1 'curPeek'.
      //    That means we generate record-level correspondences when justified by full 
      //    child-correspondences
      //
      for (SummaryNode iNode: t1.preorder()) {
        int iNodeIdx = iNode.preorderCount();
        matchConfig[iNodeIdx] = null;
        DistancePair[] allINodeMatches = allCosts.get(iNodeIdx);
        if (allINodeMatches != null) {
          matchConfig[iNodeIdx] = allINodeMatches[curPeek[iNodeIdx]-1];
        }
      }

      //
      // 2. Modify the current matchConfig s.t. if ALL of a non-leaf's children match ALL of
      //    the children of a non-leaf, then the two non-leafs also match.
      //    Because of the potential record hierarchy, this procedure needs to be repeated until 
      //    there is an iteration in which no new matches are found (or until the roots are matched).
      //
      for (Map.Entry<Integer, SummaryNode> elt: t1NonLeafs.entrySet()) {
        SummaryNode iNode = elt.getValue();
        if (matchConfig[iNode.preorderCount()] != null) {
          continue;
        }

        // For each child of this t1 internal node, place the matching node's parent into a set
        TreeMap<Integer, SummaryNode> observedMatchParents = new TreeMap<Integer, SummaryNode>();
        for (SummaryNode iChild: iNode.children()) {
          int iChildIdx = iChild.preorderCount();
          DistancePair jMatch = matchConfig[iChildIdx];
          if (jMatch != null) {
            if (jMatch.getNode() == null) {
              observedMatchParents.put(-1, iChild);
            } else {
              SummaryNode jMatchParent = jMatch.getNode().getParent();
              observedMatchParents.put(jMatchParent.preorderCount(), jMatchParent);
            }
          }
        }

        // If the parent-set has just one element, then internal node iNode 
        // should be matched to the singleton elt in the parent-set.
        if (observedMatchParents.size() == 1) {
          int matchIdx = observedMatchParents.firstKey().intValue();
          if (matchIdx >= 0) {
            SummaryNode jMatchParent = observedMatchParents.get(matchIdx);
            matchConfig[iNode.preorderCount()] = new DistancePair(0, iNode, jMatchParent);
          }
        }
      }

      //
      // 3. Compute the total match costs.  
      // a. The first component is the TRANSFORM costs of the discovered 1:1 leaf matches.
      //    (Valid matches among non-leafs are free.)
      //
      double total = 0;
      for (int iNodeIdx = 0; iNodeIdx < matchConfig.length; iNodeIdx++) {
        if (matchConfig[iNodeIdx] != null) {
          // Get the transform cost
          total += matchConfig[iNodeIdx].getCost();
        }
      }

      //
      // 3b. Compute DELETE penalties.  These are elts in t1 that are NOT MATCHED to anything
      //     in t2.  Non-leaf nodes that are unmatched DO incur penalties.
      //
      //     While we're there, compute the set of items in t2 that DO have a matched elt.
      //
      int numDuplicates = 0;
      HashSet<Integer> observedT2Nodes = new HashSet<Integer>();
      for (SummaryNode iNode: t1.preorder()) {
        int iNodeIdx = iNode.preorderCount();
        if (matchConfig[iNodeIdx] == null || matchConfig[iNodeIdx].getNode() == null) {
          total += iNode.deleteCost();
        } else {
          int jIdx = matchConfig[iNodeIdx].getNode().preorderCount();
          if (observedT2Nodes.contains(jIdx)) {
            numDuplicates++;
          } else {
            observedT2Nodes.add(jIdx);
          }
        }
      }

      //
      // 3c. Compute CREATE penalties.  These count for any items in the target schema t2
      //     that have gone unmapped.  
      //
      for (SummaryNode jNode: t2.preorder()) {
        int jIdx = jNode.preorderCount();
        if (! observedT2Nodes.contains(jIdx)) {
          total += jNode.createCost();
        }
      }

      //
      // 4.  Impose a penalty for duplicate mappings in t2.
      // 
      
      //
      // Is it the best mapping so far?
      //
      if (total < bestCost) {
        bestCost = total;
        System.arraycopy(matchConfig, 0, bestMatchConfig, 0, bestMatchConfig.length);
      }

      /////////////////////////////////////////////
      // Find the next configuration to evaluate (leaf-level "peek").
      // We try to do a 'breadth-first search' rather than go deep on
      // a single peeklist.  This makes it easier to find the best match sooner,
      // and thus abort the process early.
      /////////////////////////////////////////////
      peeksRemain = false;
      int minSeen = Integer.MAX_VALUE;
      int minIndex = -1;
      for (int i = 0; i < curPeek.length; i++) {
        if (curPeek[i] == 0) {
          continue;
        } else {
          if (curPeek[i] < numToPeek[i]) {
            curPeek[i]++;
            for (int j = i-1; j >= 0; j--) {
              if (curPeek[j] > 0) {
                curPeek[j] = 1;
              }
            }
            peeksRemain = true;
            break;
          }
        }
      }
    }
    long endTime = System.currentTimeMillis();
    System.err.println("Evaluting peeks: " + ((endTime - startTime) / 1000.0) + " over " + numIters + " iterations.");
    
    //
    // ALMOST DONE: We have the best match.  Now we translate it into a series of SchemaMappingOps 
    //
    List<SchemaMappingOp> bestOps = new ArrayList<SchemaMappingOp>();
    HashSet<Integer> bestMapTargets = new HashSet<Integer>();
    for (int i = 0; i < bestMatchConfig.length; i++) {
      if (bestMatchConfig[i] != null && bestMatchConfig[i].getNode() != null) {
        int dstIdx = bestMatchConfig[i].getNode().preorderCount();
        bestOps.add(new SchemaMappingOp(SchemaMappingOp.TRANSFORM_OP, this, i, other, dstIdx));
        bestMapTargets.add(dstIdx);
      } else {
        bestOps.add(new SchemaMappingOp(SchemaMappingOp.DELETE_OP, this, i));
      }
    }
    for (SummaryNode jNode: t2.preorder()) {
      int jIdx = jNode.preorderCount();
      if (jNode.children().size() == 0 && ! bestMapTargets.contains(jIdx)) {
        bestOps.add(new SchemaMappingOp(SchemaMappingOp.CREATE_OP, other, jIdx));
      }
    }
    //
    // All done!
    //
    return new SchemaMapping(this, other, bestCost, bestOps);
  }
  **/

  /**
   * Greedy Mapping is sloppy, but very fast.  It repeatedly accepts the best-looking pairwise
   * match, until there is nothing left to match.  Seems to work well so far, but needs to be
   * tested more.
   */
  SchemaMapping findGreedyMapping(SchemaStatisticalSummary other, SummaryNode t1, SummaryNode t2, Map<Integer, SummaryNode> t1Leafs, Map<Integer, SummaryNode> t2Leafs, Map<Integer, SummaryNode> t1NonLeafs, Map<Integer, SummaryNode> t2NonLeafs, Set<DistancePair> allKnownCostPairs) {
    int totalSrcs = t1Leafs.size();
    int totalDsts = t2Leafs.size();
    Set<Integer> observedSrcs = new TreeSet<Integer>();
    Set<Integer> observedDsts = new TreeSet<Integer>();    
    List<DistancePair> matching = new ArrayList<DistancePair>();
    List<SchemaMappingOp> outputOps = new ArrayList<SchemaMappingOp>();
    double totalCost = 0;

    //
    // Find all the leaf-level matches
    //
    Map<Integer, SummaryNode> transformMap = new TreeMap<Integer, SummaryNode>();
    for (DistancePair dp: allKnownCostPairs) {
      int srcId = dp.getSrc().preorderCount();
      int dstId = dp.getNode().preorderCount();

      if ((! observedSrcs.contains(srcId)) && (! observedDsts.contains(dstId))) {
        matching.add(dp);
        observedSrcs.add(srcId);
        observedDsts.add(dstId);
        outputOps.add(new SchemaMappingOp(SchemaMappingOp.TRANSFORM_OP, this, srcId, other, dstId, dp.getCost()));
        transformMap.put(srcId, dp.getNode());
        totalCost += dp.getCost();
        if (matching.size() >= Math.min(totalSrcs, totalDsts)) {
          break;
        }
      }
    }

    //
    // Look for internal nodes that should be matched.  If ALL of an internal node's children
    // match ALL of another internal node's children, then the two internal nodes also match.
    //
    for (Map.Entry<Integer, SummaryNode> elt: t1NonLeafs.entrySet()) {
      SummaryNode iNode = elt.getValue();
      SortedSet<Integer> knownDstParents = new TreeSet<Integer>();
      for (SummaryNode iChild: iNode.children()) {
        int iChildIdx = iChild.preorderCount();
        SummaryNode dstNode = transformMap.get(iChildIdx);
        if (dstNode != null) {
          knownDstParents.add(dstNode.getParent().preorderCount());
        }        
      }

      // There's just one parent of the destination nodes, so we have found an internal node match.
      if (knownDstParents.size() == 1) {
        Integer dstIdx = knownDstParents.first();
        SummaryNode dstNode = t2NonLeafs.get(dstIdx);
        outputOps.add(new SchemaMappingOp(SchemaMappingOp.TRANSFORM_OP, this, iNode.preorderCount(), other, dstIdx, 0));
        observedSrcs.add(iNode.preorderCount());
        observedDsts.add(dstIdx);
      }
    }

    //
    // If a node is in the source, but not the dest, then we need to DELETE it.
    // Compute the DELETE costs here.
    //
    for (SummaryNode iNode: t1.preorder()) {
      int iNodeIdx = iNode.preorderCount();
      if (! observedSrcs.contains(iNodeIdx)) {
        totalCost += iNode.deleteCost();
        outputOps.add(new SchemaMappingOp(SchemaMappingOp.DELETE_OP, this, iNodeIdx, iNode.deleteCost()));
      }
    }

    //
    // If a node is in the dest, but not the source, then we need to CREATE it.
    // Compute the CREATE costs here.
    // 
    for (SummaryNode jNode: t2.preorder()) {
      int jNodeIdx = jNode.preorderCount();
      if (! observedDsts.contains(jNodeIdx)) {
        totalCost += jNode.createCost();
        outputOps.add(new SchemaMappingOp(SchemaMappingOp.CREATE_OP, other, jNodeIdx, jNode.createCost()));
      }
    }
    return new SchemaMapping(this, other, totalCost, outputOps);
  }

  class DistancePair implements Comparable {
    double cost;
    SummaryNode src;
    SummaryNode target;
    public DistancePair(double cost, SummaryNode src, SummaryNode target) {
      this.cost = cost;
      this.src = src;
      this.target = target;
    }
    public int compareTo(Object o) {
      DistancePair other = (DistancePair) o;
      if (cost < other.cost) {
        return -1;
      } else if (cost > other.cost) {
        return 1;
      } else {
        int cmp = src.preorderCount() - other.src.preorderCount();
        if (cmp == 0) {
          cmp = target.preorderCount() - other.target.preorderCount();
        }
        return cmp;
      }
    }
    public double getCost() {
      return cost;
    }
    public SummaryNode getSrc() {
      return src;
    }
    public SummaryNode getNode() {
      return target;
    }
    public int getIdx() {
      return target.preorderCount();
    }      
    public String toString() {
      if (target != null) {
        return "" + target.getDesc(false) + " cost=" + cost;
      } else {
        return " DELETE cost=" + cost;
      }
    }
  }

  ////////////////////////////////////////////////
  // String representation of the overall summary object
  ////////////////////////////////////////////////
  public String getDatasetLabel() {
    return datasetLabel;
  }
  public String dumpSummary() {
    return this.root.dumpSummary(0);
  }
  public String getDesc(int nodeid) {
    return root.getDesc(nodeid);
  }
  public String getLabel(int nodeid) {
    return root.getLabel(nodeid);
  }
  public String getTypeDesc(int nodeid) {
    return root.getTypeDesc(nodeid);
  }
  public String getDocStr(int nodeid) {
    return root.getDocStr(nodeid);
  }

  ////////////////////////////////////////////////
  // Serialization/deserialization
  ////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.write(MAGIC);
    out.write(VERSION);
    root.write(out);
    UTF8.writeString(out, datasetLabel);
  }

  public void readFields(DataInput in) throws IOException {
    byte magic = in.readByte();
    byte version = in.readByte();
    this.root = readAndCreate(in);
    this.root.computePreorder(-1);
    this.datasetLabel = UTF8.readString(in);
  }
}