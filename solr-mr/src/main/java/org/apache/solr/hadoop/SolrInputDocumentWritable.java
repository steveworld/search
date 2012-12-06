/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

public class SolrInputDocumentWritable implements Writable {
  private static long count = 0;
  private SolrInputDocument sid;

  public SolrInputDocumentWritable() {
  }

  public SolrInputDocumentWritable(SolrInputDocument sid) {
    this.sid = sid;
  }

  public SolrInputDocument getSolrInputDocument() {
    return sid;
  }

  @Override
  public String toString() {
    return sid.toString();
  }

  Text name = new Text();
  Text value = new Text();
  FloatWritable boost = new FloatWritable();

  @Override
  public void write(DataOutput out) throws IOException {
//    JavaBinCodec codec = new JavaBinCodec();
//    FastOutputStream daos = FastOutputStream.wrap(DataOutputOutputStream.constructOutputStream(out));
//    codec.init(daos);
//    try {
//      if (count++ < 10) { System.err.println(toString()); }
//      daos.writeLong(MARKER);
//      codec.writeVal(sid);
//    } finally {
//      daos.flushBuffer();
//    }

    Collection<SolrInputField> values = sid.values();
    out.writeInt(values.size());
    for (SolrInputField fval: values) {
      name.set(fval.getName());
      name.write(out);
      value.set(fval.getValue().toString());
      value.write(out);
      boost.set(fval.getBoost());
      boost.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
//    JavaBinCodec codec = new JavaBinCodec();
//    FastInputStream dis = FastInputStream.wrap(DataInputInputStream.constructInputStream(in));
//    try {
//      System.err.println("PDH readval starting");
//      long marker = dis.readLong();
//      if (marker != MARKER) {
//        throw new RuntimeException("Invalid version (expected " + MARKER +
//            ", but " + marker + ") or the data in not in 'javabin' format");
//      }
//
//    sid = (SolrInputDocument)codec.readVal(dis);
//    System.err.println(toString());
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    sid = new SolrInputDocument();

    int count = in.readInt();
    while(count-- > 0) {
      name.readFields(in);
      value.readFields(in);
      boost.readFields(in);
      sid.addField(name.toString(), value.toString(), boost.get());
    }
  }

}
