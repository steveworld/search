/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.solr.tika;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.tika.metadata.Metadata;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that {@link StripNonCharContentHandler} properly strips illegal characters.
 */
public class TestStripNonCharContentHandler extends TikaIndexerTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    myInitCore(DEFAULT_BASE_DIR);
  }

  /**
   * Returns string "foobar" with illegal characters interspersed.
   */
  private String getFoobarWithNonChars() {
    char illegalChar = '\uffff';
    StringBuilder builder = new StringBuilder();
    builder.append(illegalChar).append(illegalChar).append("foo").append(illegalChar)
      .append(illegalChar).append("bar").append(illegalChar).append(illegalChar);
    return builder.toString();
  }

  /**
   * Test that the ContentHandler properly strips the illegal characters
   */
  @Test
  public void testTransformValue() {
    String fieldName = "user_name";
    assertFalse("foobar".equals(getFoobarWithNonChars()));

    SolrCollection coll = indexer.getSolrCollection();
    Metadata metadata = new Metadata();
    // load illegal char string into a metadata field and generate a new document,
    // which will cause the ContentHandler to be invoked.
    metadata.set(fieldName, getFoobarWithNonChars());
    StripNonCharContentHandlerFactory contentHandlerFactory =
      new StripNonCharContentHandlerFactory(coll.getDateFormats());
    SolrContentHandler contentHandler =
      contentHandlerFactory.createSolrContentHandler(metadata, coll.getSolrParams(), coll.getSchema());
    SolrInputDocument doc = contentHandler.newDocument();
    String foobar = doc.getFieldValue(fieldName).toString();
    assertTrue("foobar".equals(foobar));
  }
}
