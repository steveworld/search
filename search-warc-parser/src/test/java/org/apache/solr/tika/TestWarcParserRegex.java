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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.tika.parser.StreamingWarcParser;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Test for regex doc-type based matching of the WarcParser
 */
public class TestWarcParserRegex extends TikaIndexerTestBase {
  private static final String sampleWarcFile = "/IAH-20080430204825-00000-blackbook.warc.gz";

  @Override
  protected Map<String, String> getContext() {
    final Map<String, String> context = super.getContext();
    // tell the TikaIndexer to pass a  GZIPInputStream to tika.  This is temporary until CDH-10671 is addressed.
    context.put("tika.decompressConcatenated", "true");
    String regex = "text/html|text/plain";
    context.put(StreamingWarcParser.MIMETYPES_TO_PARSE_PROPERTY, regex);
    return context;
  }

  private SolrIndexer getSolrIndexer(String regex, int expectedCount) throws Exception {
    Map<String, String> context = getContext();
    context.put(StreamingWarcParser.MIMETYPES_TO_PARSE_PROPERTY, regex);
    Config config = ConfigFactory.parseMap(context);
   
    SolrIndexer solrIndexer =
      new TikaIndexer(new SolrInspector().createSolrCollection(config, indexer.getSolrCollection().getDocumentLoader()), config);
    deleteAllDocuments(solrIndexer);
    return solrIndexer;
  }
  
  private void verifyDocCount(String regex, int expectedCount) throws Exception {
    SolrIndexer solrIndexer = getSolrIndexer(regex, expectedCount);
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + sampleWarcFile
    };
    Map<String,Integer> expectedRecords = new HashMap();
    expectedRecords.put(path + sampleWarcFile, expectedCount);
    testDocumentTypesInternal(files, expectedRecords, solrIndexer, false);
    tearDown(solrIndexer);
  }

  /**
   * Test that a multi document warc file is parsed
   * into the correct number of documents given the regex.
   */
  @Test
  public void testWARCFileMultiDocCountRegex() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + sampleWarcFile
    };

    int expectedHTMLDocCount = 140;
    verifyDocCount("text/html", expectedHTMLDocCount);
    int expectedPlainTextDocCount = 37;
    verifyDocCount("text/plain", expectedPlainTextDocCount);

    // ensure the regex-or is the sum of both of the doc counts
    Map<String,Integer> expectedRecords = new HashMap();
    expectedRecords.put(path + sampleWarcFile, expectedHTMLDocCount + expectedPlainTextDocCount);
    deleteAllDocuments();
    testDocumentTypesInternal(files, expectedRecords, indexer, false);
  }
}
