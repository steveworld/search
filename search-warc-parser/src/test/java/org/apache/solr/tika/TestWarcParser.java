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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestWarcParser extends TikaIndexerTestBase {
  private static final String sampleWarcFile = "/IAH-20080430204825-00000-blackbook.warc.gz";
  private Map<String,Integer> expectedRecords = new HashMap();

  @Override
  protected Map<String, String> getContext() {
    final Map<String, String> context = super.getContext();
    // tell the TikaIndexer to pass a  GZIPInputStream to tika.  This is temporary until CDH-10671 is addressed.
    context.put("tika.decompressConcatenated", "true");
    return context;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    String path = RESOURCES_DIR + "/test-documents";
    expectedRecords.put(path + sampleWarcFile, 140);
  }

  /**
   * Returns a HashMap of expectedKey -> expectedValue.
   * File format is required to alternate lines of keys and values.
   *
   * @param file
   * @throws IOException
   */
  private HashMap<String, ExpectedResult> getExpectedOutput(String file) throws IOException{
    HashMap<String, ExpectedResult> map = new HashMap<String, ExpectedResult>();
    List<String> lines = Files.readLines(new File(file), Charsets.UTF_8);
    Iterator<String> it = lines.iterator();
    String contains = "#contains";
    while (it.hasNext()) {
      String key = it.next();
      if (!it.hasNext()) {
        throw new IOException("Unexpected file format for " + file
          + ".  Expected alternativing key/value lines");
      }
      String value = it.next();
      ExpectedResult.CompareType compareType = ExpectedResult.CompareType.equals;
      if (key.endsWith(contains)) {
        key = key.substring(0, key.length() - contains.length());
        compareType = ExpectedResult.CompareType.contains;
      }
      HashSet<String> values = null;
      ExpectedResult existing = map.get(key);
      if (existing != null) {
        values = existing.getFieldValues();
        values.add(value);
      }
      else {
        values = new HashSet<String>();
      }
      values.add(value);
      map.put(key, new ExpectedResult(values, compareType));
    }
    return map;
  }

  /**
   * Test that Solr queries on a parsed warc document
   * return the expected content and fields.
   */
  @Test
  public void testWARCFileContent() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    String testFilePrefix = "sample_html";
    String testFile = testFilePrefix + ".warc.gz";
    String expectedFile = testFilePrefix + ".gold";
    String[] files = new String[] {
      path + "/" + testFile
    };
    testDocumentTypesInternal(files, expectedRecords);
    HashMap<String, ExpectedResult> expectedResultMap = getExpectedOutput(path + "/" + expectedFile);
    testDocumentContent(expectedResultMap);
  }

  /**
   * Test that a multi document warc file is parsed
   * into the correct number of documents.
   */
  @Test
  public void testWARCFileMultiDocCount() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + sampleWarcFile
    };
    testDocumentTypesInternal(files, expectedRecords);
  }

  /**
   * Test that Solr queries on a parsed multi-doc warc document
   * return the expected content and fields.
   */
  @Test
  public void testWARCFileMultiDocContent() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + sampleWarcFile
    };
    testDocumentTypesInternal(files, expectedRecords);
    String testFileSuffix = ".warc.gz";
    String expectedFileSuffix = ".gold";
    String expectedFile = sampleWarcFile.substring(0, sampleWarcFile.length() - testFileSuffix.length()) + expectedFileSuffix;
    HashMap<String, ExpectedResult> expectedResultMap = getExpectedOutput(path + "/" + expectedFile);
    testDocumentContent(expectedResultMap);
  }

  private void testDocumentContent(HashMap<String, ExpectedResult> expectedResultMap) throws Exception {
    SolrCollection collection = indexer.getSolrCollection();
    QueryResponse rsp = ((SolrServerDocumentLoader)collection.getDocumentLoader()).getSolrServer().query(new SolrQuery("*:*").setRows(Integer.MAX_VALUE));
    // Check that every expected field/values shows up in the actual query
    for (Entry<String, ExpectedResult> current : expectedResultMap.entrySet()) {
      String field = current.getKey();
      for (String expectedFieldValue : current.getValue().getFieldValues()) {
        ExpectedResult.CompareType compareType = current.getValue().getCompareType();
        boolean foundField = false;

        for (SolrDocument doc : rsp.getResults()) {
          Collection<Object> actualFieldValues = doc.getFieldValues(field);
          if (compareType == ExpectedResult.CompareType.equals) {
            if (actualFieldValues != null && actualFieldValues.contains(expectedFieldValue)) {
              foundField = true;
              break;
            }
          }
          else {
            for (Iterator<Object> it = actualFieldValues.iterator(); it.hasNext(); ) {
              String actualValue = it.next().toString();  // test only supports string comparison
              if (actualFieldValues != null && actualValue.contains(expectedFieldValue)) {
                foundField = true;
                break;
              }
            }
          }
        }
        assert(foundField); // didn't find expected field/value in query
      }
    }
  }

  /**
   * Representation of the expected output of a SolrQuery.
   */
  private static class ExpectedResult {
    private HashSet<String> fieldValues;
    private enum CompareType {
      equals,    // Compare with equals, i.e. actual.equals(expected)
      contains;  // Compare with contains, i.e. actual.contains(expected)
    }
    private CompareType compareType;

    public ExpectedResult(HashSet<String> fieldValues, CompareType compareType) {
      this.fieldValues = fieldValues;
      this.compareType = compareType;
    }
    public HashSet<String> getFieldValues() { return fieldValues; }
    public CompareType getCompareType() { return compareType; }
  }
}
