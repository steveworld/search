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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrCellTest extends TikaIndexerTestBase {
  
  private Map<String,Integer> expectedRecords = new HashMap();

  private static final String SAMPLE_FILE = "/sample-statuses-20120906-141433.avro";
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    myInitCore("solrcelltest");
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    String path = RESOURCES_DIR + "/test-documents";
    expectedRecords.put(path + SAMPLE_FILE, 2);
  }

  @Test
  public void testDocCount() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + SAMPLE_FILE
    };
    testDocumentTypesInternal(files, expectedRecords, indexer, false);
  }

}
