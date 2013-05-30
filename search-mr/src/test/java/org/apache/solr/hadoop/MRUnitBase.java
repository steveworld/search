/*
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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.solr.hadoop.morphline.MorphlineMapRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

public class MRUnitBase extends Assert {
  
  protected static final String RESOURCES_DIR = "target/test-classes";
  protected static final String DOCUMENTS_DIR = RESOURCES_DIR + "/test-documents";
  protected static File solrHomeZip;

  @BeforeClass
  public static void setupClass() throws Exception {
    solrHomeZip = SolrOutputFormat.createSolrHomeZip(new File(RESOURCES_DIR + "/solr/mrunit"));
    assertNotNull(solrHomeZip);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    solrHomeZip.delete();
  }
  
  protected void setupHadoopConfig(Configuration config) {
    config.set(SolrOutputFormat.ZIP_NAME, solrHomeZip.getName());
    config.set(MorphlineMapRunner.MORPHLINE_FILE_PARAM, RESOURCES_DIR + "/test-morphlines/solrCellDocumentTypes.conf");
  }
}
