/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.morphline;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;
import com.cloudera.cdk.morphline.api.Collector;
import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Compiler;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Notifications;
import com.cloudera.cdk.morphline.stdlib.PipeBuilder;
import com.google.common.collect.ListMultimap;
import com.typesafe.config.Config;
import com.yammer.metrics.core.MetricsRegistry;

@ThreadLeakAction({Action.WARN})
@ThreadLeakLingering(linger = 0)
@ThreadLeakZombies(Consequence.CONTINUE)
@ThreadLeakScope(Scope.NONE)
@SuppressCodecs({"Lucene3x", "Lucene40"})
public class SolrMorphlineZkTest extends AbstractFullDistribZkTestBase {
  
  private static final String RESOURCES_DIR = "target/test-classes";
  private static final File SOLR_HOME_DIR = new File(RESOURCES_DIR + "/solr/collection1");

  private Collector collector;
  private Command morphline;

  @Override
  public String getSolrHome() {
    return SOLR_HOME_DIR.getPath();
  }
  
  public SolrMorphlineZkTest() {
    fixShardCount = true;
    sliceCount = 3;
    shardCount = 3;
  }
  
  @BeforeClass
  public static void setupClass() throws Exception {
    createTempDir();
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("host", "127.0.0.1");
    System.setProperty("numShards", Integer.toString(sliceCount));
    uploadConfFiles();
    collector = new Collector();
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("host");
    System.clearProperty("numShards");
  }
  
  @Test
  @Override
  public void testDistribSearch() throws Exception {
    super.testDistribSearch();
  }
  
  @Override
  public void doTest() throws Exception {
    
    waitForRecoveriesToFinish(false);
    
    Config config = parse("test-morphlines/loadSolrBasic.conf");    
    morphline = createMorphline(config);
    Record record = new Record();
    record.put(Fields.ID, "id0-innsbruck");
    record.put("text", "mytext");
    record.put("user_screen_name", "foo");
    record.put("first_name", "Nadja"); // will be sanitized
    startSession();
    Notifications.notifyBeginTransaction(morphline);
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getNumStartEvents());
    Notifications.notifyCommitTransaction(morphline);
    Record expected = new Record();
    expected.put(Fields.ID, "id0-innsbruck");
    expected.put("text", "mytext");
    expected.put("user_screen_name", "foo");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    
    //cloudClient.commit();
    cloudClient.commit(false, true, true);
    //Thread.sleep(3000);
    QueryResponse rsp = cloudClient.query(new SolrQuery("*:*"));   
    //System.out.println(rsp);
    Iterator<SolrDocument> iter = rsp.getResults().iterator();
    assertEquals(expected.getFields(), next(iter));
    assertFalse(iter.hasNext());
    
    Notifications.notifyRollbackTransaction(morphline);
    Notifications.notifyShutdown(morphline);
    cloudClient.shutdown();
  }

  private ListMultimap<String, Object> next(Iterator<SolrDocument> iter) {
    SolrDocument doc = iter.next();
    Record record = toRecord(doc);
    record.removeAll("_version_"); // the values of this field are unknown and internal to solr
    return record.getFields();    
  }
  
  private Record toRecord(SolrDocument doc) {
    Record record = new Record();
    for (String key : doc.keySet()) {
      record.getFields().replaceValues(key, doc.getFieldValues(key));        
    }
    return record;
  }
  
  private Config parse(String file) {
    SolrLocator locator = new SolrLocator(createMorphlineContext());
    locator.setCollectionName("collection1");
    locator.setZkHost(zkServer.getZkAddress());
    //locator.setServerUrl(cloudJettys.get(0).url); // TODO: download IndexSchema from solrUrl not yet implemented
    //locator.setSolrHomeDir(SOLR_HOME_DIR.getPath());
    Config config = new Compiler().parse(file, locator.toConfig("SOLR_LOCATOR"));
    config = config.getConfigList("morphlines").get(0);
    return config;
  }
  
  private Command createMorphline(Config config) {
    return new PipeBuilder().build(config, null, collector, createMorphlineContext());
  }

  private MorphlineContext createMorphlineContext() {
    return new SolrMorphlineContext.Builder()
      .setFaultTolerance(new FaultTolerance(false,  false))
      .setMetricsRegistry(new MetricsRegistry())
      .build();
  }
  
  private void startSession() {
    Notifications.notifyStartSession(morphline);
  }

  @Override
  public JettySolrRunner createJetty(File solrHome, String dataDir,
      String shardList, String solrConfigOverride, String schemaOverride)
      throws Exception {
    
    JettySolrRunner jetty = new JettySolrRunner(solrHome.getAbsolutePath(),
        context, 0, solrConfigOverride, schemaOverride);

    jetty.setShards(shardList);
    
    if (System.getProperty("collection") == null) {
      System.setProperty("collection", "collection1");
    }
    
    jetty.start();
    
    System.clearProperty("collection");
    
    return jetty;
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String name) throws Exception {
    putConfig(zkClient, solrhome, name, name);
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String srcName, String destName)
      throws Exception {
    
    File file = new File(solrhome, "conf" + File.separator + srcName);
    if (!file.exists()) {
      // LOG.info("skipping " + file.getAbsolutePath() +
      // " because it doesn't exist");
      return;
    }
    
    String destPath = "/configs/conf1/" + destName;
    // LOG.info("put " + file.getAbsolutePath() + " to " + destPath);
    zkClient.makePath(destPath, file, false, true);
  }
  
  private void uploadConfFiles() throws Exception {
    // upload our own config files
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), 10000);
    putConfig(zkClient, SOLR_HOME_DIR, "solrconfig.xml");
    putConfig(zkClient, SOLR_HOME_DIR, "schema.xml");
    putConfig(zkClient, SOLR_HOME_DIR, "elevate.xml");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_en.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_ar.txt");
    
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_bg.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_ca.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_cz.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_da.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_el.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_es.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_eu.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_de.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_fa.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_fi.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_fr.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_ga.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_gl.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_hi.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_hu.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_hy.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_id.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_it.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_ja.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_lv.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_nl.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_no.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_pt.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_ro.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_ru.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_sv.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_th.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stopwords_tr.txt");
    
    putConfig(zkClient, SOLR_HOME_DIR, "lang/contractions_ca.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/contractions_fr.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/contractions_ga.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "lang/contractions_it.txt");
    
    putConfig(zkClient, SOLR_HOME_DIR, "lang/stemdict_nl.txt");
    
    putConfig(zkClient, SOLR_HOME_DIR, "lang/hyphenations_ga.txt");
    
    putConfig(zkClient, SOLR_HOME_DIR, "stopwords.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "protwords.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "currency.xml");
    putConfig(zkClient, SOLR_HOME_DIR, "open-exchange-rates.json");
    putConfig(zkClient, SOLR_HOME_DIR, "mapping-ISOLatin1Accent.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "old_synonyms.txt");
    putConfig(zkClient, SOLR_HOME_DIR, "synonyms.txt");
    zkClient.close();
  }
  
}
