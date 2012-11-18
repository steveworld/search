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

package org.apache.flume.sink.solr.indexer;


import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This should include tests against the example solr config
 * 
 * This lets us try various SolrServer implementations with the same tests.
 * 
 *
 * @since solr 1.3
 */
public class SolrExampleTests extends SolrJettyTestBase
{
  private static final String RESOURCES_DIR = "target/test-classes";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(
        RESOURCES_DIR + "/solr/collection1/conf/solrconfig.xml", 
        RESOURCES_DIR + "/solr/collection1/conf/schema.xml",
        RESOURCES_DIR + "/solr"
        );
  }
  
//  public String getSolrHome() { return new File(RESOURCES_DIR + "/solr/collection1/conf").getAbsolutePath(); }

  protected void assertNumFound( String query, int num ) throws SolrServerException
  {
    QueryResponse rsp = getSolrServer().query( new SolrQuery( query ) );
    if( num != rsp.getResults().getNumFound() ) {
      fail( "expected: "+num +" but had: "+rsp.getResults().getNumFound() + " :: " + rsp.getResults() );
    }
  }

 @Test
 public void testAddDelete() throws Exception
  {    
    SolrServer server = getSolrServer();
    
    // Empty the database...
    server.deleteByQuery( "*:*" );// delete everything!
    
    SolrInputDocument[] doc = new SolrInputDocument[3];
    for( int i=0; i<3; i++ ) {
      doc[i] = new SolrInputDocument();
      doc[i].setField( "id", i + " & 222", 1.0f );
    }
    String id = (String) doc[0].getField( "id" ).getFirstValue();
    
    server.add( doc[0] );
    server.commit();
    assertNumFound( "*:*", 1 ); // make sure it got in
    
    // make sure it got in there
    server.deleteById( id );
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got out
    
    // add it back 
    server.add( doc[0] );
    server.commit();
    assertNumFound( "*:*", 1 ); // make sure it got in
    server.deleteByQuery( "id:\""+ClientUtils.escapeQueryChars(id)+"\"" );
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got out
    
    // Add two documents
    for( SolrInputDocument d : doc ) {
      server.add( d );
    }
    server.commit();
    assertNumFound( "*:*", 3 ); // make sure it got in
    
    // should be able to handle multiple delete commands in a single go
    List<String> ids = new ArrayList<String>();
    for( SolrInputDocument d : doc ) {
      ids.add(d.getFieldValue("id").toString());
    }
    server.deleteById(ids);
    server.commit();
    assertNumFound( "*:*", 0 ); // make sure it got out
  }
  
 @Test
 public void testQueryWithParams() throws SolrServerException {
   SolrServer server = getSolrServer();
   SolrQuery q = new SolrQuery("query");
   q.setParam("debug", true);
   QueryResponse resp = server.query(q);
   assertEquals(
       "server didn't respond with debug=true, didn't we pass in the parameter?",
       "true",
       ((NamedList) resp.getResponseHeader().get("params")).get("debug"));
 }
   
}
