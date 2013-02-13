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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.tika.TikaIndexer;
import org.junit.Assert;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValueFactory;

/** See https://github.com/typesafehub/config */
public class TestHoconConfig extends Assert {

	@Test
	public void testBasic() {
		Config conf = ConfigFactory.load("test-application").getConfig(getClass().getPackage().getName() + ".test");
		
    assertEquals(conf.getString("foo.bar"), "1234");
    assertEquals(conf.getInt("foo.bar"), 1234);
		assertEquals(conf.getInt("moo.bar"), 56789); // read from reference.config
		
		Config subConfig = conf.getConfig("foo");
    assertNotNull(subConfig);
    assertEquals(subConfig.getString("bar"), "1234");
    
    assertFalse(conf.hasPath("missing.foox.barx"));
    try {
      conf.getString("missing.foox.barx");
      fail("Failed to detect missing param");
    } catch (ConfigException.Missing e) {} 

    Iterator userNames = Arrays.asList("nadja", "basti").iterator();
		Iterator passwords = Arrays.asList("nchangeit", "bchangeit").iterator();
		for (Config user : conf.getConfigList("users")) {
			assertEquals(user.getString("userName"), userNames.next());
			assertEquals(user.getString("password"), passwords.next());
		}
		assertFalse(userNames.hasNext());
		assertFalse(passwords.hasNext());
		
		assertEquals(conf.getStringList("files.paths"), Arrays.asList("dir/file1.log", "dir/file2.txt"));
		Iterator schemas = Arrays.asList("schema1.json", "schema2.json").iterator();
		Iterator globs = Arrays.asList("*.log*", "*.txt*").iterator();
		for (Config fileMapping : conf.getConfigList("files.fileMappings")) {
			assertEquals(fileMapping.getString("schema"), schemas.next());
			assertEquals(fileMapping.getString("glob"), globs.next());
		}
		assertFalse(schemas.hasNext());
		assertFalse(globs.hasNext());    
				
//		Object list2 = conf.entrySet();
//		Object list2 = conf.getAnyRef("users.userName");
//		assertEquals(conf.getString("users.user.userName"), "nadja");
	}
	
  @Test
	public void testParseMap() { // test access based on path
    final Map<String, String> map = new HashMap();
    map.put(TikaIndexer.TIKA_CONFIG_LOCATION, "src/test/resources/tika-config.xml");
    map.put(TikaIndexer.SOLR_COLLECTION_LIST + ".testcoll." + TikaIndexer.SOLR_CLIENT_HOME, "target/test-classes/solr/collection1");
//    Config config = ConfigValueFactory.fromMap(new Context(map).getParameters()).toConfig();
    Config config = ConfigFactory.parseMap(map);
	  String filePath = config.getString(TikaIndexer.TIKA_CONFIG_LOCATION);
	  assertEquals(map.get(TikaIndexer.TIKA_CONFIG_LOCATION), filePath);
    Config subConfig = config.getConfig(TikaIndexer.SOLR_COLLECTION_LIST).getConfig("testcoll");
    assertEquals("target/test-classes/solr/collection1", subConfig.getString(TikaIndexer.SOLR_CLIENT_HOME));
	}
  
  @Test
  public void testFromMap() { // test access based on key
    final Map<String, String> map = new HashMap();
    map.put(TikaIndexer.TIKA_CONFIG_LOCATION, "src/test/resources/tika-config.xml");
    String key = TikaIndexer.SOLR_COLLECTION_LIST + ".testcoll." + TikaIndexer.SOLR_CLIENT_HOME;
    map.put(key, "target/test-classes/solr/collection1");
    ConfigObject config = ConfigValueFactory.fromMap(map);
    String filePath = config.get(TikaIndexer.TIKA_CONFIG_LOCATION).unwrapped().toString();
    assertEquals(map.get(TikaIndexer.TIKA_CONFIG_LOCATION), filePath);
    assertEquals(map.get(key), config.get(key).unwrapped().toString());
  }

}
