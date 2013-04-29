/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.morphline.api;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.cloudera.cdk.morphline.cmd.MorphlineBuilder;
import com.typesafe.config.Config;
import com.yammer.metrics.core.MetricsRegistry;

public class AbstractMorphlineTest extends Assert {
  
  protected Collector collector;
  protected Command morphline;
  
//  protected static final String RESOURCES_DIR = "target/test-classes";
  
  @Before
  public void setUp() throws Exception {
    collector = new Collector();
  }
  
  @After
  public void tearDown() throws Exception {
    collector = null;
    morphline = null;
  }
    
  protected Command createMorphline(String file) throws IOException {
    return new MorphlineBuilder().build(parse(file), null, collector, createMorphlineContext());
  }

  protected Command createMorphline(Config config) {
    return new MorphlineBuilder().build(config, null, collector, createMorphlineContext());
  }

  protected void deleteAllDocuments() {
    collector.reset();
  }
  
  protected Config parse(String file) throws IOException {
//    Config config = Configs.parse(file);
    Config config = Configs.parse(new File("src/test/resources/" + file + ".conf"));
    config = config.getConfigList("morphlines").get(0);
    return config;
  }
  
  private MorphlineContext createMorphlineContext() {
    return new MorphlineContext.Builder().setMetricsRegistry(new MetricsRegistry()).build();
  }
  
}
