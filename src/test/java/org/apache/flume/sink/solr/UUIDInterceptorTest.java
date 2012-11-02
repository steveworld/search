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
package org.apache.flume.sink.solr;

import junit.framework.Assert;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;

public class UUIDInterceptorTest {

  private static final String ID = "id";
  
  @Test
  public void testBasic() throws Exception {    
    Context context = new Context();
    context.put("headerName", ID);
    context.put("preserveExisting", "true");
    Event event = new SimpleEvent();
    Assert.assertTrue(build(context).intercept(event).getHeaders().get(ID).startsWith("ss#"));
  }

  @Test
  public void testPreserveExisting() throws Exception {    
    Context context = new Context();
    context.put("headerName", ID);
    context.put("preserveExisting", "true");
    Event event = new SimpleEvent();
    event.getHeaders().put(ID, "foo");
    Assert.assertEquals("foo", build(context).intercept(event).getHeaders().get(ID));
  }

  private UUIDInterceptor build(Context context) {
    UUIDInterceptor.Builder builder = new UUIDInterceptor.Builder();
    builder.configure(context);
    UUIDInterceptor interceptor = builder.build();
    return interceptor;
  }
  
//  @Test
  private void testUUIDGenerationPerformance() throws Exception {    
    testUUIDGenerationPerformance(Generators.timeBasedGenerator(EthernetAddress.fromInterface()));
    testUUIDGenerationPerformance(Generators.randomBasedGenerator());
  }
  
  private void testUUIDGenerationPerformance(NoArgGenerator uuidGenerator) throws Exception {
    long xstartTime = System.currentTimeMillis();
    int iters = 1000000;
    int x = 0;
    for (int i = 0; i < iters; i++) {
      x += uuidGenerator.generate().toString().length();
    }
    System.out.println(x);
    float secs = (System.currentTimeMillis() - xstartTime) / 1000.0f;
    System.out.println(uuidGenerator.getClass().getName() + " took " + secs + " secs, iters/sec=" + (iters / secs));
  }

}
