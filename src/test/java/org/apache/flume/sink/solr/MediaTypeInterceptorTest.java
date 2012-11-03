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

import java.io.File;
import java.util.Collections;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.tika.metadata.Metadata;
import org.junit.Test;

public class MediaTypeInterceptorTest {

  private static final String ID = "stream.type";
  private static final String RESOURCES_DIR = "target/test-classes";
  
  @Test
  public void testPlainText() throws Exception {    
    Context context = createContext();
    Event event = EventBuilder.withBody("foo".getBytes("UTF-8"));
    Assert.assertEquals("text/plain", detect(context, event));
  }

  @Test
  public void testUnknownType() throws Exception {    
    Context context = createContext();
    Event event = EventBuilder.withBody(new byte[] {3, 4, 5, 6});
    Assert.assertEquals("application/octet-stream", detect(context, event));
  }

  @Test
  public void testXML() throws Exception {    
    Context context = createContext();
    Event event = EventBuilder.withBody("<?xml version=\"1.0\"?><foo/>".getBytes("UTF-8"));
    Assert.assertEquals("application/xml", detect(context, event));
  }

  public void testXML11() throws Exception {    
    Context context = createContext();
    Event event = EventBuilder.withBody("<?xml version=\"1.1\"?><foo/>".getBytes("UTF-8"));
    Assert.assertEquals("application/xml", detect(context, event));
  }

  public void testXMLAnyVersion() throws Exception {    
    Context context = createContext();
    Event event = EventBuilder.withBody("<?xml version=\"\"?><foo/>".getBytes("UTF-8"));
    Assert.assertEquals("application/xml", detect(context, event));
  }

  @Test
  public void testXMLasTextPlain() throws Exception {    
    Context context = createContext();
    Event event = EventBuilder.withBody("<foo/>".getBytes("UTF-8"));
    Assert.assertEquals("text/plain", detect(context, event));
  }

  @Test
  public void testPreserveExisting() throws Exception {    
    Context context = createContext();
    Event event = EventBuilder.withBody("foo".getBytes("UTF-8"));
    event.getHeaders().put(ID, "fooType");
    Assert.assertEquals("fooType", detect(context, event));
  }
  
  @Test
  public void testVariousFileTypes() throws Exception {    
    Context context = createContext();
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testBMPfp.txt", "text/plain", "text/plain", 
        path + "/boilerplate.html", "application/xhtml+xml", "application/xhtml+xml",
        path + "/NullHeader.docx", "application/zip", "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        path + "/testWORD_various.doc", "application/x-tika-msoffice", "application/msword",         
        path + "/testPDF.pdf", "application/pdf", "application/pdf",
        path + "/testJPEG_EXIF.jpg", "image/jpeg", "image/jpeg",
        path + "/testXML.xml", "application/xml", "application/xml",
        path + "/cars.csv", "text/plain", "text/csv",
        path + "/cars.csv.gz", "application/x-gzip", "application/x-gzip",
        path + "/cars.tar.gz", "application/x-gtar", "application/x-gtar",
        path + "/sample-statuses-20120906-141433.avro", "avro/binary", "avro/binary",
    };
    
    for (int i = 0; i < files.length; i += 3) {
      byte[] body = FileUtils.readFileToByteArray(new File(files[i+0]));
      Event event = EventBuilder.withBody(body);
      Assert.assertEquals(files[i+1], detect(context, event));
    }
    
    for (int i = 0; i < files.length; i += 3) {
      byte[] body = FileUtils.readFileToByteArray(new File(files[i+0]));
      Map headers = Collections.singletonMap(Metadata.RESOURCE_NAME_KEY, new File(files[i+0]).getName());
      Event event = EventBuilder.withBody(body, headers);
      Assert.assertEquals(files[i+2], detect(context, event));
    }
    
    for (int i = 0; i < files.length; i += 3) {
      byte[] body = FileUtils.readFileToByteArray(new File(files[i+0]));
      Map headers = Collections.singletonMap(Metadata.RESOURCE_NAME_KEY, new File(files[i+0]).getPath());
      Event event = EventBuilder.withBody(body, headers);
      Assert.assertEquals(files[i+2], detect(context, event));
    }
  }

  private Context createContext() {
    Context context = new Context();
    context.put("headerName", ID);
    context.put("preserveExisting", "true");
    return context;
  }

  private String detect(Context context, Event event) {
    MediaTypeInterceptor.Builder builder = new MediaTypeInterceptor.Builder();
    builder.configure(context);
    return builder.build().intercept(event).getHeaders().get(ID);
  }

}
