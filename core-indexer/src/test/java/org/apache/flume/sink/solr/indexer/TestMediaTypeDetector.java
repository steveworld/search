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
package org.apache.flume.sink.solr.indexer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.tika.metadata.Metadata;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestMediaTypeDetector extends Assert {

  private static final String RESOURCES_DIR = "target/test-classes";
  //private static final String RESOURCES_DIR = "src/test/resources";
  
  @Test
  public void testPlainText() throws Exception { 
    StreamEvent event = createEvent("foo".getBytes("UTF-8"));
    assertEquals("text/plain", detect(event, false));
  }

  @Test
  public void testUnknownType() throws Exception {    
    StreamEvent event = createEvent(new byte[] {3, 4, 5, 6});
    assertEquals("application/octet-stream", detect(event, false));
  }

  @Test
  public void testUnknownEmptyType() throws Exception {    
    StreamEvent event = createEvent(new byte[0]);
    assertEquals("application/octet-stream", detect(event, false));
  }

  @Ignore
  @Test
  public void testNullType() throws Exception {    
    StreamEvent event = createEvent(null);
    assertEquals("application/octet-stream", detect(event, false));
  }

  @Test
  public void testXML() throws Exception {    
    StreamEvent event = createEvent("<?xml version=\"1.0\"?><foo/>".getBytes("UTF-8"));
    assertEquals("application/xml", detect(event, false));
  }

  public void testXML11() throws Exception {    
    StreamEvent event = createEvent("<?xml version=\"1.1\"?><foo/>".getBytes("UTF-8"));
    assertEquals("application/xml", detect(event, false));
  }

  public void testXMLAnyVersion() throws Exception {    
    StreamEvent event = createEvent("<?xml version=\"\"?><foo/>".getBytes("UTF-8"));
    assertEquals("application/xml", detect(event, false));
  }

  @Test
  public void testXMLasTextPlain() throws Exception {    
    StreamEvent event = createEvent("<foo/>".getBytes("UTF-8"));
    assertEquals("text/plain", detect(event, false));
  }

  @Test
  public void testVariousFileTypes() throws Exception {    
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
        
        path + "/testPPT_various.ppt", "application/x-tika-msoffice", "application/vnd.ms-powerpoint",
        path + "/testPPT_various.pptx", "application/x-tika-ooxml", "application/vnd.openxmlformats-officedocument.presentationml.presentation",        
        path + "/testEXCEL.xlsx", "application/x-tika-ooxml", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        path + "/testEXCEL.xls", "application/vnd.ms-excel", "application/vnd.ms-excel",
        path + "/testPages.pages",  "application/zip", "application/vnd.apple.pages",
        path + "/testNumbers.numbers", "application/zip", "application/vnd.apple.numbers",
        path + "/testKeynote.key", "application/zip", "application/vnd.apple.keynote",
        
        path + "/testRTFVarious.rtf", "application/rtf", "application/rtf",
        path + "/complex.mbox", "text/plain", "application/mbox",        
        path + "/test-outlook.msg", "application/x-tika-msoffice", "application/vnd.ms-outlook",
        path + "/testEMLX.emlx", "message/x-emlx", "message/x-emlx",
        path + "/testRFC822",  "message/rfc822", "message/rfc822",
        path + "/rsstest.rss", "application/rss+xml", "application/rss+xml",
        path + "/testDITA.dita", "application/dita+xml; format=task", "application/dita+xml; format=task",
        
        path + "/testMP3i18n.mp3", "audio/mpeg", "audio/mpeg",
        path + "/testAIFF.aif", "audio/x-aiff", "audio/x-aiff",
        path + "/testFLAC.flac", "audio/x-flac", "audio/x-flac",
        path + "/testFLAC.oga", "audio/ogg", "audio/ogg",
        path + "/testVORBIS.ogg",  "audio/ogg", "audio/ogg",
        path + "/testMP4.m4a", "audio/mp4", "audio/mp4",
        path + "/testWAV.wav",  "audio/x-wav", "audio/x-wav",
        path + "/testWMA.wma",  "audio/x-ms-wma", "audio/x-ms-wma",
        
        path + "/testFLV.flv", "video/x-flv", "video/x-flv",
        path + "/testWMV.wmv",  "video/x-ms-wmv", "video/x-ms-wmv",
        
        path + "/testBMP.bmp", "image/x-ms-bmp", "image/x-ms-bmp",
        path + "/testPNG.png", "image/png", "image/png",        
        path + "/testPSD.psd", "image/vnd.adobe.photoshop", "image/vnd.adobe.photoshop",        
        path + "/testSVG.svg", "image/svg+xml", "image/svg+xml",        
        path + "/testTIFF.tif", "image/tiff", "image/tiff",        

        path + "/test-documents.7z",  "application/x-7z-compressed", "application/x-7z-compressed",
        path + "/test-documents.cpio",  "application/x-cpio", "application/x-cpio",
        path + "/test-documents.tar",  "application/x-gtar", "application/x-gtar",
        path + "/test-documents.tbz2",  "application/x-bzip2", "application/x-bzip2",
        path + "/test-documents.tgz",  "application/x-gzip", "application/x-gzip",
        path + "/test-documents.zip",  "application/zip", "application/zip",
        path + "/test-zip-of-zip.zip",  "application/zip", "application/zip",
        path + "/testJAR.jar",  "application/zip", "application/java-archive",
        
        path + "/testKML.kml",  "application/vnd.google-earth.kml+xml", "application/vnd.google-earth.kml+xml",
        path + "/testRDF.rdf",  "application/rdf+xml", "application/rdf+xml",
        path + "/testTrueType.ttf",  "application/x-font-ttf", "application/x-font-ttf",
        path + "/testVISIO.vsd",  "application/x-tika-msoffice", "application/vnd.visio",
        path + "/testWAR.war",  "application/zip", "application/x-tika-java-web-archive",
        path + "/testWindows-x86-32.exe",  "application/x-msdownload; format=pe32", "application/x-msdownload; format=pe32",
        path + "/testWINMAIL.dat",  "application/vnd.ms-tnef", "application/vnd.ms-tnef",
        path + "/testWMF.wmf",  "application/x-msmetafile", "application/x-msmetafile",
    };
    
    for (int i = 0; i < files.length; i += 3) {
      byte[] body = FileUtils.readFileToByteArray(new File(files[i+0]));
      StreamEvent event = new StreamEvent(new ByteArrayInputStream(body), new HashMap());
      assertEquals(files[i+1], detect(event, false));
    }
    
    for (int i = 0; i < files.length; i += 3) {
      byte[] body = FileUtils.readFileToByteArray(new File(files[i+0]));
      Map headers = Collections.singletonMap(Metadata.RESOURCE_NAME_KEY, new File(files[i+0]).getName());
      StreamEvent event = new StreamEvent(new ByteArrayInputStream(body), headers);
      assertEquals(files[i+2], detect(event, true));
    }
    
    for (int i = 0; i < files.length; i += 3) {
      byte[] body = FileUtils.readFileToByteArray(new File(files[i+0]));
      Map headers = Collections.singletonMap(Metadata.RESOURCE_NAME_KEY, new File(files[i+0]).getPath());
      StreamEvent event = new StreamEvent(new ByteArrayInputStream(body), headers);
      assertEquals(files[i+2], detect(event, true));
    }
  }

  private StreamEvent createEvent(byte[] bytes) {
    return new StreamEvent(new ByteArrayInputStream(bytes), new HashMap());
  }
  
  private String detect(StreamEvent event, boolean includeHeaders) {
    MediaTypeDetector detector = new MediaTypeDetector(null);
    return detector.getMediaType(event, detector.getMetadata(event,  includeHeaders));
  }

}
