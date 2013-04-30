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
package com.cloudera.cdk.morphline.tika;

import java.io.File;

import org.junit.Test;

import com.cloudera.cdk.morphline.api.AbstractMorphlineTest;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.avro.ReadAvroContainerBuilder;
import com.cloudera.cdk.morphline.base.Fields;
import com.google.common.io.Files;

public class DetectMimeTypesTest extends AbstractMorphlineTest {
  
  private static final String AVRO_MIME_TYPE = ReadAvroContainerBuilder.MIME_TYPE;  
  
  private static final File AVRO_FILE = new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433.avro");
  private static final File JPG_FILE = new File(RESOURCES_DIR + "/test-documents/testJPEG_EXIF.jpg");
  
  @Test
  public void testDetectMimeTypesWithFile() throws Exception {
    // config file uses mimeTypesFiles : [src/test/resources/org/apache/tika/mime/custom-mimetypes.xml] 
    testDetectMimeTypesInternal("test-morphlines/detectMimeTypesWithFile");    
  }
  
  @Test
  public void testDetectMimeTypesWithString() throws Exception {
    // config file uses mimeTypesString : """ some mime types go here """ 
    testDetectMimeTypesInternal("test-morphlines/detectMimeTypesWithString");    
  }
  
  private void testDetectMimeTypesInternal(String configFile) throws Exception {
    // verify that Avro is classified as Avro  
    morphline = createMorphline(configFile);    
    Record record = new Record();    
    record.put(Fields.ATTACHMENT_BODY, Files.toByteArray(AVRO_FILE));
    startSession();
    morphline.process(record);
    assertEquals(AVRO_MIME_TYPE, collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_MIME_TYPE));

    // verify that JPG isnt' classified as JPG because this morphline uses includeDefaultMimeTypes : false 
    collector.reset();
    record = new Record();    
    record.put(Fields.ATTACHMENT_BODY, Files.toByteArray(JPG_FILE));
    startSession();
    morphline.process(record);
    assertEquals("application/octet-stream", collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_MIME_TYPE));
  }
  
  @Test
  public void testDetectMimeTypesWithDefaultMimeTypes() throws Exception {
    morphline = createMorphline("test-morphlines/detectMimeTypesWithDefaultMimeTypes");    
    Record record = new Record();    
    record.put(Fields.ATTACHMENT_BODY, Files.toByteArray(JPG_FILE));
    startSession();
    morphline.process(record);
    assertEquals("image/jpeg", collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_MIME_TYPE));
  }

  @Test
  public void testMimeTypeAlreadySpecifiedOnInputRemainsUnchanged() throws Exception {
    morphline = createMorphline("test-morphlines/detectMimeTypesWithDefaultMimeTypes");    
    Record record = new Record();    
    record.put(Fields.ATTACHMENT_BODY, Files.toByteArray(JPG_FILE));
    record.put(Fields.ATTACHMENT_MIME_TYPE, "foo/bar");
    startSession();
    morphline.process(record);
    assertEquals("foo/bar", collector.getFirstRecord().getFirstValue(Fields.ATTACHMENT_MIME_TYPE));
  }

}
