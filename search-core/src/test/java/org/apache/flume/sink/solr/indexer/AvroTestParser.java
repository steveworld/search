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

import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.flume.sink.solr.indexer.parser.AvroParser;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;

/** Avro parser that uses an explicitly specified schema */
public class AvroTestParser extends AvroParser {

  private static Schema schema;
  
  public static final String MEDIA_TYPE = "avro/indexertest+schemaless";

  public AvroTestParser() {
    super();
    setSupportedTypes(Collections.singleton(MediaType.parse(MEDIA_TYPE)));
  }
  
  public static void setSchema(Schema newSchema) {
    schema = newSchema;
  }
  
  @Override
  protected Schema getSchema(Schema oldSchema, Metadata metadata, ParseContext context) {
    return schema;
  }
  
}
