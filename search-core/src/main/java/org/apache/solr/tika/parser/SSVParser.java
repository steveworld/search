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
package org.apache.solr.tika.parser;

import java.util.Collections;

import org.apache.tika.mime.MediaType;

/**
 * Space separated values parser that extracts search documents from CSV records
 * (using Apache Tika and Solr Cell) and loads them into Solr.
 */
public class SSVParser extends ReentrantParser {

  private static final MediaType MEDIA_TYPE = MediaType.parse("text/space-separated-values");

  @Override
  protected DelimitedValuesParser createInstance() {
    DelimitedValuesParser parser = new DelimitedValuesParser();
    parser.setSeparatorChar(' ');
    parser.setSupportedTypes(Collections.singleton(MEDIA_TYPE));    
    return parser;
  }

}
