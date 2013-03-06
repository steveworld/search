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
import java.util.Set;

import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.ParseContext;

/**
 * Dummy parser that always produces an empty XHTML document without even
 * attempting to parse the given document stream. For example, this can be used
 * to just index the file metadata without indexing the content.
 */
public class NullParser extends EmptyParser {

  private static final MediaType MEDIA_TYPE = MediaType.parse("application/null-tika-parser");

  @Override
  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return Collections.singleton(MEDIA_TYPE);
  }
  
}
