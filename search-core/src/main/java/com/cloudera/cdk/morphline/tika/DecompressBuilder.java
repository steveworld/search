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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pkg.CompressorParser;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.Fields;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.parser.AbstractParser;
import com.google.common.io.Closeables;
import com.typesafe.config.Config;

/**
 * Command that decompresses the first attachment. Implementation adapted from {@link CompressorParser}.
 */
public final class DecompressBuilder implements CommandBuilder {

  @Override
  public Set<String> getNames() {
    return Collections.singleton("decompress");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Decompress(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Decompress extends AbstractParser {
    
    private boolean decompressConcatenated;    
    
    public Decompress(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      if (!config.hasPath(SUPPORTED_MIME_TYPES)) {
        for (MediaType mediaType : new CompressorParser().getSupportedTypes(new ParseContext())) {
          addSupportedMimeType(mediaType);
        }
      }
    }
 
    @Override
    public boolean process(Record record, InputStream stream) {
      EmbeddedExtractor extractor = new EmbeddedExtractor();

      String name = (String) record.getFirstValue(Fields.ATTACHMENT_NAME);
      if (name != null) {
        if (name.endsWith(".tbz")) {
          name = name.substring(0, name.length() - 4) + ".tar";
        } else if (name.endsWith(".tbz2")) {
          name = name.substring(0, name.length() - 5) + ".tar";
        } else if (name.endsWith(".bz")) {
          name = name.substring(0, name.length() - 3);
        } else if (name.endsWith(".bz2")) {
          name = name.substring(0, name.length() - 4);
        } else if (name.endsWith(".xz")) {
          name = name.substring(0, name.length() - 3);
        } else if (name.endsWith(".pack")) {
          name = name.substring(0, name.length() - 5);
        } else if (name.length() > 0) {
          name = GzipUtils.getUncompressedFilename(name);
        }
      }

      // At the end we want to close the compression stream to release
      // any associated resources, but the underlying document stream
      // should not be closed
      stream = new CloseShieldInputStream(stream);

      // Ensure that the stream supports the mark feature
      stream = new BufferedInputStream(stream);

      CompressorInputStream cis;
      try {
        CompressorStreamFactory factory = new CompressorStreamFactory();
        cis = factory.createCompressorInputStream(stream);
      } catch (CompressorException e) {
        throw new MorphlineRuntimeException("Unable to uncompress document stream", e);
      }

      try {
        return extractor.parseEmbedded(cis, record, name, getChild());
      } finally {
        Closeables.closeQuietly(cis);
      }
    }

    /**
     * @return an input stream/metadata tuple to use. If appropriate, stream will be capable of
     * decompressing concatenated compressed files.
     */
    private InputStreamMetadata detectCompressInputStream(InputStream inputStream, Metadata metadata) {
      if (decompressConcatenated) {
        String resourceName = metadata.get(Metadata.RESOURCE_NAME_KEY);
        if (resourceName != null && GzipUtils.isCompressedFilename(resourceName)) {
          try {
            CompressorInputStream cis = new GzipCompressorInputStream(inputStream, true);
            Metadata entryData = cloneMetadata(metadata);
            String newName = GzipUtils.getUncompressedFilename(resourceName);
            entryData.set(Metadata.RESOURCE_NAME_KEY, newName);
            return new InputStreamMetadata(cis, entryData);
          } catch (IOException ioe) {
            LOG.warn("Unable to create compressed input stream able to read concatenated stream", ioe);
          }
        }
      }
      return new InputStreamMetadata(inputStream, metadata);
    }

    /**
     * @return a clone of metadata
     */
    private Metadata cloneMetadata(Metadata metadata) {
      Metadata clone = new Metadata();
      for (String name : metadata.names()) {
        String [] str = metadata.getValues(name);
        for (int i = 0; i < str.length; ++i) {
          clone.add(name, str[i]);
        }
      }
      return clone;
    }

    
    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static final class InputStreamMetadata {
      
      private InputStream inputStream;
      private Metadata metadata;
   
      public InputStreamMetadata(InputStream inputStream, Metadata metadata) {
        this.inputStream = inputStream;
        this.metadata = metadata;
      }
    }
    
  }

}