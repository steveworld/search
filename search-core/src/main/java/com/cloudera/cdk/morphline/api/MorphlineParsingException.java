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

import com.typesafe.config.Config;

/**
 * Indicates a parser error.
 */
public class MorphlineParsingException extends MorphlineRuntimeException {

  private static final long serialVersionUID = 1L;

  public MorphlineParsingException(String msg, Config config) {
    super(msg + render(config));
  }

  public MorphlineParsingException(String msg, Config config, Throwable th) {
    super(msg + render(config), th);
  }

  private static String render(Config config) {
    return " near: " + config.root().render();
  }

}
