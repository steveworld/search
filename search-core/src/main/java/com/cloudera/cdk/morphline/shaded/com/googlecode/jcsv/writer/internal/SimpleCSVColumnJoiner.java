/**
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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.internal;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.CSVStrategy;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.util.CSVUtil;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.CSVColumnJoiner;

/**
 * This is a simple implementation of the CSVColumnJoiner.
 * It just performs an <code>CSVUtil.implode</code> on the
 * incoming String array.
 *
 * If you need a full support of the CSV formatting standard
 * you should use
 * {@link com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.internal.CSVColumnJoinerImpl}
 */
public class SimpleCSVColumnJoiner implements CSVColumnJoiner {

	/**
	 * Performs a CSVUtil.implode() to concat the columns, it uses
	 * the delimiter specified by the csv strategy.
	 */
	@Override
	public String joinColumns(String[] data, CSVStrategy strategy) {
		return CSVUtil.implode(data, String.valueOf(strategy.getDelimiter()));
	}

}
