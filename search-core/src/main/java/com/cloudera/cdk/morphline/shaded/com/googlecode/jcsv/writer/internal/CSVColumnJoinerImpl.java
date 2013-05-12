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

import java.util.regex.Pattern;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.CSVStrategy;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.util.CSVUtil;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.CSVColumnJoiner;

/**
 * This is the default implementation of the CSVColumnJoiner.
 *
 * This implementation follows the csv formatting standard, described in:
 * http://en.wikipedia.org/wiki/Comma-separated_values
 *
 * If you have a more specific csv format, such as constant column widths or
 * columns that do not need to be quoted, you may consider to write a more simple
 * but performant CSVColumnJoiner.
 */
public class CSVColumnJoinerImpl implements CSVColumnJoiner {

	@Override
	public String joinColumns(String[] data, CSVStrategy strategy) {
		final String delimiter = String.valueOf(strategy.getDelimiter());
		final String quote = String.valueOf(strategy.getQuoteCharacter());
		final String doubleQuote = quote + quote;

		// check each column for delimiter or quote characters
		// and escape them if neccessary
		for (int i = 0; i < data.length; i++) {
			if (data[i].contains(delimiter) || data[i].contains(quote)) {
				if (data[i].contains(quote)) {
					data[i] = data[i].replaceAll(Pattern.quote(quote), doubleQuote);
				}

				data[i] = quote + data[i] + quote;
			}
		}

		return CSVUtil.implode(data, delimiter);
	}

}
