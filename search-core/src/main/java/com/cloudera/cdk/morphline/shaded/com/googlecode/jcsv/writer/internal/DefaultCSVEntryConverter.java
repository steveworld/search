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

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.CSVEntryConverter;

/**
 * The (simple) default implementation of the CSVEntryConverter. It just returns
 * the input that it reveives.
 *
 * Might be useful if you want wo parse a csv file into a List<String>.
 *
 */
public class DefaultCSVEntryConverter implements CSVEntryConverter<String[]> {
	/**
	 * Simply returns the data that it receives.
	 *
	 * @param data
	 *            the incoming data
	 *
	 * @return the incoming data ;)
	 */
	@Override
	public String[] convertEntry(String[] data) {
		return data;
	}
}
