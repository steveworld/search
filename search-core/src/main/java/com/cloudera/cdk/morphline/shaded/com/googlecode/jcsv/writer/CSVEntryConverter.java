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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer;

/**
 * The CSVEntryConverter receives a java object and converts it
 * into a String[] array that will be written to the output stream.
 *
 * @param <E> The Type that will be converted
 */
public interface CSVEntryConverter<E> {
	/**
	 * Converts an object of type E into a String[] array,
	 * that will be written into the csv file.
	 *
	 * @param e that object that will be converted
	 * @return the data
	 */
	public String[] convertEntry(E e);
}
