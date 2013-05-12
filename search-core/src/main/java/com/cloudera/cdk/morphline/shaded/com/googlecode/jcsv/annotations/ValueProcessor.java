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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.annotations;

/**
 * The ValueProcessor is used to convert a string value
 * into an object of type E. This is used for annotation parsing.
 *
 * The implementations for the primitives and String are located
 * in com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.annotations.processors
 *
 * @param <E> the destination type
 */
public interface ValueProcessor<E> {
	/**
	 * Converts value into an object of type E.
	 *
	 * @param value the value that should be converted
	 * @return the converted object
	 */
	public E processValue(String value);
}
