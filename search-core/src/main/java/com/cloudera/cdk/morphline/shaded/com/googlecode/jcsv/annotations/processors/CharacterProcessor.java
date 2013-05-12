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
package com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.annotations.processors;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.annotations.ValueProcessor;

/**
 * Processes character values.
 *
 * @author Eike Bergmann
 *
 */
public class CharacterProcessor implements ValueProcessor<Character> {
	/**
	 * Converts value into a character using {@link String#charAt(int)}
	 *
	 * @return Character the result
	 * @throws IllegalArgumentException
	 *             if the value's length is not 1
	 */
	@Override
	public Character processValue(String value) {
		if (value == null || value.length() != 1) {
			throw new IllegalArgumentException(String.format("%s is not a valud character, it's length must be 1",
					value));
		}

		return value.charAt(0);
	}
}
