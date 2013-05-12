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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * The MapToColumn annotation is used to instruct the AnnotationEntryParser.
 * You can specify the column in the csv file and which value processor
 * should be used for converting.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface MapToColumn {
	/**
	 * The column of the data in the csv file.
	 * This parameter is required.
	 *
	 * @return the column in the csv file
	 */
	int column();

	/**
	 * The type of the data.
	 * If set, the appropriate ValueProcessor for this class
	 * will be used to process the data of the csv column.
	 * If not set, the type of the field will be used to find
	 * the appropriate column processor.
	 * This parameter is optional.
	 *
	 * @return the type of the data
	 */
	Class<?> type() default Default.class;

	/**
	 * The default value for the type parameter.
	 */
	public static class Default {}
}