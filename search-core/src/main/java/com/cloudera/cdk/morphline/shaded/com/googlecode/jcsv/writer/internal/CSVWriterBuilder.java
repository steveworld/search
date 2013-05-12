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

import java.io.Writer;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.CSVStrategy;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.util.Builder;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.CSVColumnJoiner;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.CSVEntryConverter;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.CSVWriter;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.internal.CSVColumnJoinerImpl;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.internal.CSVWriterBuilder;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.internal.CSVWriterImpl;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.internal.DefaultCSVEntryConverter;

/**
 * The builder that creates the CSVWriterImpl instance.
 *
 * @param <E> The Type of the records
 */
public class CSVWriterBuilder<E> implements Builder<CSVWriter<E>>{
	final Writer writer;
	CSVStrategy strategy = CSVStrategy.DEFAULT;
	CSVEntryConverter<E> entryConverter;
	CSVColumnJoiner columnJoiner = new CSVColumnJoinerImpl();

	/**
	 * Creates a Builder for the CSVWriterImpl
	 *
	 * @param writer the character output stream
	 */
	public CSVWriterBuilder(Writer writer) {
		this.writer = writer;
	}

	/**
	 * Sets the strategy that the CSVWriterImpl will use.
	 *
	 * @param strategy the csv strategy
	 * @return this builder
	 */
	public CSVWriterBuilder<E> strategy(CSVStrategy strategy) {
		this.strategy = strategy;
		return this;
	}

	/**
	 * Sets the entry converter that the CSVWriterImpl will use.
	 *
	 * @param entryConverter the entry converter
	 * @return this builder
	 */
	public CSVWriterBuilder<E> entryConverter(CSVEntryConverter<E> entryConverter) {
		this.entryConverter = entryConverter;
		return this;
	}

	/**
	 * Sets the column joiner strategy that the CSVWriterImpl will use.
	 * If you don't specify your own csv tokenizer strategy, the default
	 * column joiner will be used.
	 * {@link com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.internal.CSVColumnJoinerImpl}
	 *
	 * @param columnJoiner the column joiner
	 * @return this builder
	 */
	public CSVWriterBuilder<E> columnJoiner(CSVColumnJoiner columnJoiner) {
		this.columnJoiner = columnJoiner;
		return this;
	}

	/**
	 * Builds the CSVWriterImpl, using the specified configuration
	 *
	 * @return the CSVWriterImpl instance
	 */
	@Override
	public CSVWriter<E> build() {
		if (entryConverter == null) {
			throw new IllegalStateException("you have to specify an entry converter");
		}

		return new CSVWriterImpl<E>(this);
	}

	/**
	 * Returns a default configured CSVWriterImpl<String[]>.
	 * It uses the DefaultCSVEntryParser that allows you to
	 * write a String[] arrayas an entry in your csv file.
	 *
	 * @param writer the character output stream
	 * @return the CSVWriterImpl
	 */
	public static CSVWriter<String[]> newDefaultWriter(Writer writer) {
		return new CSVWriterBuilder<String[]>(writer).entryConverter(new DefaultCSVEntryConverter()).build();
	}
}
