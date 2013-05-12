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

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.CSVStrategy;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.CSVColumnJoiner;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.CSVEntryConverter;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.CSVWriter;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.writer.internal.CSVWriterBuilder;

public class CSVWriterImpl<E> implements CSVWriter<E> {

	private final Writer writer;
	private final CSVStrategy strategy;
	private final CSVEntryConverter<E> entryConverter;
	private final CSVColumnJoiner columnJoiner;

	CSVWriterImpl(CSVWriterBuilder<E> builder) {
		this.writer = builder.writer;
		this.strategy = builder.strategy;
		this.entryConverter = builder.entryConverter;
		this.columnJoiner = builder.columnJoiner;
	}

	@Override
	public void writeAll(List<E> data) throws IOException {
		for (E e : data) {
			write(e);
		}
	}

	@Override
	public void write(E e) throws IOException {
		StringBuilder sb = new StringBuilder();

		String[] columns = entryConverter.convertEntry(e);
		String line = columnJoiner.joinColumns(columns, strategy);

		sb.append(line);
		sb.append(System.getProperty("line.separator"));

		writer.append(sb.toString());
	}

	@Override
	public void flush() throws IOException {
		writer.flush();
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}
}
