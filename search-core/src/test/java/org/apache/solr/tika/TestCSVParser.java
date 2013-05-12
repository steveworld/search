/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.tika;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Array;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.CSVStrategy;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.CSVReader;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.CSVReaderBuilder;
import com.cloudera.cdk.morphline.shaded.com.googlecode.jcsv.reader.internal.DefaultCSVEntryParser;

public class TestCSVParser extends Assert {
  
  private static final String RESOURCES_DIR = "target/test-classes/test-documents";

  @Test
  public void testBasic() throws IOException {
    boolean isTrim = false;
    char separatorChar = ',';
    char commentChar = '#';

    File file = new File(RESOURCES_DIR + File.separator + "csvdata.csv"); 
    Reader reader = new InputStreamReader(new BufferedInputStream(new FileInputStream(file)), "UTF-8");
    //CSVReader csvReader = new CSVReader(reader, separatorChar, quoteChar, numLeadingLinesToSkip);
    CSVStrategy strategy = new CSVStrategy(separatorChar, '"', commentChar, false, true);
    CSVReader<String[]> csvReader = new CSVReaderBuilder(reader).strategy(strategy).entryParser(new DefaultCSVEntryParser()).build();  // newDefaultReader(reader);
    
    File expectedValuesFile = new File(RESOURCES_DIR + File.separator + "csvdata-expected-values.txt"); 
    BufferedReader expectedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(expectedValuesFile)), "UTF-8"));
    String line;
    long recNum = 0;
    while ((line = expectedReader.readLine()) != null) {
      String[] expectedCols = line.split(":");
      if (line.endsWith(":")) {
        expectedCols = concat(expectedCols, new String[]{""});
      }
      assertTrue("cols.length: " + expectedCols.length, expectedCols.length >= 1);
      if (expectedCols[0].startsWith("#")) {
        continue;
      }
      int expectedRecNum = Integer.parseInt(expectedCols[0]);
      expectedCols = Arrays.copyOfRange(expectedCols, 1, expectedCols.length);
      for (int i = 0; i < expectedCols.length; i++) {
        expectedCols[i] = expectedCols[i].replace("\\n", "\n");
        expectedCols[i] = expectedCols[i].replace("\\r", "\r");
      }
      
      String[] cols;
      while ((cols = csvReader.readNext()) != null) {
        for (int i = 0; i < cols.length; i++) {
          cols[i] = isTrim ? cols[i].trim() : cols[i];
        }
        recNum++;
//        System.out.println("recNum:" + recNum + ":" + Arrays.asList(cols));
        if (recNum == expectedRecNum) {
//          System.out.println("expect="+Arrays.asList(expectedCols));
//          System.out.println("actual="+Arrays.asList(cols));
          assertArrayEquals(expectedCols, cols);
          break;
        }
      }
    }
//    assertNull(csvReader.readNext());
    expectedReader.close();
    csvReader.close();
  }
  
  private static <T> T[] concat(T[]... arrays) {    
    if (arrays.length == 0) throw new IllegalArgumentException();
    Class clazz = null;
    int length = 0;
    for (T[] array : arrays) {
      clazz = array.getClass();
      length += array.length;
    }
    T[] result = (T[]) Array.newInstance(clazz.getComponentType(), length);
    int pos = 0;
    for (T[] array : arrays) {
      System.arraycopy(array, 0, result, pos, array.length);
      pos += array.length;
    }
    return result;
  }

}
