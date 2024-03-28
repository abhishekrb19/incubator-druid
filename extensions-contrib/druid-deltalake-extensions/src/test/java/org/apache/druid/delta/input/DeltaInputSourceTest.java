/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.delta.input;

import com.google.common.collect.ImmutableList;
import io.delta.kernel.expressions.AlwaysTrue;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.delta.filter.DeltaAndFilter;
import org.apache.druid.delta.filter.DeltaBinaryOperatorFilter;
import org.apache.druid.delta.filter.DeltaFilter;
import org.apache.druid.delta.filter.DeltaNotFilter;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DeltaInputSourceTest
{
  @Before
  public void setUp()
  {
    System.setProperty("user.timezone", "UTC");
  }

  private final String OBSERVATIONS_DATA = "/Users/abhishek/Desktop/opensource-druid/druid/extensions-contrib/druid-deltalake-extensions/src/test/resources/observation_data";

  private final InputRowSchema OBSERVATIONS_SCHEMA = new InputRowSchema(
//        new TimestampSpec("timestamp", "auto", null),
//      new TimestampSpec("date", "millis", null),
      new TimestampSpec("date", "posix", null),
      new DimensionsSpec(
          ImmutableList.of(
              new LongDimensionSchema("id"),
              new LongDimensionSchema("birthday"),
              new StringDimensionSchema("name"),
              new LongDimensionSchema("age"),
              new DoubleDimensionSchema("salary"),
              new FloatDimensionSchema("bonus"),
              new LongDimensionSchema("yoe"),
              new StringDimensionSchema("is_fulltime"),
              new LongDimensionSchema("last_vacation_time")
          )
      ),
      ColumnsFilter.all()
  );

  @Test
  public void testSampleObservationDeltaTable() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(OBSERVATIONS_DATA, null, null);

    final InputSourceReader inputSourceReader = deltaInputSource.reader(OBSERVATIONS_SCHEMA, null, null);

    List<InputRowListPlusRawValues> actualSampledRows = sampleAllRows(inputSourceReader);
    System.out.println("Sampled rows count" + actualSampledRows.size());
//    Assert.assertEquals(DeltaTestUtils.EXPECTED_ROWS.size(), actualSampledRows.size());

    for (InputRowListPlusRawValues sampledRow : actualSampledRows) {
      System.out.println("Sampled row: " + sampledRow.getRawValues());
//    }
//
//    for (int idx = 0; idx < DeltaTestUtils.EXPECTED_ROWS.size(); idx++) {
//      Map<String, Object> expectedRow = DeltaTestUtils.EXPECTED_ROWS.get(idx);
//      InputRowListPlusRawValues actualSampledRow = actualSampledRows.get(idx);
//      Assert.assertNull(actualSampledRow.getParseException());
//
//      Map<String, Object> actualSampledRawVals = actualSampledRow.getRawValues();
//      Assert.assertNotNull(actualSampledRawVals);
//      Assert.assertNotNull(actualSampledRow.getRawValuesList());
//      Assert.assertEquals(1, actualSampledRow.getRawValuesList().size());
//
//      for (String key : expectedRow.keySet()) {
//        if (DeltaTestUtils.FULL_SCHEMA.getTimestampSpec().getTimestampColumn().equals(key)) {
//          final long expectedMillis = (Long) expectedRow.get(key);
//          Assert.assertEquals(expectedMillis, actualSampledRawVals.get(key));
//        } else {
//          Assert.assertEquals(expectedRow.get(key), actualSampledRawVals.get(key));
//        }
//      }
//    }
    }
  }

  @Test
  public void testReadAllObservationsTable() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(OBSERVATIONS_DATA, null, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        OBSERVATIONS_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    System.out.println("Actual read rows count: " + actualReadRows.size());
    for (InputRow inputRow: actualReadRows) {
      System.out.println("Input row:" + inputRow.getTimestamp());
    }
    System.out.println("Actual read rows count: " + actualReadRows.size());
  }

  @Test
  public void testReadAllObservationsTableWithTimeFilter() throws IOException
  {
//    final DeltaFilter timeFilter =f new DeltaBinaryOperatorFilter.DeltaEqualsFilter("date", "1704412800");
    final DeltaFilter timeFilter = new DeltaBinaryOperatorFilter.DeltaEqualsFilter("date", "2024-01-10");
    final DeltaInputSource deltaInputSource = new DeltaInputSource(OBSERVATIONS_DATA, null, timeFilter);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        OBSERVATIONS_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    System.out.println("Actual read rows count: " + actualReadRows.size());
    for (InputRow inputRow: actualReadRows) {
      System.out.println("Input row:" + inputRow.getTimestamp());
    }
    System.out.println("Actual read rows count: " + actualReadRows.size());
  }

  @Test
  public void testSampleDeltaTable() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null ,null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(DeltaTestUtils.FULL_SCHEMA, null, null);

    List<InputRowListPlusRawValues> actualSampledRows = sampleAllRows(inputSourceReader);
    Assert.assertEquals(DeltaTestUtils.EXPECTED_ROWS.size(), actualSampledRows.size());

    for (int idx = 0; idx < DeltaTestUtils.EXPECTED_ROWS.size(); idx++) {
      Map<String, Object> expectedRow = DeltaTestUtils.EXPECTED_ROWS.get(idx);
      InputRowListPlusRawValues actualSampledRow = actualSampledRows.get(idx);
      Assert.assertNull(actualSampledRow.getParseException());

      Map<String, Object> actualSampledRawVals = actualSampledRow.getRawValues();
      Assert.assertNotNull(actualSampledRawVals);
      Assert.assertNotNull(actualSampledRow.getRawValuesList());
      Assert.assertEquals(1, actualSampledRow.getRawValuesList().size());

      for (String key : expectedRow.keySet()) {
        if (DeltaTestUtils.FULL_SCHEMA.getTimestampSpec().getTimestampColumn().equals(key)) {
          final long expectedMillis = (Long) expectedRow.get(key);
          Assert.assertEquals(expectedMillis, actualSampledRawVals.get(key));
        } else {
          Assert.assertEquals(expectedRow.get(key), actualSampledRawVals.get(key));
        }
      }
    }
  }

  @Test
  public void testReadAllDeltaTable() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.FULL_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.FULL_SCHEMA);
  }

  @Test
  public void testReadAllDeltaTableSubSchema1() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.SCHEMA_1,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_1);
  }

  @Test
  public void testReadPartitionedDeltaTableNoFilter() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource("src/test/resources/employee-delta-table-partitioned-2", null, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.FULL_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    printActualRows(actualReadRows);
//    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_1);
  }

  @Test
  public void testReadPartitionedDeltaTableEqualsFilter() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(
        "src/test/resources/employee-delta-table-partitioned-2",
        null,
        new DeltaBinaryOperatorFilter("=", "name", "Employee1")
    );
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.FULL_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    printActualRows(actualReadRows);
//    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_1);
  }

  @Test
  public void testReadPartitionedDeltaTableGreaterThanFilter() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(
        "src/test/resources/employee-delta-table-partitioned-2",
        null,
        new DeltaBinaryOperatorFilter(">=", "age", "26")
    );
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.FULL_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    printActualRows(actualReadRows);
//    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_1);
  }

  @Test
  public void testReadPartitionedDeltaTableLessThanFilter() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(
        "src/test/resources/employee-delta-table-partitioned-2",
        null,
        new DeltaBinaryOperatorFilter("<", "age", "23")
    );
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.FULL_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    printActualRows(actualReadRows);
//    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_1);
  }

  @Test
  public void testReadPartitionedDeltaTableAndFilter() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(
        "src/test/resources/employee-delta-table-partitioned-2",
        null,
        new DeltaAndFilter(
            ImmutableList.of(
                new DeltaBinaryOperatorFilter("=", "name", "Employee1"),
                new DeltaBinaryOperatorFilter(">=", "age", "8")
            )
        )
    );
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.FULL_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    printActualRows(actualReadRows);
//    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_1);
  }

  @Test
  public void testReadPartitionedDeltaTableNotFilter() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(
        "src/test/resources/employee-delta-table-partitioned-2",
        null,
        new DeltaNotFilter(
            new DeltaBinaryOperatorFilter("=", "name", "Employee1")
        )
    );
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.FULL_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    printActualRows(actualReadRows);
//    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_1);
  }

  @Test
  public void testDeltaLakeWithCreateSplitsWithNotFilter()
  {
    final DeltaFilter notFilter = new DeltaNotFilter(
        new DeltaBinaryOperatorFilter("=", "name", "Employee1")
    );

    final DeltaInputSource deltaInputSource = new DeltaInputSource(
        "src/test/resources/employee-delta-table-partitioned-2",
        null,
        notFilter
    );
    final List<InputSplit<DeltaSplit>> splits = deltaInputSource.createSplits(null, null)
                                                                .collect(Collectors.toList());
    Assert.assertEquals(DeltaTestUtils.SPLIT_TO_EXPECTED_ROWS.size(), splits.size());

    for (InputSplit<DeltaSplit> split : splits) {
      final DeltaSplit deltaSplit = split.get();
      final DeltaInputSource deltaInputSourceWithSplit = new DeltaInputSource(
          DeltaTestUtils.DELTA_TABLE_PATH,
          deltaSplit,
          notFilter
      );
      List<InputSplit<DeltaSplit>> splitsResult = deltaInputSourceWithSplit.createSplits(null, null)
                                                                           .collect(Collectors.toList());
      Assert.assertEquals(1, splitsResult.size());
      Assert.assertEquals(deltaSplit, splitsResult.get(0).get());
    }
  }

  @Test
  public void testReadPartitionedDeltaTableNotFilterComplex() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(
        "src/test/resources/employee-delta-table-partitioned-2",
        null,
        new DeltaNotFilter(
            new DeltaAndFilter(
                ImmutableList.of(
                    new DeltaBinaryOperatorFilter("=", "name", "Employee1"),
                    new DeltaBinaryOperatorFilter(">=", "age", "8")
                )
            )
        )
    );
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.FULL_SCHEMA,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    printActualRows(actualReadRows);
//    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_1);
  }

  @Test
  public void testReadAllDeltaTableWithSubSchema2() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null, null);
    final InputSourceReader inputSourceReader = deltaInputSource.reader(
        DeltaTestUtils.SCHEMA_2,
        null,
        null
    );
    final List<InputRow> actualReadRows = readAllRows(inputSourceReader);
    validateRows(DeltaTestUtils.EXPECTED_ROWS, actualReadRows, DeltaTestUtils.SCHEMA_2);
  }

  @Test
  public void testDeltaLakeWithCreateSplits()
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null, null);
    final List<InputSplit<DeltaSplit>> splits = deltaInputSource.createSplits(null, null)
                                                                .collect(Collectors.toList());
    Assert.assertEquals(DeltaTestUtils.SPLIT_TO_EXPECTED_ROWS.size(), splits.size());

    for (InputSplit<DeltaSplit> split : splits) {
      final DeltaSplit deltaSplit = split.get();
      final DeltaInputSource deltaInputSourceWithSplit = new DeltaInputSource(
          DeltaTestUtils.DELTA_TABLE_PATH,
          deltaSplit,
          null
      );
      List<InputSplit<DeltaSplit>> splitsResult = deltaInputSourceWithSplit.createSplits(null, null)
                                                                           .collect(Collectors.toList());
      Assert.assertEquals(1, splitsResult.size());
      Assert.assertEquals(deltaSplit, splitsResult.get(0).get());
    }
  }

  @Test
  public void testDeltaLakeWithReadSplits() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DeltaTestUtils.DELTA_TABLE_PATH, null, null);
    final List<InputSplit<DeltaSplit>> splits = deltaInputSource.createSplits(null, null)
                                                                .collect(Collectors.toList());
    Assert.assertEquals(DeltaTestUtils.SPLIT_TO_EXPECTED_ROWS.size(), splits.size());

    for (int idx = 0; idx < splits.size(); idx++) {
      final InputSplit<DeltaSplit> split = splits.get(idx);
      final DeltaSplit deltaSplit = split.get();
      final DeltaInputSource deltaInputSourceWithSplit = new DeltaInputSource(
          DeltaTestUtils.DELTA_TABLE_PATH,
          deltaSplit,
          null
      );
      final InputSourceReader inputSourceReader = deltaInputSourceWithSplit.reader(
          DeltaTestUtils.FULL_SCHEMA,
          null,
          null
      );
      final List<InputRow> actualRowsInSplit = readAllRows(inputSourceReader);
      final List<Map<String, Object>> expectedRowsInSplit = DeltaTestUtils.SPLIT_TO_EXPECTED_ROWS.get(idx);
      validateRows(expectedRowsInSplit, actualRowsInSplit, DeltaTestUtils.FULL_SCHEMA);
    }
  }

  @Test
  public void testNullTable()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new DeltaInputSource(null, null, null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "tablePath cannot be null."
        )
    );
  }

  @Test
  public void testSplitNonExistentTable()
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource("non-existent-table", null, null);

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> deltaInputSource.createSplits(null, null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "tablePath[non-existent-table] not found."
        )
    );
  }

  @Test
  public void testReadNonExistentTable()
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource("non-existent-table", null, null);

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> deltaInputSource.reader(null, null, null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "tablePath[non-existent-table] not found."
        )
    );
  }

  private List<InputRowListPlusRawValues> sampleAllRows(InputSourceReader reader) throws IOException
  {
    List<InputRowListPlusRawValues> rows = new ArrayList<>();
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  private List<InputRow> readAllRows(InputSourceReader reader) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  private void printActualRows(
      final List<InputRow> actualReadRows
  )
  {
    for (InputRow actualRow : actualReadRows) {
      System.out.println("ACTUAL ROW:" + actualRow);
    }
  }

  private void validateRows(
      final List<Map<String, Object>> expectedRows,
      final List<InputRow> actualReadRows,
      final InputRowSchema schema
  )
  {
//    Assert.assertEquals(expectedRows.size(), actualReadRows.size());

    for (int idx = 0; idx < expectedRows.size(); idx++) {
      final Map<String, Object> expectedRow = expectedRows.get(idx);
      final InputRow actualInputRow = actualReadRows.get(idx);
      System.out.println("Actual read row" + actualInputRow);
      for (String key : expectedRow.keySet()) {
        if (!schema.getColumnsFilter().apply(key)) {
//          Assert.assertNull(actualInputRow.getRaw(key));
        } else {
          if (schema.getTimestampSpec().getTimestampColumn().equals(key)) {
            final long expectedMillis = (Long) expectedRow.get(key) * 1000;
//            Assert.assertEquals(expectedMillis, actualInputRow.getTimestampFromEpoch());
//            Assert.assertEquals(DateTimes.utc(expectedMillis), actualInputRow.getTimestamp());
          } else {
//            Assert.assertEquals(expectedRow.get(key), actualInputRow.getRaw(key));
          }
        }
      }
    }
  }
}
