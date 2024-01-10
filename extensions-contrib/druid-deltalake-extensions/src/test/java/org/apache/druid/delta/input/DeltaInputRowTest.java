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

import io.delta.kernel.Scan;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class DeltaInputRowTest
{
  @Test
  public void testDeltaInputRow() throws TableNotFoundException, IOException
  {
    final TableClient tableClient = DefaultTableClient.create(new Configuration());
    final Scan scan = DeltaTestUtil.getScan(tableClient);

    CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(tableClient);
    int totalRecordCount = 0;
    while (scanFileIter.hasNext()) {
      try (CloseableIterator<FilteredColumnarBatch> data =
               Scan.readData(
                   tableClient,
                   scan.getScanState(tableClient),
                   scanFileIter.next().getRows(),
                   Optional.empty()
               )) {
        while (data.hasNext()) {
          FilteredColumnarBatch dataReadResult = data.next();
          Row next = dataReadResult.getRows().next();
          DeltaInputRow deltaInputRow = new DeltaInputRow(
              next,
              DeltaTestUtil.SCHEMA
          );
          Assert.assertNotNull(deltaInputRow);
          Assert.assertEquals(DeltaTestUtil.DIMENSIONS, deltaInputRow.getDimensions());

          Map<String, Object> expectedRow = DeltaTestUtil.EXPECTED_ROWS.get(totalRecordCount);
          Assert.assertEquals(expectedRow, deltaInputRow.getRawRowAsMap());

          for (String dimension : DeltaTestUtil.DIMENSIONS) {
            Assert.assertEquals(expectedRow.get(dimension), deltaInputRow.getDimension(dimension).get(0));
          }
          totalRecordCount += 1;
        }
      }
    }
    Assert.assertEquals(DeltaTestUtil.EXPECTED_ROWS.size(), totalRecordCount);
  }
}