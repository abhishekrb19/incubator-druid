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

import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.error.InvalidInput;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encodes the row and schema information from the Delta Lake.
 */
public class DeltaInputRow implements InputRow
{
  private final io.delta.kernel.data.Row row;
  private final StructType schema;
  private final Object2IntMap<String> fieldNameToOrdinal = new Object2IntOpenHashMap<>();
  private final InputRow delegateRow;

  private static final ZoneId ZONE_ID = ZoneId.systemDefault();

  public DeltaInputRow(io.delta.kernel.data.Row row, InputRowSchema inputRowSchema)
  {
    this.row = row;
    this.schema = row.getSchema();
    List<String> fieldNames = this.schema.fieldNames();
    for (int i = 0; i < fieldNames.size(); ++i) {
      fieldNameToOrdinal.put(fieldNames.get(i), i);
    }
    fieldNameToOrdinal.defaultReturnValue(-1);

    Map<String, Object> theMap = new HashMap<>();
    for (String fieldName : fieldNames) {
      theMap.put(fieldName, _getRaw(fieldName));
    }
    delegateRow = MapInputRowParser.parse(inputRowSchema, theMap);
  }

  @Override
  public List<String> getDimensions()
  {
    return delegateRow.getDimensions();
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return delegateRow.getTimestampFromEpoch();
  }

  @Override
  public DateTime getTimestamp()
  {
    return delegateRow.getTimestamp();
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    return delegateRow.getDimension(dimension);
  }

  @Nullable
  @Override
  public Object getRaw(String dimension)
  {
    return delegateRow.getRaw(dimension);
  }

  @Nullable
  @Override
  public Number getMetric(String metric)
  {
    return delegateRow.getMetric(metric);
  }

  @Override
  public int compareTo(Row o)
  {
    return this.getTimestamp().compareTo(o.getTimestamp());
  }

  @Override
  public String toString()
  {
    return "DeltaInputRow{" +
           "row=" + row +
           ", schema=" + schema +
           ", fieldNameToOrdinal=" + fieldNameToOrdinal +
           ", delegateRow=" + delegateRow +
           '}';
  }

  public Map<String, Object> getRawRowAsMap()
  {
    return RowSerde.convertRowToJsonObject(row);
  }

  @Nullable
  private Object _getRaw(String dimension)
  {
    StructField field = schema.get(dimension);
    if (field == null || field.isMetadataColumn()) {
      return null;
    }

    int ordinal = fieldNameToOrdinal.getInt(dimension);
    if (ordinal < 0) {
      return null;
    }
    return getValue(field.getDataType(), row, ordinal);
  }

  @Nullable
  private static Object getValue(DataType dataType, io.delta.kernel.data.Row dataRow, int columnOrdinal)
  {
    if (dataRow.isNullAt(columnOrdinal)) {
      return null;
    } else if (dataType instanceof BooleanType) {
      return dataRow.getBoolean(columnOrdinal);
    } else if (dataType instanceof ByteType) {
      return dataRow.getByte(columnOrdinal);
    } else if (dataType instanceof ShortType) {
      return dataRow.getShort(columnOrdinal);
    } else if (dataType instanceof IntegerType) {
      return dataRow.getInt(columnOrdinal);
    } else if (dataType instanceof DateType) {
      // DateType data is stored internally as the number of days since 1970-01-01
      int daysSinceEpochUTC = dataRow.getInt(columnOrdinal);
      return LocalDate.ofEpochDay(daysSinceEpochUTC).atStartOfDay(ZONE_ID).toEpochSecond();
    } else if (dataType instanceof LongType) {
      return dataRow.getLong(columnOrdinal);
    } else if (dataType instanceof TimestampType) {
      // TimestampType data is stored internally as the number of microseconds since epoch
      long microSecsSinceEpochUTC = dataRow.getLong(columnOrdinal);
      LocalDateTime dateTime = LocalDateTime.ofEpochSecond(
          microSecsSinceEpochUTC / 1_000_000 /* epochSecond */,
          (int) (1000 * microSecsSinceEpochUTC % 1_000_000) /* nanoOfSecond */,
          ZoneOffset.UTC
      );
      return dateTime.atZone(ZONE_ID).toInstant().toEpochMilli();
    } else if (dataType instanceof FloatType) {
      return dataRow.getFloat(columnOrdinal);
    } else if (dataType instanceof DoubleType) {
      return dataRow.getDouble(columnOrdinal);
    } else if (dataType instanceof StringType) {
      return dataRow.getString(columnOrdinal);
    } else if (dataType instanceof BinaryType) {
      return new String(dataRow.getBinary(columnOrdinal));
    } else if (dataType instanceof DecimalType) {
      return dataRow.getDecimal(columnOrdinal).longValue();
    } else {
      throw InvalidInput.exception("Unsupported data type[%s]", dataType);
    }
  }
}
