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

package org.apache.druid.delta.filter;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.druid.error.InvalidInput;

public class LiteralHelper
{
  public static Literal dataTypeToLiteral(
      final StructType snapshotSchema,
      final String columnName,
      final String value
  )
  {
    StructField structField = snapshotSchema.get(columnName);
    if (structField == null) {
      throw InvalidInput.exception("columnName[%s] doesn't exist in schema[%s]",
                                   columnName, snapshotSchema);
    }

    DataType dataType = structField.getDataType();
    if (dataType instanceof StringType) {
      return Literal.ofString(value);
    } else if (dataType instanceof IntegerType) {
      return Literal.ofInt(Integer.parseInt(value));
    } else if (dataType instanceof ShortType) {
      return Literal.ofShort(Short.parseShort(value));
    } else if (dataType instanceof LongType) {
      return Literal.ofLong(Long.parseLong(value));
    } else {
      throw InvalidInput.exception("Unsupported dataType[%s] for value[%s]", dataType, value);
    }
  }
}
