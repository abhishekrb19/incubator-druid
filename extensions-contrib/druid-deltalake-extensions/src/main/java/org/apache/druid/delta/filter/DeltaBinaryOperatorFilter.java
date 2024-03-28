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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import org.apache.druid.error.InvalidInput;

public class DeltaBinaryOperatorFilter implements DeltaFilter
{
  @JsonProperty
  private final String operator;

  @JsonProperty
  private final String column;

  @JsonProperty
  private final String value;

  @JsonCreator
  public DeltaBinaryOperatorFilter(
      @JsonProperty("operator") String operator,
      @JsonProperty("column") String column,
      @JsonProperty("value") String value
  )
  {
    if (operator == null) {
      throw InvalidInput.exception("operator is required for binary filters.");
    }
    if (column == null) {
      throw InvalidInput.exception("column is required for binary filters.");
    }
    if (value == null) {
      throw InvalidInput.exception("value is required for binary filters.");
    }
    this.operator = operator;
    this.column = column;
    this.value = value;
  }

  @Override
  public Predicate getFilterPredicate(StructType snapshotSchema)
  {
    return new Predicate(
        operator,
        ImmutableList.of(
            new Column(column),
            LiteralHelper.dataTypeToLiteral(snapshotSchema, column, value)
        )
    );
  }

  public static class DeltaEqualsFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaEqualsFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super("=", column, value);
    }
  }


  public static class DeltaGreaterThanFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaGreaterThanFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super(">", column, value);
    }
  }

  public static class DeltaGreaterThanOrEqualsFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaGreaterThanOrEqualsFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super(">=", column, value);
    }
  }


  public static class DeltaLessThanFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaLessThanFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super("<", column, value);
    }
  }

  public static class DeltaLessThanOrEqualsFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaLessThanOrEqualsFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super("<=", column, value);
    }
  }
}
