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
  private final String filterOperator;

  @JsonProperty
  private final String filterColumn;

  @JsonProperty
  private final String filterValue;

  @JsonCreator
  public DeltaBinaryOperatorFilter(
      @JsonProperty("filterOperator") String filterOperator,
      @JsonProperty("filterColumn") String filterColumn,
      @JsonProperty("filterValue") String filterValue
  )
  {
    if (filterOperator == null) {
      throw InvalidInput.exception("filterOperator is required for binary filters.");
    }
    if (filterColumn == null) {
      throw InvalidInput.exception("filterColumn is required for binary filters.");
    }
    if (filterValue == null) {
      throw InvalidInput.exception("filterValue is required for binary filters.");
    }
    this.filterOperator = filterOperator;
    this.filterColumn = filterColumn;
    this.filterValue = filterValue;
  }

  @Override
  public Predicate getFilterPredicate(StructType snapshotSchema)
  {
    return new Predicate(
        filterOperator,
        ImmutableList.of(
            new Column(filterColumn),
            LiteralHelper.dataTypeToLiteral(snapshotSchema, filterColumn, filterValue)
        )
    );
  }
}
