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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;

import java.util.Objects;

/**
 * AggregatorSummary is a summary of the {@link AggregatorFactory} used primarily for caching
 * on the broker. The information contained here is intended to be a stripped down summarized
 * representation of the {@link AggregatorFactory} for a column.
 * E.g., "longSum" in case of {@link LongSumAggregatorFactory}
 * "hyperUnique" for {@link HyperUniquesAggregatorFactory}
 */
public class AggregatorSummary
{
  @JsonCreator
  public AggregatorSummary(
      @JsonProperty("type") String type
  )
  {
    this.type = type;
  }

  @JsonProperty
  public final String type;

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return "AggregatorSummary{" +
           "type='" + type + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AggregatorSummary that = (AggregatorSummary) o;
    return type.equals(that.type);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type);
  }
}
