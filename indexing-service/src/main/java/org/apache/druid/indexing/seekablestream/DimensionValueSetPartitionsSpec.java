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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * {@link StreamingPartitionsSpec} that records, per published segment, the distinct values observed for a configured set
 * of dimensions and stamps them onto a {@link org.apache.druid.timeline.partition.DimensionValueSetShardSpec} so the
 * broker can prune segments at query time without waiting for compaction. The collection and stamping are performed by
 * {@link DimensionValueSetCollector}.
 *
 * <p>Use low-to-medium cardinality dimensions; the {@code partitionDimensions} here should be kept in sync with the
 * {@code partitionDimensions} of the compaction config for the same datasource.
 */
public class DimensionValueSetPartitionsSpec implements StreamingPartitionsSpec
{
  public static final String TYPE = "dim_value_set";

  private final List<String> partitionDimensions;

  @JsonCreator
  public DimensionValueSetPartitionsSpec(
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions
  )
  {
    this.partitionDimensions = partitionDimensions == null ? Collections.emptyList() : partitionDimensions;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Nullable
  @Override
  public StreamingShardSpecCollector createCollector()
  {
    // Nothing to collect without dimensions; treat as feature-off so segments are published unchanged.
    if (partitionDimensions.isEmpty()) {
      return null;
    }
    return new DimensionValueSetCollector(partitionDimensions);
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
    DimensionValueSetPartitionsSpec that = (DimensionValueSetPartitionsSpec) o;
    return Objects.equals(partitionDimensions, that.partitionDimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionDimensions);
  }

  @Override
  public String toString()
  {
    return "DimensionValueSetPartitionsSpec{partitionDimensions=" + partitionDimensions + '}';
  }
}
