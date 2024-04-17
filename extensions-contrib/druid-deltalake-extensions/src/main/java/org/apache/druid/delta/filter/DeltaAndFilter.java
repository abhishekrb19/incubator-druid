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
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import org.apache.druid.error.InvalidInput;

import java.util.List;

public class DeltaAndFilter implements DeltaFilter
{
  @JsonProperty
  private final List<DeltaFilter> predicates;

  @JsonCreator
  public DeltaAndFilter(
      @JsonProperty("filters") List<DeltaFilter> filters
  )
  {
    if (filters == null) {
      throw InvalidInput.exception("Delta and filter requires 2 filter predicates and must be non-empty.");
    }
    if (filters.size() != 2) {
      throw InvalidInput.exception(
          "Delta and filter requires 2 filter predicates, but provided [%d].",
          filters.size()
      );
    }
    this.predicates = filters;
  }

  @Override
  public Predicate getFilterPredicate(StructType snapshotSchema)
  {
    // This is simple for now. We can do a recursive flatten.
    return new And(
        predicates.get(0).getFilterPredicate(snapshotSchema),
        predicates.get(1).getFilterPredicate(snapshotSchema)
    );
  }
}
