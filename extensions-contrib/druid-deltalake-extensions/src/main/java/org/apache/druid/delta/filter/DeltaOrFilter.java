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
import io.delta.kernel.expressions.Or;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import org.apache.druid.error.InvalidInput;

import java.util.List;

public class DeltaOrFilter implements DeltaFilter
{
  @JsonProperty
  private final List<DeltaFilter> predicates;

  @JsonCreator
  public DeltaOrFilter(
      @JsonProperty("predicates") List<DeltaFilter> predicates
  )
  {
    if (predicates == null) {
      throw InvalidInput.exception("predicates[%s] is invalid. Delta or filter requires 2 fields", predicates);
    }
    if (predicates.size() != 2) {
      throw InvalidInput.exception(
          "Delta or filter requires 2 fields, but provided [%d].",
          predicates.size()
      );
    }
    this.predicates = predicates;
  }

  @Override
  public Predicate getFilterPredicate(StructType snapshotSchema)
  {
    // Maybe do a nested / recursive flatten?
    return new Or(
        predicates.get(0).getFilterPredicate(snapshotSchema),
        predicates.get(1).getFilterPredicate(snapshotSchema)
    );
  }
}
