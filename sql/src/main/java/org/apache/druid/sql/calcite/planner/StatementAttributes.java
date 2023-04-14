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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * StatementAttributes holds the attributes of a SQL statement. It's used in EXPLAIN PLAN result.
 */
public final class StatementAttributes
{
  private final String statementKind;

  @Nullable
  private final SqlNode targetDataSource;

  public StatementAttributes(
      @JsonProperty("statementKind") final String statementKind,
      @JsonProperty("targetDataSource") @Nullable final SqlNode targetDataSource)
  {
    this.statementKind = statementKind;
    this.targetDataSource = targetDataSource;
  }

  /**
   * @return the statement kind of a SQL statement. For example, SELECT, INSERT, or REPLACE.
   */
  @JsonProperty
  public String getStatementKind()
  {
    return statementKind;
  }

  /**
   * @return the target datasource in a SQL statement. Returns null
   * for SELECT/non-DML statements where there is no target datasource.
   */
  @Nullable
  @JsonProperty
  public String getTargetDataSource()
  {
    return targetDataSource == null ? null : targetDataSource.toString();
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
    StatementAttributes that = (StatementAttributes) o;
    return Objects.equals(statementKind, that.statementKind) &&
           Objects.equals(targetDataSource, that.targetDataSource
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(statementKind, targetDataSource);
  }

  @Override
  public String toString()
  {
    return "StatementAttributes{" +
           "statementKind='" + statementKind + '\'' +
           ", targetDataSource=" + targetDataSource +
           '}';
  }
}
