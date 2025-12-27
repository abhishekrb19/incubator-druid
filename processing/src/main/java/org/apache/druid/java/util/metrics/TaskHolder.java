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

package org.apache.druid.java.util.metrics;

import org.apache.druid.query.DruidMetrics;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Provides identifying information for a task. Implementations return {@code null}
 * when used in server processes that are not {@code CliPeon}. Note that t
 */
public interface TaskHolder
{
  /**
   * @return the datasource name for the task, or {@code null} if called from a server that is not {@code CliPeon}.
   */
  @Nullable
  String getDataSource();

  /**
   * @return the taskId, or {@code null} if called from a server that is not {@code CliPeon}.
   */
  @Nullable
  String getTaskId();

  /**
   * @return the taskId, or {@code null} if called from a server that is not {@code CliPeon}.
   */
  @Nullable
  String getTaskType();

  /**
   * @return the taskId, or {@code null} if called from a server that is not {@code CliPeon}.
   */
  @Nullable
  String getGroupId();

  /**
   * @return a map of task holder dimensions from the provided {@link TaskHolder} if {@link TaskHolder#getDataSource()}
   * and {@link TaskHolder#getTaskId()} are non-null.
   * <p>The task ID ({@link TaskHolder#getTaskId()}) is added to both {@link DruidMetrics#TASK_ID}
   * and {@link DruidMetrics#ID} dimensions to the map for backward compatibility. {@link DruidMetrics#ID} is
   * deprecated because it's ambiguous and will be removed in a future release.</p>
   */
  Map<String, String> getMetricDimensions();

  /**
   * Helper utility for TaskHolder implementations.
   */
  static Map<String, String> getMetricDimensions(String dataSource, String taskId, String taskType, String groupId)
  {
    return Map.of(
        DruidMetrics.DATASOURCE, dataSource,
        DruidMetrics.TASK_ID, taskId,
        DruidMetrics.ID, taskId,
        DruidMetrics.TASK_TYPE, taskType,
        DruidMetrics.GROUP_ID, groupId
    );
  }
}
