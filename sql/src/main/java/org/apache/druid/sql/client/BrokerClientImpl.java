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

package org.apache.druid.sql.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.sql.http.ExplainPlanResponse;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlTaskStatus;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.List;

public class BrokerClientImpl implements BrokerClient
{
  private static final Logger log = new EmittingLogger(BrokerClientImpl.class);

  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public BrokerClientImpl(final ServiceClient client, final ObjectMapper jsonMapper)
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ListenableFuture<SqlTaskStatus> submitTask(final SqlQuery sqlQuery)
  {
    log.info("Submitting task with query[%s].", sqlQuery);
    final String path = "/druid/v2/sql/task/";
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, sqlQuery),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), SqlTaskStatus.class)
    );
  }

  @Override
  public ListenableFuture<List<ExplainPlanResponse>> explainPlanFor(SqlQuery sqlQuery)
  {
    final SqlQuery explainSqlQuery = new SqlQuery(
        StringUtils.format("EXPLAIN PLAN FOR %s", sqlQuery.getQuery()),
        null,
        false,
        false,
        false,
        null,
        null
    );
    log.info("Submitting explain query[%s].", explainSqlQuery);
    final String path = "/druid/v2/sql/task/";
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, explainSqlQuery),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<List<ExplainPlanResponse>>() {})
    );
  }
}

