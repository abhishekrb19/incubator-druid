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

package org.apache.druid.indexing.batch;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.guice.SupervisorModule;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.sql.client.BrokerClient;
import org.apache.druid.sql.http.ExplainPlanResponse;
import org.apache.druid.sql.http.SqlQuery;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class ScheduledBatchSupervisorTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private BrokerClient brokerClient;
  private ScheduledBatchScheduler scheduler;
  private SqlQuery query;

  @Before
  public void setUp()
  {
    brokerClient = Mockito.mock(BrokerClient.class);
    scheduler = Mockito.mock(ScheduledBatchScheduler.class);

    OBJECT_MAPPER.setInjectableValues(
        new InjectableValues.Std()
            .addValue(BrokerClient.class, brokerClient)
            .addValue(ObjectMapper.class, OBJECT_MAPPER)
            .addValue(ScheduledBatchScheduler.class, scheduler)
    );
    OBJECT_MAPPER.registerModules(
        new SupervisorModule().getJacksonModules()
    );

    query = new SqlQuery(
        "REPLACE INTO foo OVERWRITE ALL SELECT TIME_PARSE(ts) AS __time, c1 FROM (VALUES('2023-01-01', 'insert_1'), ('2023-01-01', 'insert_2'), ('2023-02-01', 'insert3')) AS t(ts, c1) PARTITIONED BY ALL ",
        null,
        false,
        false,
        false,
        null,
        null
    );

    final ExplainPlanResponse explainPlanResponse = new ExplainPlanResponse(
        "",
        "",
        "{\"statementType\":\"REPLACE\",\"targetDataSource\":\"foo\",\"partitionedBy\":{\"type\":\"all\"},\"replaceTimeChunks\":\"all\"}"
    );
    Mockito.when(brokerClient.explainPlanFor(query))
           .thenReturn(Futures.immediateFuture(ImmutableList.of(explainPlanResponse)));
  }

  @Test
  public void testStartStopSupervisorForActiveSpec()
  {
    final ScheduledBatchSupervisorSpec activeSpec = new ScheduledBatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        false,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        brokerClient
    );
    final ScheduledBatchSupervisor supervisor = activeSpec.createSupervisor();
    assertEquals(ScheduledBatchSupervisor.State.RUNNING, supervisor.getState());

    supervisor.start();
    supervisor.stop(false);

    Mockito.verify(scheduler, Mockito.times(1)).startScheduledIngestion(activeSpec.getId(), activeSpec.getSchedulerConfig(), activeSpec.getSpec());
    Mockito.verify(scheduler, Mockito.times(1)).stopScheduledIngestion(activeSpec.getId());
  }

  @Test
  public void testStartStopSupervisorForSuspendedSpec()
  {
    final ScheduledBatchSupervisorSpec suspendedSpec = new ScheduledBatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        true,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        brokerClient
    );

    final ScheduledBatchSupervisor supervisor = suspendedSpec.createSupervisor();
    assertEquals(ScheduledBatchSupervisor.State.SUSPENDED, supervisor.getState());

    supervisor.start();
    supervisor.stop(false);

    Mockito.verify(scheduler, Mockito.times(2)).stopScheduledIngestion(suspendedSpec.getId());
  }

  @Test
  public void testGetStatus()
  {
    final ScheduledBatchSupervisorSpec activeSpec = new ScheduledBatchSupervisorSpec(
        query,
        new UnixCronSchedulerConfig("* * * * *"),
        false,
        null,
        null,
        OBJECT_MAPPER,
        scheduler,
        brokerClient
    );
    final ScheduledBatchSupervisor supervisor = activeSpec.createSupervisor();
    final SupervisorReport<ScheduledBatchSupervisorSnapshot> observedStatus = supervisor.getStatus();
    assertEquals(activeSpec.getId(), observedStatus.getId());
    Mockito.verify(scheduler, Mockito.times(1)).getSchedulerSnapshot(activeSpec.getId());
  }
}
