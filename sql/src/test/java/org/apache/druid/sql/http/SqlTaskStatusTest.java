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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class SqlTaskStatusTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();

    DruidException underlyingException = DruidException.forPersona(DruidException.Persona.DEVELOPER)
        .ofCategory(DruidException.Category.INVALID_INPUT)
        .build("error message");

    final SqlTaskStatus status = new SqlTaskStatus(
        "taskid",
        TaskState.FAILED,
        new ErrorResponse(underlyingException)
    );

    final SqlTaskStatus status2 = mapper.readValue(mapper.writeValueAsString(status), SqlTaskStatus.class);

    Assert.assertEquals(status.getTaskId(), status2.getTaskId());
    Assert.assertEquals(status.getState(), status2.getState());
    Assert.assertNotNull(status.getError());
    Assert.assertNotNull(status2.getError());
    Assert.assertEquals(status.getError().getAsMap(), status2.getError().getAsMap());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SqlTaskStatus.class).usingGetClass().verify();
  }
}
