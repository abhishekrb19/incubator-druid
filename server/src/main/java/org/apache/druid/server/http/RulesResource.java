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

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.coordinator.rules.Rules;
import org.apache.druid.server.http.security.RulesResourceFilter;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 */
@Path("/druid/coordinator/v1/rules")
public class RulesResource
{
  public static final String RULES_ENDPOINT = "/druid/coordinator/v1/rules";

  private static final String AUDIT_HISTORY_TYPE = "rules";

  private final MetadataRuleManager databaseRuleManager;
  private final AuditManager auditManager;

  @Inject
  public RulesResource(
      MetadataRuleManager databaseRuleManager,
      AuditManager auditManager
  )
  {
    this.databaseRuleManager = databaseRuleManager;
    this.auditManager = auditManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getRules()
  {
    return Response.ok(databaseRuleManager.getAllRules()).build();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(RulesResourceFilter.class)
  public Response getDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("full") final String full
  )
  {
    if (full != null) {
      return Response.ok(databaseRuleManager.getRulesWithDefault(dataSourceName))
                     .build();
    }
    return Response.ok(databaseRuleManager.getRules(dataSourceName))
                   .build();
  }

  // default value is used for backwards compatibility
  @POST
  @Path("/{dataSourceName}")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(RulesResourceFilter.class)
  public Response setDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName,
      final List<Rule> rules,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    try {
      validateRules(rules);
      final AuditInfo auditInfo = new AuditInfo(author, comment, req.getRemoteAddr());
      if (databaseRuleManager.overrideRule(dataSourceName, rules, auditInfo)) {
        return Response.ok().build();
      } else {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
      }
    }
    catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.of("error", e.getMessage()))
                     .build();
    }
  }

  @GET
  @Path("/{dataSourceName}/history")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(RulesResourceFilter.class)
  public Response getDatasourceRuleHistory(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("interval") final String interval,
      @QueryParam("count") final Integer count
  )
  {
    try {
      return Response.ok(getRuleHistory(dataSourceName, interval, count)).build();
    }
    catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                     .build();
    }
  }

  @GET
  @Path("/history")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getDatasourceRuleHistory(
      @QueryParam("interval") final String interval,
      @QueryParam("count") final Integer count
  )
  {
    try {
      return Response.ok(getRuleHistory(null, interval, count)).build();
    }
    catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                     .build();
    }
  }

  private List<AuditEntry> getRuleHistory(
      final String dataSourceName,
      final String interval,
      final Integer count
  ) throws IllegalArgumentException
  {
    if (interval == null && count != null) {
      if (dataSourceName != null) {
        return auditManager.fetchAuditHistory(dataSourceName, AUDIT_HISTORY_TYPE, count);
      }
      return auditManager.fetchAuditHistory(AUDIT_HISTORY_TYPE, count);
    }

    Interval theInterval = interval == null ? null : Intervals.of(interval);
    if (dataSourceName != null) {
      return auditManager.fetchAuditHistory(dataSourceName, AUDIT_HISTORY_TYPE, theInterval);
    }
    return auditManager.fetchAuditHistory(AUDIT_HISTORY_TYPE, theInterval);
  }

  /**
   * Validate rules. Throws an exception if a rule contain an interval that will overshadow another rules' interval.
   * Rules that will be evaluated at some point are considered to be non-overshadowing.
   * @param rules Datasource rules.
   */
  private void validateRules(final List<Rule> rules)
  {
    if (rules == null) {
      return;
    }
    final DateTime now = DateTimes.nowUtc();
    for (int i = 0; i < rules.size() - 1; i++) {
      final Rule currRule = rules.get(i);
      final Rule nextRule = rules.get(i + 1);
      final Interval currInterval = currRule.getInterval(now);
      final Interval nextInterval = nextRule.getInterval(now);
      if (currInterval.contains(nextInterval)) {
        // If the current rule overshaows the next rule even at the intervals' boundaries, then we know that the next
        // rule will always be a no-op. Also, a forever rule spans eternity and overshadows everything that follows it.
        if (Rules.FOREVER_INTERVAL.equals(currInterval) ||
                    (currRule.getInterval(currInterval.getStart()).contains(nextRule.getInterval(currInterval.getStart()))
                     && currRule.getInterval(currInterval.getEnd()).contains(nextRule.getInterval(currInterval.getEnd())))) {
          throw InvalidInput.exception(
              "Rule[%s] has an interval that contains interval for rule[%s]. The interval[%s] also covers interval[%s].",
              currRule,
              nextRule,
              currInterval,
              nextInterval
          );
        }
      }
    }
  }
}
