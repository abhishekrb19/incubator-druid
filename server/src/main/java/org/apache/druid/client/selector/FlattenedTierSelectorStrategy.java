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

package org.apache.druid.client.selector;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A tier selector strategy that flattens all servers from configured priorities into a single pool
 * and uses the ServerSelectorStrategy (random/connectionCount) to select from that pool.
 *
 * Unlike CustomTierSelectorStrategy which checks priorities in order (priority 2, then 1, then 0),
 * FlattenedTierSelectorStrategy treats all configured priorities equally and load-balances across
 * all servers in those priorities.
 *
 * Configuration example:
 * druid.broker.select.tier=flattened
 * druid.broker.select.tier.flattened.priorities=[2,1]
 *
 * With this configuration and ConnectionCountServerSelectorStrategy:
 * - All servers with priority 2 or 1 are combined into a single pool
 * - The server with the lowest connection count from this combined pool is selected
 * - Servers with any other priority values are ignored
 *
 * This is useful when you want to:
 * 1. Isolate certain priority tiers (strict filtering)
 * 2. Load-balance evenly across those tiers without priority preference
 */
public class FlattenedTierSelectorStrategy extends AbstractTierSelectorStrategy
{
  private static final Logger log = new Logger(FlattenedTierSelectorStrategy.class);
  public static final String TYPE = "flattened";

  private final ServerSelectorStrategy serverSelectorStrategy;
  private final FlattenedTierSelectorStrategyConfig config;
  private final Set<Integer> configuredPriorities;

  @JsonCreator
  public FlattenedTierSelectorStrategy(
      @JacksonInject ServerSelectorStrategy serverSelectorStrategy,
      @JacksonInject FlattenedTierSelectorStrategyConfig config
  )
  {
    super(serverSelectorStrategy);
    this.serverSelectorStrategy = serverSelectorStrategy;
    this.config = config;
    this.configuredPriorities = new HashSet<>(config.getPriorities());
  }

  @Override
  public <T> List<QueryableDruidServer> pick(
      Query<T> query,
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
      DataSegment segment,
      int numServersToPick
  )
  {
    // Flatten all servers from configured priorities into a single pool
    // Use LinkedHashSet to maintain insertion order for deterministic behavior
    Set<QueryableDruidServer> flattenedServerPool = new LinkedHashSet<>();

    // Iterate through all priorities and collect servers from configured priorities
    for (Int2ObjectMap.Entry<Set<QueryableDruidServer>> entry : prioritizedServers.int2ObjectEntrySet()) {
      int priority = entry.getIntKey();
      Set<QueryableDruidServer> servers = entry.getValue();

      if (configuredPriorities.contains(priority)) {
        // Priority is in configured list, add all its servers to the flattened pool
        flattenedServerPool.addAll(servers);
      } else {
        // Handle unconfigured priority - log debug message
        log.debug(
            "Flattened mode: Priority [%d] not in configured list [%s], ignoring servers [%s]",
            priority,
            config.getPriorities(),
            servers
        );
      }
      // TODO: handle the case where there's no priorities set? An empty priorities configured, in which case balance everything?
    }

    // If no servers found in configured priorities, return empty list
    if (flattenedServerPool.isEmpty()) {
      log.warn(
          "No servers found with configured priorities %s. Available priorities were: %s",
          config.getPriorities(),
          prioritizedServers.keySet()
      );
      return Collections.emptyList();
    }

    // Use ServerSelectorStrategy to pick from the flattened pool
    // This delegates to random/connectionCount/etc. strategy
    List<QueryableDruidServer> selectedServers = serverSelectorStrategy.pick(
        query,
        flattenedServerPool,
        segment,
        numServersToPick
    );
    log.info("GRRR given[%d], flattened[%d], selected[%d] --- givenServers[%s], flattened serverPool[%s], selectedServers[%s]",
                  prioritizedServers.size(), flattenedServerPool.size(), selectedServers.size(),
                  prioritizedServers, selectedServers, selectedServers);
    return selectedServers;
  }

  @Override
  public Comparator<Integer> getComparator()
  {
    // Return a default comparator - not really used since we override pick() completely
    // But required by interface
    return Integer::compare;
  }

  public FlattenedTierSelectorStrategyConfig getConfig()
  {
    return config;
  }

  @Override
  public String toString()
  {
    return "FlattenedTierSelectorStrategy{" +
           "config=" + config +
           '}';
  }
}
