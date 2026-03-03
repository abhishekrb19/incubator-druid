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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class StrictTierSelectorStrategy extends AbstractTierSelectorStrategy
{
  private static final Logger log = new Logger(StrictTierSelectorStrategy.class);
  public static final String TYPE = "strict";

  private final StrictTierSelectorStrategyConfig config;
  private final List<Integer> configuredPriorities;
  private final Comparator<Integer> comparator;

  @JsonCreator
  public StrictTierSelectorStrategy(
      @JacksonInject final ServerSelectorStrategy serverSelectorStrategy,
      @JacksonInject final StrictTierSelectorStrategyConfig config
  )
  {
    super(serverSelectorStrategy);
    this.config = config;
    this.configuredPriorities = config.getPriorities();

    final Map<Integer, Integer> lookup = new HashMap<>();
    int pos = 0;
    for (Integer integer : config.getPriorities()) {
      lookup.put(integer, pos);
      pos++;
    }

    // Tiers with priorities explicitly specified in the custom priority list config always have higher priority than
    // those not and those not specified fall back to use the highest priority strategy among themselves
    this.comparator = (p1, p2) -> {
      final Integer rank1 = lookup.get(p1);
      final Integer rank2 = lookup.get(p2);

      if (rank1 != null && rank2 != null) {
        return Integer.compare(rank1, rank2);
      }
      if (rank1 != null) {
        return -1;
      }
      if (rank2 != null) {
        return 1;
      }

      // Fall back to highest priority first
      return Integer.compare(p2, p1);
    };
  }

  @Override
  public Comparator<Integer> getComparator()
  {
    return comparator;
  }

  public StrictTierSelectorStrategyConfig getConfig()
  {
    return config;
  }

  @Override
  public <T> List<QueryableDruidServer> pick(
      Query<T> query,
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
      DataSegment segment,
      int numServersToPick
  )
  {
    // if there's no match between configuredPriorities and prioritizedServers's priorities, then return empty list
    // Get all actual priorities present in the server map (already in comparator order)
    // Create a filtered map with only configured priorities, preserving order
    final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> filteredPrioritizedServers = new Int2ObjectRBTreeMap<>(getComparator()); // fixme: define comparator

    // Iterate through entries in tree order to preserve priority ordering
    for (Int2ObjectMap.Entry<Set<QueryableDruidServer>> entry : prioritizedServers.int2ObjectEntrySet()) {
      int priority = entry.getIntKey();
      Set<QueryableDruidServer> servers = entry.getValue();

      if (configuredPriorities.contains(priority)) {
        // Priority is in configured list, include it (preserves the same Set reference)
        filteredPrioritizedServers.put(priority, servers);
      } else {
        // Handle unconfigured priority - log warning
        log.warn(
            "Priority [%d] not in configured list [%s] so ignore servers [%s]",
            priority, this.configuredPriorities, servers
        );
      }
    }

    // If no matching priorities found, return empty list
    if (filteredPrioritizedServers.isEmpty()) {
      log.warn(
          "Servers found with configured priorities %s. Available priorities were: %s",
          this.configuredPriorities, prioritizedServers.keySet()
      );
      return Collections.emptyList();
    }

    log.info(
        "Found [%d] filtered servers[%s] for query[%s]",
        filteredPrioritizedServers.size(),
        filteredPrioritizedServers,
        query
    );
    return super.pick(query, filteredPrioritizedServers, segment, numServersToPick);
  }

  @Override
  public String toString()
  {
    return "StrictTierSelectorStrategy{" +
           "configuredPriorities=" + configuredPriorities +
           '}';
  }
}
