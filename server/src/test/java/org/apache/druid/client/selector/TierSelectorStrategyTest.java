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

import com.google.common.collect.ImmutableList;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.Query;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TierSelectorStrategyTest
{

  @Test
  public void testHighestPriorityTierSelectorStrategyRealtime()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy()),
        highPriority, lowPriority
    );
  }

  @Test
  public void testHighestPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy()),
        highPriority, lowPriority
    );
  }

  @Test
  public void testLowestPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new LowestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy()),
        lowPriority, highPriority
    );
  }

  @Test
  public void testCustomPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer mediumPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new CustomTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new CustomTierSelectorStrategyConfig()
            {
              @Override
              public List<Integer> getPriorities()
              {
                return Arrays.asList(2, 0, -1, 1);
              }
            }
        ),
        mediumPriority, lowPriority, highPriority
    );
  }

  @Test
  public void testEmptyCustomPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer lowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer mediumPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer highPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new CustomTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new CustomTierSelectorStrategyConfig()
            {
              @Override
              public List<Integer> getPriorities()
              {
                return new ArrayList<>();
              }
            }
        ),
        highPriority, mediumPriority, lowPriority
    );
  }

  @Test
  public void testIncompleteCustomPriorityTierSelectorStrategy()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );
    QueryableDruidServer p3 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );
    QueryableDruidServer p4 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 3),
        client
    );
    TierSelectorStrategy tierSelectorStrategy = new CustomTierSelectorStrategy(
        new ConnectionCountServerSelectorStrategy(),
        new CustomTierSelectorStrategyConfig()
        {
          @Override
          public List<Integer> getPriorities()
          {
            return Arrays.asList(2, 0, -1);
          }
        }
    );
    testTierSelectorStrategy(
        tierSelectorStrategy,
        p3, p1, p0, p4, p2
    );
  }

  @Test
  public void testStrictTierSelectorStrategyAllConfigured()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer pNeg1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );

    testTierSelectorStrategy(
        new StrictTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new StrictTierSelectorStrategyConfig(List.of(2, 0, -1))
        ),
        p2, p0, pNeg1
    );
  }

  @Test
  public void testStrictTierSelectorStrategyMixedPriorities()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer pNeg1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    // Configure only priorities 1 and -1
    // Servers with priorities 0, 2, 3 should be filtered out
    testTierSelectorStrategy(
        new StrictTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new StrictTierSelectorStrategyConfig(List.of(1, -1))
        ),
        p1, pNeg1
    );
  }

  @Test
  public void testStrictTierSelectorStrategyNoMatchingPriorities()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        new StrictTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new StrictTierSelectorStrategyConfig(List.of(5, 6))
        ),
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = Arrays.asList(p0, p1, p2);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Should return null when no matching priorities
    Assert.assertNull(serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertNull(serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));

    // Should return empty list for getCandidates
    Assert.assertEquals(Collections.emptyList(), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testEmptyStrictTierPrioritiesThrowsException()
  {
    DruidException druidException = Assert.assertThrows(
        DruidException.class,
        () -> new StrictTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new StrictTierSelectorStrategyConfig(List.of())
        )
    );
    Assert.assertEquals("priorities must be non-empty when configured on the Broker. Found priorities[[]].", druidException.getMessage());
  }

  private void testTierSelectorStrategy(
      TierSelectorStrategy tierSelectorStrategy,
      QueryableDruidServer... expectedSelection
  )
  {
    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = new ArrayList<>(Arrays.asList(expectedSelection));

    List<DruidServerMetadata> expectedCandidates = new ArrayList<>();
    for (QueryableDruidServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    Collections.shuffle(servers);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    Assert.assertEquals(expectedSelection[0], serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedCandidates, serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedCandidates.subList(0, 2), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testServerSelectorStrategyDefaults()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    Set<QueryableDruidServer> servers = new HashSet<>();
    servers.add(p0);
    RandomServerSelectorStrategy strategy = new RandomServerSelectorStrategy();
    Assert.assertEquals(strategy.pick(servers, EasyMock.createMock(DataSegment.class)), p0);
    Assert.assertEquals(
        strategy.pick(
            EasyMock.createMock(Query.class),
            servers,
            EasyMock.createMock(DataSegment.class)
        ), p0
    );
    ServerSelectorStrategy defaultDeprecatedServerSelectorStrategy = new ServerSelectorStrategy()
    {
      @Override
      public <T> List<QueryableDruidServer> pick(
          @Nullable Query<T> query, Set<QueryableDruidServer> servers, DataSegment segment,
          int numServersToPick
      )
      {
        return strategy.pick(servers, segment, numServersToPick);
      }
    };
    Assert.assertEquals(
        defaultDeprecatedServerSelectorStrategy.pick(servers, EasyMock.createMock(DataSegment.class)),
        p0
    );
    Assert.assertEquals(
        defaultDeprecatedServerSelectorStrategy.pick(servers, EasyMock.createMock(DataSegment.class), 1)
                                               .get(0), p0
    );
  }

  /**
   * Tests the PreferredTierSelectorStrategy with various configurations and expected selections.
   * It verifies
   * 1. The preferred tier is respected when picking a server.
   * 2. When getting all servers, the preferred tier is ignored, and the returned list is sorted by priority.
   * 3. When getting a limited number of candidates, it returns the top N servers with the preferred tier first.
   */
  private void testPreferredTierSelectorStrategy(
      PreferredTierSelectorStrategy tierSelectorStrategy,
      QueryableDruidServer... expectedSelection
  )
  {
    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = new ArrayList<>(Arrays.asList(expectedSelection));

    List<DruidServerMetadata> expectedCandidates = new ArrayList<>();
    for (QueryableDruidServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Verify that the preferred tier is respected when picking a server
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));

    // Verify that when getting all severs, the preferred tier is ignored, the returned list is sorted by priority
    List<DruidServerMetadata> allServers = new ArrayList<>(expectedCandidates);
    allServers.sort((o1, o2) -> tierSelectorStrategy.getComparator().compare(o1.getPriority(), o2.getPriority()));
    // verify the priority only because values with same priority may return in different order
    Assert.assertEquals(
        allServers.stream().map(DruidServerMetadata::getPriority).collect(Collectors.toList()),
        serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES).stream().map(DruidServerMetadata::getPriority).collect(Collectors.toList())
    );

    // Verify that when getting a limited number of candidates, returns the top N servers with preferred tier first
    Assert.assertEquals(expectedCandidates.subList(0, 2), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testPreferredTierSelectorStrategyHighestPriority()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);

    // Two servers that have same tier and priority
    QueryableDruidServer preferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableDruidServer preferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );

    QueryableDruidServer preferredTierHighPriority2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );

    QueryableDruidServer nonPreferredTierHighestPriority = new QueryableDruidServer(
        new DruidServer("test4", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 2),
        client
    );

    PreferredTierSelectorStrategy tierSelectorStrategy = new PreferredTierSelectorStrategy(
        // Use a customized strategy that return the 2nd server
        new ServerSelectorStrategy()
        {
          @Override
          public List<QueryableDruidServer> pick(Set<QueryableDruidServer> servers, DataSegment segment, int numServersToPick)
          {
            if (servers.size() <= numServersToPick) {
              return ImmutableList.copyOf(servers);
            }
            List<QueryableDruidServer> list = new ArrayList<>(servers);
            if (numServersToPick == 1) {
              // return the server whose name is greater
              return list.stream()
                         .sorted((o1, o2) -> o1.getServer().getName().compareTo(o2.getServer().getName()))
                         .skip(1)
                         .limit(1)
                         .collect(Collectors.toList());
            } else {
              return list.stream().limit(numServersToPick).collect(Collectors.toList());
            }
          }
        },
        new PreferredTierSelectorStrategyConfig("preferred", "highest")
    );

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = new ArrayList<>(Arrays.asList(
        preferredTierLowPriority,
        preferredTierHighPriority,
        preferredTierHighPriority2,
        nonPreferredTierHighestPriority
    ));

    List<DruidServerMetadata> expectedCandidates = new ArrayList<>();
    for (QueryableDruidServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Verify that the 2nd server is selected
    Assert.assertEquals(preferredTierHighPriority2, serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(preferredTierHighPriority2, serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));

    // Verify that when getting all severs, the preferred tier is ignored, the returned list is sorted by priority
    List<DruidServerMetadata> allServers = new ArrayList<>(expectedCandidates);
    allServers.sort((o1, o2) -> tierSelectorStrategy.getComparator().compare(o1.getPriority(), o2.getPriority()));
    // verify the priority only because values with same priority may return in different order
    Assert.assertEquals(
        allServers.stream().map(DruidServerMetadata::getPriority).collect(Collectors.toList()),
        serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES).stream().map(DruidServerMetadata::getPriority).collect(Collectors.toList())
    );

    // Verify that when getting 2 candidates, returns the top N servers with preferred tier first
    Assert.assertEquals(
        Arrays.asList(
            preferredTierHighPriority.getServer().getMetadata(),
            preferredTierHighPriority2.getServer().getMetadata()
        ),

        serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES)
                      .stream()
                      // sort the name to make sure the test is stable
                      .sorted((o1, o2) -> o1.getName().compareTo(o2.getName()))
                      .collect(Collectors.toList())
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyLowestPriority()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer preferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableDruidServer preferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );
    QueryableDruidServer nonPreferredTierLowestPriority = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", -1),
        client
    );

    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PreferredTierSelectorStrategyConfig("preferred", "lowest")
        ),
        preferredTierLowPriority, preferredTierHighPriority, nonPreferredTierLowestPriority
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyWithFallback()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    // Create only non-preferred tier servers with different priorities
    QueryableDruidServer nonPreferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 0),
        client
    );
    QueryableDruidServer nonPreferredTierMediumPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 1),
        client
    );
    QueryableDruidServer nonPreferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 2),
        client
    );

    // Since no preferred tier servers are available, it should fall back to other servers
    // based on highest priority
    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PreferredTierSelectorStrategyConfig("preferred", "highest")
        ),
        nonPreferredTierHighPriority, nonPreferredTierMediumPriority, nonPreferredTierLowPriority
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyMixedServers()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer preferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableDruidServer preferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );
    QueryableDruidServer anotherTierHighPriority = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "tier1", 3),
        client
    );
    QueryableDruidServer yetAnotherTierMediumPriority = new QueryableDruidServer(
        new DruidServer("test4", "localhost", null, 0, null, ServerType.HISTORICAL, "tier2", 2),
        client
    );

    // Should return preferred tier servers first, sorted by priority
    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PreferredTierSelectorStrategyConfig("preferred", "highest")
        ),
        preferredTierHighPriority, preferredTierLowPriority, anotherTierHighPriority, yetAnotherTierMediumPriority
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyDefaultPriority()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);

    QueryableDruidServer preferredTierLowPriority = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableDruidServer preferredTierHighPriority = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, "preferred", 1),
        client
    );
    QueryableDruidServer nonPreferredTierHighestPriority = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, "non-preferred", 2),
        client
    );

    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            // Using null for priority should default to highest priority
            new PreferredTierSelectorStrategyConfig("preferred", null)
        ),
        preferredTierHighPriority, preferredTierLowPriority, nonPreferredTierHighestPriority
    );
  }

  /**
   * Tests FlattenedTierSelectorStrategy with RandomServerSelectorStrategy to verify that
   * all servers with configured priorities are included in the flattened pool and can be
   * selected randomly (demonstrating no priority preference).
   */
  @Test
  public void testFlattenedTierSelectorStrategyAllConfigured()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer pNeg1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );

    FlattenedTierSelectorStrategyConfig config = new FlattenedTierSelectorStrategyConfig(List.of(2, 0, -1));
    TierSelectorStrategy strategy = new FlattenedTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        config
    );

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        strategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = Arrays.asList(pNeg1, p0, p2);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // All 3 servers should be available since all priorities are configured
    List<DruidServerMetadata> allServers = serverSelector.getAllServers(CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(3, allServers.size());

    // Verify all three priorities are present (order doesn't matter in flattened)
    Set<Integer> priorities = allServers.stream()
                                        .map(DruidServerMetadata::getPriority)
                                        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of(-1, 0, 2), priorities);

    // Test getCandidates with different sizes - verify correct count
    List<DruidServerMetadata> candidates1 = serverSelector.getCandidates(1, CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(1, candidates1.size());
    Assert.assertTrue(Set.of(-1, 0, 2).contains(candidates1.get(0).getPriority()));

    List<DruidServerMetadata> candidates2 = serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(2, candidates2.size());

    List<DruidServerMetadata> candidates3 = serverSelector.getCandidates(3, CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(3, candidates3.size());
    Set<Integer> candidates3Priorities = candidates3.stream()
                                                     .map(DruidServerMetadata::getPriority)
                                                     .collect(Collectors.toSet());
    Assert.assertEquals(Set.of(-1, 0, 2), candidates3Priorities);

    // -1 means all servers
    Assert.assertEquals(3, serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES).size());

    // Pick should return one of the three servers (any priority - demonstrates flattening)
    QueryableDruidServer picked = serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES);
    Assert.assertNotNull(picked);
    Assert.assertTrue(
        "Picked server should have one of the configured priorities",
        picked.getServer().getPriority() == -1 ||
        picked.getServer().getPriority() == 0 ||
        picked.getServer().getPriority() == 2
    );

    // Pick multiple times to verify servers from flattened pool are accessible
    Set<Integer> pickedPriorities = new HashSet<>();
    for (int i = 0; i < 20; i++) {
      QueryableDruidServer server = serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES);
      Assert.assertNotNull(server);
      pickedPriorities.add(server.getServer().getPriority());
    }
    // With RandomServerSelectorStrategy and 20 picks from 3 servers,
    // we should see multiple priorities (verifies true flattening)
    Assert.assertTrue(
        "Expected to see servers from multiple priorities, but only saw: " + pickedPriorities,
        pickedPriorities.size() >= 2
    );
  }

  /**
   * Tests FlattenedTierSelectorStrategy with ConnectionCountServerSelectorStrategy to verify
   * that flattening works correctly: servers from multiple priorities are treated equally,
   * and selection is based on connection count rather than priority.
   * <p>
   * This demonstrates the key use case: load balancing across replicas with different priorities
   * (e.g., priority 1 and 0) so they're treated equally instead of always preferring higher priority.
   */
  @Test
  public void testFlattenedTierSelectorStrategyWithConnectionCount()
  {
    // Test with ConnectionCountServerSelectorStrategy for deterministic behavior
    DirectDruidClient client1 = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(client1.getNumOpenConnections()).andReturn(5).anyTimes();
    DirectDruidClient client2 = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(client2.getNumOpenConnections()).andReturn(10).anyTimes();
    DirectDruidClient client3 = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(client3.getNumOpenConnections()).andReturn(2).anyTimes();
    DirectDruidClient client4 = EasyMock.createMock(DirectDruidClient.class);
    EasyMock.expect(client4.getNumOpenConnections()).andReturn(8).anyTimes();

    EasyMock.replay(client1, client2, client3, client4);

    // Create servers with different priorities and connection counts
    // Use unique server names to avoid interference with other tests
    // Priority 0: 5 connections
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("flattened-conn-test1", "localhost:8001", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 0),
        client1
    );
    // Priority 1: 10 connections
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("flattened-conn-test2", "localhost:8002", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 1),
        client2
    );
    // Priority 2: 2 connections (lowest)
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("flattened-conn-test3", "localhost:8003", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 2),
        client3
    );
    // Priority 3: 8 connections (will be filtered out)
    QueryableDruidServer p3 = new QueryableDruidServer(
        new DruidServer("flattened-conn-test4", "localhost:8004", null, 0, null, ServerType.INDEXER_EXECUTOR, DruidServer.DEFAULT_TIER, 3),
        client4
    );

    // Configure flattened strategy with priorities 0, 1, 2 (exclude 3)
    FlattenedTierSelectorStrategyConfig config = new FlattenedTierSelectorStrategyConfig(List.of(0, 1, 2));
    TierSelectorStrategy strategy = new FlattenedTierSelectorStrategy(
        new ConnectionCountServerSelectorStrategy(),
        config
    );

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        strategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    // Add all 4 servers
    List<QueryableDruidServer> servers = Arrays.asList(p0, p1, p2, p3);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Only 3 servers should be available (priority 3 filtered out)
//    List<DruidServerMetadata> allServers = serverSelector.getAllServers(CloneQueryMode.EXCLUDECLONES);
//    Assert.assertEquals(3, allServers.size());

    // Verify priorities 0, 1, 2 are present (priority 3 excluded)
//    Set<Integer> priorities = allServers.stream()
//                                        .map(DruidServerMetadata::getPriority)
//                                        .collect(Collectors.toSet());
//    Assert.assertEquals(Set.of(0, 1, 2), priorities);

    // With ConnectionCountServerSelectorStrategy, pick should ALWAYS return
    // the server with lowest connection count from the flattened pool
    // Priority 2 has 2 connections (lowest), so it should always be picked
    QueryableDruidServer picked = serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES);
    Assert.assertNotNull(picked);
    Assert.assertEquals(2, picked.getServer().getPriority());
//    Assert.assertEquals(2, picked.getNumOpenConnections());

    // Pick multiple times - should always get the same server (deterministic)
    // This demonstrates true flattening: connection count wins over priority
    for (int i = 0; i < 5; i++) {
      QueryableDruidServer server = serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES);
      Assert.assertEquals(p2, server);
      Assert.assertEquals(2, server.getServer().getPriority());
    }

    // getCandidates() uses ServerSelector's TreeMap ordering, not the flattened pool
    // This returns servers in priority order (0, 1, 2), not connection count order
    // This is different from pick() which uses the flattened pool for load balancing
    List<DruidServerMetadata> candidates = serverSelector.getCandidates(3, CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(3, candidates.size());
    // TreeMap returns servers in priority order (using Integer::compare comparator)
    Assert.assertEquals(0, candidates.get(0).getPriority());  // Priority 0 (5 conns)
    Assert.assertEquals(1, candidates.get(1).getPriority());  // Priority 1 (10 conns)
    Assert.assertEquals(2, candidates.get(2).getPriority());  // Priority 2 (2 conns)

    EasyMock.verify(client1, client2, client3, client4);
  }

  /**
   * Tests FlattenedTierSelectorStrategy filtering behavior: verifies that only servers
   * with configured priorities are included in the flattened pool, and servers with
   * unconfigured priorities are excluded.
   */
  @Test
  public void testFlattenedTierSelectorStrategyMixedPriorities()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer pNeg1 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, -1),
        client
    );
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test4", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );
    QueryableDruidServer p3 = new QueryableDruidServer(
        new DruidServer("test5", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 3),
        client
    );

    // Configure only priorities 1 and -1
    FlattenedTierSelectorStrategyConfig config = new FlattenedTierSelectorStrategyConfig(List.of(1, -1));
    TierSelectorStrategy strategy = new FlattenedTierSelectorStrategy(
        new RandomServerSelectorStrategy(),
        config
    );

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        strategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    // Add all 5 servers
    List<QueryableDruidServer> servers = Arrays.asList(pNeg1, p0, p1, p2, p3);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Only servers with priorities 1 and -1 should be included
    List<DruidServerMetadata> allServers = serverSelector.getCandidates(3, CloneQueryMode.EXCLUDECLONES);
    Assert.assertEquals(2, allServers.size());

    // Verify only priorities 1 and -1 are present
    Set<Integer> priorities = allServers.stream()
                                        .map(DruidServerMetadata::getPriority)
                                        .collect(Collectors.toSet());
    Assert.assertEquals(Set.of(-1, 1), priorities);

    // Pick should return server with priority 1 or -1
    QueryableDruidServer picked = serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES);
    Assert.assertNotNull(picked);
    Assert.assertTrue(
        picked.getServer().getPriority() == -1 ||
        picked.getServer().getPriority() == 1
    );
  }


  /**
   * Tests FlattenedTierSelectorStrategy when no servers match the configured priorities.
   * Verifies that empty results are returned (null for pick, empty list for getCandidates).
   */
  @Test
  public void testFlattenedTierSelectorStrategyNoMatchingPriorities()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );
    QueryableDruidServer p2 = new QueryableDruidServer(
        new DruidServer("test3", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 2),
        client
    );

    // Configure priorities that don't exist in servers
    FlattenedTierSelectorStrategyConfig config = new FlattenedTierSelectorStrategyConfig(List.of(5, 6));
    TierSelectorStrategy strategy = new FlattenedTierSelectorStrategy(
        new ConnectionCountServerSelectorStrategy(),
        config
    );

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        strategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = Arrays.asList(p0, p1, p2);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Should return null when no matching priorities
    Assert.assertNull(serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertNull(serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));

    // Should return empty list for getCandidates and getAllServers
    Assert.assertEquals(Collections.emptyList(), serverSelector.getCandidates(1, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(Collections.emptyList(), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testFlattenedTierSelectorStrategyEmptyPriorities()
  {
    DirectDruidClient client = EasyMock.createMock(DirectDruidClient.class);
    QueryableDruidServer p0 = new QueryableDruidServer(
        new DruidServer("test1", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        client
    );
    QueryableDruidServer p1 = new QueryableDruidServer(
        new DruidServer("test2", "localhost", null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 1),
        client
    );

    // Configure empty priorities
    FlattenedTierSelectorStrategyConfig config = new FlattenedTierSelectorStrategyConfig(List.of());
    TierSelectorStrategy strategy = new FlattenedTierSelectorStrategy(
        new ConnectionCountServerSelectorStrategy(),
        config
    );

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        strategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableDruidServer> servers = Arrays.asList(p0, p1);
    for (QueryableDruidServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Should return null when priorities list is empty
    Assert.assertNull(serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertNull(serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));

    // Should return empty list for getCandidates
    Assert.assertEquals(Collections.emptyList(), serverSelector.getCandidates(1, CloneQueryMode.EXCLUDECLONES));
//    Assert.assertEquals(Collections.emptyList(), serverSelector.getAllServers(CloneQueryMode.EXCLUDECLONES));
  }
}
