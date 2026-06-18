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

package org.apache.druid.indexing.seekablestream;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.ShardSpec;

/**
 * Accumulates information from the rows a streaming task ingests and, at publish time, stamps each segment with a
 * prunable {@link ShardSpec} derived from that information, so the broker can prune the segment at query time without
 * waiting for compaction. This is the pluggable "collect something per row, then build a shard spec" operation behind
 * {@link StreamingPartitionsSpec}: a strategy is added by writing a new {@link StreamingPartitionsSpec} subtype plus a
 * matching collector, without touching the task runner. The built-in implementation collects the distinct values of a
 * configured set of dimensions (see {@code DimensionValueSetCollector}); future strategies could instead collect
 * min/max ranges or build bloom filters for higher-cardinality columns.
 *
 * <p>One instance is created per task run (via {@link StreamingPartitionsSpec#createCollector()}) and shared across all
 * of that task's segments.
 *
 * <h3>Thread-safety contract (required of all implementations)</h3>
 * The task's run loop calls {@link #collect} (writes) while the segment-publish path calls {@link #annotate} (reads).
 * The publish path runs the annotation inside a {@code Futures.transform(..., MoreExecutors.directExecutor())}
 * continuation, so {@link #annotate} executes on whichever thread completes the publish future — not a dedicated
 * thread — and may run concurrently with {@link #collect} for the same {@link SegmentId}. Implementations <b>must</b> be
 * safe under that race (the built-in implementation keys a {@code ConcurrentHashMap} by {@link SegmentId} with
 * {@code Collections.synchronizedSet} value sets, snapshotting each set under its own monitor before iterating).
 * {@link #markRestartSpanned} is called only at task startup, before any {@link #collect};
 * {@link #onSegmentPublished} is called only from the publish-success callback.
 */
public interface StreamingShardSpecCollector
{
  /**
   * Records whatever information this strategy needs from {@code row} for {@code segmentId}. Called on the run-loop
   * thread, once per row successfully added to {@code segmentId}.
   */
  void collect(SegmentId segmentId, InputRow row);

  /**
   * Marks {@code segmentId} as restored from disk across a task restart. Such a segment's pre-restart rows are not
   * re-read, so the collected information is incomplete; to avoid wrongly pruning those rows, {@link #annotate} must
   * return a non-pruning shard spec for it. Called only at task startup, before the run loop begins; idempotent.
   */
  void markRestartSpanned(SegmentId segmentId);

  /**
   * Returns {@code segment} stamped with a shard spec derived from the information collected for it. When the segment
   * is restart-spanned (see {@link #markRestartSpanned}) or nothing was collected for it, this returns a non-pruning
   * fallback shard spec.
   *
   * <p>Implementations <b>must</b> keep the shard-spec class uniform within a time interval: all segments handed to a
   * single publish must share one shard-spec class, or
   * {@link org.apache.druid.segment.realtime.appenderator.SegmentPublisherHelper} rejects the publish. In practice this
   * means returning the same shard-spec type for both the pruning and the non-pruning (fallback) case, differing only
   * in the declared values. Must be safe to call concurrently with {@link #collect}.
   */
  DataSegment annotate(DataSegment segment);

  /**
   * Notifies the collector that {@code segmentId} has been successfully published and handed off, so any per-segment
   * state accumulated for it can be released. Called only from the publish-success callback.
   */
  void onSegmentPublished(SegmentId segmentId);
}
