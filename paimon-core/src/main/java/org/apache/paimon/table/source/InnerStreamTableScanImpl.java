/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.CompactionChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorFollowUpScanner;
import org.apache.paimon.table.source.snapshot.DeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.InputChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.SnapshotSplitReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;

/** {@link StreamTableScan} implementation for streaming planning. */
public class InnerStreamTableScanImpl extends AbstractInnerTableScan
        implements InnerStreamTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(InnerStreamTableScanImpl.class);

    private final CoreOptions options;
    private final SnapshotManager snapshotManager;
    private final boolean supportStreamingReadOverwrite;

    private StartingScanner startingScanner;
    private FollowUpScanner followUpScanner;
    private BoundedChecker boundedChecker;
    private boolean isFullPhaseEnd = false;
    @Nullable private Long nextSnapshotId;

    public InnerStreamTableScanImpl(
            CoreOptions options,
            SnapshotSplitReader snapshotSplitReader,
            SnapshotManager snapshotManager,
            boolean supportStreamingReadOverwrite) {
        super(options, snapshotSplitReader);
        this.options = options;
        this.snapshotManager = snapshotManager;
        this.supportStreamingReadOverwrite = supportStreamingReadOverwrite;
    }

    @Override
    public InnerStreamTableScanImpl withFilter(Predicate predicate) {
        snapshotSplitReader.withFilter(predicate);
        return this;
    }

    @Override
    public Plan plan() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(true);
        }
        if (followUpScanner == null) {
            followUpScanner = createFollowUpScanner();
        }
        if (boundedChecker == null) {
            boundedChecker = createBoundedChecker();
        }

        if (nextSnapshotId == null) {
            return tryFirstPlan();
        } else {
            return nextPlan();
        }
    }

    private Plan tryFirstPlan() {
        StartingScanner.Result result = startingScanner.scan(snapshotManager, snapshotSplitReader);
        if (result instanceof StartingScanner.ScannedResult) {
            long currentSnapshotId = ((StartingScanner.ScannedResult) result).currentSnapshotId();
            nextSnapshotId = currentSnapshotId + 1;
            isFullPhaseEnd =
                    boundedChecker.shouldEndInput(snapshotManager.snapshot(currentSnapshotId));
        } else if (result instanceof StartingScanner.NextSnapshot) {
            nextSnapshotId = ((StartingScanner.NextSnapshot) result).nextSnapshotId();
            isFullPhaseEnd =
                    snapshotManager.snapshotExists(nextSnapshotId - 1)
                            && boundedChecker.shouldEndInput(
                                    snapshotManager.snapshot(nextSnapshotId - 1));
        }
        return DataFilePlan.fromResult(result);
    }

    private Plan nextPlan() {
        while (true) {
            if (isFullPhaseEnd) {
                throw new EndOfScanException();
            }

            if (!snapshotManager.snapshotExists(nextSnapshotId)) {
                LOG.debug(
                        "Next snapshot id {} does not exist, wait for the snapshot generation.",
                        nextSnapshotId);
                return new DataFilePlan(Collections.emptyList());
            }

            Snapshot snapshot = snapshotManager.snapshot(nextSnapshotId);

            if (boundedChecker.shouldEndInput(snapshot)) {
                throw new EndOfScanException();
            }

            // first check changes of overwrite
            if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE
                    && supportStreamingReadOverwrite) {
                LOG.debug("Find overwrite snapshot id {}.", nextSnapshotId);
                Plan overwritePlan =
                        followUpScanner.getOverwriteChangesPlan(
                                nextSnapshotId, snapshotSplitReader);
                nextSnapshotId++;
                return overwritePlan;
            } else if (followUpScanner.shouldScanSnapshot(snapshot)) {
                LOG.debug("Find snapshot id {}.", nextSnapshotId);
                Plan plan = followUpScanner.scan(nextSnapshotId, snapshotSplitReader);
                nextSnapshotId++;
                return plan;
            } else {
                nextSnapshotId++;
            }
        }
    }

    private FollowUpScanner createFollowUpScanner() {
        if (options.toConfiguration().get(CoreOptions.STREAMING_COMPACT)) {
            return new ContinuousCompactorFollowUpScanner();
        }

        CoreOptions.ChangelogProducer changelogProducer = options.changelogProducer();
        FollowUpScanner followUpScanner;
        switch (changelogProducer) {
            case NONE:
                followUpScanner = new DeltaFollowUpScanner();
                break;
            case INPUT:
                followUpScanner = new InputChangelogFollowUpScanner();
                break;
            case FULL_COMPACTION:
                // this change in data split reader will affect both starting scanner and follow-up
                snapshotSplitReader.withLevelFilter(level -> level == options.numLevels() - 1);
                followUpScanner = new CompactionChangelogFollowUpScanner();
                break;
            case LOOKUP:
                // this change in data split reader will affect both starting scanner and follow-up
                snapshotSplitReader.withLevelFilter(level -> level > 0);
                followUpScanner = new CompactionChangelogFollowUpScanner();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown changelog producer " + changelogProducer.name());
        }
        return followUpScanner;
    }

    private BoundedChecker createBoundedChecker() {
        Long boundedWatermark = options.scanBoundedWatermark();
        return boundedWatermark != null
                ? BoundedChecker.watermark(boundedWatermark)
                : BoundedChecker.neverEnd();
    }

    @Nullable
    @Override
    public Long checkpoint() {
        return nextSnapshotId;
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
    }

    @Override
    public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
        if (nextSnapshot == null) {
            return;
        }

        String consumerId = options.consumerId();
        if (consumerId != null) {
            snapshotSplitReader
                    .consumerManager()
                    .recordConsumer(consumerId, new Consumer(nextSnapshot));
        }
    }
}
