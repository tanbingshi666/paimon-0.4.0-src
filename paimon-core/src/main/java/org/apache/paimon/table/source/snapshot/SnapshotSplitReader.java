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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.operation.ScanKind;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.Filter;

import java.util.List;

/** Read splits from specified {@link Snapshot} with given configuration. */
public interface SnapshotSplitReader {

    ConsumerManager consumerManager();

    SnapshotSplitReader withSnapshot(long snapshotId);

    SnapshotSplitReader withFilter(Predicate predicate);

    SnapshotSplitReader withKind(ScanKind scanKind);

    SnapshotSplitReader withLevelFilter(Filter<Integer> levelFilter);

    SnapshotSplitReader withBucket(int bucket);

    /** Get splits from snapshot. */
    List<DataSplit> splits();

    /** Get splits from an overwrite snapshot. */
    List<DataSplit> overwriteSplits();
}
