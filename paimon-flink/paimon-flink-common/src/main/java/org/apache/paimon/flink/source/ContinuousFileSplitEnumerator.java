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

package org.apache.paimon.flink.source;

import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A continuously monitoring enumerator.
 */
public class ContinuousFileSplitEnumerator
        implements SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileSplitEnumerator.class);

    private final SplitEnumeratorContext<FileStoreSourceSplit> context;

    private final Map<Integer, LinkedList<FileStoreSourceSplit>> bucketSplits;

    private final long discoveryInterval;

    private final Set<Integer> readersAwaitingSplit;

    private final FileStoreSourceSplitGenerator splitGenerator;

    private final StreamTableScan scan;

    /**
     * Default batch splits size to avoid exceed `akka.framesize`.
     */
    private final int splitBatchSize;

    @Nullable
    private Long nextSnapshotId;

    private boolean finished = false;

    public ContinuousFileSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> remainSplits,
            @Nullable Long nextSnapshotId,
            long discoveryInterval,
            int splitBatchSize,
            StreamTableScan scan) {
        checkArgument(discoveryInterval > 0L);
        this.context = checkNotNull(context);
        this.bucketSplits = new HashMap<>();
        // 如果使用上一次 checkpoint 那么可能存在 split
        addSplits(remainSplits);
        this.nextSnapshotId = nextSnapshotId;
        // 动态发现新文件间隔
        this.discoveryInterval = discoveryInterval;
        // 批处理情况下分配多少个 split 给 subtask 默认 10
        this.splitBatchSize = splitBatchSize;
        this.readersAwaitingSplit = new HashSet<>();
        // split 文件生成器
        this.splitGenerator = new FileStoreSourceSplitGenerator();
        // InnerStreamTableScanImpl
        this.scan = scan;
    }

    private void addSplits(Collection<FileStoreSourceSplit> splits) {
        splits.forEach(this::addSplit);
    }

    private void addSplit(FileStoreSourceSplit split) {
        bucketSplits
                .computeIfAbsent(((DataSplit) split.split()).bucket(), i -> new LinkedList<>())
                .add(split);
    }

    private void addSplitsBack(Collection<FileStoreSourceSplit> splits) {
        new LinkedList<>(splits).descendingIterator().forEachRemaining(this::addSplitToHead);
    }

    private void addSplitToHead(FileStoreSourceSplit split) {
        bucketSplits
                .computeIfAbsent(((DataSplit) split.split()).bucket(), i -> new LinkedList<>())
                .addFirst(split);
    }

    @Override
    public void start() {
        // 启动 split enumerator
        context.callAsync(
                // 1 进行数据切片 返回 DataFilePlan
                scan::plan,
                // 2 每隔一定时间动态发现新文件是否生成 如果发现新文件则生成 split
                this::processDiscoveredSplits, 0, discoveryInterval);
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
        // 添加 SourceReader 也即 SourceReader 注册操作
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // 处理 SourceReader 发送请求 split 并分配 split
        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        LOG.error("Received unrecognized event: {}", sourceEvent);
        // SourceEvents 是 SplitEnumerator 和 SourceReader 之间来回传递的自定义事件
        // 可以利用此机制来执行复杂的协调任务
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> splits, int subtaskId) {
        LOG.debug("File Source Enumerator adds splits back: {}", splits);
        // SourceReader 失败时会调用 addSplitsBack() 方法
        // SplitEnumerator 应当收回已经被分配，但尚未被该 SourceReader 确认(acknowledged)的分片
        addSplitsBack(splits);
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) {
        // 执行 source checkpoint
        List<FileStoreSourceSplit> splits = new ArrayList<>();
        bucketSplits.values().forEach(splits::addAll);
        final PendingSplitsCheckpoint checkpoint =
                new PendingSplitsCheckpoint(splits, nextSnapshotId);

        LOG.debug("Source Checkpoint is {}", checkpoint);
        return checkpoint;
    }

    // ------------------------------------------------------------------------

    private void processDiscoveredSplits(TableScan.Plan plan, Throwable error) {
        if (error != null) {
            if (error instanceof EndOfScanException) {
                // finished
                LOG.debug("Catching EndOfStreamException, the stream is finished.");
                finished = true;
                assignSplits();
            } else {
                LOG.error("Failed to enumerate files", error);
            }
            return;
        }

        nextSnapshotId = scan.checkpoint();

        if (plan.splits().isEmpty()) {
            return;
        }

        // 1 创建切片信息 (已经切片好了 存储在 Plan)
        // 也即往 bucketSplits (按 bucket 聚合) 添加
        addSplits(
                // 将 DataSplit 转化为 FileStoreSourceSplit
                splitGenerator.createSplits(plan)
        );
        // 2 执行切片分配
        assignSplits();
    }

    private void assignSplits() {
        // 1 执行分配切片信息
        // 默认情况下 一个 SourceReader subtask 被分配 10 split 切片进行读取
        Map<Integer, List<FileStoreSourceSplit>> assignment = createAssignment();

        if (finished) {
            Iterator<Integer> iterator = readersAwaitingSplit.iterator();
            while (iterator.hasNext()) {
                Integer reader = iterator.next();
                if (!assignment.containsKey(reader)) {
                    context.signalNoMoreSplits(reader);
                    iterator.remove();
                }
            }
        }
        // 2 移除已经分配好的 SourceReader
        assignment.keySet().forEach(readersAwaitingSplit::remove);

        // 2 SplitEnumerator 接收到切片 后续给下游的 SourceReader 分配切片读取数据
        context.assignSplits(new SplitsAssignment<>(assignment));
    }

    private Map<Integer, List<FileStoreSourceSplit>> createAssignment() {
        Map<Integer, List<FileStoreSourceSplit>> assignment = new HashMap<>();

        // 1 bucketSplits 维护了每个桶下的所有切片信息
        bucketSplits.forEach(
                (bucket, splits) -> {
                    if (splits.size() > 0) {
                        // To ensure the order of consumption, the data of the same bucket is given
                        // to a task to be consumed.
                        // 2 计算 SourceReader subtask 任务下标索引
                        // 桶索引 % SourceReader 并行度 = SourceReader subtask 任务下标索引
                        int task = bucket % context.currentParallelism();

                        // 3 判断 SourceReader 是否已经向 SplitEnumerator 注册
                        if (readersAwaitingSplit.contains(task)) {
                            // if the reader that requested another split has failed in the
                            // meantime, remove
                            // it from the list of waiting readers
                            // 如果 SourceReader 请求另一个 Split 已经失败了 则 SplitEnumerator 移除对应的 SourceReader
                            if (!context.registeredReaders().containsKey(task)) {
                                readersAwaitingSplit.remove(task);
                                return;
                            }

                            // 4 默认情况下 一个 SourceReader 被分配 10 split 切片进行读取
                            List<FileStoreSourceSplit> taskAssignment =
                                    assignment.computeIfAbsent(task, i -> new ArrayList<>());
                            if (taskAssignment.size() < splitBatchSize) {
                                // 5 从缓存弹出 split
                                taskAssignment.add(splits.poll());
                            }
                        }
                    }
                });
        return assignment;
    }
}
