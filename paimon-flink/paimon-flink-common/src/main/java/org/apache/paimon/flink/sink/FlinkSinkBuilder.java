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

package org.apache.paimon.flink.sink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Builder for {@link FileStoreSink}.
 */
public class FlinkSinkBuilder {

    private final FileStoreTable table;

    private DataStream<RowData> input;
    private Lock.Factory lockFactory = Lock.emptyFactory();
    @Nullable
    private Map<String, String> overwritePartition;
    @Nullable
    private LogSinkFunction logSinkFunction;
    @Nullable
    private Integer parallelism;
    @Nullable
    private String commitUser;
    @Nullable
    private StoreSinkWrite.Provider sinkProvider;

    public FlinkSinkBuilder(FileStoreTable table) {
        // Append-Only -> AppendOnlyFileStoreTable
        // Primary-Key -> ChangelogWithKeyFileStoreTable
        this.table = table;
    }

    public FlinkSinkBuilder withInput(DataStream<RowData> input) {
        // 上游输入 DataStream
        this.input = input;
        return this;
    }

    public FlinkSinkBuilder withLockFactory(Lock.Factory lockFactory) {
        // 默认 EmptyFactory
        this.lockFactory = lockFactory;
        return this;
    }

    public FlinkSinkBuilder withOverwritePartition(Map<String, String> overwritePartition) {
        // 默认 null
        this.overwritePartition = overwritePartition;
        return this;
    }

    public FlinkSinkBuilder withLogSinkFunction(@Nullable LogSinkFunction logSinkFunction) {
        // 默认 null
        this.logSinkFunction = logSinkFunction;
        return this;
    }

    public FlinkSinkBuilder withParallelism(@Nullable Integer parallelism) {
        // Sink 并行度 默认根据 flink 全局并行度而言
        // 可以配置 sink.parallelism = x
        this.parallelism = parallelism;
        return this;
    }

    @VisibleForTesting
    public FlinkSinkBuilder withSinkProvider(
            String commitUser, StoreSinkWrite.Provider sinkProvider) {
        this.commitUser = commitUser;
        this.sinkProvider = sinkProvider;
        return this;
    }

    public DataStreamSink<?> build() {
        // 1 分区器 也即将相同 bucket 的数据发送给 Sink
        BucketingStreamPartitioner<RowData> partitioner =
                new BucketingStreamPartitioner<>(
                        // 根据分区+桶计算上游数据发送到 Sink 哪个 subtask
                        // 计算逻辑如下：
                        // 一条记录的分区 ( abs(hashcode % Sink 并行度) + 分桶值 ) % Sink 并行度
                        // 得到下游 Sink subtask channel 的下标
                        new RowDataChannelComputer(table.schema(), logSinkFunction != null));

        // 2 添加分区算子
        PartitionTransformation<RowData> partitioned =
                new PartitionTransformation<>(input.getTransformation(), partitioner);

        // 3 如果并行度存在 则设置
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }

        StreamExecutionEnvironment env = input.getExecutionEnvironment();

        // 4 创建 FinkSink
        FileStoreSink sink =
                new FileStoreSink(table, lockFactory, overwritePartition, logSinkFunction);
        return commitUser != null && sinkProvider != null
                ? sink.sinkFrom(new DataStream<>(env, partitioned), commitUser, sinkProvider)
                // 5 将 FlinkSink 添加到 pipeline
                : sink.sinkFrom(new DataStream<>(env, partitioned));
    }
}
