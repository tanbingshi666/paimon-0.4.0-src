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

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import java.io.Serializable;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_LOOKUP_WAIT;

/**
 * Abstract sink of paimon.
 */
public abstract class FlinkSink<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    protected final FileStoreTable table;
    private final boolean isOverwrite;

    public FlinkSink(FileStoreTable table, boolean isOverwrite) {
        // Append-Only -> AppendOnlyFileStoreTable
        // Primary-Key -> ChangelogWithKeyFileStoreTable
        this.table = table;
        // 默认 false
        this.isOverwrite = isOverwrite;
    }

    private StoreSinkWrite.Provider createWriteProvider(CheckpointConfig checkpointConfig) {
        boolean waitCompaction;
        // 1 默认 write-only = false
        // 如果配置为 true 则 Sink Writer 会跳过 compact 和 snapshot 过期检测
        if (table.coreOptions().writeOnly()) {
            waitCompaction = false;
        } else {
            Options options = table.coreOptions().toConfiguration();

            // 2 获取 Sink 表 changelog-producer
            ChangelogProducer changelogProducer = table.coreOptions().changelogProducer();
            // 2.1 如果配置 changelog-producer = lookup 则每次 checkpoint的时候都会进行 compact
            // 也即 waitCompaction = true
            waitCompaction =
                    changelogProducer == ChangelogProducer.LOOKUP
                            && options.get(CHANGELOG_PRODUCER_LOOKUP_WAIT);

            int deltaCommits = -1;
            // 2.2 获取 full-compaction.delta-commits 对应的值 默认 null
            if (options.contains(FULL_COMPACTION_DELTA_COMMITS)) {
                deltaCommits = options.get(FULL_COMPACTION_DELTA_COMMITS);
            } else if (options.contains(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL)) {
                long fullCompactionThresholdMs =
                        options.get(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL).toMillis();
                deltaCommits =
                        (int)
                                (fullCompactionThresholdMs
                                        / checkpointConfig.getCheckpointInterval());
            }

            // 2.3 如果 Sink 表配置了 changelog-producer = full-compaction
            if (changelogProducer == ChangelogProducer.FULL_COMPACTION || deltaCommits >= 0) {
                // 获取多少次 checkpoint 执行 full-compact 默认 1
                int finalDeltaCommits = Math.max(deltaCommits, 1);
                return (table, commitUser, state, ioManager) ->
                        // 2.4 创建全局 full-compact SinkWriter
                        new GlobalFullCompactionSinkWrite(
                                table,
                                commitUser,
                                state,
                                ioManager,
                                isOverwrite,
                                waitCompaction,
                                finalDeltaCommits);
            }
        }

        // 3 针对的是 Append-Only 表创建 StoreSinkWriteImpl
        return (table, commitUser, state, ioManager) ->
                new StoreSinkWriteImpl(
                        table, commitUser, state, ioManager, isOverwrite, waitCompaction);
    }

    public DataStreamSink<?> sinkFrom(DataStream<T> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        // 1 对于新任务 commitUser 使用 UUID 生成 后续该 commitUser 保存到状态中 以便后续重启使用
        String initialCommitUser = UUID.randomUUID().toString();

        // 3 Sink
        return sinkFrom(
                input,
                initialCommitUser,
                // 2 创建 WriteProvider (GlobalFullCompactionSinkWrite or StoreSinkWriteImpl)
                createWriteProvider(input.getExecutionEnvironment().getCheckpointConfig()));
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<T> input, String commitUser, StoreSinkWrite.Provider sinkProvider) {
        // 1 获取流式执行环境
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        // 2 获取 pipeline 作用任务
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        // 3 获取 checkpoint 配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 4 判断是否为流式作用
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        // 5 判断是否开启 checkpoint
        boolean streamingCheckpointEnabled =
                isStreaming && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            assertCheckpointConfiguration(env);
        }

        // 6 创建 commit 信息类型
        CommittableTypeInfo typeInfo = new CommittableTypeInfo();
        // 7 RowDataStoreWriteOperator 算子进行数据写磁盘
        SingleOutputStreamOperator<Committable> written =
                input.transform(
                                WRITER_NAME + " -> " + table.name(),
                                typeInfo,
                                // 7.1 创建 RowDataStoreWriteOperator
                                // 这个 RowDataStoreWriteOperator 真正将数据写入磁盘文件
                                // 这个 RowDataStoreWriteOperator 很核心和重要
                                createWriteOperator(sinkProvider, isStreaming, commitUser))
                        // 7.2 设置数据写入并行度
                        .setParallelism(input.getParallelism());

        // 8 将上游 SinkWriter 发送过来的 Committer 信息进行 checkpoint
        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME + " -> " + table.name(),
                                typeInfo,
                                // 8.1 创建 CommitterOperator 算子
                                new CommitterOperator(
                                        streamingCheckpointEnabled,
                                        commitUser,
                                        // 创建 StoreCommitter 用来定时检测哪些 snapshot 或者 partition 过期清理
                                        createCommitterFactory(streamingCheckpointEnabled),
                                        // 创建 RestoreAndFailCommittableStateManager
                                        createCommittableStateManager()))
                        // 8.2 CommitterOperator 算子的并行度为 1
                        .setParallelism(1)
                        .setMaxParallelism(1);
        // 9 完成整个 pipeline 的构建
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    private void assertCheckpointConfiguration(StreamExecutionEnvironment env) {
        Preconditions.checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "Paimon sink currently does not support unaligned checkpoints. Please set "
                        + ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key()
                        + " to false.");
        Preconditions.checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "Paimon sink currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()
                        + " to exactly-once");
    }

    protected abstract OneInputStreamOperator<T, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, boolean isStreaming, String commitUser);

    protected abstract SerializableFunction<String, Committer> createCommitterFactory(
            boolean streamingCheckpointEnabled);

    protected abstract CommittableStateManager createCommittableStateManager();
}
