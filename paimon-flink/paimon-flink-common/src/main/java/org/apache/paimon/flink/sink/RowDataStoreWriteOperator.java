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

import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.log.LogWriteCallback;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * A {@link PrepareCommitOperator} to write {@link RowData}. Record schema is fixed.
 */
public class RowDataStoreWriteOperator extends PrepareCommitOperator<RowData> {

    private static final long serialVersionUID = 3L;

    private final FileStoreTable table;
    @Nullable
    private final LogSinkFunction logSinkFunction;
    private final StoreSinkWrite.Provider storeSinkWriteProvider;
    private final String initialCommitUser;

    private transient StoreSinkWriteState state;
    private transient StoreSinkWrite write;
    private transient SimpleContext sinkContext;
    @Nullable
    private transient LogWriteCallback logCallback;

    /**
     * We listen to this ourselves because we don't have an {@link InternalTimerService}.
     */
    private long currentWatermark = Long.MIN_VALUE;

    public RowDataStoreWriteOperator(
            FileStoreTable table,
            @Nullable LogSinkFunction logSinkFunction,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        // 表
        this.table = table;
        this.logSinkFunction = logSinkFunction;
        // Writer 提供器
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.initialCommitUser = initialCommitUser;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Committable>> output) {
        super.setup(containingTask, config, output);
        if (logSinkFunction != null) {
            FunctionUtils.setFunctionRuntimeContext(logSinkFunction, getRuntimeContext());
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // Each job can only have one user name and this name must be consistent across restarts.
        // We cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        // 1 获取 commitUser
        String commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        // 2 创建 RowDataChannelComputer
        RowDataChannelComputer channelComputer =
                new RowDataChannelComputer(table.schema(), logSinkFunction != null);

        // 3 根据并行度设置 channel 个数
        channelComputer.setup(getRuntimeContext().getNumberOfParallelSubtasks());

        // 4 创建 StoreSinkWriteState
        state =
                new StoreSinkWriteState(
                        context,
                        (tableName, partition, bucket) ->
                                channelComputer.channel(partition, bucket)
                                        == getRuntimeContext().getIndexOfThisSubtask());

        // 5 创建 Writer 器
        // 有两种类型：GlobalFullCompactionSinkWrite、StoreSinkWriteImpl
        write =
                storeSinkWriteProvider.provide(
                        table,
                        commitUser,
                        state,
                        getContainingTask().getEnvironment().getIOManager());

        if (logSinkFunction != null) {
            StreamingFunctionUtils.restoreFunctionState(context, logSinkFunction);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.sinkContext = new SimpleContext(getProcessingTimeService());
        if (logSinkFunction != null) {
            FunctionUtils.openFunction(logSinkFunction, new Configuration());
            logCallback = new LogWriteCallback();
            logSinkFunction.setWriteCallback(logCallback);
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);

        this.currentWatermark = mark.getTimestamp();
        if (logSinkFunction != null) {
            logSinkFunction.writeWatermark(
                    new org.apache.flink.api.common.eventtime.Watermark(mark.getTimestamp()));
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        sinkContext.timestamp = element.hasTimestamp() ? element.getTimestamp() : null;

        SinkRecord record;
        try {
            // 接收上游发送过来的数据进行写操作
            // 如果表配置了 full-compact 则调用 GlobalFullCompactionSinkWrite.write()
            // 否则调用 StoreSinkWriteImpl.write()
            record = write.write(new FlinkRowWrapper(element.getValue()));
        } catch (Exception e) {
            throw new IOException(e);
        }

        // 如果存在 log.system 则双写
        if (logSinkFunction != null) {
            // write to log store, need to preserve original pk (which includes partition fields)
            SinkRecord logRecord = write.toLogRecord(record);
            logSinkFunction.invoke(logRecord, sinkContext);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        write.snapshotState();
        state.snapshotState();

        if (logSinkFunction != null) {
            StreamingFunctionUtils.snapshotFunctionState(
                    context, getOperatorStateBackend(), logSinkFunction);
        }
    }

    @Override
    public void finish() throws Exception {
        super.finish();

        if (logSinkFunction != null) {
            logSinkFunction.finish();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        write.close();
        if (logSinkFunction != null) {
            FunctionUtils.closeFunction(logSinkFunction);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        if (logSinkFunction instanceof CheckpointListener) {
            ((CheckpointListener) logSinkFunction).notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        if (logSinkFunction instanceof CheckpointListener) {
            ((CheckpointListener) logSinkFunction).notifyCheckpointAborted(checkpointId);
        }
    }

    @Override
    protected List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = write.prepareCommit(doCompaction, checkpointId);

        if (logCallback != null) {
            try {
                logSinkFunction.flush();
            } catch (Exception e) {
                throw new IOException(e);
            }
            logCallback
                    .offsets()
                    .forEach(
                            (k, v) ->
                                    committables.add(
                                            new Committable(
                                                    checkpointId,
                                                    Committable.Kind.LOG_OFFSET,
                                                    new LogOffsetCommittable(k, v))));
        }

        return committables;
    }

    private class SimpleContext implements SinkFunction.Context {

        @Nullable
        private Long timestamp;

        private final ProcessingTimeService processingTimeService;

        public SimpleContext(ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        @Override
        public long currentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public Long timestamp() {
            return timestamp;
        }
    }
}
