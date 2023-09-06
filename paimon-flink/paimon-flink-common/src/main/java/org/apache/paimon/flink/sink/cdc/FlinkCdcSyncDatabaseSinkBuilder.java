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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.sink.BucketingStreamPartitioner;
import org.apache.paimon.flink.utils.SingleOutputStreamOperatorUtils;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for {@link FlinkCdcSink} when syncing the whole database into one Paimon database. Each
 * database table will be written into a separate Paimon table.
 *
 * <p>This builder will create a separate sink for each Paimon sink table. Thus this implementation
 * is not very efficient in resource saving.
 *
 * @param <T> CDC change event type
 */
public class FlinkCdcSyncDatabaseSinkBuilder<T> {

    private DataStream<T> input = null;
    private EventParser.Factory<T> parserFactory = null;
    private List<FileStoreTable> tables = new ArrayList<>();
    private Lock.Factory lockFactory = Lock.emptyFactory();

    @Nullable
    private Integer parallelism;

    public FlinkCdcSyncDatabaseSinkBuilder<T> withInput(DataStream<T> input) {
        this.input = input;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withParserFactory(
            EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withTables(List<FileStoreTable> tables) {
        this.tables = tables;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withLockFactory(Lock.Factory lockFactory) {
        this.lockFactory = lockFactory;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(input);
        Preconditions.checkNotNull(parserFactory);

        StreamExecutionEnvironment env = input.getExecutionEnvironment();

        /**
         * 实时读取 mysql 整库数据并根据表名进行侧输出流输出
         * 表数据和表 schema change 都是通过侧输出流输出到下游
         */
        SingleOutputStreamOperator<Void> parsed =
                input.forward()
                        .process(new CdcMultiTableParsingProcessFunction<>(parserFactory))
                        .setParallelism(input.getParallelism());

        /**
         * 处理 mysql 数据
         */
        for (FileStoreTable table : tables) {
            /**
             * 处理表 schema change 变化
             */
            DataStream<Void> schemaChangeProcessFunction =
                    SingleOutputStreamOperatorUtils.getSideOutput(
                                    parsed,
                                    CdcMultiTableParsingProcessFunction
                                            .createUpdatedDataFieldsOutputTag(table.name()))
                            .process(
                                    new UpdatedDataFieldsProcessFunction(
                                            new SchemaManager(table.fileIO(), table.location())));
            schemaChangeProcessFunction.getTransformation().setParallelism(1);
            schemaChangeProcessFunction.getTransformation().setMaxParallelism(1);

            /**
             * 处理表数据
             */
            BucketingStreamPartitioner<CdcRecord> partitioner =
                    new BucketingStreamPartitioner<>(new CdcRecordChannelComputer(table.schema()));
            PartitionTransformation<CdcRecord> partitioned =
                    new PartitionTransformation<>(
                            SingleOutputStreamOperatorUtils.getSideOutput(
                                            parsed,
                                            CdcMultiTableParsingProcessFunction
                                                    .createRecordOutputTag(table.name()))
                                    .getTransformation(),
                            partitioner);
            if (parallelism != null) {
                partitioned.setParallelism(parallelism);
            }

            /**
             * 拼接 pipeline
             */
            FlinkCdcSink sink = new FlinkCdcSink(table, lockFactory);
            sink.sinkFrom(new DataStream<>(env, partitioned));
        }
    }
}
