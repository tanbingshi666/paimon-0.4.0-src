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
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;

import javax.annotation.Nullable;

/**
 * Builder for {@link FlinkCdcSink} when syncing multiple database tables into one Paimon table.
 *
 * @param <T> CDC change event type
 */
public class FlinkCdcSyncTableSinkBuilder<T> {

    private DataStream<T> input = null;
    private EventParser.Factory<T> parserFactory = null;
    private FileStoreTable table = null;
    private Lock.Factory lockFactory = Lock.emptyFactory();

    @Nullable private Integer parallelism;

    public FlinkCdcSyncTableSinkBuilder<T> withInput(DataStream<T> input) {
        this.input = input;
        return this;
    }

    public FlinkCdcSyncTableSinkBuilder<T> withParserFactory(EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
        return this;
    }

    public FlinkCdcSyncTableSinkBuilder<T> withTable(FileStoreTable table) {
        this.table = table;
        return this;
    }

    public FlinkCdcSyncTableSinkBuilder<T> withLockFactory(Lock.Factory lockFactory) {
        this.lockFactory = lockFactory;
        return this;
    }

    public FlinkCdcSyncTableSinkBuilder<T> withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public DataStreamSink<?> build() {
        Preconditions.checkNotNull(input);
        Preconditions.checkNotNull(parserFactory);
        Preconditions.checkNotNull(table);

        SingleOutputStreamOperator<CdcRecord> parsed =
                input.forward()
                        .process(new CdcParsingProcessFunction<>(parserFactory))
                        .setParallelism(input.getParallelism());

        DataStream<Void> schemaChangeProcessFunction =
                SingleOutputStreamOperatorUtils.getSideOutput(
                                parsed, CdcParsingProcessFunction.NEW_DATA_FIELD_LIST_OUTPUT_TAG)
                        .process(
                                new UpdatedDataFieldsProcessFunction(
                                        new SchemaManager(table.fileIO(), table.location())));
        schemaChangeProcessFunction.getTransformation().setParallelism(1);
        schemaChangeProcessFunction.getTransformation().setMaxParallelism(1);

        BucketingStreamPartitioner<CdcRecord> partitioner =
                new BucketingStreamPartitioner<>(new CdcRecordChannelComputer(table.schema()));
        PartitionTransformation<CdcRecord> partitioned =
                new PartitionTransformation<>(parsed.getTransformation(), partitioner);
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }

        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        FlinkCdcSink sink = new FlinkCdcSink(table, lockFactory);
        return sink.sinkFrom(new DataStream<>(env, partitioned));
    }
}
