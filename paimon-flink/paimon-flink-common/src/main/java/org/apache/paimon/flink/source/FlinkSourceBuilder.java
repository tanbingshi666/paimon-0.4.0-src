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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.CoreOptions.StreamingReadMode;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.Projection;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.paimon.flink.source.operator.MonitorFunction;
import org.apache.paimon.flink.utils.TableScanUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.StreamingReadMode.FILE;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;

/**
 * Source builder to build a Flink {@link StaticFileStoreSource} or {@link
 * ContinuousFileStoreSource}. This is for normal read/write jobs.
 */
public class FlinkSourceBuilder {

    private final ObjectIdentifier tableIdentifier;
    private final Table table;
    private final Options conf;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;
    @Nullable
    private int[][] projectedFields;
    @Nullable
    private Predicate predicate;
    @Nullable
    private LogSourceProvider logSourceProvider;
    @Nullable
    private Integer parallelism;
    @Nullable
    private Long limit;
    @Nullable
    private WatermarkStrategy<RowData> watermarkStrategy;
    @Nullable
    private List<Split> splits;

    public FlinkSourceBuilder(ObjectIdentifier tableIdentifier, Table table) {
        // 表的唯一标识符
        this.tableIdentifier = tableIdentifier;
        // 表
        this.table = table;
        // 表属性选项
        this.conf = Options.fromMap(table.options());
    }

    public FlinkSourceBuilder withContinuousMode(boolean isContinuous) {
        // 是否为流式
        this.isContinuous = isContinuous;
        return this;
    }

    public FlinkSourceBuilder withEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public FlinkSourceBuilder withProjection(int[][] projectedFields) {
        // 一般情况下为 null
        this.projectedFields = projectedFields;
        return this;
    }

    public FlinkSourceBuilder withPredicate(Predicate predicate) {
        // 一般情况下为 null
        this.predicate = predicate;
        return this;
    }

    public FlinkSourceBuilder withLimit(@Nullable Long limit) {
        // 一般情况下为 null
        this.limit = limit;
        return this;
    }

    public FlinkSourceBuilder withLogSourceProvider(LogSourceProvider logSourceProvider) {
        // 一般情况下为 null
        this.logSourceProvider = logSourceProvider;
        return this;
    }

    public FlinkSourceBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public FlinkSourceBuilder withWatermarkStrategy(
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        // 如果表定义了 watermark
        this.watermarkStrategy = watermarkStrategy;
        return this;
    }

    public FlinkSourceBuilder withSplits(List<Split> splits) {
        this.splits = splits;
        return this;
    }

    private ReadBuilder createReadBuilder() {
        // 构建 ReadBuilder
        return table.newReadBuilder()
                .withProjection(projectedFields)
                .withFilter(predicate);
    }

    private DataStream<RowData> buildStaticFileSource() {
        return toDataStream(
                new StaticFileStoreSource(
                        createReadBuilder(),
                        limit,
                        Options.fromMap(table.options())
                                .get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE),
                        splits));
    }

    private DataStream<RowData> buildContinuousFileSource() {
        // 创建 Source DataStream 读取 Paimon 表数据
        return toDataStream(
                // 创建 ContinuousFileStoreSource
                new ContinuousFileStoreSource(
                        // 创建 ReadBuilderImpl
                        createReadBuilder(),
                        table.options(),
                        limit));
    }

    private DataStream<RowData> toDataStream(
            // ContinuousFileStoreSource
            Source<RowData, ?, ?> source) {
        // 创建 Source DataStream
        DataStreamSource<RowData> dataStream =
                env.fromSource(
                        // SourceFunction
                        source,
                        // watermark 生成策略
                        watermarkStrategy == null
                                ? WatermarkStrategy.noWatermarks()
                                : watermarkStrategy,
                        // Source Name
                        tableIdentifier.asSummaryString(),
                        // Source 输出数据类型
                        produceTypeInfo());

        // 如果存在并行度 则设置
        if (parallelism != null) {
            dataStream.setParallelism(parallelism);
        }
        return dataStream;
    }

    private TypeInformation<RowData> produceTypeInfo() {
        RowType rowType = toLogicalType(table.rowType());
        LogicalType produceType =
                Optional.ofNullable(projectedFields)
                        .map(Projection::of)
                        .map(p -> p.project(rowType))
                        .orElse(rowType);
        return InternalTypeInfo.of(produceType);
    }

    public DataStream<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }

        // 1 判断执行 Flink 任务为流式还是批次
        if (isContinuous) {

            // 2 校验表流读模式是否合法
            // 也即校验 Primary-Key表的 merge-engine 和 changelog-producer 是否合法
            TableScanUtils.streamingReadingValidate(table);

            // TODO visit all options through CoreOptions
            // 3 获取读取表数据模式 key = scan.mode 默认 value = default (全增量一体 LATEST_FULL)
            // 还可以配置其他读取模式
            StartupMode startupMode = CoreOptions.startupMode(conf);

            // 4 获取流读模式 key = streaming-read-mode 默认 value = null
            // 有两种情况：FILE、LOG
            StreamingReadMode streamingReadMode = CoreOptions.streamReadType(conf);

            // 这种情况一般是读取 log system 数据 比如 kafka
            if (logSourceProvider != null && streamingReadMode != FILE) {
                if (startupMode != StartupMode.LATEST_FULL) {
                    return toDataStream(logSourceProvider.createSource(null));
                } else {
                    return toDataStream(
                            HybridSource.<RowData, StaticFileStoreSplitEnumerator>builder(
                                            LogHybridSourceFactory.buildHybridFirstSource(
                                                    table, projectedFields, predicate))
                                    .addSource(
                                            new LogHybridSourceFactory(logSourceProvider),
                                            Boundedness.CONTINUOUS_UNBOUNDED)
                                    .build());
                }
            } else {
                // 5 判断读取数据的时候是否添加了 consumer-id hint 选项
                if (conf.contains(CoreOptions.CONSUMER_ID)) {
                    // 5.1 有 consumer-id 情况下读取表数据
                    return buildContinuousStreamOperator();
                } else {
                    // 5.2 没有 consumer-id 情况下读取表数据
                    return buildContinuousFileSource();
                }
            }
        } else {
            return buildStaticFileSource();
        }
    }

    private DataStream<RowData> buildContinuousStreamOperator() {
        DataStream<RowData> dataStream;
        if (limit != null) {
            throw new IllegalArgumentException(
                    "Cannot limit streaming source, please use batch execution mode.");
        }
        // 构建 Source DataStream
        dataStream =
                MonitorFunction.buildSource(
                        // 流式执行环境 StreamExecutionEnvironment
                        env,
                        // Source 名字
                        tableIdentifier.asSummaryString(),
                        // Source 输出类型
                        produceTypeInfo(),
                        // 读取数据器 ReadBuilderImpl
                        createReadBuilder(),
                        // key = continuous.discovery-interval 默认 value = 10s
                        conf.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis());

        // 并行度不为 null
        if (parallelism != null) {
            dataStream.getTransformation().setParallelism(parallelism);
        }

        // 设置 Source watermark 生成策略
        if (watermarkStrategy != null) {
            dataStream = dataStream.assignTimestampsAndWatermarks(watermarkStrategy);
        }

        // 返回 Source DataStream
        return dataStream;
    }
}
