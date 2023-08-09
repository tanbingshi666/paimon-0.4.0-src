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
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkConnectorOptions.WatermarkEmitStrategy;
import org.apache.paimon.flink.PaimonDataStreamScanProvider;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.flink.lookup.FileStoreLookupFunction;
import org.apache.paimon.flink.lookup.LookupRuntimeProviderFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.ChangelogValueCountFileStoreTable;
import org.apache.paimon.table.ChangelogWithKeyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Projection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.LOG_CONSISTENCY;
import static org.apache.paimon.CoreOptions.LOG_SCAN_REMOVE_NORMALIZE;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_GROUP;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_EMIT_STRATEGY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_WATERMARK_IDLE_TIMEOUT;

/**
 * Table source to create {@link StaticFileStoreSource} or {@link ContinuousFileStoreSource} under
 * batch mode or change-tracking is disabled. For streaming mode with change-tracking enabled and
 * FULL scan mode, it will create a {@link
 * org.apache.flink.connector.base.source.hybrid.HybridSource} of {@code
 * LogHybridSourceFactory.FlinkHybridFirstSource} and kafka log source created by {@link
 * LogSourceProvider}.
 */
public class DataTableSource extends FlinkTableSource
        implements LookupTableSource, SupportsWatermarkPushDown {

    private final ObjectIdentifier tableIdentifier;
    private final boolean streaming;
    private final DynamicTableFactory.Context context;
    @Nullable
    private final LogStoreTableFactory logStoreTableFactory;

    @Nullable
    private WatermarkStrategy<RowData> watermarkStrategy;

    public DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        // 往下追
        this(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                null,
                null,
                null,
                null);
    }

    private DataTableSource(
            ObjectIdentifier tableIdentifier,
            Table table,
            boolean streaming,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory,
            @Nullable Predicate predicate,
            @Nullable int[][] projectFields,
            @Nullable Long limit,
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        super(table, predicate, projectFields, limit);
        // 表的标识符
        this.tableIdentifier = tableIdentifier;
        // 是否为流式
        this.streaming = streaming;
        // 动态表上下文
        this.context = context;
        // 一般情况下为 null
        this.logStoreTableFactory = logStoreTableFactory;
        // 一般情况下为 null
        this.predicate = predicate;
        // 一般情况下为 null
        this.projectFields = projectFields;
        // 一般情况下为 null
        this.limit = limit;
        // 一般情况下为 null
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // 获取 changelog 模型

        // 1 如果为批模式 changelog mode 为 insert-only
        if (!streaming) {
            // batch merge all, return insert only
            return ChangelogMode.insertOnly();
        }

        // 2 如果为流式 Append-Only 表 changelog mode 为 insert-only
        if (table instanceof AppendOnlyFileStoreTable) {
            return ChangelogMode.insertOnly();
        } else if (table instanceof ChangelogValueCountFileStoreTable) {
            return ChangelogMode.all();

            // 3 如果为流式 Primary-Key 表 changelog mode 根据情况而定
        } else if (table instanceof ChangelogWithKeyFileStoreTable) {
            // 3.1 获取表选项属性
            Options options = Options.fromMap(table.options());

            // 3.2 获取选项值中 key = log.scan.remove-normalize 默认 value = false
            if (options.get(LOG_SCAN_REMOVE_NORMALIZE)) {
                return ChangelogMode.all();
            }

            // 3.3 获取选项值中 key = changelog-producer 默认 value = none
            // 这种情况下就是定义了 Primary-Key Table 但是不是默认行为
            // 也即表选项属性中定义了 changelog-producer = INPUT/FULL_COMPACTION/LOOKUP 其中一种
            // 则返回 ChangelogMode.all()
            if (logStoreTableFactory == null
                    && options.get(CHANGELOG_PRODUCER) != ChangelogProducer.NONE) {
                return ChangelogMode.all();
            }

            // optimization: transaction consistency and all changelog mode avoid the generation of
            // normalized nodes. See FlinkTableSink.getChangelogMode validation.
            // 3.4 一般情况下 changelog-producer = none 则返回 ChangelogMode.upsert()
            return options.get(LOG_CONSISTENCY) == LogConsistency.TRANSACTIONAL
                    && options.get(LOG_CHANGELOG_MODE) == LogChangelogMode.ALL
                    ? ChangelogMode.all()
                    : ChangelogMode.upsert();
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Table subclass "
                            + table.getClass().getName()
                            + " for streaming mode.");
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // 这里核心获取读取数据 Source DataStream

        LogSourceProvider logSourceProvider = null;
        // 1 一般情况下 logStoreTableFactory 为 null
        if (logStoreTableFactory != null) {
            logSourceProvider =
                    logStoreTableFactory.createSourceProvider(context, scanContext, projectFields);
        }

        // 2 如果定义表的时候指定 watermark 则 watermarkStrategy 不为空 否则为 null
        WatermarkStrategy<RowData> watermarkStrategy = this.watermarkStrategy;
        // 3 获取表选项属性
        Options options = Options.fromMap(table.options());
        if (watermarkStrategy != null) {
            // 2.1 获取 key = scan.watermark.emit.strategy 默认 value = ON_EVENT
            // watermark 生成策略
            WatermarkEmitStrategy emitStrategy = options.get(SCAN_WATERMARK_EMIT_STRATEGY);
            if (emitStrategy == WatermarkEmitStrategy.ON_EVENT) {
                watermarkStrategy = new OnEventWatermarkStrategy(watermarkStrategy);
            }

            // 2.2 获取 key = scan.watermark.idle-timeout 默认 value = null
            // 如果一个并行度/分区超过了一定时间阈值没有接收到数据 则该并行度/分区被考虑为空闲
            Duration idleTimeout = options.get(SCAN_WATERMARK_IDLE_TIMEOUT);
            if (idleTimeout != null) {
                watermarkStrategy = watermarkStrategy.withIdleness(idleTimeout);
            }

            // 2.3 获取 key = scan.watermark.alignment.group 默认 value = null
            // Source 端并行度 watermark 对齐组
            String watermarkAlignGroup = options.get(SCAN_WATERMARK_ALIGNMENT_GROUP);
            if (watermarkAlignGroup != null) {
                try {
                    watermarkStrategy =
                            WatermarkAlignUtils.withWatermarkAlignment(
                                    watermarkStrategy,
                                    watermarkAlignGroup,
                                    options.get(SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT),
                                    options.get(SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL));
                } catch (NoSuchMethodError error) {
                    throw new RuntimeException(
                            "Flink 1.14 does not support watermark alignment, please check your Flink version.",
                            error);
                }
            }
        }

        // 4 构建 Flink Source
        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(tableIdentifier, table)
                        .withContinuousMode(streaming)
                        .withLogSourceProvider(logSourceProvider)
                        .withProjection(projectFields)
                        .withPredicate(predicate)
                        .withLimit(limit)
                        .withWatermarkStrategy(watermarkStrategy);

        // 5 创建 PaimonDataStreamScanProvider
        return new PaimonDataStreamScanProvider(
                !streaming,
                // 5.1 配置 StreamExecutionEnvironment
                env -> configureSource(sourceBuilder, env)
        );
    }

    private DataStream<RowData> configureSource(
            FlinkSourceBuilder sourceBuilder, StreamExecutionEnvironment env) {
        // 1 获取表属性
        Options options = Options.fromMap(this.table.options());
        // 2 获取 key = scan.parallelism 默认为空
        Integer parallelism = options.get(FlinkConnectorOptions.SCAN_PARALLELISM);
        List<Split> splits = null;
        // 3 获取 key = scan.infer-parallelism 默认为 false
        if (options.get(FlinkConnectorOptions.INFER_SCAN_PARALLELISM)) {
            // for streaming mode, set the default parallelism to the bucket number.
            // 4 对应流式模式 默认并行度为表的 bucket 数
            if (streaming) {
                parallelism = options.get(CoreOptions.BUCKET);
            } else {
                splits = table.newReadBuilder().withFilter(predicate).newScan().plan().splits();
                if (null != splits) {
                    parallelism = splits.size();
                }
                if (null != limit && limit > 0) {
                    int limitCount =
                            limit >= Integer.MAX_VALUE ? Integer.MAX_VALUE : limit.intValue();
                    parallelism = Math.min(parallelism, limitCount);
                }

                parallelism = null == parallelism ? 1 : Math.max(1, parallelism);
            }
        }

        // 如果定义 Source 的并行度呢？
        // 1 一般情况下 在没有任何配置项下 默认并行度等于 Flink 定义的并行度 比如在命令行指定、在配置文件指定
        // 2 流式模式下 如果 Paimon 表指定了 scan.infer-parallelism = true 则默认并行度等于表的 bucket 个数
        // 3 流式模式下 如果 Paimon 表指定了 scan.infer-parallelism = true 则进行数据切分操作

        // 5 构建 DataStream 里面封装了 SourceFunction
        return sourceBuilder.withParallelism(parallelism)
                .withSplits(splits)
                .withEnv(env)
                .build();
    }

    @Override
    public DynamicTableSource copy() {
        return new DataTableSource(
                tableIdentifier,
                table,
                streaming,
                context,
                logStoreTableFactory,
                predicate,
                projectFields,
                limit,
                watermarkStrategy);
    }

    @Override
    public String asSummaryString() {
        return "Paimon-DataSource";
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        if (limit != null) {
            throw new RuntimeException(
                    "Limit push down should not happen in Lookup source, but it is " + limit);
        }
        int[] projection =
                projectFields == null
                        ? IntStream.range(0, table.rowType().getFieldCount()).toArray()
                        : Projection.of(projectFields).toTopLevelIndexes();
        int[] joinKey = Projection.of(context.getKeys()).toTopLevelIndexes();
        return LookupRuntimeProviderFactory.create(
                new FileStoreLookupFunction(table, projection, joinKey, predicate));
    }
}
