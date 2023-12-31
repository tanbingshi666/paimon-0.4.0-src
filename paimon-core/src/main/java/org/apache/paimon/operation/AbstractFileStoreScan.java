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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.BucketSelector;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Default implementation of {@link FileStoreScan}.
 */
public abstract class AbstractFileStoreScan implements FileStoreScan {

    private final FieldStatsArraySerializer partitionStatsConverter;
    private final RowDataToObjectArrayConverter partitionConverter;
    protected final RowType bucketKeyType;
    private final SnapshotManager snapshotManager;
    private final ManifestFile.Factory manifestFileFactory;
    private final ManifestList manifestList;
    private final int numOfBuckets;
    private final boolean checkNumOfBuckets;

    private final ConcurrentMap<Long, TableSchema> tableSchemas;
    private final SchemaManager schemaManager;

    private Predicate partitionFilter;
    private BucketSelector bucketSelector;

    private Long specifiedSnapshotId = null;
    private Integer specifiedBucket = null;
    private List<ManifestFileMeta> specifiedManifests = null;
    private ScanKind scanKind = ScanKind.ALL;
    private Filter<Integer> levelFilter = null;

    private ManifestCacheFilter manifestCacheFilter = null;

    public AbstractFileStoreScan(
            RowType partitionType,
            RowType bucketKeyType,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets) {
        this.partitionStatsConverter = new FieldStatsArraySerializer(partitionType);
        this.partitionConverter = new RowDataToObjectArrayConverter(partitionType);
        checkArgument(bucketKeyType.getFieldCount() > 0, "The bucket keys should not be empty.");
        this.bucketKeyType = bucketKeyType;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.manifestFileFactory = manifestFileFactory;
        this.manifestList = manifestListFactory.create();
        this.numOfBuckets = numOfBuckets;
        this.checkNumOfBuckets = checkNumOfBuckets;
        this.tableSchemas = new ConcurrentHashMap<>();
    }

    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    protected FileStoreScan withBucketKeyFilter(Predicate predicate) {
        this.bucketSelector = BucketSelector.create(predicate, bucketKeyType).orElse(null);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(List<BinaryRow> partitions) {
        PredicateBuilder builder = new PredicateBuilder(partitionConverter.rowType());
        Function<BinaryRow, Predicate> partitionToPredicate =
                p -> {
                    List<Predicate> fieldPredicates = new ArrayList<>();
                    Object[] partitionObjects = partitionConverter.convert(p);
                    for (int i = 0; i < partitionConverter.getArity(); i++) {
                        Object partition = partitionObjects[i];
                        fieldPredicates.add(builder.equal(i, partition));
                    }
                    return PredicateBuilder.and(fieldPredicates);
                };
        List<Predicate> predicates =
                partitions.stream()
                        .filter(p -> p.getFieldCount() > 0)
                        .map(partitionToPredicate)
                        .collect(Collectors.toList());
        if (predicates.isEmpty()) {
            return this;
        } else {
            return withPartitionFilter(PredicateBuilder.or(predicates));
        }
    }

    @Override
    public FileStoreScan withBucket(int bucket) {
        this.specifiedBucket = bucket;
        return this;
    }

    @Override
    public FileStoreScan withPartitionBucket(BinaryRow partition, int bucket) {
        if (manifestCacheFilter != null) {
            checkArgument(
                    manifestCacheFilter.test(partition, bucket),
                    String.format(
                            "This is a bug! The partition %s and bucket %s is filtered!",
                            partition, bucket));
        }
        withPartitionFilter(Collections.singletonList(partition));
        withBucket(bucket);
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(long snapshotId) {
        this.specifiedSnapshotId = snapshotId;
        if (specifiedManifests != null) {
            throw new IllegalStateException("Cannot set both snapshot id and manifests.");
        }
        return this;
    }

    @Override
    public FileStoreScan withManifestList(List<ManifestFileMeta> manifests) {
        this.specifiedManifests = manifests;
        if (specifiedSnapshotId != null) {
            throw new IllegalStateException("Cannot set both snapshot id and manifests.");
        }
        return this;
    }

    @Override
    public FileStoreScan withKind(ScanKind scanKind) {
        this.scanKind = scanKind;
        return this;
    }

    @Override
    public FileStoreScan withLevelFilter(Filter<Integer> levelFilter) {
        this.levelFilter = levelFilter;
        return this;
    }

    @Override
    public FileStoreScan withManifestCacheFilter(ManifestCacheFilter manifestFilter) {
        this.manifestCacheFilter = manifestFilter;
        return this;
    }

    @Override
    public Plan plan() {
        // 1 默认情况下 specifiedManifests = null
        List<ManifestFileMeta> manifests = specifiedManifests;
        // 2 默认情况下等于 snapshot 最大值
        Long snapshotId = specifiedSnapshotId;
        if (manifests == null) {
            if (snapshotId == null) {
                snapshotId = snapshotManager.latestSnapshotId();
            }
            if (snapshotId == null) {
                manifests = Collections.emptyList();
            } else {
                // 3 读取指定 snapshot 文件 JSON 内容
                // 格式为: {table_name_dir}/snapshot/snapshot-{snapshotId}
                Snapshot snapshot = snapshotManager.snapshot(snapshotId);
                // 4 根据 snapshot 内容读取 manifests 元数据内容
                manifests = readManifests(snapshot);
            }
        }

        final Long readSnapshot = snapshotId;
        final List<ManifestFileMeta> readManifests = manifests;

        // 一个 ManifestEntry 代表着一个 data-file 是 add 还是 delete
        List<ManifestEntry> entries;
        try {
            entries =
                    FileUtils.COMMON_IO_FORK_JOIN_POOL
                            .submit(
                                    () ->
                                            // 将读取到 snapshot 文件内容对应的 manifests list 进行处理
                                            // 一个 manifests list 对应多个 data-files
                                            readManifests
                                                    .parallelStream()
                                                    .filter(this::filterManifestFileMeta)
                                                    .flatMap(m -> readManifestFileMeta(m).stream())
                                                    .filter(this::filterByStats)
                                                    .collect(Collectors.toList()))
                            .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to read ManifestEntry list concurrently", e);
        }

        List<ManifestEntry> files = new ArrayList<>();
        for (ManifestEntry file : ManifestEntry.mergeEntries(entries)) {
            // 判断一个 data-file 的 bucket 数是否一致
            if (checkNumOfBuckets && file.totalBuckets() != numOfBuckets) {
                String partInfo =
                        partitionConverter.getArity() > 0
                                ? "partition "
                                + FileStorePathFactory.getPartitionComputer(
                                        partitionConverter.rowType(),
                                        FileStorePathFactory.PARTITION_DEFAULT_NAME
                                                .defaultValue())
                                .generatePartValues(file.partition())
                                : "table";
                throw new RuntimeException(
                        String.format(
                                "Try to write %s with a new bucket num %d, but the previous bucket num is %d. "
                                        + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
                                partInfo, numOfBuckets, file.totalBuckets()));
            }

            // bucket filter should not be applied along with partition filter
            // because the specifiedBucket is computed against the current numOfBuckets
            // however entry.bucket() was computed against the old numOfBuckets
            // and thus the filtered manifest entries might be empty
            // which renders the bucket check invalid
            //
            if (
                //  是否检测过滤 bucket
                    filterByBucket(file) &&
                            // 是否选择指定的 bucket
                            filterByBucketSelector(file) &&
                            // 是否过滤指定 level 级别的数据
                            filterByLevel(file)) {
                // 目标读取数据 data-file 的元数据信息
                files.add(file);
            }
        }

        return new Plan() {
            @Nullable
            @Override
            public Long snapshotId() {
                return readSnapshot;
            }

            @Override
            public List<ManifestEntry> files() {
                return files;
            }
        };
    }

    private List<ManifestFileMeta> readManifests(Snapshot snapshot) {
        switch (scanKind) {
            case ALL:
                // 默认情况下 scanKind = ALL
                // 读取 snapshot 文件内容的 baseManifestList、deltaManifestList 对应的 manifests 元数据
                return snapshot.dataManifests(manifestList);
            case DELTA:
                // 如果是 DELTA 则读取 Snapshot 的最新变化数据
                return snapshot.deltaManifests(manifestList);
            case CHANGELOG:
                if (snapshot.version() > Snapshot.TABLE_STORE_02_VERSION) {
                    return snapshot.changelogManifests(manifestList);
                }

                // compatible with Paimon 0.2, we'll read extraFiles in DataFileMeta
                // see comments on DataFileMeta#extraFiles
                if (snapshot.commitKind() == Snapshot.CommitKind.APPEND) {
                    return snapshot.deltaManifests(manifestList);
                }
                throw new IllegalStateException(
                        String.format(
                                "Incremental scan does not accept %s snapshot",
                                snapshot.commitKind()));
            default:
                throw new UnsupportedOperationException("Unknown scan kind " + scanKind.name());
        }
    }

    // ------------------------------------------------------------------------
    // Start Thread Safe Methods: The following methods need to be thread safe because they will be
    // called by multiple threads
    // ------------------------------------------------------------------------

    /**
     * Note: Keep this thread-safe.
     */
    protected TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(id, key -> schemaManager.schema(id));
    }

    /**
     * Note: Keep this thread-safe.
     */
    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        return partitionFilter == null
                || partitionFilter.test(
                manifest.numAddedFiles() + manifest.numDeletedFiles(),
                manifest.partitionStats().fields(partitionStatsConverter));
    }

    /**
     * Note: Keep this thread-safe.
     */
    private boolean filterByBucket(ManifestEntry entry) {
        return (specifiedBucket == null || entry.bucket() == specifiedBucket);
    }

    /**
     * Note: Keep this thread-safe.
     */
    private boolean filterByBucketSelector(ManifestEntry entry) {
        return (bucketSelector == null
                || bucketSelector.select(entry.bucket(), entry.totalBuckets()));
    }

    /**
     * Note: Keep this thread-safe.
     */
    private boolean filterByLevel(ManifestEntry entry) {
        return (levelFilter == null || levelFilter.test(entry.file().level()));
    }

    /**
     * Note: Keep this thread-safe.
     */
    protected abstract boolean filterByStats(ManifestEntry entry);

    /**
     * Note: Keep this thread-safe.
     */
    private List<ManifestEntry> readManifestFileMeta(ManifestFileMeta manifest) {
        return manifestFileFactory
                .create()
                .read(manifest.fileName(), manifestCacheRowFilter(), manifestEntryRowFilter());
    }

    /**
     * Note: Keep this thread-safe.
     */
    private Filter<InternalRow> manifestEntryRowFilter() {
        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter =
                ManifestEntrySerializer.totalBucketGetter();
        return row -> {
            if ((partitionFilter != null
                    && !partitionFilter.test(
                    partitionConverter.convert(partitionGetter.apply(row))))) {
                return false;
            }

            if (specifiedBucket != null && numOfBuckets == totalBucketGetter.apply(row)) {
                return specifiedBucket.intValue() == bucketGetter.apply(row);
            }

            return true;
        };
    }

    /**
     * Note: Keep this thread-safe.
     */
    private Filter<InternalRow> manifestCacheRowFilter() {
        if (manifestCacheFilter == null) {
            return Filter.alwaysTrue();
        }

        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter =
                ManifestEntrySerializer.totalBucketGetter();
        return row -> {
            if (numOfBuckets != totalBucketGetter.apply(row)) {
                return true;
            }

            return manifestCacheFilter.test(partitionGetter.apply(row), bucketGetter.apply(row));
        };
    }

    // ------------------------------------------------------------------------
    // End Thread Safe Methods
    // ------------------------------------------------------------------------
}
