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

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFilePathFactory.CHANGELOG_FILE_PREFIX;
import static org.apache.paimon.predicate.PredicateBuilder.containsFields;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/**
 * {@link FileStoreRead} implementation for {@link KeyValueFileStore}.
 */
public class KeyValueFileStoreRead implements FileStoreRead<KeyValue> {

    private final TableSchema tableSchema;
    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final boolean valueCountMode;

    @Nullable
    private int[][] keyProjectedFields;

    @Nullable
    private List<Predicate> filtersForOverlappedSection;

    @Nullable
    private List<Predicate> filtersForNonOverlappedSection;

    @Nullable
    private int[][] valueProjection;

    public KeyValueFileStoreRead(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            KeyValueFieldsExtractor extractor) {
        this.tableSchema = schemaManager.schema(schemaId);
        this.readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schemaId,
                        keyType,
                        valueType,
                        formatDiscover,
                        pathFactory,
                        extractor);
        this.keyComparator = keyComparator;
        this.mfFactory = mfFactory;
        this.valueCountMode = tableSchema.trimmedPrimaryKeys().isEmpty();
    }

    public KeyValueFileStoreRead withKeyProjection(int[][] projectedFields) {
        readerFactoryBuilder.withKeyProjection(projectedFields);
        this.keyProjectedFields = projectedFields;
        return this;
    }

    public KeyValueFileStoreRead withValueProjection(int[][] projectedFields) {
        this.valueProjection = projectedFields;
        readerFactoryBuilder.withValueProjection(projectedFields);
        return this;
    }

    @Override
    public FileStoreRead<KeyValue> withFilter(Predicate predicate) {
        List<Predicate> allFilters = new ArrayList<>();
        List<Predicate> pkFilters = null;
        List<String> primaryKeys = tableSchema.trimmedPrimaryKeys();
        Set<String> nonPrimaryKeys =
                tableSchema.fieldNames().stream()
                        .filter(name -> !primaryKeys.contains(name))
                        .collect(Collectors.toSet());
        for (Predicate sub : splitAnd(predicate)) {
            allFilters.add(sub);
            if (!containsFields(sub, nonPrimaryKeys)) {
                if (pkFilters == null) {
                    pkFilters = new ArrayList<>();
                }
                // TODO Actually, the index is wrong, but it is OK.
                //  The orc filter just use name instead of index.
                pkFilters.add(sub);
            }
        }
        // Consider this case:
        // Denote (seqNumber, key, value) as a record. We have two overlapping runs in a section:
        //   * First run: (1, k1, 100), (2, k2, 200)
        //   * Second run: (3, k1, 10), (4, k2, 20)
        // If we push down filter "value >= 100" for this section, only the first run will be read,
        // and the second run is lost. This will produce incorrect results.
        //
        // So for sections with overlapping runs, we only push down key filters.
        // For sections with only one run, as each key only appears once, it is OK to push down
        // value filters.
        filtersForNonOverlappedSection = allFilters;
        filtersForOverlappedSection = valueCountMode ? allFilters : pkFilters;
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(DataSplit split) throws IOException {
        // 1 判断切片是否为增量
        if (split.isIncremental()) {
            KeyValueFileReaderFactory readerFactory =
                    readerFactoryBuilder.build(
                            split.partition(), split.bucket(), true, filtersForOverlappedSection);
            // Return the raw file contents without merging
            List<ConcatRecordReader.ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
            for (DataFileMeta file : split.files()) {
                suppliers.add(
                        () -> {
                            // We need to check extraFiles to be compatible with Paimon 0.2.
                            // See comments on DataFileMeta#extraFiles.
                            String fileName = changelogFile(file).orElse(file.fileName());
                            return readerFactory.createRecordReader(
                                    file.schemaId(), fileName, file.level());
                        });
            }
            RecordReader<KeyValue> concatRecordReader = ConcatRecordReader.create(suppliers);
            return split.reverseRowKind()
                    ? new ReverseReader(concatRecordReader)
                    : concatRecordReader;
        } else {
            // Sections are read by SortMergeReader, which sorts and merges records by keys.
            // So we cannot project keys or else the sorting will be incorrect.
            // 2 正常情况下 读取 Primary-Key 需要排序、合并
            // 2.1 构建 KeyValueFileReaderFactory 处理哪些 sorted-run 对应的 data-files 有重复 key的文件
            KeyValueFileReaderFactory overlappedSectionFactory =
                    readerFactoryBuilder.build(
                            split.partition(), split.bucket(), false, filtersForOverlappedSection);
            // 2.2 构建 KeyValueFileReaderFactory 处理哪些 sorted-run 对应的 data-files 没有重复 key 的文件
            KeyValueFileReaderFactory nonOverlappedSectionFactory =
                    readerFactoryBuilder.build(
                            split.partition(),
                            split.bucket(),
                            false,
                            filtersForNonOverlappedSection);

            List<ConcatRecordReader.ReaderSupplier<KeyValue>> sectionReaders = new ArrayList<>();

            // 2.3 根据 Primary-Key 的合并引擎创建对应的  MergeFunction
            MergeFunctionWrapper<KeyValue> mergeFuncWrapper =
                    new ReducerMergeFunctionWrapper(mfFactory.create(valueProjection));

            // 2.4 根据一个切片信息 一个切片信息默认对应多个 data-files 一个切片对应的数据量大于等于 128MB
            // 将一个切片信息对应的 data-files 划分为多个 sorted-run (也即将重叠的 key 划分为一个 sorted-run)
            // 也即 sectionReaders 有多少个 sorted-run 就有多少个 SortMergeReader
            for (List<SortedRun> section :
                    new IntervalPartition(split.files(), keyComparator).partition()) {
                // 添加 SortMergeReader
                sectionReaders.add(
                        () ->
                                // 创建 SortMergeReader
                                MergeTreeReaders.readerForSection(
                                        section,
                                        section.size() > 1
                                                ? overlappedSectionFactory
                                                : nonOverlappedSectionFactory,
                                        keyComparator,
                                        mergeFuncWrapper));
            }
            // 创建 DropDeleteReader
            DropDeleteReader reader =
                    new DropDeleteReader(ConcatRecordReader.create(sectionReaders));

            // Project results from SortMergeReader using ProjectKeyRecordReader.
            return keyProjectedFields == null ? reader : projectKey(reader, keyProjectedFields);
        }
    }

    private Optional<String> changelogFile(DataFileMeta fileMeta) {
        for (String file : fileMeta.extraFiles()) {
            if (file.startsWith(CHANGELOG_FILE_PREFIX)) {
                return Optional.of(file);
            }
        }
        return Optional.empty();
    }

    private RecordReader<KeyValue> projectKey(
            RecordReader<KeyValue> reader, int[][] keyProjectedFields) {
        ProjectedRow projectedRow = ProjectedRow.from(keyProjectedFields);
        return reader.transform(kv -> kv.replaceKey(projectedRow.replaceRow(kv.key())));
    }
}
