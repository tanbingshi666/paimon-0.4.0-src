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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.NewFilesIncrement;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link RecordWriter} to write records and generate {@link CompactIncrement}.
 */
public class MergeTreeWriter implements RecordWriter<KeyValue>, MemoryOwner {

    private final boolean writeBufferSpillable;
    private final int sortMaxFan;
    private final IOManager ioManager;

    private final RowType keyType;
    private final RowType valueType;
    private final CompactManager compactManager;
    private final Comparator<InternalRow> keyComparator;
    private final MergeFunction<KeyValue> mergeFunction;
    private final KeyValueFileWriterFactory writerFactory;
    private final boolean commitForceCompact;
    private final ChangelogProducer changelogProducer;

    private final LinkedHashSet<DataFileMeta> newFiles;
    private final LinkedHashSet<DataFileMeta> newFilesChangelog;
    private final LinkedHashMap<String, DataFileMeta> compactBefore;
    private final LinkedHashSet<DataFileMeta> compactAfter;
    private final LinkedHashSet<DataFileMeta> compactChangelog;

    private long newSequenceNumber;
    private WriteBuffer writeBuffer;

    public MergeTreeWriter(
            boolean writeBufferSpillable,
            int sortMaxFan,
            IOManager ioManager,
            CompactManager compactManager,
            long maxSequenceNumber,
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            KeyValueFileWriterFactory writerFactory,
            boolean commitForceCompact,
            ChangelogProducer changelogProducer,
            @Nullable CommitIncrement increment) {
        this.writeBufferSpillable = writeBufferSpillable;
        this.sortMaxFan = sortMaxFan;
        this.ioManager = ioManager;
        this.keyType = writerFactory.keyType();
        this.valueType = writerFactory.valueType();
        this.compactManager = compactManager;
        this.newSequenceNumber = maxSequenceNumber + 1;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.writerFactory = writerFactory;
        this.commitForceCompact = commitForceCompact;
        this.changelogProducer = changelogProducer;

        this.newFiles = new LinkedHashSet<>();
        this.newFilesChangelog = new LinkedHashSet<>();
        this.compactBefore = new LinkedHashMap<>();
        this.compactAfter = new LinkedHashSet<>();
        this.compactChangelog = new LinkedHashSet<>();
        if (increment != null) {
            newFiles.addAll(increment.newFilesIncrement().newFiles());
            newFilesChangelog.addAll(increment.newFilesIncrement().changelogFiles());
            increment
                    .compactIncrement()
                    .compactBefore()
                    .forEach(f -> compactBefore.put(f.fileName(), f));
            compactAfter.addAll(increment.compactIncrement().compactAfter());
            compactChangelog.addAll(increment.compactIncrement().changelogFiles());
        }
    }

    private long newSequenceNumber() {
        return newSequenceNumber++;
    }

    @VisibleForTesting
    CompactManager compactManager() {
        return compactManager;
    }

    @Override
    public void setMemoryPool(MemorySegmentPool memoryPool) {
        this.writeBuffer =
                new SortBufferWriteBuffer(
                        keyType,
                        valueType,
                        memoryPool,
                        writeBufferSpillable,
                        sortMaxFan,
                        ioManager);
    }

    @Override
    public void write(KeyValue kv) throws Exception {
        long sequenceNumber =
                kv.sequenceNumber() == KeyValue.UNKNOWN_SEQUENCE
                        ? newSequenceNumber()
                        : kv.sequenceNumber();

        // 将记录写入内存
        boolean success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
        if (!success) {
            // 如果内存写满了 则刷写到磁盘 里面就涉及到是否检测合并
            flushWriteBuffer(false, false);
            // 在添加记录到内存
            success = writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
            if (!success) {
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    @Override
    public void compact(boolean fullCompaction) throws Exception {
        flushWriteBuffer(true, fullCompaction);
    }

    @Override
    public void addNewFiles(List<DataFileMeta> files) {
        files.forEach(compactManager::addNewFile);
    }

    @Override
    public Collection<DataFileMeta> dataFiles() {
        return compactManager.allFiles();
    }

    @Override
    public long memoryOccupancy() {
        return writeBuffer.memoryOccupancy();
    }

    @Override
    public void flushMemory() throws Exception {
        boolean success = writeBuffer.flushMemory();
        if (!success) {
            flushWriteBuffer(false, false);
        }
    }

    private void flushWriteBuffer(boolean waitForLatestCompaction, boolean forcedFullCompaction)
            throws Exception {
        // 1 来到这里 一般情况下 writeBuffer 有数据的
        if (writeBuffer.size() > 0) {
            // 2 判断是否合并 一般针对的 Primary-Key
            // 判断 level0 中 sorted-run 是否达到阈值
            // 默认 level0 中 sorted-run 个数大于 5(num-sorted-run.compaction-trigger) + 1
            if (compactManager.shouldWaitCompaction()) {
                waitForLatestCompaction = true;
            }

            // 3 如果 Primary-Key 表 changelog-producer = input 则创建 RollingFileWriter 用来写 input changelog
            final RollingFileWriter<KeyValue, DataFileMeta> changelogWriter =
                    changelogProducer == ChangelogProducer.INPUT
                            ? writerFactory.createRollingChangelogFileWriter(0)
                            : null;
            // 4 创建 RollingFileWriter 写正常数据
            final RollingFileWriter<KeyValue, DataFileMeta> dataWriter =
                    writerFactory.createRollingMergeTreeFileWriter(0);

            try {
                // 5 遍历 writeBuffer
                // 然后将对应的数据 changelog 数据、正常数据并分别写入到内存或者磁盘
                writeBuffer.forEach(
                        keyComparator,
                        mergeFunction,
                        // 5.1 写 changelog 数据 (一般先内存写 内存满了再往磁盘写)
                        changelogWriter == null ? null : changelogWriter::write,
                        // 5.2 写正常数据 (一般先内存写 内存满了再往磁盘写)
                        dataWriter::write);
            } finally {
                // 6 关闭写通道 也即将数据刷写到磁盘
                if (changelogWriter != null) {
                    changelogWriter.close();
                }
                dataWriter.close();
            }

            // 7 添加 changelog 的数据文件元数据 DataFileMeta
            if (changelogWriter != null) {
                newFilesChangelog.addAll(changelogWriter.result());
            }

            // 8 添加正常数据元数据 DataFileMeta
            for (DataFileMeta fileMeta : dataWriter.result()) {
                newFiles.add(fileMeta);
                // 往 Level0 添加sorted-run 的数据文件元数据
                compactManager.addNewFile(fileMeta);
            }

            // 9 write 缓存区清除
            writeBuffer.clear();
        }

        // 10 尝试同步执行合并 也即 11 触发合并 的时候添加合并线程
        // 如果合并线程没有结束 则这里就会阻塞等待 影响写入
        trySyncLatestCompaction(waitForLatestCompaction);

        // 11 触发合并 也即添加合并线程 MergeTreeCompactTask
        compactManager.triggerCompaction(forcedFullCompaction);
    }

    @Override
    public CommitIncrement prepareCommit(boolean waitCompaction) throws Exception {
        flushWriteBuffer(waitCompaction, false);
        trySyncLatestCompaction(waitCompaction || commitForceCompact);
        return drainIncrement();
    }

    @Override
    public void sync() throws Exception {
        trySyncLatestCompaction(true);
    }

    private CommitIncrement drainIncrement() {
        NewFilesIncrement newFilesIncrement =
                new NewFilesIncrement(
                        new ArrayList<>(newFiles), new ArrayList<>(newFilesChangelog));
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        new ArrayList<>(compactBefore.values()),
                        new ArrayList<>(compactAfter),
                        new ArrayList<>(compactChangelog));

        newFiles.clear();
        newFilesChangelog.clear();
        compactBefore.clear();
        compactAfter.clear();
        compactChangelog.clear();

        return new CommitIncrement(newFilesIncrement, compactIncrement);
    }

    private void updateCompactResult(CompactResult result) {
        Set<String> afterFiles =
                result.after().stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
        for (DataFileMeta file : result.before()) {
            if (compactAfter.remove(file)) {
                // This is an intermediate file (not a new data file), which is no longer needed
                // after compaction and can be deleted directly, but upgrade file is required by
                // previous snapshot and following snapshot, so we should ensure:
                // 1. This file is not the output of upgraded.
                // 2. This file is not the input of upgraded.
                if (!compactBefore.containsKey(file.fileName())
                        && !afterFiles.contains(file.fileName())) {
                    writerFactory.deleteFile(file.fileName());
                }
            } else {
                compactBefore.put(file.fileName(), file);
            }
        }
        compactAfter.addAll(result.after());
        compactChangelog.addAll(result.changelog());
    }

    private void trySyncLatestCompaction(boolean blocking) throws Exception {
        // 1 获取合并结果
        Optional<CompactResult> result = compactManager.getCompactionResult(blocking);
        result.ifPresent(this::updateCompactResult);
    }

    @Override
    public void close() throws Exception {
        // cancel compaction so that it does not block job cancelling
        compactManager.cancelCompaction();
        sync();
        compactManager.close();

        // delete temporary files
        List<DataFileMeta> delete = new ArrayList<>(newFiles);
        newFiles.clear();

        for (DataFileMeta file : newFilesChangelog) {
            writerFactory.deleteFile(file.fileName());
        }
        newFilesChangelog.clear();

        for (DataFileMeta file : compactAfter) {
            // upgrade file is required by previous snapshot, so we should ensure that this file is
            // not the output of upgraded.
            if (!compactBefore.containsKey(file.fileName())) {
                delete.add(file);
            }
        }

        compactAfter.clear();

        for (DataFileMeta file : compactChangelog) {
            writerFactory.deleteFile(file.fileName());
        }
        compactChangelog.clear();

        for (DataFileMeta file : delete) {
            writerFactory.deleteFile(file.fileName());
        }
    }
}
