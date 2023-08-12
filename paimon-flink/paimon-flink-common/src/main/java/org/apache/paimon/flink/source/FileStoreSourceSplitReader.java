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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * The {@link SplitReader} implementation for the file store source.
 */
public class FileStoreSourceSplitReader<T> implements SplitReader<T, FileStoreSourceSplit> {

    private final RecordsFunction<T> recordsFunction;

    private final TableRead tableRead;

    @Nullable
    private final RecordLimiter limiter;

    private final Queue<FileStoreSourceSplit> splits;

    private final Pool<FileStoreRecordIterator> pool;

    @Nullable
    private LazyRecordReader currentReader;
    @Nullable
    private String currentSplitId;
    private long currentNumRead;
    private RecordIterator<InternalRow> currentFirstBatch;

    public FileStoreSourceSplitReader(
            RecordsFunction<T> recordsFunction,
            TableRead tableRead,
            @Nullable RecordLimiter limiter) {
        this.recordsFunction = recordsFunction;
        // Primary-Key -> KeyValueTableRead
        this.tableRead = tableRead;
        this.limiter = limiter;
        this.splits = new LinkedList<>();
        // 读取数据存储到阻塞队列
        this.pool = new Pool<>(1);
        // 添加 FileStoreRecordIterator
        this.pool.add(new FileStoreRecordIterator());
    }

    @Override
    public RecordsWithSplitIds<T> fetch() throws IOException {
        // 1 检测读取切片数据
        checkSplitOrStartNext();

        // pool first, pool size is 1, the underlying implementation does not allow multiple batches
        // to be read at the same time
        // 2 从 pool 获取 FileStoreRecordIterator
        FileStoreRecordIterator iterator = pool();

        RecordIterator<InternalRow> nextBatch;
        if (currentFirstBatch != null) {
            nextBatch = currentFirstBatch;
            currentFirstBatch = null;
        } else {
            // 一般情况下 在流读情况下 是没有终点的 故执行 currentReader.recordReader().readBatch()
            nextBatch = reachLimit() ?
                    null :
                    // 执行 LazyRecordReader.recordReader()
                    // 基于 Append-Only 模型下 返回 RowDataFileRecordReader
                    // 基于 Primary-Key 模型下 返回 RowDataRecordReader
                    currentReader.recordReader()
                            // RowDataFileRecordReader.readBatch() 读取数据
                            // 默认读取一次读取 1024 条数
                            // 如果是 Primary-Key 最终调用 SortMergeReader 进行读取 data-file
                            .readBatch();
        }

        if (nextBatch == null) {
            pool.recycler().recycle(iterator);
            return finishSplit();
        }

        // 3 创建 FileRecords 并将读取的数据进行传递到下游进行计算
        // 读取完数据返回 等待下一次读取
        return recordsFunction.createRecords(currentSplitId, iterator.replace(nextBatch));
    }

    private boolean reachLimit() {
        return limiter != null && limiter.reachLimit();
    }

    private FileStoreRecordIterator pool() throws IOException {
        try {
            return this.pool.pollEntry();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted");
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FileStoreSourceSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        // 添加 SplitReader 需要读取哪些切片信息
        splits.addAll(splitsChange.splits());
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {
        if (currentReader != null) {
            if (currentReader.lazyRecordReader != null) {
                currentReader.lazyRecordReader.close();
            }
        }
    }

    private void checkSplitOrStartNext() throws IOException {
        if (currentReader != null) {
            return;
        }

        // 1 获取一个切片信息
        final FileStoreSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }

        // 2 设置当前读取切片数据的切片 ID
        currentSplitId = nextSplit.splitId();

        // 3 获取读取切片数据的读取器
        currentReader = new LazyRecordReader(nextSplit.split());

        // 4 表示需要等于多少条数据
        currentNumRead = nextSplit.recordsToSkip();
        if (limiter != null) {
            limiter.add(currentNumRead);
        }

        // 一般情况下 currentNumRead = 0
        if (currentNumRead > 0) {
            // 5 读取数据到指定位置
            seek(currentNumRead);
        }
    }

    private void seek(long toSkip) throws IOException {
        while (true) {

            // 1 根据切片信息封装读取数据器 RowDataFileRecordReader
            RecordIterator<InternalRow> nextBatch = currentReader.recordReader()
                    .readBatch();

            if (nextBatch == null) {
                throw new RuntimeException(
                        String.format(
                                "skip(%s) more than the number of remaining elements.", toSkip));
            }
            while (toSkip > 0 && nextBatch.next() != null) {
                toSkip--;
            }

            if (toSkip == 0) {
                // 记录当前读取切片数据的最终位置
                currentFirstBatch = nextBatch;
                return;
            }

            nextBatch.releaseBatch();
        }
    }

    private RecordsWithSplitIds<T> finishSplit() throws IOException {
        if (currentReader != null) {
            if (currentReader.lazyRecordReader != null) {
                currentReader.lazyRecordReader.close();
            }
            currentReader = null;
        }

        final RecordsWithSplitIds<T> finishRecords =
                recordsFunction.createRecordsWithFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    private class FileStoreRecordIterator implements BulkFormat.RecordIterator<RowData> {

        private RecordIterator<InternalRow> iterator;

        private final MutableRecordAndPosition<RowData> recordAndPosition =
                new MutableRecordAndPosition<>();

        public FileStoreRecordIterator replace(RecordIterator<InternalRow> iterator) {
            this.iterator = iterator;
            this.recordAndPosition.set(null, RecordAndPosition.NO_OFFSET, currentNumRead);
            return this;
        }

        @Nullable
        @Override
        public RecordAndPosition<RowData> next() {
            if (reachLimit()) {
                return null;
            }
            InternalRow row;
            try {
                row = iterator.next();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (row == null) {
                return null;
            }

            recordAndPosition.setNext(new FlinkRowData(row));
            currentNumRead++;
            if (limiter != null) {
                limiter.increment();
            }
            return recordAndPosition;
        }

        @Override
        public void releaseBatch() {
            this.iterator.releaseBatch();
            pool.recycler().recycle(this);
        }
    }

    /**
     * Lazy to create {@link RecordReader} to improve performance for limit.
     */
    private class LazyRecordReader {

        private final Split split;

        private RecordReader<InternalRow> lazyRecordReader;

        private LazyRecordReader(Split split) {
            this.split = split;
        }

        public RecordReader<InternalRow> recordReader() throws IOException {
            if (lazyRecordReader == null) {
                // 根据切片信息封装读取数据器 tableRead 等于如下
                // Append-Only -> RowDataFileRecordReader
                // Primary-Key -> KeyValueTableRead
                lazyRecordReader = tableRead.createReader(split);
            }
            return lazyRecordReader;
        }
    }
}
