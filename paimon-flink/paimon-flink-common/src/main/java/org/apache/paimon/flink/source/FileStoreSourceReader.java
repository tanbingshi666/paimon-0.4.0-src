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

import org.apache.paimon.table.source.TableRead;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * A {@link SourceReader} that read records from {@link FileStoreSourceSplit}.
 */
public final class FileStoreSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
        T, RowData, FileStoreSourceSplit, FileStoreSourceSplitState> {

    public FileStoreSourceReader(
            RecordsFunction<T> recordsFunction,
            SourceReaderContext readerContext,
            TableRead tableRead,
            @Nullable Long limit) {
        // 往下追
        this(
                recordsFunction,
                readerContext,
                tableRead,
                limit == null ? null : new RecordLimiter(limit));
    }

    private FileStoreSourceReader(
            RecordsFunction<T> recordsFunction,
            SourceReaderContext readerContext,
            TableRead tableRead,
            @Nullable RecordLimiter limiter) {
        // limiter is created in SourceReader, it can be shared in all split readers
        // 往下追
        super(
                // 创建 FileStoreSourceSplitReader 也即根据切片信息读取数据
                () -> new FileStoreSourceSplitReader<>(recordsFunction, tableRead, limiter),
                recordsFunction,
                readerContext.getConfiguration(),
                readerContext);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        // 如果当前 SourceReader Task 没有切片可读取 则项 SlitEnumerator 发送请求 Split
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, FileStoreSourceSplitState> finishedSplitIds) {
        // this method is called each time when we consume one split
        // it is possible that one response from the coordinator contains multiple splits
        // we should only require for more splits after we've consumed all given splits
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected FileStoreSourceSplitState initializedState(FileStoreSourceSplit split) {
        return new FileStoreSourceSplitState(split);
    }

    @Override
    protected FileStoreSourceSplit toSplitType(
            String splitId, FileStoreSourceSplitState splitState) {
        return splitState.toSourceSplit();
    }
}
