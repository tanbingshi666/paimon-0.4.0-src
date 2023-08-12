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

package org.apache.paimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.KeyValueFileStoreRead;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.KeyComparatorSupplier;

import java.util.Comparator;
import java.util.function.Supplier;

/**
 * {@link FileStore} for querying and updating {@link KeyValue}s.
 */
public class KeyValueFileStore extends AbstractFileStore<KeyValue> {

    private static final long serialVersionUID = 1L;

    private final RowType bucketKeyType;
    private final RowType keyType;
    private final RowType valueType;
    private final KeyValueFieldsExtractor keyValueFieldsExtractor;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;

    public KeyValueFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            CoreOptions options,
            RowType partitionType,
            RowType bucketKeyType,
            RowType keyType,
            RowType valueType,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            MergeFunctionFactory<KeyValue> mfFactory) {
        super(fileIO, schemaManager, schemaId, options, partitionType);
        this.bucketKeyType = bucketKeyType;
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyValueFieldsExtractor = keyValueFieldsExtractor;
        // Primary-Key 合并引擎类型
        this.mfFactory = mfFactory;
        this.keyComparatorSupplier = new KeyComparatorSupplier(keyType);
    }

    @Override
    public KeyValueFileStoreScan newScan() {
        return newScan(false);
    }

    @Override
    public KeyValueFileStoreRead newRead() {
        // 创建 KeyValueFileStoreRead
        return new KeyValueFileStoreRead(
                fileIO,
                schemaManager,
                schemaId,
                keyType,
                valueType,
                newKeyComparator(),
                mfFactory,
                FileFormatDiscover.of(options),
                pathFactory(),
                keyValueFieldsExtractor);
    }

    @Override
    public KeyValueFileStoreWrite newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    @Override
    public KeyValueFileStoreWrite newWrite(String commitUser, ManifestCacheFilter manifestFilter) {
        return new KeyValueFileStoreWrite(
                fileIO,
                schemaManager,
                schemaId,
                commitUser,
                keyType,
                valueType,
                keyComparatorSupplier,
                mfFactory,
                pathFactory(),
                snapshotManager(),
                newScan(true).withManifestCacheFilter(manifestFilter),
                options,
                keyValueFieldsExtractor);
    }

    private KeyValueFileStoreScan newScan(boolean forWrite) {
        return new KeyValueFileStoreScan(
                partitionType,
                bucketKeyType,
                keyType,
                snapshotManager(),
                schemaManager,
                schemaId,
                keyValueFieldsExtractor,
                manifestFileFactory(forWrite),
                manifestListFactory(forWrite),
                options.bucket(),
                forWrite);
    }

    @Override
    public Comparator<InternalRow> newKeyComparator() {
        return keyComparatorSupplier.get();
    }
}
