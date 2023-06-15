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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.InnerStreamTableScan;
import org.apache.paimon.table.source.InnerStreamTableScanImpl;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.InnerTableScanImpl;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.snapshot.SnapshotSplitReader;
import org.apache.paimon.table.source.snapshot.SnapshotSplitReaderImpl;
import org.apache.paimon.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.paimon.utils.SnapshotManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.paimon.CoreOptions.PATH;

/** Abstract {@link FileStoreTable}. */
public abstract class AbstractFileStoreTable implements FileStoreTable {

    private static final long serialVersionUID = 1L;

    protected final FileIO fileIO;
    protected final Path path;
    protected final TableSchema tableSchema;

    public AbstractFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this.fileIO = fileIO;
        this.path = path;
        if (!tableSchema.options().containsKey(PATH.key())) {
            // make sure table is always available
            Map<String, String> newOptions = new HashMap<>(tableSchema.options());
            newOptions.put(PATH.key(), path.toString());
            tableSchema = tableSchema.copy(newOptions);
        }
        this.tableSchema = tableSchema;
    }

    @VisibleForTesting
    public abstract FileStore<?> store();

    @Override
    public SnapshotSplitReader newSnapshotSplitReader() {
        return new SnapshotSplitReaderImpl(
                store().newScan(),
                tableSchema,
                coreOptions(),
                snapshotManager(),
                splitGenerator(),
                nonPartitionFilterConsumer());
    }

    @Override
    public InnerTableScan newScan() {
        return new InnerTableScanImpl(coreOptions(), newSnapshotSplitReader(), snapshotManager());
    }

    @Override
    public InnerStreamTableScan newStreamScan() {
        return new InnerStreamTableScanImpl(
                coreOptions(),
                newSnapshotSplitReader(),
                snapshotManager(),
                supportStreamingReadOverwrite());
    }

    public abstract SplitGenerator splitGenerator();

    protected abstract boolean supportStreamingReadOverwrite();

    public abstract BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer();

    protected abstract FileStoreTable copy(TableSchema newTableSchema);

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        Map<String, String> options = tableSchema.options();
        // check option is not immutable
        dynamicOptions.forEach(
                (k, v) -> {
                    if (!Objects.equals(v, options.get(k))) {
                        SchemaManager.checkAlterTableOption(k);
                    }
                });

        return internalCopyWithoutCheck(dynamicOptions);
    }

    @Override
    public FileStoreTable internalCopyWithoutCheck(Map<String, String> dynamicOptions) {
        Map<String, String> options = new HashMap<>(tableSchema.options());

        // merge non-null dynamic options into schema.options
        dynamicOptions.forEach(
                (k, v) -> {
                    if (v == null) {
                        options.remove(k);
                    } else {
                        options.put(k, v);
                    }
                });

        Options newOptions = Options.fromMap(options);

        // set path always
        newOptions.set(PATH, path.toString());

        // set dynamic options with default values
        CoreOptions.setDefaultValues(newOptions);

        // copy a new table schema to contain dynamic options
        TableSchema newTableSchema = tableSchema.copy(newOptions.toMap());

        // validate schema with new options
        SchemaValidation.validateTableSchema(newTableSchema);

        // see if merged options contain time travel option
        newTableSchema = tryTimeTravel(newOptions).orElse(newTableSchema);

        return copy(newTableSchema);
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        Map<String, String> options = tableSchema.options();
        SchemaManager schemaManager = new SchemaManager(fileIO(), location());
        Optional<TableSchema> optionalLatestSchema = schemaManager.latest();
        if (optionalLatestSchema.isPresent()) {
            TableSchema newTableSchema = optionalLatestSchema.get();
            newTableSchema = newTableSchema.copy(options);
            SchemaValidation.validateTableSchema(newTableSchema);
            return copy(newTableSchema);
        } else {
            return this;
        }
    }

    protected SchemaManager schemaManager() {
        return new SchemaManager(fileIO(), path);
    }

    @Override
    public CoreOptions coreOptions() {
        return store().options();
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    @Override
    public Path location() {
        return path;
    }

    @Override
    public TableSchema schema() {
        return tableSchema;
    }

    @Override
    public SnapshotManager snapshotManager() {
        return store().snapshotManager();
    }

    @Override
    public TableCommitImpl newCommit(String commitUser) {
        return new TableCommitImpl(
                store().newCommit(commitUser),
                coreOptions().writeOnly() ? null : store().newExpire(),
                coreOptions().writeOnly() ? null : store().newPartitionExpire(commitUser));
    }

    private Optional<TableSchema> tryTimeTravel(Options options) {
        CoreOptions coreOptions = new CoreOptions(options);
        Long snapshotId;

        switch (coreOptions.startupMode()) {
            case FROM_SNAPSHOT:
            case FROM_SNAPSHOT_FULL:
                snapshotId = coreOptions.scanSnapshotId();
                if (snapshotManager().snapshotExists(snapshotId)) {
                    long schemaId = snapshotManager().snapshot(snapshotId).schemaId();
                    return Optional.of(schemaManager().schema(schemaId).copy(options.toMap()));
                }
                return Optional.empty();
            case FROM_TIMESTAMP:
                Snapshot snapshot =
                        StaticFromTimestampStartingScanner.timeTravelToTimestamp(
                                snapshotManager(), coreOptions.scanTimestampMills());
                if (snapshot != null) {
                    long schemaId = snapshot.schemaId();
                    return Optional.of(schemaManager().schema(schemaId).copy(options.toMap()));
                }
                return Optional.empty();
            default:
                return Optional.empty();
        }
    }
}
