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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.operation.AbstractFileStoreScan;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Filter for manifest cache, this is used in {@link AbstractFileStoreScan} for improving cache
 * utilization. NOTE: Please use this interface with caution and make sure that only filtered data
 * is required, otherwise it will cause correctness issues.
 */
@ThreadSafe
@FunctionalInterface
public interface ManifestCacheFilter {

    boolean test(BinaryRow partition, int bucket);
}
