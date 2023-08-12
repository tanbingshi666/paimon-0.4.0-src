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

package org.apache.paimon.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Ordered packing for input items.
 */
public class OrderedPacking {
    private OrderedPacking() {
    }

    public static <T> List<List<T>> pack(
            Iterable<T> items, Function<T, Long> weightFunc, long targetWeight) {
        List<List<T>> packed = new ArrayList<>();

        List<T> binItems = new ArrayList<>();
        long binWeight = 0L;

        // 遍历每个 data-file 元数据
        for (T item : items) {
            // 判断 data-file 的文件大小与 openFileCost(默认4M) 取最大值
            long weight = weightFunc.apply(item);
            // targetWeight 默认等于 128MB
            // 累加计算：如果累加文件大小大于 128MB 则之前的文件作为一个切片
            if (binWeight + weight > targetWeight) {
                packed.add(binItems);
                binItems = new ArrayList<>();
                binWeight = 0;
            }

            binWeight += weight;
            binItems.add(item);
        }

        if (binItems.size() > 0) {
            packed.add(binItems);
        }
        return packed;
    }
}
