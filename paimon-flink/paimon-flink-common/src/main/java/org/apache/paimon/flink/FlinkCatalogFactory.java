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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

import java.util.Collections;
import java.util.Set;

/**
 * Factory for {@link FlinkCatalog}.
 * Paimon 管理 Catalog 有两种方式：
 * 1 文件系统方式： FileSystemCatalog
 * 2 HMS 方式： HiveCatalog
 * <p>
 * Paimon 中 FlinkCatalog 封装了 Catalog 的两种具体实现并且提供了
 * FlinkTableFactory 类 其提供 DynamicTableSourceFactory、DynamicTableSinkFactory
 */
public class FlinkCatalogFactory implements org.apache.flink.table.factories.CatalogFactory {

    public static final String IDENTIFIER = "paimon";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key("default-database")
                    .stringType()
                    .defaultValue(Catalog.DEFAULT_DATABASE);

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public FlinkCatalog createCatalog(Context context) {
        // 一般情况下 创建 paimon catalog 都会执行到这里 其中 context = DefaultCatalogContext
        // 创建 catalog
        return createCatalog(
                // 获取 catalog 名字
                context.getName(),
                // 创建 CatalogContext
                CatalogContext.create(
                        // CREATE CATALOG 属性 (除了 type = paimon)
                        Options.fromMap(context.getOptions()),
                        new FlinkFileIOLoader()),
                // 类加载器
                context.getClassLoader());
    }

    public static FlinkCatalog createCatalog(
            String catalogName, CatalogContext context, ClassLoader classLoader) {
        // 创建 FlinkCatalog
        return new FlinkCatalog(
                // 创建 CatalogFactory
                // Paimon 提供了两种方式管理 catalog
                // 第一种方式基于文件系统：FileSystemCatalogFactory
                // 第二种方式基于 hive metastore：HiveCatalogFactory
                // 我们公司基于 hive metastore 故创建 HiveCatalog
                CatalogFactory.createCatalog(context, classLoader),
                catalogName,
                context.options().get(DEFAULT_DATABASE));
    }

    public static FlinkCatalog createCatalog(String catalogName, Catalog catalog) {
        return new FlinkCatalog(catalog, catalogName, Catalog.DEFAULT_DATABASE);
    }

    public static Catalog createPaimonCatalog(Options catalogOptions) {
        return CatalogFactory.createCatalog(
                CatalogContext.create(catalogOptions, new FlinkFileIOLoader()));
    }
}
