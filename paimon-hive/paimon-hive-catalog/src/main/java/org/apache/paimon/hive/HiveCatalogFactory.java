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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.hive.conf.HiveConf;

import static org.apache.paimon.hive.HiveCatalog.createHiveConf;
import static org.apache.paimon.hive.HiveCatalogOptions.HADOOP_CONF_DIR;
import static org.apache.paimon.hive.HiveCatalogOptions.HIVE_CONF_DIR;
import static org.apache.paimon.hive.HiveCatalogOptions.IDENTIFIER;

/**
 * Factory to create {@link HiveCatalog}.
 */
public class HiveCatalogFactory implements CatalogFactory {

    private static final ConfigOption<String> METASTORE_CLIENT_CLASS =
            ConfigOptions.key("metastore.client.class")
                    .stringType()
                    .defaultValue("org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
                    .withDescription(
                            "Class name of Hive metastore client.\n"
                                    + "NOTE: This class must directly implements "
                                    + "org.apache.hadoop.hive.metastore.IMetaStoreClient.");

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog create(FileIO fileIO, Path warehouse, CatalogContext context) {
        // 1 获取 uri 属性值
        // 比如 'uri' = 'thrift://hadoop102:9083'
        String uri =
                Preconditions.checkNotNull(
                        context.options().get(CatalogOptions.URI),
                        CatalogOptions.URI.key()
                                + " must be set for paimon "
                                + IDENTIFIER
                                + " catalog");

        // 2 获取 hive-conf-dir 属性值
        String hiveConfDir = context.options().get(HIVE_CONF_DIR);
        // 3 获取 hadoop-conf-dir 属性值
        String hadoopConfDir = context.options().get(HADOOP_CONF_DIR);
        // 4 创建 HiveConf
        HiveConf hiveConf = createHiveConf(hiveConfDir, hadoopConfDir);

        // always using user-set parameters overwrite hive-site.xml parameters
        context.options().toMap().forEach(hiveConf::set);
        // 5 覆盖 HiveConf 的属性值
        // hive.metastore.uris hive.metastore.warehouse.dir
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse.toUri().toString());

        // 6 获取 Hive Metastore 客户端类 默认 HiveMetaStoreClient
        String clientClassName = context.options().get(METASTORE_CLIENT_CLASS);

        // 7 创建 HiveCatalog
        return new HiveCatalog(fileIO, hiveConf, clientClassName, context.options().toMap());
    }
}
