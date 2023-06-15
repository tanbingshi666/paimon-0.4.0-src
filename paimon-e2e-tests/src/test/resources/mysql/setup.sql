-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- In production you would almost certainly limit the replication user must be on the follower (slave) machine,
-- to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
-- However, in this database we'll grant the test user 'paimonuser' all privileges:
--
GRANT ALL PRIVILEGES ON *.* TO 'paimonuser'@'%';

-- ################################################################################
--  MySqlCdcE2eTestBase#testSyncTable
-- ################################################################################

CREATE DATABASE paimon_sync_table;
USE paimon_sync_table;

CREATE TABLE schema_evolution_1 (
    pt INT,
    _id INT,
    v1 VARCHAR(10),
    PRIMARY KEY (_id)
);

CREATE TABLE schema_evolution_2 (
    pt INT,
    _id INT,
    v1 VARCHAR(10),
    PRIMARY KEY (_id)
);

-- ################################################################################
--  MySqlCdcE2eTestBase#testSyncDatabase
-- ################################################################################

CREATE DATABASE paimon_sync_database;
USE paimon_sync_database;

CREATE TABLE t1 (
    k INT,
    v INT,
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    PRIMARY KEY (k1, k2)
);

-- to make sure we use JDBC Driver correctly
CREATE DATABASE paimon_sync_database1;
USE paimon_sync_database1;

CREATE TABLE t1 (
    k INT,
    v INT,
    PRIMARY KEY (k)
);

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    PRIMARY KEY (k1, k2)
);

-- ################################################################################
--  MySqlIgnoreCaseE2EeTest#testSyncDatabase
-- ################################################################################

CREATE DATABASE paimon_ignore_CASE;
USE paimon_ignore_CASE;

CREATE TABLE T (
    k INT,
    UPPERCASE_V0 VARCHAR(20),
    PRIMARY KEY (k)
);

-- to make sure we use JDBC Driver correctly
CREATE DATABASE paimon_ignore_CASE1;
USE paimon_ignore_CASE1;

CREATE TABLE T (
    k INT,
    UPPERCASE_V0 VARCHAR(20),
    PRIMARY KEY (k)
);

-- ################################################################################
--  MySqlComputedColumnE2ETest#testSyncTable
-- ################################################################################

CREATE DATABASE test_computed_column;
USE test_computed_column;

CREATE TABLE T (
    pk INT,
    _datetime DATETIME,
    PRIMARY KEY (pk)
);
